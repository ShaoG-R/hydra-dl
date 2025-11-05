//! 任务分配器模块
//! 
//! 负责管理任务分配、空闲 worker 队列和失败任务重试

use crate::{Result, DownloadError};
use crate::utils::range_writer::{AllocatedRange, RangeAllocator};
use crate::task::WorkerTask;
use kestrel_timer::{TaskId, TimerService, TimerTask};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use log::{debug, error};

/// 失败的 Range 信息
pub(super) type FailedRange = (AllocatedRange, String);

/// 已分配的任务（封装任务和取消通道）
pub(super) struct AllocatedTask {
    /// 任务本身
    task: WorkerTask,
    /// 分配到的 worker ID
    worker_id: usize,
    /// 取消信号发送器
    cancel_tx: tokio::sync::oneshot::Sender<()>,
}

impl AllocatedTask {
    /// 解构为独立部分
    pub(super) fn into_parts(self) -> (WorkerTask, usize, tokio::sync::oneshot::Sender<()>) {
        (self.task, self.worker_id, self.cancel_tx)
    }
}

/// 失败任务信息
/// 
/// 用于跟踪待重试的失败任务
#[derive(Debug)]
pub(super) struct FailedTaskInfo {
    /// 失败的 range
    pub(super) range: AllocatedRange,
    /// 当前重试次数
    pub(super) retry_count: usize,
}

/// 任务分配器
/// 
/// 负责管理任务分配、空闲 worker 队列和失败任务重试
pub(super) struct TaskAllocator {
    /// Range 分配器
    allocator: RangeAllocator,
    /// 下载 URL
    url: String,
    /// 空闲 worker ID 队列
    pub(super) idle_workers: VecDeque<usize>,
    /// 待重试的失败任务映射（定时器 TaskId -> 失败任务信息）
    failed_tasks: FxHashMap<TaskId, FailedTaskInfo>,
    /// 已就绪的重试任务队列（定时器已触发但尚未分配）
    ready_retry_queue: VecDeque<FailedTaskInfo>,
    /// 永久失败的任务（达到最大重试次数）
    permanently_failed: Vec<(AllocatedRange, String)>,
}

impl TaskAllocator {
    /// 创建新的任务分配器
    pub(super) fn new(
        allocator: RangeAllocator,
        url: String,
    ) -> Self {
        Self {
            allocator,
            url,
            idle_workers: VecDeque::new(),
            failed_tasks: FxHashMap::default(),
            ready_retry_queue: VecDeque::new(),
            permanently_failed: Vec::new(),
        }
    }
    
    /// 尝试为空闲 worker 分配任务
    /// 
    /// 优先从重试队列中取任务，如果队列为空则分配新任务
    /// 
    /// # Arguments
    /// 
    /// * `chunk_size` - 要分配的分块大小（仅用于新任务）
    /// 
    /// # Returns
    /// 
    /// 返回 AllocatedTask（封装了任务、worker_id 和取消发送器），如果没有空闲 worker 或没有剩余空间则返回 None
    pub(super) fn try_allocate_task_to_idle_worker(&mut self, chunk_size: u64) -> Option<AllocatedTask> {
        // 从队列中获取空闲 worker
        let worker_id = self.idle_workers.pop_front()?;
        
        // 优先从重试队列中取任务
        if let Some(retry_info) = self.ready_retry_queue.pop_front() {
            let (start, end) = retry_info.range.as_file_range();
            debug!(
                "从重试队列分配任务 range {}..{}, 重试次数 {}",
                start,
                end,
                retry_info.retry_count
            );
            
            // 创建取消通道
            let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
            
            let task = WorkerTask::Range {
                url: self.url.clone(),
                range: retry_info.range,
                retry_count: retry_info.retry_count,
                cancel_rx,
            };
            
            return Some(AllocatedTask {
                task,
                worker_id,
                cancel_tx,
            });
        }
        
        // 重试队列为空，分配新任务
        let remaining = self.allocator.remaining();
        if remaining == 0 {
            // 没有剩余任务，将 worker 放回队列
            self.idle_workers.push_back(worker_id);
            return None;
        }
        
        // 计算实际分配大小（不超过剩余空间）
        let alloc_size = chunk_size.min(remaining);
        
        // 分配 range
        let range = self.allocator.allocate(alloc_size)?;
        
        // 创建取消通道
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        
        // 创建任务（首次分配，重试次数为 0）
        let task = WorkerTask::Range {
            url: self.url.clone(),
            range,
            retry_count: 0,
            cancel_rx,
        };
        
        Some(AllocatedTask {
            task,
            worker_id,
            cancel_tx,
        })
    }
    
    /// 获取剩余待分配的字节数
    pub(super) fn remaining(&self) -> u64 {
        self.allocator.remaining()
    }
    
    /// 标记 worker 为空闲状态
    /// 
    /// # Arguments
    /// 
    /// * `worker_id` - 要标记为空闲的 worker ID
    pub(super) fn mark_worker_idle(&mut self, worker_id: usize) {
        self.idle_workers.push_back(worker_id);
    }
    
    /// 根据定时器 TaskId 取出失败任务信息
    /// 
    /// # Arguments
    /// 
    /// * `timer_id` - 定时器任务 ID
    /// 
    /// # Returns
    /// 
    /// 返回对应的失败任务信息，如果不存在则返回 None
    pub(super) fn pop_failed_task(&mut self, timer_id: TaskId) -> Option<FailedTaskInfo> {
        self.failed_tasks.remove(&timer_id)
    }
    
    /// 将失败任务推入就绪重试队列
    /// 
    /// 当定时器触发但没有空闲 worker 时，将任务推入此队列
    /// 任务会在下次有 worker 空闲时优先分配
    /// 
    /// # Arguments
    /// 
    /// * `task_info` - 失败任务信息
    pub(super) fn push_ready_retry_task(&mut self, task_info: FailedTaskInfo) {
        let (start, end) = task_info.range.as_file_range();
        debug!(
            "推入重试任务到就绪队列 range {}..{}, 重试次数 {}",
            start,
            end,
            task_info.retry_count
        );
        self.ready_retry_queue.push_back(task_info);
    }
    
    /// 记录失败任务
    /// 
    /// # Arguments
    /// 
    /// * `range` - 失败的 range
    /// * `retry_count` - 当前重试次数
    /// * `delay` - 延迟重试的时间
    /// * `timer_service` - 定时器服务
    /// 
    /// # Returns
    /// 
    /// 成功返回 Ok(())，失败返回错误
    pub(super) fn record_failed_task(
        &mut self,
        range: AllocatedRange,
        retry_count: usize,
        delay: std::time::Duration,
        timer_service: &TimerService,
    ) -> Result<()> {
        let (start, end) = range.as_file_range();
        debug!(
            "记录失败任务 range {}..{}, 重试次数 {}, 将在 {:.1}s 后重试",
            start,
            end,
            retry_count,
            delay.as_secs_f64()
        );
        
        // 创建定时器任务（无回调，仅通知）
        let timer_task = TimerTask::new_oneshot(delay, None);
        let timer_id = timer_task.get_id();
        
        // 注册到 TimerService
        timer_service.register(timer_task)
            .map_err(|e| DownloadError::Other(format!("注册定时器失败: {:?}", e)))?;
        
        // 存储映射关系
        self.failed_tasks.insert(timer_id, FailedTaskInfo {
            range,
            retry_count,
        });
        
        Ok(())
    }
    
    /// 记录永久失败的任务
    /// 
    /// # Arguments
    /// 
    /// * `range` - 失败的 range
    /// * `error` - 错误信息
    pub(super) fn record_permanent_failure(&mut self, range: AllocatedRange, error: String) {
        let (start, end) = range.as_file_range();
        error!(
            "任务永久失败 range {}..{}: {}",
            start,
            end,
            error
        );
        self.permanently_failed.push((range, error));
    }
    
    /// 检查是否有永久失败的任务
    pub(super) fn has_permanent_failures(&self) -> bool {
        !self.permanently_failed.is_empty()
    }
    
    /// 获取永久失败任务的详细信息
    pub(super) fn get_permanent_failures(&self) -> &[(AllocatedRange, String)] {
        &self.permanently_failed
    }
    
    /// 获取待重试的任务数量
    /// 
    /// 包括定时器等待中的任务和已就绪等待分配的任务
    pub(super) fn pending_retry_count(&self) -> usize {
        self.failed_tasks.len() + self.ready_retry_queue.len()
    }
    
    /// 获取下载 URL
    pub(super) fn url(&self) -> &str {
        &self.url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::range_writer::RangeAllocator;
    use kestrel_timer::{TimerWheel, config::ServiceConfig};

    #[test]
    fn test_task_allocator_basic() {
        let allocator = RangeAllocator::new(1000);
        let task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        // 初始状态
        assert_eq!(task_allocator.remaining(), 1000);
        assert_eq!(task_allocator.pending_retry_count(), 0);
        assert!(!task_allocator.has_permanent_failures());
    }

    #[test]
    fn test_mark_worker_idle_and_allocate() {
        let allocator = RangeAllocator::new(1000);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        // 标记 worker 为空闲
        task_allocator.mark_worker_idle(0);
        task_allocator.mark_worker_idle(1);
        
        assert_eq!(task_allocator.idle_workers.len(), 2);
        
        // 分配任务给第一个空闲 worker
        let result = task_allocator.try_allocate_task_to_idle_worker(100);
        assert!(result.is_some());
        
        let allocated = result.unwrap();
        let (task, worker_id, _cancel_tx) = allocated.into_parts();
        assert_eq!(worker_id, 0); // 应该分配给第一个空闲的 worker
        
        // 验证任务
        match task {
            WorkerTask::Range { url, range, retry_count, .. } => {
                assert_eq!(url, "http://example.com/file");
                let (start, end) = range.as_file_range();
                assert_eq!(start, 0);
                assert_eq!(end, 100);
                assert_eq!(retry_count, 0);
            }
        }
        
        // 剩余空间应该减少
        assert_eq!(task_allocator.remaining(), 900);
        
        // 空闲队列应该少了一个
        assert_eq!(task_allocator.idle_workers.len(), 1);
    }

    #[test]
    fn test_allocate_with_no_idle_workers() {
        let allocator = RangeAllocator::new(1000);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        // 没有空闲 worker
        let result = task_allocator.try_allocate_task_to_idle_worker(100);
        assert!(result.is_none());
    }

    #[test]
    fn test_allocate_with_no_remaining_space() {
        let allocator = RangeAllocator::new(0); // 没有剩余空间
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        task_allocator.mark_worker_idle(0);
        
        // 尝试分配但没有剩余空间
        let result = task_allocator.try_allocate_task_to_idle_worker(100);
        assert!(result.is_none());
        
        // Worker 应该被放回队列
        assert_eq!(task_allocator.idle_workers.len(), 1);
    }

    #[test]
    fn test_allocate_respects_chunk_size() {
        let allocator = RangeAllocator::new(1000);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        task_allocator.mark_worker_idle(0);
        
        // 分配 250 字节
        let result = task_allocator.try_allocate_task_to_idle_worker(250);
        assert!(result.is_some());
        
        let allocated = result.unwrap();
        let (task, _, _cancel_tx) = allocated.into_parts();
        match task {
            WorkerTask::Range { range, .. } => {
                assert_eq!(range.len(), 250);
            }
        }
        
        assert_eq!(task_allocator.remaining(), 750);
    }

    #[test]
    fn test_allocate_caps_at_remaining() {
        let allocator = RangeAllocator::new(100);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        task_allocator.mark_worker_idle(0);
        
        // 请求 500 字节但只剩 100 字节
        let result = task_allocator.try_allocate_task_to_idle_worker(500);
        assert!(result.is_some());
        
        let allocated = result.unwrap();
        let (task, _, _cancel_tx) = allocated.into_parts();
        match task {
            WorkerTask::Range { range, .. } => {
                assert_eq!(range.len(), 100); // 应该被限制在剩余空间
            }
        }
        
        assert_eq!(task_allocator.remaining(), 0);
    }

    #[tokio::test]
    async fn test_record_and_pop_failed_task() {
        let allocator = RangeAllocator::new(1000);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        // 创建定时器服务
        let timer = TimerWheel::with_defaults();
        let mut timer_service = timer.create_service(ServiceConfig::default());
        
        // 分配一个 range 来模拟失败
        task_allocator.mark_worker_idle(0);
        let allocated = task_allocator.try_allocate_task_to_idle_worker(100).unwrap();
        let (task, _, _cancel_tx) = allocated.into_parts();
        let range = match task {
            WorkerTask::Range { range, .. } => range,
        };
        
        // 记录失败任务
        let result = task_allocator.record_failed_task(
            range.clone(),
            1,
            std::time::Duration::from_millis(100),
            &timer_service,
        );
        assert!(result.is_ok());
        
        // 应该有一个待重试的任务
        assert_eq!(task_allocator.pending_retry_count(), 1);
        
        // 获取定时器 ID（从 timer_service 获取下一个超时）
        let timeout_rx = timer_service.take_receiver().unwrap();
        
        // 等待定时器触发
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        
        if let Some(timer_id) = timeout_rx.recv().await {
            // 取出失败任务
            let failed_info = task_allocator.pop_failed_task(timer_id.task_id());
            assert!(failed_info.is_some());
            
            let info = failed_info.unwrap();
            assert_eq!(info.retry_count, 1);
            let (info_start, info_end) = info.range.as_file_range();
            let (range_start, range_end) = range.as_file_range();
            assert_eq!(info_start, range_start);
            assert_eq!(info_end, range_end);
            
            // 取出后应该没有待重试的任务了
            assert_eq!(task_allocator.pending_retry_count(), 0);
        }
    }

    #[test]
    fn test_record_permanent_failure() {
        let allocator = RangeAllocator::new(1000);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        // 分配一个 range
        task_allocator.mark_worker_idle(0);
        let allocated = task_allocator.try_allocate_task_to_idle_worker(100).unwrap();
        let (task, _, _cancel_tx) = allocated.into_parts();
        let range = match task {
            WorkerTask::Range { range, .. } => range,
        };
        
        // 记录永久失败
        task_allocator.record_permanent_failure(range.clone(), "Connection timeout".to_string());
        
        assert!(task_allocator.has_permanent_failures());
        
        let failures = task_allocator.get_permanent_failures();
        assert_eq!(failures.len(), 1);
        let (failure_start, failure_end) = failures[0].0.as_file_range();
        let (range_start, range_end) = range.as_file_range();
        assert_eq!(failure_start, range_start);
        assert_eq!(failure_end, range_end);
        assert_eq!(failures[0].1, "Connection timeout");
    }

    #[test]
    fn test_multiple_permanent_failures() {
        let allocator = RangeAllocator::new(1000);
        let mut task_allocator = TaskAllocator::new(allocator, "http://example.com/file".to_string());
        
        task_allocator.mark_worker_idle(0);
        task_allocator.mark_worker_idle(1);
        
        // 分配并失败两个任务
        let allocated1 = task_allocator.try_allocate_task_to_idle_worker(100).unwrap();
        let (task1, _, _cancel_tx1) = allocated1.into_parts();
        let range1 = match task1 { WorkerTask::Range { range, .. } => range };
        
        let allocated2 = task_allocator.try_allocate_task_to_idle_worker(100).unwrap();
        let (task2, _, _cancel_tx2) = allocated2.into_parts();
        let range2 = match task2 { WorkerTask::Range { range, .. } => range };
        
        task_allocator.record_permanent_failure(range1, "Error 1".to_string());
        task_allocator.record_permanent_failure(range2, "Error 2".to_string());
        
        assert!(task_allocator.has_permanent_failures());
        assert_eq!(task_allocator.get_permanent_failures().len(), 2);
    }
}

