//! 任务队列管理模块
//!
//! 负责管理待分配任务和空闲 Worker 队列

use crate::task::WorkerTask;
use lite_sync::oneshot::lite;
use log::debug;
use ranged_mmap::{AllocatedRange, RangeAllocator};
use std::collections::VecDeque;
use std::num::NonZeroU64;

/// 失败任务信息
///
/// 用于跟踪待重试的失败任务
#[derive(Debug)]
pub struct FailedTaskInfo {
    /// 失败的 range
    pub range: AllocatedRange,
    /// 当前重试次数
    pub retry_count: usize,
}

/// 待派发的任务（包含取消通道）
pub struct PendingTask {
    /// 任务本身
    pub task: WorkerTask,
    /// 目标 worker ID
    pub worker_id: u64,
    /// 取消信号发送器
    pub cancel_tx: lite::Sender<()>,
}

/// 任务队列
///
/// 管理待分配的新任务和重试任务队列
/// 纯同步逻辑，无 I/O 操作
pub struct TaskQueue {
    /// Range 分配器
    allocator: RangeAllocator,
    /// 下载 URL
    url: String,
    /// 空闲 worker ID 队列
    idle_workers: VecDeque<u64>,
    /// 已就绪的重试任务队列
    ready_retry_queue: VecDeque<FailedTaskInfo>,
}

impl TaskQueue {
    /// 创建新的任务队列
    pub fn new(allocator: RangeAllocator, url: String) -> Self {
        Self {
            allocator,
            url,
            idle_workers: VecDeque::new(),
            ready_retry_queue: VecDeque::new(),
        }
    }

    /// 尝试分配任务给空闲 worker
    ///
    /// 优先分配重试任务，其次分配新任务
    /// 返回 `None` 表示没有空闲 worker 或没有可分配的任务
    pub fn try_allocate(&mut self, chunk_size: u64) -> Option<PendingTask> {
        // 从队列中获取空闲 worker
        let worker_id = self.idle_workers.pop_front()?;

        // 优先从重试队列中取任务
        if let Some(retry_info) = self.ready_retry_queue.pop_front() {
            return Some(self.create_retry_task(worker_id, retry_info));
        }

        // 重试队列为空，分配新任务
        self.try_allocate_new_task(worker_id, chunk_size)
    }

    /// 创建重试任务
    fn create_retry_task(&self, worker_id: u64, info: FailedTaskInfo) -> PendingTask {
        let (start, end) = info.range.as_range_tuple();
        debug!(
            "从重试队列分配任务 range {}..{}, 重试次数 {}",
            start, end, info.retry_count
        );

        let (cancel_tx, cancel_rx) = lite::channel();
        let task = WorkerTask::Range {
            url: self.url.clone(),
            range: info.range,
            retry_count: info.retry_count,
            cancel_rx,
        };

        PendingTask {
            task,
            worker_id,
            cancel_tx,
        }
    }

    /// 尝试分配新任务
    fn try_allocate_new_task(&mut self, worker_id: u64, chunk_size: u64) -> Option<PendingTask> {
        let remaining = self.allocator.remaining();
        if remaining == 0 {
            // 没有剩余任务，将 worker 放回队列
            self.idle_workers.push_back(worker_id);
            return None;
        }

        // 计算实际分配大小（不超过剩余空间）
        let alloc_size = chunk_size.min(remaining);

        // 分配 range
        let range = self
            .allocator
            .allocate(NonZeroU64::new(alloc_size).unwrap())
            .ok()?;

        // 创建取消通道
        let (cancel_tx, cancel_rx) = lite::channel();

        // 创建任务（首次分配，重试次数为 0）
        let task = WorkerTask::Range {
            url: self.url.clone(),
            range,
            retry_count: 0,
            cancel_rx,
        };

        Some(PendingTask {
            task,
            worker_id,
            cancel_tx,
        })
    }

    /// 获取剩余待分配的字节数
    #[inline]
    pub fn remaining(&self) -> u64 {
        self.allocator.remaining()
    }

    /// 标记 worker 为空闲状态
    #[inline]
    pub fn mark_worker_idle(&mut self, worker_id: u64) {
        self.idle_workers.push_back(worker_id);
    }

    /// 将重试任务推入就绪队列
    pub fn enqueue_retry(&mut self, info: FailedTaskInfo) {
        let (start, end) = info.range.as_range_tuple();
        debug!(
            "推入重试任务到就绪队列 range {}..{}, 重试次数 {}",
            start, end, info.retry_count
        );
        self.ready_retry_queue.push_back(info);
    }

    /// 获取就绪重试任务数量
    #[inline]
    pub fn ready_retry_count(&self) -> usize {
        self.ready_retry_queue.len()
    }

    /// 获取空闲 worker 数量
    #[inline]
    pub fn idle_worker_count(&self) -> usize {
        self.idle_workers.len()
    }

    /// 检查是否有空闲 worker
    #[inline]
    pub fn has_idle_workers(&self) -> bool {
        !self.idle_workers.is_empty()
    }

    /// 获取下载 URL
    #[inline]
    #[allow(dead_code)]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// 直接访问底层分配器（用于测试）
    #[allow(dead_code)]
    #[cfg(test)]
    pub fn allocator_mut(&mut self) -> &mut RangeAllocator {
        &mut self.allocator
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ranged_mmap::MmapFile;
    use tempfile::NamedTempFile;

    fn create_test_queue(size: u64) -> TaskQueue {
        let temp_file = NamedTempFile::new().unwrap();
        let (_file, allocator) =
            MmapFile::create(temp_file.path(), NonZeroU64::new(size).unwrap()).unwrap();
        TaskQueue::new(allocator, "http://example.com/file.bin".to_string())
    }

    #[test]
    fn test_initial_state() {
        let queue = create_test_queue(1000);
        assert_eq!(queue.remaining(), 1000);
        assert_eq!(queue.ready_retry_count(), 0);
        assert_eq!(queue.idle_worker_count(), 0);
        assert_eq!(queue.url(), "http://example.com/file.bin");
    }

    #[test]
    fn test_idle_worker_fifo() {
        let mut queue = create_test_queue(1000);

        queue.mark_worker_idle(0);
        queue.mark_worker_idle(1);
        queue.mark_worker_idle(2);

        assert_eq!(queue.idle_worker_count(), 3);
        assert!(queue.has_idle_workers());

        // 分配时应按 FIFO 顺序
        let task = queue.try_allocate(100).unwrap();
        assert_eq!(task.worker_id, 0);

        let task = queue.try_allocate(100).unwrap();
        assert_eq!(task.worker_id, 1);
    }

    #[test]
    fn test_retry_task_priority() {
        let mut queue = create_test_queue(1000);

        // 先分配一个 range 用于重试
        queue.mark_worker_idle(0);
        let pending = queue.try_allocate(100).unwrap();
        let range = match pending.task {
            WorkerTask::Range { range, .. } => range,
        };

        // 将其作为重试任务入队
        queue.enqueue_retry(FailedTaskInfo {
            range,
            retry_count: 2,
        });

        // 标记新 worker 为空闲
        queue.mark_worker_idle(1);

        // 分配应优先返回重试任务
        let task = queue.try_allocate(200).unwrap();
        assert_eq!(task.worker_id, 1);

        match task.task {
            WorkerTask::Range { retry_count, .. } => {
                assert_eq!(retry_count, 2);
            }
        }
    }

    #[test]
    fn test_no_allocation_without_idle_workers() {
        let mut queue = create_test_queue(1000);
        let result = queue.try_allocate(100);
        assert!(result.is_none());
    }

    #[test]
    fn test_worker_returned_when_no_work() {
        let mut queue = create_test_queue(100);

        // 分配完所有空间
        queue.mark_worker_idle(0);
        let _ = queue.try_allocate(100).unwrap();
        assert_eq!(queue.remaining(), 0);

        // 再次尝试分配
        queue.mark_worker_idle(1);
        let result = queue.try_allocate(100);

        // 应该返回 None，但 worker 被放回队列
        assert!(result.is_none());
        assert_eq!(queue.idle_worker_count(), 1);
    }

    #[test]
    fn test_allocation_respects_remaining() {
        let mut queue = create_test_queue(500);
        queue.mark_worker_idle(0);

        // 请求 1000 但只有 500
        let task = queue.try_allocate(1000).unwrap();

        match task.task {
            WorkerTask::Range { range, .. } => {
                assert_eq!(range.len(), 500);
            }
        }
    }
}
