//! 任务分配器 Actor 模块
//!
//! 使用 Actor 模式管理任务分配、超时监听、任务结果监听和取消监听

use crate::download::worker_health_checker::WorkerCancelRequest;
use crate::pool::download::DownloadWorkerHandle;
use crate::task::{RangeResult, WorkerTask};
use crate::utils::io_traits::HttpClient;
use crate::{DownloadError, Result};
use kestrel_timer::{TaskId, TaskNotification, TimerService, TimerTask, spsc};
use lite_sync::oneshot::lite;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use ranged_mmap::{AllocatedRange, RangeAllocator};
use rustc_hash::{FxHashMap, FxHashSet};
use smr_swap::LocalReader;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// 失败的 Range 信息
pub(super) type FailedRange = (AllocatedRange, String);

/// 任务完成结果
#[derive(Debug)]
pub(super) enum CompletionResult {
    /// 所有任务成功完成
    Success,
    /// 有任务永久失败
    PermanentFailure { failures: Vec<FailedRange> },
}

/// TaskAllocator Actor 的消息类型
pub(super) enum AllocatorMessage {
    /// 注册新启动的 workers（标记为空闲并自动触发任务分配）
    RegisterNewWorkers { worker_ids: Vec<u64> },
    /// 关闭 Actor
    Shutdown,
}

/// 已分配的任务（封装任务和取消通道）
pub(super) struct AllocatedTask {
    /// 任务本身
    task: WorkerTask,
    /// 分配到的 worker ID
    worker_id: u64,
    /// 取消信号发送器
    cancel_tx: lite::Sender<()>,
}

impl AllocatedTask {
    /// 解构为独立部分
    pub(super) fn into_parts(self) -> (WorkerTask, u64, lite::Sender<()>) {
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

/// 任务分配器内部状态
///
/// 管理 range 分配、空闲 worker 和失败任务
struct TaskAllocatorState {
    /// Range 分配器
    allocator: RangeAllocator,
    /// 下载 URL
    url: String,
    /// 空闲 worker ID 队列
    pub(super) idle_workers: VecDeque<u64>,
    /// 待重试的失败任务映射（定时器 TaskId -> 失败任务信息）
    failed_tasks: FxHashMap<TaskId, FailedTaskInfo>,
    /// 已就绪的重试任务队列（定时器已触发但尚未分配）
    ready_retry_queue: VecDeque<FailedTaskInfo>,
    /// 永久失败的任务（达到最大重试次数）
    permanently_failed: Vec<(AllocatedRange, String)>,
}

impl TaskAllocatorState {
    /// 创建新的任务分配器状态
    fn new(allocator: RangeAllocator, url: String) -> Self {
        Self {
            allocator,
            url,
            idle_workers: VecDeque::new(),
            failed_tasks: FxHashMap::default(),
            ready_retry_queue: VecDeque::new(),
            permanently_failed: Vec::new(),
        }
    }

    /// 尝试为空闲 worker 分配任务（优先重试队列，其次新任务）
    pub(super) fn try_allocate_task_to_idle_worker(
        &mut self,
        chunk_size: u64,
    ) -> Option<AllocatedTask> {
        // 从队列中获取空闲 worker
        let worker_id = self.idle_workers.pop_front()?;

        // 优先从重试队列中取任务
        if let Some(retry_info) = self.ready_retry_queue.pop_front() {
            let (start, end) = retry_info.range.as_range_tuple();
            debug!(
                "从重试队列分配任务 range {}..{}, 重试次数 {}",
                start, end, retry_info.retry_count
            );

            // 创建取消通道
            let (cancel_tx, cancel_rx) = lite::channel();

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
    pub(super) fn mark_worker_idle(&mut self, worker_id: u64) {
        self.idle_workers.push_back(worker_id);
    }

    /// 根据定时器 ID 取出失败任务信息
    pub(super) fn pop_failed_task(&mut self, timer_id: TaskId) -> Option<FailedTaskInfo> {
        self.failed_tasks.remove(&timer_id)
    }

    /// 将失败任务推入就绪重试队列（当定时器触发但无空闲 worker 时）
    pub(super) fn push_ready_retry_task(&mut self, task_info: FailedTaskInfo) {
        let (start, end) = task_info.range.as_range_tuple();
        debug!(
            "推入重试任务到就绪队列 range {}..{}, 重试次数 {}",
            start, end, task_info.retry_count
        );
        self.ready_retry_queue.push_back(task_info);
    }

    /// 记录失败任务并注册定时器
    pub(super) fn record_failed_task(
        &mut self,
        range: AllocatedRange,
        retry_count: usize,
        delay: std::time::Duration,
        timer_service: &TimerService,
    ) -> Result<()> {
        let (start, end) = range.as_range_tuple();
        debug!(
            "记录失败任务 range {}..{}, 重试次数 {}, 将在 {:.1}s 后重试",
            start,
            end,
            retry_count,
            delay.as_secs_f64()
        );

        let task_handle = timer_service.allocate_handle();
        let task_id = task_handle.task_id();

        // 创建定时器任务（无回调，仅通知）
        let timer_task = TimerTask::new_oneshot(delay, None);

        // 注册到 TimerService
        timer_service
            .register(task_handle, timer_task)
            .map_err(|e| DownloadError::Other(format!("注册定时器失败: {:?}", e)))?;

        // 存储映射关系
        self.failed_tasks
            .insert(task_id, FailedTaskInfo { range, retry_count });

        Ok(())
    }

    /// 记录永久失败的任务
    pub(super) fn record_permanent_failure(&mut self, range: AllocatedRange, error: String) {
        let (start, end) = range.as_range_tuple();
        error!("任务永久失败 range {}..{}: {}", start, end, error);
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

    /// 获取待重试的任务数量（包括定时器等待中和已就绪的）
    pub(super) fn pending_retry_count(&self) -> usize {
        self.failed_tasks.len() + self.ready_retry_queue.len()
    }

    /// 获取下载 URL
    pub(super) fn url(&self) -> &str {
        &self.url
    }
}

/// TaskAllocator Actor 实体
///
/// 管理任务分配、超时监听、结果处理和取消处理
pub(super) struct TaskAllocatorActor<C: HttpClient> {
    /// 内部状态
    state: TaskAllocatorState,
    /// 定时器服务
    timer_service: TimerService,
    /// 定时器超时接收器
    timeout_rx: spsc::Receiver<TaskNotification, 32>,
    /// Worker 结果接收器
    result_rx: mpsc::Receiver<RangeResult>,
    /// 取消请求接收器
    cancel_rx: mpsc::Receiver<WorkerCancelRequest>,
    /// 消息接收器
    message_rx: mpsc::Receiver<AllocatorMessage>,
    /// 配置
    config: Arc<crate::config::DownloadConfig>,
    /// Worker 句柄映射
    worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle<C>>>,
    /// 任务取消 sender 映射
    cancel_senders: FxHashMap<u64, lite::Sender<()>>,
    /// 活跃 worker 集合（与 WorkerHealthChecker 共享）
    active_workers: Arc<RwLock<FxHashSet<u64>>>,
    /// 完成通知发送器（当所有任务完成时发送）
    completion_tx: Option<oneshot::Sender<CompletionResult>>,
}

impl<C: HttpClient + Clone> TaskAllocatorActor<C> {
    /// 创建新的 TaskAllocator Actor 并启动
    pub(super) fn new(
        allocator: RangeAllocator,
        url: String,
        mut timer_service: TimerService,
        result_rx: mpsc::Receiver<RangeResult>,
        cancel_rx: mpsc::Receiver<WorkerCancelRequest>,
        config: Arc<crate::config::DownloadConfig>,
        worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle<C>>>,
        active_workers: Arc<RwLock<FxHashSet<u64>>>,
    ) -> (TaskAllocatorHandle, oneshot::Receiver<CompletionResult>) {
        let (message_tx, message_rx) = mpsc::channel(100);
        let (completion_tx, completion_rx) = oneshot::channel();

        let timeout_rx = timer_service
            .take_receiver()
            .expect("Failed to take timer receiver");

        let actor = Self {
            state: TaskAllocatorState::new(allocator, url),
            timer_service,
            timeout_rx,
            result_rx,
            cancel_rx,
            message_rx,
            config,
            worker_handles,
            cancel_senders: FxHashMap::default(),
            active_workers,
            completion_tx: Some(completion_tx),
        };

        // 启动 Actor
        let actor_handle = tokio::spawn(actor.run());

        let handle = TaskAllocatorHandle {
            message_tx,
            actor_handle,
        };

        (handle, completion_rx)
    }

    /// 发送任务给指定的 worker
    ///
    /// 统一的任务发送逻辑，处理错误和状态更新
    async fn send_task_to_worker(
        &mut self,
        worker_id: u64,
        task: WorkerTask,
        cancel_tx: lite::Sender<()>,
    ) -> bool {
        let handle = {
            let worker_handles = self.worker_handles.load();
            worker_handles.get(&worker_id).cloned()
        };
        if let Some(handle) = handle {
            if let Err(e) = handle.send_task(task).await {
                error!("分配任务失败: {:?}", e);
                self.state.mark_worker_idle(worker_id);
                return false;
            }
            self.cancel_senders.insert(worker_id, cancel_tx);
            self.active_workers.write().insert(worker_id);
            true
        } else {
            error!("Worker #{} 不存在", worker_id);
            self.state.mark_worker_idle(worker_id);
            false
        }
    }

    /// 从失败任务信息创建 WorkerTask
    fn create_retry_task(
        &self,
        info: &FailedTaskInfo,
        cancel_rx: lite::Receiver<()>,
    ) -> WorkerTask {
        WorkerTask::Range {
            url: self.state.url().to_string(),
            range: info.range.clone(),
            retry_count: info.retry_count,
            cancel_rx,
        }
    }

    /// 清理 worker 状态并标记为空闲
    fn cleanup_worker(&mut self, worker_id: u64) {
        self.cancel_senders.remove(&worker_id);
        self.active_workers.write().remove(&worker_id);
        self.state.mark_worker_idle(worker_id);
    }

    /// 检查是否所有任务已完成
    fn check_completion(&mut self) -> Option<CompletionResult> {
        // 先检查永久失败
        if self.state.has_permanent_failures() {
            let failures = self.state.get_permanent_failures().to_vec();
            info!("遇到永久失败，终止下载：{} 个任务失败", failures.len());
            return Some(CompletionResult::PermanentFailure { failures });
        }

        // 检查是否所有任务已成功完成
        if self.state.remaining() == 0
            && self.state.pending_retry_count() == 0
            && self.active_workers.read().is_empty()
        {
            info!("所有任务已成功完成");
            return Some(CompletionResult::Success);
        }

        None
    }

    /// 运行 Actor 主循环
    pub(super) async fn run(mut self) {
        info!("TaskAllocator Actor 启动");

        loop {
            tokio::select! {
                // 处理定时器超时事件
                Some(notification) = self.timeout_rx.recv() => {
                    self.handle_retry_timeout(notification.task_id()).await;
                }

                // 处理 worker 结果
                Some(result) = self.result_rx.recv() => {
                    if !self.handle_worker_result(result).await {
                        break;
                    }
                }

                // 处理取消请求
                Some(request) = self.cancel_rx.recv() => {
                    self.handle_cancel_request(request).await;
                }

                // 处理消息
                Some(msg) = self.message_rx.recv() => {
                    if !self.handle_message(msg).await {
                        break;
                    }
                }

                else => {
                    debug!("所有通道已关闭，Actor 退出");
                    break;
                }
            }
        }

        info!("TaskAllocator Actor 停止");
    }

    /// 处理消息
    async fn handle_message(&mut self, msg: AllocatorMessage) -> bool {
        match msg {
            AllocatorMessage::RegisterNewWorkers { worker_ids } => {
                for worker_id in worker_ids {
                    // 只标记为空闲，不立即标记为活跃
                    // worker 只有在真正接收任务时才会被标记为活跃
                    self.state.mark_worker_idle(worker_id);
                }
                // 注册完成后立即尝试为空闲 workers 分配任务
                self.allocate_tasks_to_idle_workers().await;
            }
            AllocatorMessage::Shutdown => {
                return false;
            }
        }
        true
    }

    /// 处理重试超时事件
    async fn handle_retry_timeout(&mut self, timer_id: TaskId) {
        let Some(info) = self.state.pop_failed_task(timer_id) else {
            return;
        };

        let (start, end) = info.range.as_range_tuple();
        info!(
            "定时器触发，重试任务 range {}..{}, 重试次数 {}",
            start, end, info.retry_count
        );

        // 尝试分配给空闲 worker
        if let Some(worker_id) = self.state.idle_workers.pop_front() {
            let (cancel_tx, cancel_rx) = lite::channel();
            let task = self.create_retry_task(&info, cancel_rx);

            if !self.send_task_to_worker(worker_id, task, cancel_tx).await {
                // 发送失败，推入就绪队列
                self.state.push_ready_retry_task(info);
            }
        } else {
            debug!("定时器触发但没有空闲 worker，推入就绪队列");
            self.state.push_ready_retry_task(info);
        }
    }

    /// 处理 worker 结果
    async fn handle_worker_result(&mut self, result: RangeResult) -> bool {
        match result {
            RangeResult::Complete { worker_id } => {
                self.handle_complete(worker_id).await;
            }
            RangeResult::DownloadFailed {
                worker_id,
                range,
                error,
                retry_count,
            } => {
                self.handle_failed(worker_id, range, error, retry_count)
                    .await;
            }
            RangeResult::WriteFailed {
                worker_id,
                range,
                error,
            } => {
                let (start, end) = range.as_range_tuple();
                error!(
                    "Worker #{} 写入失败，终止下载 (range: {}..{}): {}",
                    worker_id, start, end, error
                );
                return false;
            }
        }

        // 检查完成条件
        if let Some(completion) = self.check_completion() {
            if let Some(tx) = self.completion_tx.take() {
                let _ = tx.send(completion);
            }
            return false;
        }

        true
    }

    /// 处理任务完成
    async fn handle_complete(&mut self, worker_id: u64) {
        self.cleanup_worker(worker_id);
        self.try_allocate_next_task(worker_id).await;
    }

    /// 处理任务失败
    async fn handle_failed(
        &mut self,
        worker_id: u64,
        range: AllocatedRange,
        error: String,
        retry_count: usize,
    ) {
        let (start, end) = range.as_range_tuple();
        warn!(
            "Worker #{} Range {}..{} 失败 (重试 {}): {}",
            worker_id, start, end, retry_count, error
        );

        self.cleanup_worker(worker_id);
        self.schedule_retry_task(range, retry_count);
        self.try_allocate_next_task(worker_id).await;
    }

    /// 处理取消请求
    async fn handle_cancel_request(&mut self, request: WorkerCancelRequest) {
        let WorkerCancelRequest { worker_id, reason } = request;

        info!("收到 Worker #{} 取消请求: {}", worker_id, reason);

        if let Some(cancel_tx) = self.cancel_senders.remove(&worker_id) {
            let _ = cancel_tx.notify(());
            self.active_workers.write().remove(&worker_id);
            info!("已取消 Worker #{} 的任务", worker_id);
        }
    }

    /// 为所有空闲 workers 分配任务
    ///
    /// 这是注册 workers 后自动触发的统一任务分配入口
    async fn allocate_tasks_to_idle_workers(&mut self) {
        info!("开始为空闲 workers 分配任务");

        while let Some(&worker_id) = self.state.idle_workers.front() {
            let chunk_size = {
                let handles = self.worker_handles.load();
                handles
                    .get(&worker_id)
                    .map(|h| h.chunk_size())
                    .unwrap_or(self.config.chunk().initial_size())
            };

            if let Some(allocated) = self.state.try_allocate_task_to_idle_worker(chunk_size) {
                let (task, worker_id, cancel_tx) = allocated.into_parts();
                debug!(
                    "为 Worker #{} 分配任务，分块大小 {} bytes",
                    worker_id, chunk_size
                );

                self.send_task_to_worker(worker_id, task, cancel_tx).await;
            } else {
                debug!("没有足够的数据为空闲 workers 分配更多任务");
                break;
            }
        }
    }

    /// 尝试为 worker 分配下一个任务
    async fn try_allocate_next_task(&mut self, worker_id: u64) {
        let chunk_size = {
            let handles = self.worker_handles.load();
            handles
                .get(&worker_id)
                .map(|h| h.chunk_size())
                .unwrap_or(self.config.chunk().initial_size())
        };

        if let Some(allocated) = self.state.try_allocate_task_to_idle_worker(chunk_size) {
            let (task, target_worker, cancel_tx) = allocated.into_parts();
            debug!(
                "Worker #{} 分配新任务到空闲 Worker #{}，分块大小 {} bytes",
                worker_id, target_worker, chunk_size
            );

            self.send_task_to_worker(target_worker, task, cancel_tx)
                .await;
        } else {
            debug!("Worker #{} 完成任务，但没有更多任务可分配", worker_id);
        }
    }

    /// 调度重试任务
    fn schedule_retry_task(&mut self, range: AllocatedRange, retry_count: usize) {
        let max_retry = self.config.retry().max_retry_count();
        let (start, end) = range.as_range_tuple();

        if retry_count >= max_retry {
            error!(
                "任务 range {}..{} 已达到最大重试次数 {}，标记为永久失败",
                start, end, max_retry
            );
            self.state
                .record_permanent_failure(range, "达到最大重试次数".to_string());
            return;
        }

        // 计算重试延迟
        let retry_delays = self.config.retry().retry_delays();
        let delay = retry_delays
            .get(retry_count)
            .copied()
            .or_else(|| retry_delays.last().copied())
            .unwrap_or(std::time::Duration::from_secs(3));

        info!(
            "任务 range {}..{} 将在 {:.1}s 后进行第 {} 次重试",
            start,
            end,
            delay.as_secs_f64(),
            retry_count + 1
        );

        // 注册重试任务
        if let Err(e) =
            self.state
                .record_failed_task(range, retry_count + 1, delay, &self.timer_service)
        {
            error!("注册重试定时器失败: {:?}", e);
            self.state
                .record_permanent_failure(range, format!("注册定时器失败: {}", e));
        }
    }
}

/// TaskAllocator Actor 的通信句柄
pub(super) struct TaskAllocatorHandle {
    message_tx: mpsc::Sender<AllocatorMessage>,
    actor_handle: tokio::task::JoinHandle<()>,
}

impl TaskAllocatorHandle {
    /// 注册新的 workers（标记为空闲并自动触发任务分配）
    ///
    /// 这是统一的 worker 注册入口，适用于：
    /// - 初始 workers 的注册
    /// - 动态添加的新 workers 的注册
    pub(super) async fn register_new_workers(&self, worker_ids: Vec<u64>) {
        let _ = self
            .message_tx
            .send(AllocatorMessage::RegisterNewWorkers { worker_ids })
            .await;
    }

    pub(super) async fn shutdown_and_wait(self) {
        let _ = self.message_tx.send(AllocatorMessage::Shutdown).await;
        let _ = self.actor_handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kestrel_timer::{TimerWheel, config::ServiceConfig};

    /// 创建测试用的 TaskAllocatorState
    ///
    /// 通过创建临时文件来获取 RangeAllocator
    fn create_test_allocator(size: u64) -> TaskAllocatorState {
        use ranged_mmap::MmapFile;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        let (_file, allocator) =
            MmapFile::create(temp_file.path(), NonZeroU64::new(size).unwrap()).unwrap();
        TaskAllocatorState::new(allocator, "http://example.com/file.bin".to_string())
    }

    /// 从 TaskAllocatorState 中分配一个 range（用于测试）
    ///
    /// 直接从底层的 RangeAllocator 分配，不经过任务分配逻辑
    fn allocate_range_from(allocator: &mut TaskAllocatorState, size: u64) -> AllocatedRange {
        allocator
            .allocator
            .allocate(NonZeroU64::new(size).unwrap())
            .expect("Failed to allocate range")
    }

    #[test]
    fn test_initial_state() {
        let allocator = create_test_allocator(1000);

        // 初始状态验证
        assert_eq!(allocator.remaining(), 1000);
        assert_eq!(allocator.pending_retry_count(), 0);
        assert!(!allocator.has_permanent_failures());
        assert_eq!(allocator.url(), "http://example.com/file.bin");
        assert_eq!(allocator.idle_workers.len(), 0);
    }

    #[test]
    fn test_idle_worker_management() {
        let mut allocator = create_test_allocator(1000);

        // 测试添加空闲 worker
        allocator.mark_worker_idle(0);
        allocator.mark_worker_idle(1);
        allocator.mark_worker_idle(2);

        assert_eq!(allocator.idle_workers.len(), 3);
        assert_eq!(allocator.idle_workers.front(), Some(&0));

        // 测试 FIFO 顺序
        allocator.mark_worker_idle(3);
        assert_eq!(allocator.idle_workers.len(), 4);
        assert_eq!(allocator.idle_workers.back(), Some(&3));
    }

    #[test]
    fn test_allocate_new_task_to_idle_worker() {
        let mut allocator = create_test_allocator(1000);

        // 标记 worker 为空闲
        allocator.mark_worker_idle(5);

        // 分配新任务
        let result = allocator.try_allocate_task_to_idle_worker(100);
        assert!(result.is_some());

        let allocated = result.unwrap();
        let (task, worker_id, _cancel_tx) = allocated.into_parts();

        // 验证分配结果
        assert_eq!(worker_id, 5);
        match task {
            WorkerTask::Range {
                url,
                range,
                retry_count,
                ..
            } => {
                assert_eq!(url, "http://example.com/file.bin");
                assert_eq!(range.len(), 100);
                assert_eq!(retry_count, 0); // 新任务重试次数为 0
            }
        }

        // 空闲队列应该为空
        assert_eq!(allocator.idle_workers.len(), 0);
        assert_eq!(allocator.remaining(), 900);
    }

    #[test]
    fn test_no_allocation_without_idle_workers() {
        let mut allocator = create_test_allocator(1000);

        // 没有空闲 worker，不应该分配任务
        let result = allocator.try_allocate_task_to_idle_worker(100);
        assert!(result.is_none());

        // 状态不应该改变
        assert_eq!(allocator.remaining(), 1000);
    }

    #[test]
    fn test_no_allocation_when_space_exhausted() {
        let mut allocator = create_test_allocator(100);

        // 分配所有空间
        allocator.mark_worker_idle(0);
        let _ = allocator.try_allocate_task_to_idle_worker(100).unwrap();

        assert_eq!(allocator.remaining(), 0);

        // 再次尝试分配
        allocator.mark_worker_idle(1);
        let result = allocator.try_allocate_task_to_idle_worker(100);

        // 应该返回 None，但 worker 应该被放回队列
        assert!(result.is_none());
        assert_eq!(allocator.idle_workers.len(), 1);
        assert_eq!(allocator.idle_workers.front(), Some(&1));
    }

    #[test]
    fn test_retry_task_has_priority() {
        let mut allocator = create_test_allocator(1000);

        // 先分配一个 range，然后将它作为重试任务
        let range = allocate_range_from(&mut allocator, 100);
        let retry_info = FailedTaskInfo {
            range,
            retry_count: 2,
        };
        allocator.push_ready_retry_task(retry_info);

        // 标记 worker 为空闲
        allocator.mark_worker_idle(0);

        // 分配任务，应该优先分配重试任务
        let result = allocator.try_allocate_task_to_idle_worker(200);
        assert!(result.is_some());

        let allocated = result.unwrap();
        let (task, worker_id, _cancel_tx) = allocated.into_parts();

        assert_eq!(worker_id, 0);
        match task {
            WorkerTask::Range {
                range, retry_count, ..
            } => {
                assert_eq!(range.len(), 100); // 重试任务的大小，不是 200
                assert_eq!(retry_count, 2); // 重试次数应该保持
            }
        }

        // 重试队列应该为空
        assert_eq!(allocator.ready_retry_queue.len(), 0);
        // remaining 应该是 900（因为我们之前分配了 100）
        assert_eq!(allocator.remaining(), 900);
    }

    #[test]
    fn test_push_and_allocate_multiple_retry_tasks() {
        let mut allocator = create_test_allocator(1000);

        // 分配两个 range 作为重试任务
        let range1 = allocate_range_from(&mut allocator, 100);
        let range2 = allocate_range_from(&mut allocator, 100);

        allocator.push_ready_retry_task(FailedTaskInfo {
            range: range1,
            retry_count: 1,
        });
        allocator.push_ready_retry_task(FailedTaskInfo {
            range: range2,
            retry_count: 2,
        });

        assert_eq!(allocator.pending_retry_count(), 2);

        // 分配第一个重试任务
        allocator.mark_worker_idle(0);
        let result1 = allocator.try_allocate_task_to_idle_worker(500);
        assert!(result1.is_some());

        let (task1, _, _) = result1.unwrap().into_parts();
        match task1 {
            WorkerTask::Range {
                range, retry_count, ..
            } => {
                assert_eq!(range.start(), 0);
                assert_eq!(range.end(), 100);
                assert_eq!(retry_count, 1);
            }
        }

        // 分配第二个重试任务
        allocator.mark_worker_idle(1);
        let result2 = allocator.try_allocate_task_to_idle_worker(500);
        assert!(result2.is_some());

        let (task2, _, _) = result2.unwrap().into_parts();
        match task2 {
            WorkerTask::Range {
                range, retry_count, ..
            } => {
                assert_eq!(range.start(), 100);
                assert_eq!(range.end(), 200);
                assert_eq!(retry_count, 2);
            }
        }

        assert_eq!(allocator.pending_retry_count(), 0);
    }

    #[tokio::test]
    async fn test_record_failed_task_with_timer() {
        let mut allocator = create_test_allocator(1000);

        // 创建定时器服务
        let timer = TimerWheel::with_defaults();
        let timer_service = timer.create_service(ServiceConfig::default());

        // 分配一个 range 作为失败任务
        let range = allocate_range_from(&mut allocator, 100);

        // 记录失败任务
        let result = allocator.record_failed_task(
            range,
            1,
            std::time::Duration::from_millis(100),
            &timer_service,
        );

        assert!(result.is_ok());
        assert_eq!(allocator.pending_retry_count(), 1);
        assert_eq!(allocator.failed_tasks.len(), 1);
        assert_eq!(allocator.ready_retry_queue.len(), 0);
    }

    #[tokio::test]
    async fn test_pop_failed_task_after_timer() {
        let mut allocator = create_test_allocator(1000);

        // 创建定时器服务
        let timer = TimerWheel::with_defaults();
        let mut timer_service = timer.create_service(ServiceConfig::default());

        // 分配一个 range
        let range = allocate_range_from(&mut allocator, 100);
        let (start, end) = range.as_range_tuple();

        // 记录失败任务
        allocator
            .record_failed_task(
                range,
                3,
                std::time::Duration::from_millis(50),
                &timer_service,
            )
            .unwrap();

        // 获取接收器
        let timeout_rx = timer_service.take_receiver().unwrap();

        // 等待定时器触发
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        if let Some(timer_id) = timeout_rx.recv().await {
            // 取出失败任务
            let failed_info = allocator.pop_failed_task(timer_id.task_id());
            assert!(failed_info.is_some());

            let info = failed_info.unwrap();
            assert_eq!(info.retry_count, 3);
            assert_eq!(info.range.start(), start);
            assert_eq!(info.range.end(), end);

            // 从 failed_tasks 中移除后，pending_retry_count 应该减少
            assert_eq!(allocator.pending_retry_count(), 0);
        }
    }

    #[test]
    fn test_record_permanent_failure() {
        let mut allocator = create_test_allocator(1000);

        let range = allocate_range_from(&mut allocator, 100);
        allocator.record_permanent_failure(range, "Network timeout".to_string());

        assert!(allocator.has_permanent_failures());

        let failures = allocator.get_permanent_failures();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0.start(), 0);
        assert_eq!(failures[0].0.end(), 100);
        assert_eq!(failures[0].1, "Network timeout");
    }

    #[test]
    fn test_multiple_permanent_failures() {
        let mut allocator = create_test_allocator(1000);

        let range1 = allocate_range_from(&mut allocator, 100);
        let range2 = allocate_range_from(&mut allocator, 100);
        let range3 = allocate_range_from(&mut allocator, 100);

        allocator.record_permanent_failure(range1, "Error 1".to_string());
        allocator.record_permanent_failure(range2, "Error 2".to_string());
        allocator.record_permanent_failure(range3, "Error 3".to_string());

        assert!(allocator.has_permanent_failures());

        let failures = allocator.get_permanent_failures();
        assert_eq!(failures.len(), 3);
        assert_eq!(failures[0].1, "Error 1");
        assert_eq!(failures[1].1, "Error 2");
        assert_eq!(failures[2].1, "Error 3");
    }

    #[tokio::test]
    async fn test_pending_retry_count_accuracy() {
        let mut allocator = create_test_allocator(1000);

        // 初始为 0
        assert_eq!(allocator.pending_retry_count(), 0);

        // 添加就绪重试任务
        let range1 = allocate_range_from(&mut allocator, 100);
        allocator.push_ready_retry_task(FailedTaskInfo {
            range: range1,
            retry_count: 1,
        });
        assert_eq!(allocator.pending_retry_count(), 1);

        // 添加更多就绪任务
        let range2 = allocate_range_from(&mut allocator, 100);
        allocator.push_ready_retry_task(FailedTaskInfo {
            range: range2,
            retry_count: 2,
        });
        assert_eq!(allocator.pending_retry_count(), 2);

        // 模拟添加定时器等待中的任务（通过直接操作 failed_tasks）
        let timer_service = TimerWheel::with_defaults().create_service(ServiceConfig::default());
        let task_handle = timer_service.allocate_handle();
        let task_id = task_handle.task_id();

        let range3 = allocate_range_from(&mut allocator, 100);
        allocator.failed_tasks.insert(
            task_id,
            FailedTaskInfo {
                range: range3,
                retry_count: 3,
            },
        );

        // 应该包括就绪队列和等待队列
        assert_eq!(allocator.pending_retry_count(), 3);
    }

    #[test]
    fn test_allocate_respects_remaining_space() {
        let mut allocator = create_test_allocator(500);

        allocator.mark_worker_idle(0);

        // 请求 1000 字节，但只有 500 字节
        let result = allocator.try_allocate_task_to_idle_worker(1000);
        assert!(result.is_some());

        let (task, _, _) = result.unwrap().into_parts();
        match task {
            WorkerTask::Range { range, .. } => {
                assert_eq!(range.len(), 500); // 应该被限制为剩余空间
            }
        }

        assert_eq!(allocator.remaining(), 0);
    }

    #[test]
    fn test_cancel_channel_created_for_each_task() {
        let mut allocator = create_test_allocator(1000);

        allocator.mark_worker_idle(0);
        let result = allocator.try_allocate_task_to_idle_worker(100);
        assert!(result.is_some());

        let allocated = result.unwrap();
        let (task, _, cancel_tx) = allocated.into_parts();

        // 验证 cancel_rx 存在于任务中
        match task {
            WorkerTask::Range { mut cancel_rx, .. } => {
                // 验证通道有效性
                // 如果 cancel_tx 被 drop，cancel_rx 会收到 Err
                drop(cancel_tx);
                assert!(cancel_rx.try_recv().is_err()); // 已经被 drop，会返回错误
            }
        }
    }
}
