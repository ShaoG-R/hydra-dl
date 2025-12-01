//! 任务分配器 Actor 模块
//!
//! 使用 Actor 模式管理任务分配、超时监听、任务结果监听和取消监听
//!
//! # 模块结构
//!
//! - `task_queue`: 任务队列管理（新任务 + 重试任务）
//! - `retry_scheduler`: 重试调度（定时器管理）
//! - `worker_dispatcher`: Worker 任务派发
//! - `completion_tracker`: 完成状态跟踪

// 子模块声明
mod completion_tracker;
mod retry_scheduler;
mod task_queue;
mod worker_dispatcher;

// 内部使用
use completion_tracker::CompletionTracker;
use retry_scheduler::{RetryConfig, RetryScheduler};
use task_queue::{PendingTask, TaskQueue};
use worker_dispatcher::WorkerDispatcher;

// 外部依赖
use crate::download::worker_health_checker::WorkerCancelRequest;
use crate::pool::download::DownloadWorkerHandle;
use crate::task::RangeResult;
use kestrel_timer::{TaskNotification, TimerService, spsc};
use log::{debug, error, info, warn};
use ranged_mmap::{AllocatedRange, allocator::sequential::Allocator as RangeAllocator};
use rustc_hash::FxHashMap;
use smr_swap::LocalReader;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

// 公开导出
pub(super) use completion_tracker::{CompletionResult, FailedRange};

/// TaskAllocator Actor 的消息类型
pub(super) enum AllocatorMessage {
    /// 注册新启动的 workers（标记为空闲并自动触发任务分配）
    RegisterNewWorkers { worker_ids: Vec<u64> },
    /// 关闭 Actor
    Shutdown,
}

/// TaskAllocator Actor 参数
pub(super) struct TaskAllocatorParams {
    /// Range 分配器
    pub allocator: RangeAllocator,
    /// 下载 URL
    pub url: String,
    /// 定时器服务
    pub timer_service: TimerService,
    /// Worker 结果接收器
    pub result_rx: mpsc::Receiver<RangeResult>,
    /// 取消请求接收器
    pub cancel_rx: mpsc::Receiver<WorkerCancelRequest>,
    /// 配置
    pub config: Arc<crate::config::DownloadConfig>,
    /// Worker 句柄映射
    pub worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle>>,
}

/// TaskAllocator Actor 实体
///
/// 使用组合模式管理任务分配、重试调度、任务派发和完成跟踪
pub(super) struct TaskAllocatorActor {
    // === 组件 ===
    /// 任务队列（新任务 + 重试任务 + 空闲 worker）
    task_queue: TaskQueue,
    /// 重试调度器（定时器管理）
    retry_scheduler: RetryScheduler,
    /// Worker 派发器（任务发送 + 取消信号）
    dispatcher: WorkerDispatcher,
    /// 完成跟踪器（永久失败 + 完成通知）
    completion_tracker: CompletionTracker,

    // === 消息通道 ===
    /// 定时器超时接收器
    timeout_rx: spsc::Receiver<TaskNotification, 32>,
    /// Worker 结果接收器
    result_rx: mpsc::Receiver<RangeResult>,
    /// 取消请求接收器
    cancel_rx: mpsc::Receiver<WorkerCancelRequest>,
    /// 消息接收器
    message_rx: mpsc::Receiver<AllocatorMessage>,
}

impl TaskAllocatorActor {
    /// 创建新的 TaskAllocator Actor 并启动
    pub(super) fn new(
        params: TaskAllocatorParams,
    ) -> (TaskAllocatorHandle, oneshot::Receiver<CompletionResult>) {
        let TaskAllocatorParams {
            allocator,
            url,
            mut timer_service,
            result_rx,
            cancel_rx,
            config,
            worker_handles,
        } = params;

        let (message_tx, message_rx) = mpsc::channel(100);
        let (completion_tx, completion_rx) = oneshot::channel();

        let timeout_rx = timer_service
            .take_receiver()
            .expect("Failed to take timer receiver");

        // 创建组件
        let task_queue = TaskQueue::new(allocator, url);
        let retry_scheduler =
            RetryScheduler::new(timer_service, RetryConfig::from_download_config(&config));
        let dispatcher = WorkerDispatcher::new(worker_handles, config.chunk().initial_size());
        let completion_tracker = CompletionTracker::new(completion_tx);

        let actor = Self {
            task_queue,
            retry_scheduler,
            dispatcher,
            completion_tracker,
            timeout_rx,
            result_rx,
            cancel_rx,
            message_rx,
        };

        // 启动 Actor
        let actor_handle = tokio::spawn(actor.run());

        let handle = TaskAllocatorHandle {
            message_tx,
            actor_handle,
        };

        (handle, completion_rx)
    }

    /// 运行 Actor 主循环
    async fn run(mut self) {
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
                    self.handle_cancel_request(request);
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

    // ========== 事件处理 ==========

    /// 处理消息
    async fn handle_message(&mut self, msg: AllocatorMessage) -> bool {
        match msg {
            AllocatorMessage::RegisterNewWorkers { worker_ids } => {
                for worker_id in worker_ids {
                    self.task_queue.mark_worker_idle(worker_id);
                }
                self.dispatch_pending_tasks().await;
            }
            AllocatorMessage::Shutdown => {
                return false;
            }
        }
        true
    }

    /// 处理重试超时事件
    async fn handle_retry_timeout(&mut self, timer_id: kestrel_timer::TaskId) {
        if let Some(info) = self.retry_scheduler.on_timer_fired(timer_id) {
            // 将就绪的重试任务入队
            self.task_queue.enqueue_retry(info);
            // 尝试派发
            self.dispatch_pending_tasks().await;
        }
    }

    /// 处理 worker 结果
    async fn handle_worker_result(&mut self, result: RangeResult) -> bool {
        match result {
            RangeResult::Complete { worker_id } => {
                self.on_task_complete(worker_id).await;
            }
            RangeResult::DownloadFailed {
                worker_id,
                range,
                error,
                retry_count,
            } => {
                self.on_task_failed(worker_id, range, error, retry_count)
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
        self.check_and_notify_completion()
    }

    /// 处理取消请求
    fn handle_cancel_request(&mut self, request: WorkerCancelRequest) {
        let WorkerCancelRequest { worker_id, reason } = request;
        info!("收到 Worker #{} 取消请求: {}", worker_id, reason);
        self.dispatcher.cancel_worker(worker_id);
    }

    // ========== 业务逻辑 ==========

    /// 任务完成处理
    async fn on_task_complete(&mut self, worker_id: u64) {
        self.dispatcher.cleanup_worker(worker_id);
        self.task_queue.mark_worker_idle(worker_id);
        self.dispatch_pending_tasks().await;
    }

    /// 任务失败处理
    async fn on_task_failed(
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

        self.dispatcher.cleanup_worker(worker_id);
        self.task_queue.mark_worker_idle(worker_id);

        // 调度重试
        self.schedule_retry(range, retry_count);

        // 尝试派发更多任务
        self.dispatch_pending_tasks().await;
    }

    /// 调度重试任务
    fn schedule_retry(&mut self, range: AllocatedRange, retry_count: usize) {
        match self.retry_scheduler.schedule(range.clone(), retry_count) {
            Ok(true) => {
                // 成功调度
            }
            Ok(false) => {
                // 达到最大重试次数
                self.completion_tracker
                    .record_permanent_failure(range, "达到最大重试次数".to_string());
            }
            Err(e) => {
                error!("注册重试定时器失败: {:?}", e);
                self.completion_tracker
                    .record_permanent_failure(range, format!("注册定时器失败: {}", e));
            }
        }
    }

    /// 派发待处理任务
    async fn dispatch_pending_tasks(&mut self) {
        while self.task_queue.has_idle_workers() {
            // 获取第一个空闲 worker 的 chunk size（不移除）
            let chunk_size = self.dispatcher.get_chunk_size(0); // 使用默认值，实际分配时会重新获取

            if let Some(pending) = self.task_queue.try_allocate(chunk_size) {
                self.dispatch_task(pending).await;
            } else {
                break;
            }
        }
    }

    /// 派发单个任务
    async fn dispatch_task(&mut self, pending: PendingTask) {
        let PendingTask {
            task,
            worker_id,
            cancel_tx,
        } = pending;

        debug!("派发任务到 Worker #{}", worker_id);

        if !self.dispatcher.dispatch(worker_id, task, cancel_tx).await {
            // 派发失败，将 worker 放回空闲队列
            self.task_queue.mark_worker_idle(worker_id);
        }
    }

    /// 检查完成条件并发送通知
    fn check_and_notify_completion(&mut self) -> bool {
        let remaining = self.task_queue.remaining();
        let pending_retries =
            self.retry_scheduler.pending_count() + self.task_queue.ready_retry_count();
        let idle_count = self.task_queue.idle_worker_count();
        let total_workers = self.dispatcher.worker_count();

        !self.completion_tracker.check_and_notify(
            remaining,
            pending_retries,
            idle_count,
            total_workers,
        )
    }
}

/// TaskAllocator Actor 的通信句柄
pub(super) struct TaskAllocatorHandle {
    message_tx: mpsc::Sender<AllocatorMessage>,
    actor_handle: tokio::task::JoinHandle<()>,
}

impl TaskAllocatorHandle {
    /// 注册新的 workers（标记为空闲并自动触发任务分配）
    pub(super) async fn register_new_workers(&self, worker_ids: Vec<u64>) {
        let _ = self
            .message_tx
            .send(AllocatorMessage::RegisterNewWorkers { worker_ids })
            .await;
    }

    /// 关闭 Actor 并等待完成
    pub(super) async fn shutdown_and_wait(self) {
        let _ = self.message_tx.send(AllocatorMessage::Shutdown).await;
        let _ = self.actor_handle.await;
    }
}

// 测试现在位于各子模块中：
// - task_queue::tests
// - retry_scheduler::tests
// - completion_tracker::tests
