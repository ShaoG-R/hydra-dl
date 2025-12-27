//! 下载任务执行器
//!
//! 封装核心的下载协程逻辑，包括：
//! - Worker 主循环（接收任务、处理关闭信号）
//! - Range 下载执行
//! - 下载结果处理（完成、取消、失败）
//! - 分块大小动态调整
//!
//! 这个模块独立于协程池，方便单元测试。

mod file_writer;
mod retry_scheduler;
pub(super) mod state;
pub(super) mod task_allocator;

use super::stats_updater::StatsUpdaterHandle;
use crate::pool::common::WorkerId;
use crate::utils::cancel_channel::CancelReceiver;
use crate::utils::writer::MmapWriter;
use crate::utils::{
    chunk_strategy::ChunkStrategy,
    fetch::{ChunkRecorder, FetchRangeResult, RangeFetcher},
    io_traits::HttpClient,
    stats::WorkerStatsRecording,
};
pub(crate) use file_writer::FileWriter;
use log::{debug, error, info, warn};
use ranged_mmap::{AllocatedRange, SplitDownResult};
use state::{Active, TaskInternalState};
use std::time::Duration;
use task_allocator::{AllocatedTask, AllocationResult, TaskAllocator};
use tokio::sync::oneshot;

/// Executor 累计统计数据
#[derive(Debug, Clone, Default)]
pub(crate) struct ExecutorCumulativeStats {
    /// 所有任务总下载字节数
    pub total_downloaded_bytes: u64,
    /// 所有任务总耗时
    pub total_consumed_time: Duration,
}

impl ExecutorCumulativeStats {
    pub fn new() -> Self {
        Self {
            total_downloaded_bytes: 0,
            total_consumed_time: Duration::ZERO,
        }
    }
}

/// 下载 Chunk 记录器
///
/// 实现 `ChunkRecorder` trait，封装 `WorkerStatsRecording` 和 `StatsUpdaterHandle`。
/// chunk_size 计算由 Executor 维护，采样成功后根据策略更新。
pub(crate) struct DownloadChunkRecorder<'a> {
    stats: &'a mut WorkerStatsRecording,
    stats_handle: &'a StatsUpdaterHandle,
    state: &'a mut TaskInternalState<Active>,
    cumulative_stats: &'a mut ExecutorCumulativeStats,
    chunk_strategy: &'a mut (dyn ChunkStrategy + Send),
}

impl ChunkRecorder for DownloadChunkRecorder<'_> {
    fn record_chunk(&mut self, bytes: u64) {
        // 采样成功时返回 WorkerStatsActive
        if let Some(active) = self.stats.record_chunk(bytes) {
            // 更新总下载字节数
            self.cumulative_stats.total_downloaded_bytes += bytes;

            // 计算并更新新的 chunk size
            let speed_stats = active.get_speed_stats();
            self.chunk_strategy
                .update_chunk_size(speed_stats.instant_speed, speed_stats.window_avg_speed);

            // 更新状态
            self.state.transition_to_running(active);

            // 发送状态更新
            self.stats_handle.send_state_update(
                self.state.as_task_state(),
                self.cumulative_stats.total_downloaded_bytes,
                self.cumulative_stats.total_consumed_time,
            );
        }
    }
}

/// Worker 执行结果
///
/// 每个 worker 在完成所有任务后通过 oneshot 通道发送此结果
#[derive(Debug)]
pub(crate) enum ExecutorResult {
    /// 所有任务成功完成
    Success {
        /// Worker ID
        worker_id: WorkerId,
    },
    /// 有任务永久失败（达到最大重试次数）
    DownloadFailed {
        /// Worker ID
        worker_id: WorkerId,
        /// 失败的 ranges 及错误信息
        failed_ranges: Vec<(AllocatedRange, String)>,
    },
    /// 写入失败（致命错误）
    WriteFailed {
        /// Worker ID
        worker_id: WorkerId,
        /// 失败的 range
        range: AllocatedRange,
        /// 错误信息
        error: String,
    },
    /// 其他致命错误（如文件写入器异常退出）
    FatalError {
        /// Worker ID
        worker_id: WorkerId,
        /// 错误信息
        error: String,
    },
}

/// 下载 Worker 的上下文
pub(crate) struct DownloadWorkerContext {
    /// 任务分配器（封装了 allocator 和重试调度）
    pub(crate) task_allocator: TaskAllocator,
    /// 下载 URL
    pub(crate) url: String,
    /// 分块策略
    pub(crate) chunk_strategy: Box<dyn ChunkStrategy + Send>,
}

/// Executor 运行时输入
///
/// 封装 `run_loop` 所需的所有输入参数
pub(crate) struct ExecutorInput {
    /// Worker 上下文
    pub context: DownloadWorkerContext,
    /// Worker 统计数据
    pub stats: WorkerStatsRecording,
    /// Stats Updater 句柄
    pub stats_handle: StatsUpdaterHandle,
    /// 结果发送通道
    pub result_tx: oneshot::Sender<ExecutorResult>,
    /// 关闭信号接收器
    pub shutdown_rx: oneshot::Receiver<()>,
    /// 取消信号接收器
    pub cancel_rx: CancelReceiver,
}

/// 正在运行的 Executor
///
/// 持有 Executor 运行时所需的所有可变状态，
/// 通过方法调用来处理状态转换，以及所有权传递。
struct RunningExecutor<'a, C> {
    // 基础组件
    worker_id: WorkerId,
    client: &'a C,

    // 运行时状态
    task_allocator: TaskAllocator,
    url: String,
    stats_recorder: WorkerStatsRecording,
    stats_handle: StatsUpdaterHandle,
    cumulative_stats: ExecutorCumulativeStats,
    chunk_strategy: Box<dyn ChunkStrategy + Send>,

    // 通道与交互
    file_writer: FileWriter,
    write_failure_rx: oneshot::Receiver<file_writer::WriteFailure>,
    shutdown_rx: oneshot::Receiver<()>,
    result_tx: oneshot::Sender<ExecutorResult>,

    // 错误记录
    failed_ranges: Vec<(AllocatedRange, String)>,
}

/// 步骤执行结果
enum StepResult<'a, C> {
    /// 继续执行下一状态
    Continue(RunningExecutor<'a, C>, ExecutorState),
    /// 执行器已终止（内部已处理清理和结果发送）
    Exit,
}

/// 内部事件循环结果
enum LoopEvent<T> {
    Shutdown,
    WriteFailed(file_writer::WriteFailure),
    WriteReceiverClosed,
    Inner(T),
}

/// 等待事件（Shutdown, WriteFailure, 或 Primary Future）
async fn wait_for_event<F, T>(
    shutdown_rx: &mut oneshot::Receiver<()>,
    write_failure_rx: &mut oneshot::Receiver<file_writer::WriteFailure>,
    future: F,
) -> LoopEvent<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::select! {
        biased;
        _ = shutdown_rx => LoopEvent::Shutdown,
        failure = write_failure_rx => {
            match failure {
                Ok(f) => LoopEvent::WriteFailed(f),
                Err(_) => LoopEvent::WriteReceiverClosed,
            }
        }
        res = future => LoopEvent::Inner(res),
    }
}

impl<'a, C: HttpClient> RunningExecutor<'a, C> {
    /// 执行 Idle 状态步骤
    fn step_idle(mut self, cancel_rx: &mut CancelReceiver) -> StepResult<'a, C> {
        // chunk_size 由 Executor 维护
        let chunk_size = self.chunk_strategy.current_size();

        match self.task_allocator.next_task(chunk_size) {
            AllocationResult::Task(task) => {
                debug!(
                    "Worker {} 获取任务 (range {}..{}, retry={})",
                    self.worker_id,
                    task.range().start(),
                    task.range().end(),
                    task.retry_count()
                );
                // 重置 cancel channel 并获取 receiver
                let task_id = self.task_allocator.current_task_id();
                let cancel_token = cancel_rx.reset(task_id);

                StepResult::Continue(
                    self,
                    ExecutorState::Working {
                        task,
                        cancel_rx: cancel_token,
                    },
                )
            }
            AllocationResult::WaitForRetry {
                delay,
                pending_count,
            } => {
                debug!(
                    "Worker {} 无新任务，等待 {:?} 后处理 {} 个待重试任务",
                    self.worker_id, delay, pending_count
                );
                StepResult::Continue(self, ExecutorState::WaitingForRetry { delay })
            }
            AllocationResult::Done => StepResult::Continue(self, ExecutorState::Draining),
        }
    }

    /// 执行 Working 状态步骤
    async fn step_working(
        mut self,
        task: AllocatedTask,
        cancel_rx: oneshot::Receiver<()>,
    ) -> StepResult<'a, C> {
        // 构建任务执行依赖
        let deps = TaskExecutionDeps {
            worker_id: self.worker_id,
            client: self.client,
            url: &self.url,
            stats_recorder: &mut self.stats_recorder,
            stats_handle: &mut self.stats_handle,
            cumulative_stats: &mut self.cumulative_stats,
            file_writer: &self.file_writer,
            failed_ranges: &mut self.failed_ranges,
            task_allocator: &mut self.task_allocator,
            chunk_strategy: &mut *self.chunk_strategy,
        };

        // 创建任务 future
        let task_future = execute_single_task(deps, task, cancel_rx);

        // 使用独立的 wait_for_event 函数
        let event = wait_for_event(
            &mut self.shutdown_rx,
            &mut self.write_failure_rx,
            task_future,
        )
        .await;

        match event {
            LoopEvent::Shutdown => {
                info!("Worker {} 收到关闭信号，退出", self.worker_id);
                self.finalize(false).await;
                StepResult::Exit
            }
            LoopEvent::WriteFailed(f) => {
                self.handle_write_failure_and_exit(Some(f)).await;
                StepResult::Exit
            }
            LoopEvent::WriteReceiverClosed => {
                self.handle_write_failure_and_exit(None).await;
                StepResult::Exit
            }
            LoopEvent::Inner(result) => match result {
                Ok(task_result) => {
                    let is_permanent_failure = matches!(task_result, TaskResult::PermanentFailure);
                    self.task_allocator.complete_task(task_result);

                    if is_permanent_failure {
                        warn!("Worker {} 检测到永久失败任务，准备退出", self.worker_id);
                        self.finalize(false).await;
                        StepResult::Exit
                    } else {
                        StepResult::Continue(self, ExecutorState::Idle)
                    }
                }
                Err((range, error_msg)) => {
                    error!("Worker {} 发生致命错误: {}", self.worker_id, error_msg);
                    let failure = file_writer::WriteFailure {
                        range,
                        error: error_msg,
                    };
                    self.handle_write_failure_and_exit(Some(failure)).await;
                    StepResult::Exit
                }
            },
        }
    }

    /// 执行 WaitingForRetry 状态步骤
    async fn step_waiting(mut self, delay: Duration) -> StepResult<'a, C> {
        let sleep_future = tokio::time::sleep(delay);

        let event = wait_for_event(
            &mut self.shutdown_rx,
            &mut self.write_failure_rx,
            sleep_future,
        )
        .await;

        match event {
            LoopEvent::Shutdown => {
                info!("Worker {} 收到关闭信号，退出", self.worker_id);
                self.finalize(false).await;
                StepResult::Exit
            }
            LoopEvent::WriteFailed(f) => {
                self.handle_write_failure_and_exit(Some(f)).await;
                StepResult::Exit
            }
            LoopEvent::WriteReceiverClosed => {
                self.handle_write_failure_and_exit(None).await;
                StepResult::Exit
            }
            LoopEvent::Inner(_) => {
                // 等待完成，激活所有重试任务
                self.task_allocator.advance_all_retries();
                StepResult::Continue(self, ExecutorState::Idle)
            }
        }
    }

    /// 执行 Draining 状态步骤
    async fn step_draining(self) -> StepResult<'a, C> {
        debug!("Worker {} 没有更多任务，等待写入完成", self.worker_id);
        self.finalize(true).await;
        StepResult::Exit
    }

    /// 处理写入失败并退出
    async fn handle_write_failure_and_exit(self, failure: Option<file_writer::WriteFailure>) {
        self.file_writer.shutdown_and_wait().await;
        self.stats_handle.send_executor_shutdown();

        debug!("Worker {} 收到写入失败信号，主循环退出", self.worker_id);

        if let Some(f) = failure {
            let _ = self.result_tx.send(ExecutorResult::WriteFailed {
                worker_id: self.worker_id,
                range: f.range,
                error: f.error,
            });
        } else {
            error!("Worker {} 写入失败通道异常关闭", self.worker_id);
            let _ = self.result_tx.send(ExecutorResult::FatalError {
                worker_id: self.worker_id,
                error: "写入通道异常关闭，Writer可能已崩溃".to_string(),
            });
        }
    }

    /// 最终结算：关闭 Writer 并发送结果
    ///
    /// drain: 是否等待所有任务写入完成
    async fn finalize(self, drain: bool) {
        if drain {
            self.file_writer.drain_and_wait().await;
        } else {
            self.file_writer.shutdown_and_wait().await;
        }

        self.stats_handle.send_executor_shutdown();

        let result = if self.failed_ranges.is_empty() {
            ExecutorResult::Success {
                worker_id: self.worker_id,
            }
        } else {
            ExecutorResult::DownloadFailed {
                worker_id: self.worker_id,
                failed_ranges: self.failed_ranges,
            }
        };

        if let Err(e) = self.result_tx.send(result) {
            error!("Worker {} 发送结果失败: {:?}", self.worker_id, e);
        }
    }
}

/// 任务执行依赖上下文
struct TaskExecutionDeps<'a, C> {
    worker_id: WorkerId,
    client: &'a C,
    url: &'a str,
    stats_recorder: &'a mut WorkerStatsRecording,
    stats_handle: &'a StatsUpdaterHandle,
    cumulative_stats: &'a mut ExecutorCumulativeStats,
    file_writer: &'a FileWriter,
    failed_ranges: &'a mut Vec<(AllocatedRange, String)>,
    task_allocator: &'a mut TaskAllocator,
    chunk_strategy: &'a mut (dyn ChunkStrategy + Send),
}

/// 执行单个任务逻辑
///
/// 这是一个独立的函数，避免直接借用 Executor 的 self
async fn execute_single_task<C: HttpClient>(
    deps: TaskExecutionDeps<'_, C>,
    task: AllocatedTask,
    cancel_rx: oneshot::Receiver<()>,
) -> Result<TaskResult, (AllocatedRange, String)> {
    let (range, retry_count, pending_state) = match task {
        AllocatedTask::New { range, state } => (range, 0, state),
        AllocatedTask::Retry {
            range,
            retry_count,
            state,
        } => (range, retry_count, state),
    };

    let (start, end) = range.as_range_tuple();
    debug!(
        "Worker {} 执行 Range 任务: {} (range {}..{}, retry {})",
        deps.worker_id, deps.url, start, end, retry_count
    );

    deps.stats_recorder.clear_samples();
    let mut active_state = pending_state.transition_to_started(range.len() as u64);
    deps.stats_handle.send_state_update(
        active_state.as_task_state(),
        deps.cumulative_stats.total_downloaded_bytes,
        deps.cumulative_stats.total_consumed_time,
    );

    // 创建 Chunk 记录器
    let mut recorder = DownloadChunkRecorder {
        stats: deps.stats_recorder,
        stats_handle: deps.stats_handle,
        state: &mut active_state,
        cumulative_stats: deps.cumulative_stats,
        chunk_strategy: deps.chunk_strategy,
    };

    use crate::utils::fetch::FetchRange;
    use crate::utils::fetch::downloader::FetchHandler;

    struct ExecutorFetchHandler;

    impl FetchHandler<Result<(), oneshot::error::RecvError>> for ExecutorFetchHandler {
        type Result = FetchRangeResult;

        fn on_complete(self, data: bytes::Bytes) -> Self::Result {
            FetchRangeResult::Complete(data)
        }

        fn on_cancelled(
            self,
            _output: Result<(), oneshot::error::RecvError>,
            data: bytes::Bytes,
            bytes_downloaded: u64,
        ) -> Self::Result {
            FetchRangeResult::Cancelled {
                bytes_downloaded,
                data,
            }
        }
    }

    let fetch_range =
        FetchRange::from_allocated_range(&range).expect("AllocatedRange 应该总是有效的");

    let fetch_result = RangeFetcher::new(deps.client, deps.url, fetch_range, &mut recorder)
        .fetch(cancel_rx, ExecutorFetchHandler)
        .await;

    // 任务结束，发送信号到辅助协程
    let state = active_state.transition_to_ended();

    deps.cumulative_stats.total_consumed_time += state.consumed_time();

    deps.stats_handle.send_state_update(
        state.into_task_state(),
        deps.cumulative_stats.total_downloaded_bytes,
        deps.cumulative_stats.total_consumed_time,
    );

    match fetch_result {
        Ok(FetchRangeResult::Complete(data)) => {
            if let Err(e) = deps.file_writer.write(range, data).await {
                warn!("Worker {} 发送写入请求失败: {:?}", deps.worker_id, e);
                return Ok(TaskResult::Success);
            }
            Ok(TaskResult::Success)
        }
        Ok(FetchRangeResult::Cancelled {
            data,
            bytes_downloaded,
        }) => {
            warn!(
                "Worker {} Range {}..{} 被取消 (已下载 {} bytes)",
                deps.worker_id, start, end, bytes_downloaded
            );

            let remaining_range = handle_partial_download(
                range,
                data,
                bytes_downloaded,
                deps.file_writer,
                deps.worker_id,
            )
            .await
            .map_err(|e| (range, e))?;

            let remaining_range = match remaining_range {
                Some(r) => r,
                None => return Ok(TaskResult::Success),
            };

            let new_retry_count = retry_count + 1;
            Ok(check_retry_or_fail(
                remaining_range, // Corrected to use remaining_range
                new_retry_count,
                deps.task_allocator,
                deps.failed_ranges,
                deps.worker_id,
            ))
        }
        Err(e) => {
            warn!(
                "Worker {} Range {}..{} 下载失败 (重试 {}): {:?}",
                deps.worker_id, start, end, retry_count, e
            );

            let new_retry_count = retry_count + 1;
            Ok(check_retry_or_fail(
                range,
                new_retry_count,
                deps.task_allocator,
                deps.failed_ranges,
                deps.worker_id,
            ))
        }
    }
}

/// 处理部分下载的数据
async fn handle_partial_download(
    range: AllocatedRange,
    data: bytes::Bytes,
    bytes_downloaded: u64,
    file_writer: &FileWriter,
    worker_id: WorkerId,
) -> Result<Option<AllocatedRange>, String> {
    let (low, remaining) = match range.split_at_align_down(bytes_downloaded) {
        SplitDownResult::Split { low, high } => (Some(low), Some(high)),
        SplitDownResult::High(high) => (None, Some(high)),
        SplitDownResult::OutOfBounds(low) => {
            let msg = format!(
                "split_at_align_down 拆分失败: range={:?}, bytes_downloaded={}",
                low, bytes_downloaded
            );
            error!("{}", msg);
            return Err(msg);
        }
    };

    if let Some(low) = low {
        let low_len = low.len() as usize;
        let partial_data = data.slice(0..low_len);
        if let Err(e) = file_writer.write(low, partial_data).await {
            let msg = format!("发送部分写入请求失败: {:?}", e);
            error!("Worker {} {}", worker_id, msg);
            return Err(msg);
        }
        debug!("Worker {} 发送部分写入请求 {} bytes", worker_id, low.len());
    }

    Ok(remaining)
}

/// 检查是否需要重试或标记为永久失败
fn check_retry_or_fail(
    range: AllocatedRange,
    retry_count: usize,
    task_allocator: &TaskAllocator,
    failed_ranges: &mut Vec<(AllocatedRange, String)>,
    worker_id: WorkerId,
) -> TaskResult {
    let max_retries = task_allocator.max_retry_count();

    if retry_count > max_retries {
        let error_msg = format!("达到最大重试次数 ({}) 后仍然失败", max_retries);
        error!(
            "Worker {} Range {}..{} {}",
            worker_id,
            range.start(),
            range.end(),
            error_msg
        );
        failed_ranges.push((range, error_msg));
        TaskResult::PermanentFailure
    } else {
        TaskResult::NeedRetry { range, retry_count }
    }
}

/// 单个任务的执行结果
pub(crate) enum TaskResult {
    /// 任务成功完成
    Success,
    /// 任务需要重试
    NeedRetry {
        range: AllocatedRange,
        retry_count: usize,
    },
    /// 任务永久失败（达到最大重试次数）
    PermanentFailure,
}

/// 执行器运行状态
enum ExecutorState {
    /// 空闲状态
    ///
    /// 准备向分配器请求下一个任务。
    Idle,

    /// 工作状态
    ///
    /// 正在执行下载任务。包含当前任务信息和取消对应的接收器。
    Working {
        task: AllocatedTask,
        cancel_rx: oneshot::Receiver<()>,
    },

    /// 等待重试状态
    ///
    /// 无新任务，但有待重试的任务未到期。
    WaitingForRetry { delay: Duration },

    /// 正在排空状态
    ///
    /// 所有任务已完成，正在等待文件写入器处理完剩余队列。
    Draining,
}

/// 下载任务执行器
///
/// 封装核心的下载业务逻辑，可独立于协程池进行测试。
///
/// # 泛型参数
///
/// - `C`: HTTP 客户端类型
pub(crate) struct DownloadTaskExecutor<C> {
    /// Worker ID
    worker_id: WorkerId,
    /// HTTP 客户端
    client: C,
    /// 文件写入器
    writer: MmapWriter,
}

impl<C> DownloadTaskExecutor<C> {
    /// 创建新的下载任务执行器
    pub(crate) fn new(worker_id: WorkerId, client: C, writer: MmapWriter) -> Self {
        Self {
            worker_id,
            client,
            writer,
        }
    }
}

impl<C: HttpClient> DownloadTaskExecutor<C> {
    /// Worker 主循环
    pub(crate) async fn run_loop(self, input: ExecutorInput) {
        let worker_id = self.worker_id;
        debug!("Worker {} 主循环启动", worker_id);

        let ExecutorInput {
            context,
            stats,
            stats_handle,
            result_tx,
            shutdown_rx,
            mut cancel_rx,
        } = input;

        // 在 Executor 内部创建 FileWriter
        let (file_writer, write_failure_rx) =
            FileWriter::new(worker_id, self.writer.clone(), stats_handle.clone(), None);

        // 初始化 RunningExecutor
        let mut executor = RunningExecutor {
            worker_id,
            client: &self.client,
            task_allocator: context.task_allocator,
            url: context.url,
            stats_recorder: stats,
            stats_handle: stats_handle.clone(),
            cumulative_stats: ExecutorCumulativeStats::new(),
            file_writer,
            write_failure_rx,
            shutdown_rx,
            result_tx,
            failed_ranges: Vec::new(),
            chunk_strategy: context.chunk_strategy,
        };

        // 初始状态
        let mut state = ExecutorState::Idle;

        loop {
            let result = match state {
                ExecutorState::Idle => executor.step_idle(&mut cancel_rx),
                ExecutorState::Working { task, cancel_rx } => {
                    executor.step_working(task, cancel_rx).await
                }
                ExecutorState::WaitingForRetry { delay } => executor.step_waiting(delay).await,
                ExecutorState::Draining => executor.step_draining().await,
            };

            match result {
                StepResult::Continue(next_executor, next_state) => {
                    executor = next_executor;
                    state = next_state;
                }
                StepResult::Exit => {
                    break;
                }
            }
        }

        debug!("Worker {} 主循环结束", worker_id);
    }
}
