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
/// chunk_size 计算已移至 StatsUpdater，此处仅负责采样和发送。
pub(crate) struct DownloadChunkRecorder<'a> {
    stats: &'a mut WorkerStatsRecording,
    stats_handle: &'a StatsUpdaterHandle,
    state: &'a mut TaskInternalState<Active>,
    cumulative_stats: &'a mut ExecutorCumulativeStats,
}

impl ChunkRecorder for DownloadChunkRecorder<'_> {
    fn record_chunk(&mut self, bytes: u64) {
        // 采样成功时返回 WorkerStatsActive
        if let Some(active) = self.stats.record_chunk(bytes) {
            // 更新总下载字节数
            self.cumulative_stats.total_downloaded_bytes += bytes;

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
}

/// 下载 Worker 的上下文
///
/// 包含任务分配器和下载 URL。
/// 分块策略已移至 StatsUpdater，通过 SmrSwap 维护 chunk_size。
pub(crate) struct DownloadWorkerContext {
    /// 任务分配器（封装了 allocator 和重试调度）
    pub(crate) task_allocator: TaskAllocator,
    /// 下载 URL
    pub(crate) url: String,
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

/// Executor 运行时状态
///
/// 封装 `execute_single_task` 所需的运行时引用
struct ExecutorRuntime<'a> {
    context: &'a mut DownloadWorkerContext,
    stats: &'a mut WorkerStatsRecording,
    stats_handle: &'a StatsUpdaterHandle,
    file_writer: &'a FileWriter,
}

/// 单个任务的执行结果
enum TaskResult {
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

/// 主循环 select 结果
enum LoopAction {
    /// 收到关闭信号
    Shutdown,
    /// 写入失败
    WriteFailed(file_writer::WriteFailure),
    /// 任务执行完成
    TaskCompleted(TaskResult),
    /// 等待重试完成
    RetryWaitCompleted,
    /// 没有更多任务
    Done,
}

/// 内部事件循环结果
enum LoopEvent<T> {
    Shutdown,
    WriteFailed(file_writer::WriteFailure),
    WriteReceiverClosed,
    Inner(T),
}

/// 等待事件（Shutdown, WriteFailure, 或 Primary Future）
///
/// 统一处理 select 逻辑，避免代码重复
async fn wait_for_event<F, T>(
    shutdown_rx: &mut oneshot::Receiver<()>,
    write_failure_rx: &mut Option<oneshot::Receiver<file_writer::WriteFailure>>,
    future: F,
) -> LoopEvent<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::select! {
        biased;
        _ = shutdown_rx => LoopEvent::Shutdown,
        failure = async {
            if let Some(rx) = write_failure_rx {
                rx.await
            } else {
                std::future::pending().await
            }
        } => {
            match failure {
                Ok(f) => LoopEvent::WriteFailed(f),
                Err(_) => {
                    // 通道已关闭，清除 Option 以免再次 polling
                    *write_failure_rx = None;
                    LoopEvent::WriteReceiverClosed
                }
            }
        }
        res = future => LoopEvent::Inner(res),
    }
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
    /// 累计的失败 ranges
    failed_ranges: Vec<(AllocatedRange, String)>,
    /// 累计统计
    cumulative_stats: ExecutorCumulativeStats,
}

impl<C> DownloadTaskExecutor<C> {
    /// 创建新的下载任务执行器
    pub(crate) fn new(worker_id: WorkerId, client: C, writer: MmapWriter) -> Self {
        Self {
            worker_id,
            client,
            writer,
            failed_ranges: Vec::new(),
            cumulative_stats: ExecutorCumulativeStats::new(),
        }
    }
}

impl<C: HttpClient> DownloadTaskExecutor<C> {
    /// Worker 主循环
    ///
    /// 运行下载 Worker 的主事件循环，通过 TaskAllocator 统一获取任务并执行。
    /// 任务分配和重试调度由 TaskAllocator 封装处理。
    pub(crate) async fn run_loop(mut self, input: ExecutorInput) {
        let worker_id = self.worker_id;
        debug!("Worker {} 主循环启动", worker_id);

        let ExecutorInput {
            mut context,
            mut stats,
            stats_handle,
            result_tx,
            mut shutdown_rx,
            mut cancel_rx,
        } = input;

        // 在 Executor 内部创建 FileWriter
        let (file_writer, rx) =
            FileWriter::new(worker_id, self.writer.clone(), stats_handle.clone(), None);
        // 使用 Option 包装以处理通道关闭的情况
        let mut write_failure_rx = Some(rx);

        loop {
            // 通过 TaskAllocator 统一获取下一个任务
            // chunk_size 从 StatsUpdaterHandle 读取（由 SmrSwap 维护）
            let chunk_size = stats_handle.read_chunk_size();

            let action = match context.task_allocator.next_task(chunk_size) {
                AllocationResult::Task(task) => {
                    let range = *task.range();
                    let retry_count = task.retry_count();

                    debug!(
                        "Worker {} 执行任务 (range {}..{}, retry={})",
                        worker_id,
                        range.start(),
                        range.end(),
                        retry_count
                    );

                    // 为当前任务创建新的 cancel oneshot channel
                    let task_id = context.task_allocator.current_task_id();
                    let task_cancel_rx = cancel_rx.reset(task_id);

                    // 创建运行时上下文
                    let mut runtime = ExecutorRuntime {
                        context: &mut context,
                        stats: &mut stats,
                        stats_handle: &stats_handle,
                        file_writer: &file_writer,
                    };

                    let task_future = self.execute_single_task(task, &mut runtime, task_cancel_rx);

                    match wait_for_event(&mut shutdown_rx, &mut write_failure_rx, task_future).await
                    {
                        LoopEvent::Shutdown => LoopAction::Shutdown,
                        LoopEvent::WriteFailed(f) => LoopAction::WriteFailed(f),
                        LoopEvent::WriteReceiverClosed => {
                            error!("Worker {} 写入失败通道异常关闭", worker_id);
                            continue;
                        }
                        LoopEvent::Inner(result) => LoopAction::TaskCompleted(result),
                    }
                }
                AllocationResult::WaitForRetry {
                    delay,
                    pending_count,
                } => {
                    debug!(
                        "Worker {} 无新任务，等待 {:?} 后处理 {} 个待重试任务",
                        worker_id, delay, pending_count
                    );

                    let sleep_future = tokio::time::sleep(delay);

                    match wait_for_event(&mut shutdown_rx, &mut write_failure_rx, sleep_future)
                        .await
                    {
                        LoopEvent::Shutdown => LoopAction::Shutdown,
                        LoopEvent::WriteFailed(f) => LoopAction::WriteFailed(f),
                        LoopEvent::WriteReceiverClosed => {
                            error!("Worker {} 写入失败通道异常关闭", worker_id);
                            continue;
                        }
                        LoopEvent::Inner(_) => LoopAction::RetryWaitCompleted,
                    }
                }
                AllocationResult::Done => LoopAction::Done,
            };

            // 统一处理 action
            match action {
                LoopAction::Shutdown => {
                    info!("Worker {} 收到关闭信号，退出", worker_id);
                    file_writer.shutdown_and_wait().await;
                    break;
                }
                LoopAction::WriteFailed(failure) => {
                    file_writer.shutdown_and_wait().await;
                    let _ = result_tx.send(ExecutorResult::WriteFailed {
                        worker_id,
                        range: failure.range,
                        error: failure.error,
                    });
                    stats_handle.send_executor_shutdown();
                    debug!("Worker {} 收到写入失败信号，主循环退出", worker_id);
                    return;
                }
                LoopAction::TaskCompleted(task_result) => match task_result {
                    TaskResult::Success => {
                        context.task_allocator.advance_task_id();
                    }
                    TaskResult::NeedRetry { range, retry_count } => {
                        context.task_allocator.schedule_retry(range, retry_count);
                        context.task_allocator.advance_task_id();
                    }
                    TaskResult::PermanentFailure => {
                        context.task_allocator.advance_task_id();
                    }
                },
                LoopAction::RetryWaitCompleted => {
                    context.task_allocator.advance_all_retries();
                }
                LoopAction::Done => {
                    debug!("Worker {} 没有更多任务，退出", worker_id);
                    // 等待所有写入请求处理完后再退出
                    file_writer.drain_and_wait().await;
                    break;
                }
            }
        }

        // Executor 关闭，通知聚合器移除统计
        stats_handle.send_executor_shutdown();

        // 主循环结束后发送最终结果
        let result = if self.failed_ranges.is_empty() {
            ExecutorResult::Success { worker_id }
        } else {
            ExecutorResult::DownloadFailed {
                worker_id,
                failed_ranges: self.failed_ranges,
            }
        };

        if let Err(e) = result_tx.send(result) {
            error!("Worker {} 发送结果失败: {:?}", worker_id, e);
        }

        debug!("Worker {} 主循环结束", worker_id);
    }

    /// 执行单个任务
    ///
    /// 返回任务执行结果，不进行重试循环
    async fn execute_single_task(
        &mut self,
        task: AllocatedTask,
        runtime: &mut ExecutorRuntime<'_>,
        cancel_rx: oneshot::Receiver<()>,
    ) -> TaskResult {
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
            self.worker_id, runtime.context.url, start, end, retry_count
        );

        runtime.stats.clear_samples();
        let mut active_state = pending_state.transition_to_started();
        runtime.stats_handle.send_state_update(
            active_state.as_task_state(),
            self.cumulative_stats.total_downloaded_bytes,
            self.cumulative_stats.total_consumed_time,
        );

        // 创建 Chunk 记录器
        let mut recorder = DownloadChunkRecorder {
            stats: runtime.stats,
            stats_handle: runtime.stats_handle,
            state: &mut active_state,
            cumulative_stats: &mut self.cumulative_stats,
        };

        // Inline fetch_range logic to avoid borrowing self immutably while state is borrowed mutably
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

        let fetch_result = RangeFetcher::new(
            &self.client,
            &runtime.context.url,
            fetch_range,
            &mut recorder,
        )
        .fetch(cancel_rx, ExecutorFetchHandler)
        .await;

        // 任务结束，发送信号到辅助协程
        let state = active_state.transition_to_ended();

        self.cumulative_stats.total_consumed_time += state.consumed_time();

        runtime.stats_handle.send_state_update(
            state.into_task_state(),
            self.cumulative_stats.total_downloaded_bytes,
            self.cumulative_stats.total_consumed_time,
        );

        self.process_fetch_result(
            fetch_result,
            range,
            retry_count,
            runtime.file_writer,
            runtime.context,
        )
        .await
    }

    /// 处理 Fetch 结果
    async fn process_fetch_result(
        &mut self,
        result: Result<FetchRangeResult, crate::utils::fetch::FetchError>,
        range: AllocatedRange,
        retry_count: usize,
        file_writer: &FileWriter,
        context: &DownloadWorkerContext,
    ) -> TaskResult {
        let (start, end) = range.as_range_tuple();

        match result {
            Ok(FetchRangeResult::Complete(data)) => {
                // 下载成功，发送写入请求到 Writer 协程
                // 写入失败由 Writer 协程通过 write_failure_rx 通知
                if let Err(e) = file_writer.write(range, data).await {
                    // 发送失败说明 Writer 协程已关闭，直接返回 Success
                    // 实际的错误会通过 write_failure_rx 通知
                    warn!("Worker {} 发送写入请求失败: {:?}", self.worker_id, e);
                    return TaskResult::Success;
                }

                TaskResult::Success
            }
            Ok(FetchRangeResult::Cancelled {
                data,
                bytes_downloaded,
            }) => {
                // 下载被取消（健康检查触发）
                warn!(
                    "Worker {} Range {}..{} 被取消 (已下载 {} bytes)",
                    self.worker_id, start, end, bytes_downloaded
                );

                let remaining_range = match self
                    .handle_partial_download(range, data, bytes_downloaded, file_writer)
                    .await
                {
                    Some(r) => r,
                    None => return TaskResult::Success, // remaining_range不存在，返回成功
                };

                let new_retry_count = retry_count + 1;
                self.check_retry_or_fail(remaining_range, new_retry_count, context)
            }
            Err(e) => {
                // 下载失败
                warn!(
                    "Worker {} Range {}..{} 下载失败 (重试 {}): {:?}",
                    self.worker_id, start, end, retry_count, e
                );

                let new_retry_count = retry_count + 1;
                self.check_retry_or_fail(range, new_retry_count, context)
            }
        }
    }

    /// 处理部分下载的数据
    async fn handle_partial_download(
        &self,
        range: AllocatedRange,
        data: bytes::Bytes,
        bytes_downloaded: u64,
        file_writer: &FileWriter,
    ) -> Option<AllocatedRange> {
        let (low, remaining) = match range.split_at_align_down(bytes_downloaded) {
            SplitDownResult::Split { low, high } => (Some(low), Some(high)),
            SplitDownResult::High(high) => (None, Some(high)),
            SplitDownResult::OutOfBounds(low) => {
                error!("split_at_align_down 拆分失败");
                (Some(low), None)
            }
        };

        if let Some(low) = low {
            let low_len = low.len() as usize;
            let partial_data = data.slice(0..low_len);
            if let Err(e) = file_writer.write(low, partial_data).await {
                error!("Worker {} 发送部分写入请求失败: {:?}", self.worker_id, e);
                return Some(range);
            }
            debug!(
                "Worker {} 发送部分写入请求 {} bytes",
                self.worker_id,
                low.len()
            );
        }

        remaining
    }

    /// 检查是否需要重试或标记为永久失败
    fn check_retry_or_fail(
        &mut self,
        range: AllocatedRange,
        retry_count: usize,
        context: &DownloadWorkerContext,
    ) -> TaskResult {
        let max_retries = context.task_allocator.max_retry_count();

        if retry_count > max_retries {
            let error_msg = format!("达到最大重试次数 ({}) 后仍然失败", max_retries);
            error!(
                "Worker {} Range {}..{} {}",
                self.worker_id,
                range.start(),
                range.end(),
                error_msg
            );
            self.failed_ranges.push((range, error_msg));
            TaskResult::PermanentFailure
        } else {
            TaskResult::NeedRetry { range, retry_count }
        }
    }
}
