//! 下载任务执行器
//!
//! 封装核心的下载协程逻辑，包括：
//! - Worker 主循环（接收任务、处理关闭信号）
//! - Range 下载执行
//! - 下载结果处理（完成、取消、失败）
//! - 分块大小动态调整
//!
//! 这个模块独立于协程池，方便单元测试。

mod retry_scheduler;
pub(super) mod task_allocator;

use task_allocator::{AllocationResult, TaskAllocator};
use crate::utils::{
    chunk_strategy::ChunkStrategy,
    fetch::{FetchRangeResult, RangeFetcher},
    io_traits::HttpClient,
    stats::WorkerStats,
    writer::MmapWriter,
};
use log::{debug, error, info, warn};
use net_bytes::{FormattedValue, SizeStandard};
use ranged_mmap::AllocatedRange;
use smr_swap::SmrSwap;
use std::num::NonZeroU64;
use tokio::sync::mpsc;

/// Worker 执行结果
///
/// 每个 worker 在完成所有任务后通过 oneshot 通道发送此结果
#[derive(Debug)]
pub(crate) enum ExecutorResult {
    /// 所有任务成功完成
    Success {
        /// Worker ID
        worker_id: u64,
    },
    /// 有任务永久失败（达到最大重试次数）
    DownloadFailed {
        /// Worker ID
        worker_id: u64,
        /// 失败的 ranges 及错误信息
        failed_ranges: Vec<(AllocatedRange, String)>,
    },
    /// 写入失败（致命错误）
    WriteFailed {
        /// Worker ID
        worker_id: u64,
        /// 失败的 range
        range: AllocatedRange,
        /// 错误信息
        error: String,
    },
}

/// 下载 Worker 的上下文
///
/// 包含每个 worker 独立的分块策略和下载 URL。
/// 只由主下载协程持有，其他辅助协程不需要访问。
pub(crate) struct DownloadWorkerContext {
    /// 该 worker 的独立分块策略
    pub(crate) chunk_strategy: Box<dyn ChunkStrategy + Send + Sync>,
    /// 任务分配器（封装了 allocator 和重试调度）
    pub(crate) task_allocator: TaskAllocator,
    /// 下载 URL
    pub(crate) url: String,
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
    /// 写入失败（致命错误，需立即终止）
    WriteFailed {
        range: AllocatedRange,
        error: String,
    },
}

/// 下载任务执行器
///
/// 封装核心的下载业务逻辑，可独立于协程池进行测试。
///
/// # 泛型参数
///
/// - `C`: HTTP 客户端类型
#[derive(Clone)]
pub(crate) struct DownloadTaskExecutor<C> {
    /// HTTP 客户端
    client: C,
    /// 共享的文件写入器
    writer: MmapWriter,
    /// 文件大小标准
    size_standard: SizeStandard,
    /// 累计的失败 ranges
    failed_ranges: Vec<(AllocatedRange, String)>,
}

impl<C> DownloadTaskExecutor<C> {
    /// 创建新的下载任务执行器
    pub(crate) fn new(client: C, writer: MmapWriter, size_standard: SizeStandard) -> Self {
        Self {
            client,
            writer,
            size_standard,
            failed_ranges: Vec::new(),
        }
    }
}

impl<C: HttpClient> DownloadTaskExecutor<C> {
    /// Worker 主循环
    ///
    /// 运行下载 Worker 的主事件循环，通过 TaskAllocator 统一获取任务并执行。
    /// 任务分配和重试调度由 TaskAllocator 封装处理。
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker 的 ID
    /// - `context`: Worker 上下文（包含 task_allocator 和 url）
    /// - `stats`: Worker 统计数据
    /// - `result_tx`: 结果发送通道（oneshot，仅在主循环结束时发送一次）
    /// - `shutdown_rx`: 关闭信号接收器
    /// - `cancel_rx`: 取消信号接收器（用于健康检查）
    pub(crate) async fn run_loop(
        mut self,
        worker_id: u64,
        mut context: DownloadWorkerContext,
        mut stats: SmrSwap<WorkerStats>,
        result_tx: tokio::sync::oneshot::Sender<ExecutorResult>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        mut cancel_rx: mpsc::Receiver<()>,
    ) {
        debug!("Worker #{} 主循环启动", worker_id);

        loop {
            tokio::select! {
                biased;
                
                _ = &mut shutdown_rx => {
                    info!("Worker #{} 收到关闭信号，主循环退出", worker_id);
                    break;
                }
                _ = std::future::ready(()) => {
                    // 通过 TaskAllocator 统一获取下一个任务
                    let chunk_size = stats.load().get_current_chunk_size();
                    
                    match context.task_allocator.next_task(chunk_size) {
                        AllocationResult::Task(task) => {
                            let range = task.range().clone();
                            let retry_count = task.retry_count();
                            
                            debug!(
                                "Worker #{} 执行任务 (range {}..{}, retry={})",
                                worker_id, range.start(), range.end(), retry_count
                            );
                            
                            match self.execute_single_task(
                                worker_id,
                                range,
                                retry_count,
                                &mut context,
                                &mut stats,
                                &mut cancel_rx,
                            ).await {
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
                                TaskResult::WriteFailed { range, error } => {
                                    // 写入失败是致命错误，立即终止 worker
                                    let _ = result_tx.send(ExecutorResult::WriteFailed {
                                        worker_id,
                                        range,
                                        error,
                                    });
                                    debug!("Worker #{} 写入失败，主循环退出", worker_id);
                                    return;
                                }
                            }
                        }
                        AllocationResult::WaitForRetry { delay, pending_count } => {
                            // 没有新任务，但有待重试任务
                            debug!(
                                "Worker #{} 无新任务，等待 {:?} 后处理 {} 个待重试任务",
                                worker_id, delay, pending_count
                            );
                            tokio::time::sleep(delay).await;
                            
                            // 等待后，将所有待重试任务提前到当前 task_id
                            context.task_allocator.advance_all_retries();
                        }
                        AllocationResult::Done => {
                            // 没有更多任务
                            debug!("Worker #{} 没有更多任务，退出", worker_id);
                            break;
                        }
                    }
                }
            }
        }

        // 主循环结束后发送最终结果
        let result = if self.failed_ranges.is_empty() {
            ExecutorResult::Success { worker_id }
        } else {
            ExecutorResult::DownloadFailed {
                worker_id,
                failed_ranges: self.failed_ranges,
            }
        };
        let _ = result_tx.send(result);
        debug!("Worker #{} 主循环结束", worker_id);
    }

    /// 执行单个任务
    ///
    /// 返回任务执行结果，不进行重试循环
    async fn execute_single_task(
        &mut self,
        worker_id: u64,
        range: AllocatedRange,
        retry_count: usize,
        context: &mut DownloadWorkerContext,
        stats: &mut SmrSwap<WorkerStats>,
        cancel_rx: &mut mpsc::Receiver<()>,
    ) -> TaskResult {
        let (start, end) = range.as_range_tuple();
        debug!(
            "Worker #{} 执行 Range 任务: {} (range {}..{}, retry {})",
            worker_id, context.url, start, end, retry_count
        );

        stats.update(|s| {
            let mut s = s.clone();
            s.set_active(true);
            s.clear_samples();
            s
        });

        let mut stats_new = stats.get().clone();

        let fetch_result = self.fetch_range(&context.url, &range, &mut stats_new, cancel_rx).await;

        stats_new.set_active(false);
        
        stats.store(stats_new.clone());

        match fetch_result {
            Ok(FetchRangeResult::Complete(data)) => {
                // 下载成功，写入文件
                if let Err(e) = self.writer.write_range(range.clone(), data.as_ref()) {
                    let error_msg = format!("写入失败: {:?}", e);
                    error!("Worker #{} {}", worker_id, error_msg);
                    return TaskResult::WriteFailed { range, error: error_msg };
                }

                // 记录 range 完成
                stats.update(|stats| {
                    let mut stats = stats.clone();
                    stats.record_range_complete();
                    stats
                });

                // 根据当前速度更新分块大小
                self.update_chunk_size(context, stats);

                TaskResult::Success
            }
            Ok(FetchRangeResult::Cancelled { data, bytes_downloaded }) => {
                // 下载被取消（健康检查触发）
                warn!(
                    "Worker #{} Range {}..{} 被取消 (已下载 {} bytes)",
                    worker_id, start, end, bytes_downloaded
                );

                let remaining_range = self.handle_partial_download(
                    worker_id,
                    range,
                    data,
                    bytes_downloaded,
                );

                let new_retry_count = retry_count + 1;
                self.check_retry_or_fail(
                    worker_id,
                    remaining_range,
                    new_retry_count,
                    context,
                )
            }
            Err(e) => {
                // 下载失败
                warn!(
                    "Worker #{} Range {}..{} 下载失败 (重试 {}): {:?}",
                    worker_id, start, end, retry_count, e
                );

                let new_retry_count = retry_count + 1;
                self.check_retry_or_fail(
                    worker_id,
                    range,
                    new_retry_count,
                    context,
                )
            }
        }
    }

    /// 处理部分下载的数据
    fn handle_partial_download(
        &self,
        worker_id: u64,
        range: AllocatedRange,
        data: bytes::Bytes,
        bytes_downloaded: u64,
    ) -> AllocatedRange {
        if bytes_downloaded == 0 {
            return range;
        }

        if let Some(nz_bytes) = NonZeroU64::new(bytes_downloaded) {
            match range.split_at(nz_bytes) {
                Ok((downloaded_range, remaining_range)) => {
                    if let Err(e) = self.writer.write_range(downloaded_range, data.as_ref()) {
                        error!("Worker #{} 写入部分数据失败: {:?}", worker_id, e);
                        return range;
                    }
                    debug!("Worker #{} 成功写入 {} bytes", worker_id, bytes_downloaded);
                    return remaining_range;
                }
                Err(e) => {
                    error!("Worker #{} 无法拆分 range: {}", worker_id, e);
                }
            }
        }
        range
    }

    /// 检查是否需要重试或标记为永久失败
    fn check_retry_or_fail(
        &mut self,
        worker_id: u64,
        range: AllocatedRange,
        retry_count: usize,
        context: &DownloadWorkerContext,
    ) -> TaskResult {
        let max_retries = context.task_allocator.max_retry_count();
        
        if retry_count > max_retries {
            let error_msg = format!(
                "达到最大重试次数 ({}) 后仍然失败",
                max_retries
            );
            error!(
                "Worker #{} Range {}..{} {}",
                worker_id,
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

    /// 执行 Range 下载（支持取消信号触发重试）
    async fn fetch_range(
        &self,
        url: &str,
        range: &AllocatedRange,
        stats: &mut WorkerStats,
        cancel_rx: &mut mpsc::Receiver<()>,
    ) -> std::result::Result<FetchRangeResult, crate::utils::fetch::FetchError> {
        use crate::utils::fetch::FetchRange;

        let fetch_range =
            FetchRange::from_allocated_range(range).expect("AllocatedRange 应该总是有效的");
        
        RangeFetcher::new(&self.client, url, fetch_range, stats)
            .fetch_with_cancel(cancel_rx)
            .await
    }


    /// 根据当前速度更新分块大小
    fn update_chunk_size(&self, context: &DownloadWorkerContext, stats: &mut SmrSwap<WorkerStats>) {
        let stats_guard = stats.load();
        let instant_speed = stats_guard.get_instant_speed();
        let window_avg_speed = stats_guard.get_window_avg_speed();

        if let (Some(instant_speed), Some(window_avg_speed)) = (instant_speed, window_avg_speed) {
            let current_chunk_size = stats_guard.get_current_chunk_size();
            let new_chunk_size = context.chunk_strategy.calculate_chunk_size(
                current_chunk_size,
                instant_speed,
                window_avg_speed,
            );

            drop(stats_guard);

            stats.update(|stats| {
                let mut stats = stats.clone();
                stats.set_current_chunk_size(new_chunk_size);
                stats
            });

            debug!(
                "Updated chunk size: {} -> {} (speed: {}, avg: {})",
                current_chunk_size,
                new_chunk_size,
                FormattedValue::new(instant_speed, self.size_standard),
                FormattedValue::new(window_avg_speed, self.size_standard)
            );
        }
    }
}

// TODO: 重写测试以适配新的 executor 架构
// #[cfg(test)]
// mod tests {}
