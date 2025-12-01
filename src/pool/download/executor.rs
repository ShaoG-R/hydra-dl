//! 下载任务执行器
//!
//! 封装核心的下载协程逻辑，包括：
//! - Worker 主循环（接收任务、处理关闭信号）
//! - Range 下载执行
//! - 下载结果处理（完成、取消、失败）
//! - 分块大小动态调整
//!
//! 这个模块独立于协程池，方便单元测试。

use crate::task::{RangeResult, WorkerTask as RangeTask};
use crate::utils::{
    chunk_strategy::ChunkStrategy,
    fetch::{FetchRangeResult, RangeFetcher},
    io_traits::HttpClient,
    stats::WorkerStats,
    writer::MmapWriter,
};
use lite_sync::oneshot::lite;
use log::{debug, error, info};
use net_bytes::{FormattedValue, SizeStandard};
use smr_swap::SmrSwap;
use tokio::sync::mpsc::{Receiver, Sender};

/// 下载 Worker 的上下文
///
/// 包含每个 worker 独立的分块策略。
/// 只由主下载协程持有，其他辅助协程不需要访问。
pub(crate) struct DownloadWorkerContext {
    /// 该 worker 的独立分块策略
    pub(crate) chunk_strategy: Box<dyn ChunkStrategy + Send + Sync>,
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
}

impl<C> DownloadTaskExecutor<C> {
    /// 创建新的下载任务执行器
    pub(crate) fn new(client: C, writer: MmapWriter, size_standard: SizeStandard) -> Self {
        Self {
            client,
            writer,
            size_standard,
        }
    }
}

impl<C: HttpClient> DownloadTaskExecutor<C> {
    /// Worker 主循环
    ///
    /// 运行下载 Worker 的主事件循环，处理任务接收、执行和结果发送。
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker 的 ID
    /// - `context`: Worker 上下文
    /// - `stats`: Worker 统计数据
    /// - `task_rx`: 任务接收通道
    /// - `result_tx`: 结果发送通道
    /// - `shutdown_rx`: 关闭信号接收器
    pub(crate) async fn run_loop(
        mut self,
        worker_id: u64,
        mut context: DownloadWorkerContext,
        mut stats: SmrSwap<WorkerStats>,
        mut task_rx: Receiver<RangeTask>,
        result_tx: Sender<RangeResult>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        debug!("Worker #{} 主循环启动", worker_id);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Worker #{} 收到关闭信号，主循环退出", worker_id);
                    break;
                }
                Some(task) = task_rx.recv() => {
                    let result = self.execute_task(worker_id, task, &mut context, &mut stats).await;
                    if let Err(e) = result_tx.send(result).await {
                        error!("Worker #{} 发送结果失败: {:?}", worker_id, e);
                    }
                }
                else => {
                    debug!("Worker #{} 任务通道关闭，退出", worker_id);
                    break;
                }
            }
        }
    }

    /// 核心业务逻辑：执行单个 Range 下载任务
    async fn execute_task(
        &mut self,
        worker_id: u64,
        task: RangeTask,
        context: &mut DownloadWorkerContext,
        stats: &mut SmrSwap<WorkerStats>,
    ) -> RangeResult {
        stats.update(|s| {
            let mut s = s.clone();
            s.set_active(true);
            s.clear_samples();
            s
        });

        let RangeTask::Range {
            url,
            range,
            retry_count,
            cancel_rx,
        } = task;

        let (start, end) = range.as_range_tuple();
        debug!(
            "Worker #{} 执行 Range 任务: {} (range {}..{}, retry {})",
            worker_id, url, start, end, retry_count
        );

        let fetch_result = self.fetch_range(&url, &range, stats, cancel_rx).await;

        let result = match fetch_result {
            Ok(FetchRangeResult::Complete(data)) => {
                self.handle_download_complete(worker_id, range, data, context, stats)
            }
            Ok(FetchRangeResult::Cancelled {
                data,
                bytes_downloaded,
            }) => self.handle_download_cancelled(
                worker_id,
                range,
                data,
                bytes_downloaded,
                retry_count,
            ),
            Err(e) => self.handle_download_failed(worker_id, range, retry_count, e),
        };

        stats.update(|s| {
            let mut s = s.clone();
            s.set_active(false);
            s
        });

        result
    }

    /// 执行 Range 下载
    async fn fetch_range(
        &self,
        url: &str,
        range: &ranged_mmap::AllocatedRange,
        stats: &mut SmrSwap<WorkerStats>,
        cancel_rx: lite::Receiver<()>,
    ) -> std::result::Result<FetchRangeResult, crate::utils::fetch::FetchError> {
        use crate::utils::fetch::FetchRange;

        let fetch_range =
            FetchRange::from_allocated_range(range).expect("AllocatedRange 应该总是有效的");
        RangeFetcher::new(&self.client, url, fetch_range, stats)
            .fetch_with_cancel(cancel_rx)
            .await
    }

    /// 处理下载完成的情况
    fn handle_download_complete(
        &self,
        worker_id: u64,
        range: ranged_mmap::AllocatedRange,
        data: bytes::Bytes,
        context: &DownloadWorkerContext,
        stats: &mut SmrSwap<WorkerStats>,
    ) -> RangeResult {
        // 写入文件
        if let Err(e) = self.writer.write_range(range, data.as_ref()) {
            let error_msg = format!("写入失败: {:?}", e);
            error!("Worker #{} {}", worker_id, error_msg);
            return RangeResult::WriteFailed {
                worker_id,
                range,
                error: error_msg,
            };
        }

        // 记录 range 完成
        stats.update(|stats| {
            let mut stats = stats.clone();
            stats.record_range_complete();
            stats
        });

        // 根据当前速度更新分块大小
        self.update_chunk_size(context, stats);

        RangeResult::Complete { worker_id }
    }

    /// 处理下载被取消的情况
    fn handle_download_cancelled(
        &self,
        worker_id: u64,
        range: ranged_mmap::AllocatedRange,
        data: bytes::Bytes,
        bytes_downloaded: u64,
        retry_count: usize,
    ) -> RangeResult {
        let (start, end) = range.as_range_tuple();

        // 没有下载任何数据，返回原始 range 重试
        if bytes_downloaded == 0 {
            let error_msg = format!("下载被取消，重试整个 range: {}..{}", start, end);
            debug!("Worker #{} {}", worker_id, error_msg);
            return RangeResult::DownloadFailed {
                worker_id,
                range,
                error: error_msg,
                retry_count,
            };
        }

        // 有部分数据下载，尝试保存并返回剩余部分
        use std::num::NonZeroU64;
        let bytes_downloaded = NonZeroU64::new(bytes_downloaded).unwrap();

        match range.split_at(bytes_downloaded) {
            Ok((downloaded_range, remaining_range)) => {
                // 尝试写入已下载的部分（忽略写入错误，继续重试剩余部分）
                if let Err(e) = self.writer.write_range(downloaded_range, data.as_ref()) {
                    error!("Worker #{} 写入已下载的部分数据失败: {:?}", worker_id, e);
                } else {
                    debug!(
                        "Worker #{} 成功写入已下载的 {} bytes",
                        worker_id, bytes_downloaded
                    );
                }

                // 返回剩余的 range 用于重试
                let error_msg = format!(
                    "下载被取消，剩余 range: {}..{}",
                    remaining_range.start(),
                    remaining_range.end()
                );
                debug!("Worker #{} {}", worker_id, error_msg);
                RangeResult::DownloadFailed {
                    worker_id,
                    range: remaining_range,
                    error: error_msg,
                    retry_count,
                }
            }
            Err(e) => {
                error!(
                    "Worker #{} 无法拆分 range: bytes_downloaded={}, range={}..{}",
                    worker_id, bytes_downloaded, start, end
                );
                RangeResult::DownloadFailed {
                    worker_id,
                    range,
                    error: e.to_string(),
                    retry_count,
                }
            }
        }
    }

    /// 处理下载失败的情况
    fn handle_download_failed(
        &self,
        worker_id: u64,
        range: ranged_mmap::AllocatedRange,
        retry_count: usize,
        error: crate::utils::fetch::FetchError,
    ) -> RangeResult {
        let (start, end) = range.as_range_tuple();
        let error_msg = format!("下载失败: {:?}", error);
        error!(
            "Worker #{} Range {}..{} {}",
            worker_id, start, end, error_msg
        );

        RangeResult::DownloadFailed {
            worker_id,
            range,
            error: error_msg,
            retry_count,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::chunk_strategy::SpeedBasedChunkStrategy;
    use crate::utils::io_traits::mock::MockHttpClient;
    use bytes::Bytes;
    use reqwest::{header::HeaderMap, StatusCode};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_executor_success() {
        use std::num::NonZeroU64;

        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJ"; // 20 bytes

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, mut allocator) =
            MmapWriter::new(save_path, NonZeroU64::new(test_data.len() as u64).unwrap()).unwrap();

        let range = allocator
            .allocate(NonZeroU64::new(test_data.len() as u64).unwrap())
            .unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert("content-range", format!("bytes 0-19/20").parse().unwrap());
        client.set_range_response(
            test_url,
            0,
            19,
            StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from_static(test_data),
        );

        // 创建执行器
        let config = Arc::new(crate::config::DownloadConfig::default());
        let mut executor = DownloadTaskExecutor::new(client, writer, SizeStandard::SI);

        // 创建上下文和统计
        let mut stats = SmrSwap::new(WorkerStats::default());
        let chunk_strategy = Box::new(SpeedBasedChunkStrategy::from_config(&config))
            as Box<dyn ChunkStrategy + Send + Sync>;
        let mut context = DownloadWorkerContext { chunk_strategy };

        // 执行任务
        let (_cancel_tx, cancel_rx) = lite::channel();
        let task = RangeTask::Range {
            url: test_url.to_string(),
            range,
            retry_count: 0,
            cancel_rx,
        };

        let result = executor
            .execute_task(0, task, &mut context, &mut stats)
            .await;

        // 验证结果
        match result {
            RangeResult::Complete { worker_id } => {
                assert_eq!(worker_id, 0);
            }
            RangeResult::DownloadFailed { error, .. } | RangeResult::WriteFailed { error, .. } => {
                panic!("任务不应该失败: {}", error);
            }
        }

        // 验证统计
        let (total_bytes, _, ranges) = stats.load().get_summary();
        assert_eq!(total_bytes, test_data.len() as u64);
        assert_eq!(ranges, 1);
    }

    #[tokio::test]
    async fn test_executor_failure() {
        use std::num::NonZeroU64;

        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, mut allocator) =
            MmapWriter::new(save_path, NonZeroU64::new(100).unwrap()).unwrap();

        let range = allocator.allocate(NonZeroU64::new(10).unwrap()).unwrap();

        // 设置失败的 Range 响应
        client.set_range_response(
            test_url,
            0,
            9,
            StatusCode::INTERNAL_SERVER_ERROR,
            HeaderMap::new(),
            Bytes::new(),
        );

        // 创建执行器
        let config = Arc::new(crate::config::DownloadConfig::default());
        let mut executor = DownloadTaskExecutor::new(client, writer, SizeStandard::SI);

        let mut stats = SmrSwap::new(WorkerStats::default());
        let chunk_strategy = Box::new(SpeedBasedChunkStrategy::from_config(&config))
            as Box<dyn ChunkStrategy + Send + Sync>;
        let mut context = DownloadWorkerContext { chunk_strategy };

        let (_cancel_tx, cancel_rx) = lite::channel();
        let task = RangeTask::Range {
            url: test_url.to_string(),
            range,
            retry_count: 0,
            cancel_rx,
        };

        let result = executor
            .execute_task(0, task, &mut context, &mut stats)
            .await;

        // 验证结果
        match result {
            RangeResult::DownloadFailed {
                worker_id, error, ..
            } => {
                assert_eq!(worker_id, 0);
                assert!(error.contains("下载失败"));
            }
            RangeResult::WriteFailed { .. } => {
                panic!("应该是下载失败，而不是写入失败");
            }
            RangeResult::Complete { .. } => {
                panic!("任务应该失败");
            }
        }
    }
}
