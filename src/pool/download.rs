//! 下载专用 Worker 协程池
//!
//! 基于通用 WorkerPool 实现的下载专用协程池，提供下载任务的并发处理能力。
//!
//! # 核心组件
//!
//! - **DownloadWorkerContext**: 下载 worker 的上下文，包含统计和分块策略
//! - **DownloadWorkerExecutor**: 下载任务执行器，处理 Range 下载和文件写入
//! - **DownloadWorkerPool**: 下载协程池，封装通用 WorkerPool 并提供下载特定方法

use super::common::{WorkerContext, WorkerExecutor, WorkerPool, WorkerResult, WorkerTask};
use crate::task::{RangeResult, WorkerTask as RangeTask};
use crate::utils::chunk_strategy::{ChunkStrategy, SpeedBasedChunkStrategy};
use crate::utils::fetch::{RangeFetcher, FetchRangeResult};
use crate::utils::io_traits::HttpClient;
use crate::utils::stats::TaskStats;
use crate::Result;
use crate::utils::writer::MmapWriter;
use async_trait::async_trait;
use log::{debug, error, info};
use std::sync::Arc;

// ==================== Task 和 Result 实现 ====================

impl WorkerTask for RangeTask {}

impl WorkerResult for RangeResult {}

impl super::common::WorkerStats for crate::utils::stats::WorkerStats {}

// ==================== 下载 Worker 上下文 ====================

/// 下载 Worker 的上下文
///
/// 包含每个 worker 独立的分块策略（统计信息已移至 Stats）
pub(crate) struct DownloadWorkerContext {
    /// 该 worker 的独立分块策略（ChunkStrategy 内部使用原子操作，无需外部锁）
    pub(crate) chunk_strategy: Box<dyn ChunkStrategy>,
}

impl WorkerContext for DownloadWorkerContext {}

// ==================== 下载任务执行器 ====================

/// 下载任务执行器
///
/// 实现 WorkerExecutor trait，定义了如何执行 Range 下载任务
///
/// # 泛型参数
///
/// - `C`: HTTP 客户端类型
#[derive(Clone)]
pub(crate) struct DownloadWorkerExecutor<C> {
    /// HTTP 客户端
    client: C,
    /// 共享的文件写入器
    writer: MmapWriter,
}

impl<C> DownloadWorkerExecutor<C> {
    /// 创建新的下载任务执行器
    ///
    /// # Arguments
    ///
    /// - `client`: HTTP 客户端
    /// - `writer`: 共享的文件写入器
    pub(crate) fn new(client: C, writer: MmapWriter) -> Self {
        Self { client, writer }
    }
}

#[async_trait]
impl<C> WorkerExecutor<RangeTask, RangeResult, DownloadWorkerContext, crate::utils::stats::WorkerStats> for DownloadWorkerExecutor<C>
where
    C: HttpClient,
{
    async fn execute(
        &self,
        worker_id: usize,
        task: RangeTask,
        context: &mut DownloadWorkerContext,
        stats: &crate::utils::stats::WorkerStats,
    ) -> RangeResult {
        match task {
            RangeTask::Range { url, range, retry_count, cancel_rx } => {
                let (start, end) = range.as_range_tuple();
                debug!(
                    "Worker #{} 执行 Range 任务: {} (range {}..{}, retry {})",
                    worker_id,
                    url,
                    start,
                    end,
                    retry_count
                );

                // 下载数据（在下载过程中会实时更新 stats）
                // 将 AllocatedRange 转换为 FetchRange
                use crate::utils::fetch::FetchRange;
                let fetch_range = FetchRange::from_allocated_range(&range)
                    .expect("AllocatedRange 应该总是有效的");
                let fetch_result = RangeFetcher::new(&self.client, &url, fetch_range, stats).fetch_with_cancel(cancel_rx).await;

                match fetch_result {
                    Ok(FetchRangeResult::Complete(data)) => {
                        // 下载完成，直接写入文件
                        match self.writer.write_range(range, data.as_ref()) {
                            Ok(_) => {
                                // 记录 range 完成
                                stats.record_range_complete();
                                
                                // 根据当前速度更新分块大小
                                let (instant_speed, valid) = stats.get_instant_speed();
                                let avg_speed = stats.get_speed();
                                if valid && instant_speed > 0.0 {
                                    let current_chunk_size = stats.get_current_chunk_size();
                                    let new_chunk_size = context.chunk_strategy.calculate_chunk_size(current_chunk_size, instant_speed, avg_speed);
                                    stats.set_current_chunk_size(new_chunk_size);
                                }

                                // 返回完成结果
                                RangeResult::Complete { worker_id }
                            }
                            Err(e) => {
                                // 写入失败
                                let error_msg = format!("写入失败: {:?}", e);
                                error!("Worker #{} {}", worker_id, error_msg);
                                RangeResult::WriteFailed {
                                    worker_id,
                                    range,
                                    error: error_msg,
                                }
                            }
                        }
                    }
                    Ok(FetchRangeResult::Cancelled { data, bytes_downloaded }) => {
                        // 下载被取消，先写入已下载的部分数据（如果有）
                        match bytes_downloaded {
                            0 => {
                                // 没有下载任何数据，返回原始 range 重试
                                let error_msg = format!("下载被取消，重试整个 range: {}..{}", start, end);
                                debug!("Worker #{} {}", worker_id, error_msg);
                                RangeResult::DownloadFailed {
                                    worker_id,
                                    range,
                                    error: error_msg,
                                    retry_count,
                                }
                            }
                            _ => {
                                // 使用 split_at 拆分 range，得到已下载部分和剩余部分
                                use std::num::NonZeroU64;
                                let bytes_downloaded = NonZeroU64::new(bytes_downloaded).unwrap();
                                match range.split_at(bytes_downloaded) {
                                    Ok((downloaded_range, remaining_range)) => {
                                        // 写入已下载的部分
                                        match self.writer.write_range(downloaded_range, data.as_ref()) {
                                            Ok(_) => {
                                                // 写入成功
                                                debug!("Worker #{} 成功写入已下载的 {} bytes", worker_id, bytes_downloaded);
                                            }
                                            Err(e) => {
                                                // 写入失败
                                                error!("Worker #{} 写入已下载的部分数据失败: {:?}", worker_id, e);
                                            }
                                        }
                                        
                                        // 返回剩余的 range 用于重试
                                        let error_msg = format!("下载被取消，剩余 range: {}..{}", remaining_range.start(), remaining_range.end());
                                        debug!("Worker #{} {}", worker_id, error_msg);
                                        return RangeResult::DownloadFailed {
                                            worker_id,
                                            range: remaining_range,
                                            error: error_msg,
                                            retry_count,
                                        };
                                    }
                                    Err(e) => {
                                        // pos >= end
                                        error!("Worker #{} 无法拆分 range: bytes_downloaded={}, range={}..{}", 
                                            worker_id, bytes_downloaded, start, end);
                                        RangeResult::DownloadFailed {
                                            worker_id,
                                            range,
                                            error: e.to_string(),
                                            retry_count,
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // 下载失败
                        let error_msg = format!("下载失败: {:?}", e);
                        error!(
                            "Worker #{} Range {}..{} {}",
                            worker_id,
                            start,
                            end,
                            error_msg
                        );
                        RangeResult::DownloadFailed {
                            worker_id,
                            range,
                            error: error_msg,
                            retry_count,
                        }
                    }
                }
            }
        }
    }
}

// ==================== 下载 Worker 协程池 ====================

/// 下载 Worker 协程池
///
/// 封装通用 WorkerPool，提供下载特定的便捷方法
pub(crate) struct DownloadWorkerPool<C: HttpClient> {
    /// 底层通用协程池
    pool: WorkerPool<RangeTask, RangeResult, DownloadWorkerContext, crate::utils::stats::WorkerStats, DownloadWorkerExecutor<C>>,
    /// 全局统计管理器（聚合所有 worker 的数据）
    global_stats: TaskStats,
    /// 下载配置（用于创建新 worker 的分块策略）
    config: Arc<crate::config::DownloadConfig>,
}

impl<C> DownloadWorkerPool<C>
where
    C: HttpClient,
{
    /// 创建新的下载协程池
    ///
    /// # Arguments
    ///
    /// - `client`: HTTP客户端（将被克隆给每个worker）
    /// - `initial_worker_count`: 初始 worker 协程数量
    /// - `writer`: 共享的 RangeWriter，所有 worker 将直接写入此文件
    /// - `config`: 下载配置，用于创建分块策略和设置速度窗口
    ///
    /// # Returns
    ///
    /// 新创建的 DownloadWorkerPool
    pub(crate) fn new(
        client: C,
        initial_worker_count: usize,
        writer: MmapWriter,
        config: Arc<crate::config::DownloadConfig>,
    ) -> Result<Self> {
        // 创建全局统计管理器（使用配置的速度配置）
        let global_stats = TaskStats::from_config(config.speed());

        // 创建执行器（直接 move writer，避免 Arc 克隆）
        let executor = DownloadWorkerExecutor::new(client, writer);

        // 为每个 worker 创建独立的上下文和统计
        let contexts_with_stats: Vec<(DownloadWorkerContext, Arc<crate::utils::stats::WorkerStats>)> = (0..initial_worker_count)
            .map(|_| {
                // 通过 parent 创建 child stats
                let worker_stats = Arc::new(global_stats.create_child());
                // 设置初始分块大小
                worker_stats.set_current_chunk_size(config.chunk().initial_size());
                // 创建独立的分块策略（每个 worker 独立一份，策略是无状态的）
                let chunk_strategy = 
                    Box::new(SpeedBasedChunkStrategy::from_config(&config)) as Box<dyn ChunkStrategy + Send + Sync>;

                let context = DownloadWorkerContext {
                    chunk_strategy,
                };
                (context, worker_stats)
            })
            .collect();

        // 创建通用协程池
        let pool = WorkerPool::new(executor, contexts_with_stats)?;

        info!("创建下载协程池，{} 个初始 workers", initial_worker_count);

        Ok(Self {
            pool,
            global_stats,
            config,
        })
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `count`: 要添加的 worker 数量
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，失败时返回错误信息
    pub(crate) async fn add_workers(&mut self, count: usize) -> Result<()> {
        // 创建新的 worker 上下文和统计
        let contexts_with_stats: Vec<(DownloadWorkerContext, Arc<crate::utils::stats::WorkerStats>)> = (0..count)
            .map(|_| {
                let worker_stats = Arc::new(self.global_stats.create_child());
                // 设置初始分块大小
                worker_stats.set_current_chunk_size(self.config.chunk().initial_size());
                // 创建独立的分块策略（策略内部使用原子操作，无需外部锁）
                let chunk_strategy = 
                    Box::new(SpeedBasedChunkStrategy::from_config(&self.config)) as Box<dyn ChunkStrategy + Send + Sync>;

                let context = DownloadWorkerContext {
                    chunk_strategy,
                };
                (context, worker_stats)
            })
            .collect();

        // 添加新 workers（使用现有的执行器）
        self.pool.add_workers(contexts_with_stats).await
    }

    /// 获取当前活跃 worker 总数
    pub(crate) fn worker_count(&self) -> usize {
        self.pool.worker_count()
    }

    /// 提交任务给指定的 worker
    pub(crate) async fn send_task(&self, task: RangeTask, worker_id: usize) -> Result<()> {
        self.pool.send_task(task, worker_id).await
    }

    /// 获取结果接收器的可变引用
    pub(crate) fn result_receiver(&mut self) -> &mut tokio::sync::mpsc::Receiver<RangeResult> {
        self.pool.result_receiver()
    }

    /// 获取指定 worker 的统计信息
    #[allow(dead_code)]
    pub(crate) fn worker_stats(&self, worker_id: usize) -> Option<Arc<crate::utils::stats::WorkerStats>> {
        self.pool.worker_stats(worker_id)
    }

    /// 获取所有 worker 的聚合统计（O(1)，无需遍历）
    pub(crate) fn get_total_stats(&self) -> (u64, f64, usize) {
        self.global_stats.get_summary()
    }

    /// 获取所有 worker 的总体下载速度（平均速度，O(1)）
    pub(crate) fn get_total_speed(&self) -> f64 {
        self.global_stats.get_speed()
    }

    /// 获取所有 worker 的总体实时速度（O(1)，无需遍历）
    ///
    /// # Returns
    ///
    /// `(实时速度 bytes/s, 是否有效)`
    pub(crate) fn get_total_instant_speed(&self) -> (f64, bool) {
        self.global_stats.get_instant_speed()
    }

    /// 获取所有 worker 的总体窗口平均速度（O(1)，无需遍历）
    ///
    /// # Returns
    ///
    /// `(窗口平均速度 bytes/s, 是否有效)`
    pub(crate) fn get_total_window_avg_speed(&self) -> (f64, bool) {
        self.global_stats.get_window_avg_speed()
    }

    /// 获取指定 worker 的当前分块大小
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    ///
    /// # Returns
    ///
    /// 当前分块大小 (bytes)，如果 worker 不存在返回默认初始分块大小
    #[inline]
    pub(crate) fn get_worker_chunk_size(&self, worker_id: usize) -> u64 {
        self.pool.worker_stats(worker_id)
            .map(|stats| stats.get_current_chunk_size())
            .unwrap_or(self.config.chunk().initial_size())
    }

    /// 优雅关闭所有 workers
    ///
    /// 发送关闭信号到所有活跃的 worker，让它们停止接收新任务并自动退出清理
    /// 
    /// # Note
    ///
    /// 此方法不会等待 workers 退出，workers 会异步自动清理
    /// 使用 `wait_for_shutdown()` 方法等待所有 workers 完成清理
    pub(crate) fn shutdown(&mut self) {
        self.pool.shutdown();
    }
    
    /// 等待所有 workers 完成清理
    ///
    /// 此方法会阻塞直到所有运行中的 workers 都完成了自动清理
    pub(crate) async fn wait_for_shutdown(&self) {
        self.pool.wait_for_shutdown().await;
    }

    /// 关闭指定的 worker
    ///
    /// 清空 worker slot，导致 task_sender 被 drop，worker 会检测到 channel 关闭并自动退出清理
    ///
    /// # Arguments
    ///
    /// - `worker_id`: 要关闭的 worker ID
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，如果 worker 不存在则返回 `Err(DownloadError::WorkerNotFound)`
    ///
    /// # Note
    ///
    /// 此方法不会等待 worker 退出，worker 会在检测到 channel 关闭后异步自动清理
    #[allow(dead_code)]
    pub(crate) fn shutdown_worker(&self, worker_id: usize) -> Result<()> {
        self.pool.shutdown_worker(worker_id)
    }

    /// 获取所有活跃 worker 的统计快照
    ///
    /// # Returns
    ///
    /// 所有活跃 worker 的统计信息向量
    pub(crate) fn get_worker_snapshots(&self) -> Vec<crate::download::WorkerStatSnapshot> {
        self.pool.workers.iter()
            .enumerate()
            .filter_map(|(id, worker_slot)| {
                // load() 返回 Arc<Option<WorkerSlot>>
                let slot_arc = worker_slot.load();
                slot_arc.as_ref().as_ref().map(|worker| {
                    let (worker_bytes, _, worker_ranges, avg_speed, instant_speed, instant_valid, _window_avg_speed, _window_avg_valid) = 
                        worker.stats.get_full_summary();
                    let current_chunk_size = worker.stats.get_current_chunk_size();
                    crate::download::WorkerStatSnapshot {
                        worker_id: id,
                        bytes: worker_bytes,
                        ranges: worker_ranges,
                        avg_speed,
                        instant_speed: if instant_valid { Some(instant_speed) } else { None },
                        current_chunk_size,
                    }
                })
            })
            .collect()
    }

    /// 获取指定 worker 的实时速度
    ///
    /// # Returns
    ///
    /// Some((实时速度 bytes/s, 是否有效)) 或 None（如果 worker 不存在）
    pub(crate) fn get_worker_instant_speed(&self, worker_id: usize) -> Option<(f64, bool)> {
        let stats = self.pool.worker_stats(worker_id)?;
        Some(stats.get_instant_speed())
    }

    /// 获取指定 worker 的窗口平均速度
    ///
    /// # Returns
    ///
    /// Some((窗口平均速度 bytes/s, 是否有效)) 或 None（如果 worker 不存在）
    pub(crate) fn get_worker_window_avg_speed(&self, worker_id: usize) -> Option<(f64, bool)> {
        let stats = self.pool.worker_stats(worker_id)?;
        Some(stats.get_window_avg_speed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::io_traits::mock::MockHttpClient;
    use bytes::Bytes;
    use reqwest::{header::HeaderMap, StatusCode};

    #[tokio::test]
    async fn test_download_worker_pool_creation() {
        use std::num::NonZeroU64;
        
        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, _) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();

        let worker_count = 4;
        let config = Arc::new(crate::config::DownloadConfig::default());
        let pool = DownloadWorkerPool::new(client, worker_count, writer, config).unwrap();

        assert_eq!(pool.worker_count(), 4);
    }

    #[tokio::test]
    async fn test_download_worker_pool_send_task() {
        use std::num::NonZeroU64;
        
        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, mut allocator) = MmapWriter::new(save_path, NonZeroU64::new(100).unwrap()).unwrap();

        let config = Arc::new(crate::config::DownloadConfig::default());
        let pool = DownloadWorkerPool::new(client, 2, writer, config).unwrap();

        // 分配一个 range
        let range = allocator.allocate(NonZeroU64::new(10).unwrap()).unwrap();

        let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let task = RangeTask::Range {
            url: "http://example.com/file.bin".to_string(),
            range,
            retry_count: 0,
            cancel_rx,
        };

        // 发送任务到 worker 0
        let result = pool.send_task(task, 0).await;
        assert!(result.is_ok(), "发送任务应该成功");
    }

    #[tokio::test]
    async fn test_download_worker_pool_stats() {
        use std::num::NonZeroU64;
        
        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, _) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();

        let worker_count = 3;
        let config = Arc::new(crate::config::DownloadConfig::default());
        let pool = DownloadWorkerPool::new(client, worker_count, writer, config).unwrap();

        // 初始统计应该都是 0
        let (total_bytes, total_secs, ranges) = pool.get_total_stats();
        assert_eq!(total_bytes, 0);
        assert!(total_secs >= 0.0);
        assert_eq!(ranges, 0);

        let speed = pool.get_total_speed();
        assert_eq!(speed, 0.0);
    }

    #[tokio::test]
    async fn test_download_worker_pool_shutdown() {
        use std::num::NonZeroU64;
        
        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, _) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();

        let config = Arc::new(crate::config::DownloadConfig::default());
        let mut pool = DownloadWorkerPool::new(client.clone(), 2, writer, config).unwrap();

        // 关闭 workers
        pool.shutdown();

        // 等待 workers 完成清理（使用事件通知，非轮询）
        pool.wait_for_shutdown().await;

        // 验证所有 worker 都已被移除（slot 为 None）
        for worker_slot in pool.pool.workers.iter() {
            assert!(worker_slot.load().is_none());
        }
    }

    #[tokio::test]
    async fn test_download_worker_executor_success() {
        use std::num::NonZeroU64;
        
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJ"; // 20 bytes

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, mut allocator) = MmapWriter::new(save_path, NonZeroU64::new(test_data.len() as u64).unwrap()).unwrap();

        let range = allocator.allocate(NonZeroU64::new(test_data.len() as u64).unwrap()).unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-19/20").parse().unwrap(),
        );
        client.set_range_response(
            test_url,
            0,
            19,
            StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from_static(test_data),
        );

        // 创建执行器（直接 move writer）
        let executor = DownloadWorkerExecutor::new(client, writer);

        // 创建上下文和统计
        let stats = crate::utils::stats::WorkerStats::default();
        let config = crate::config::DownloadConfig::default();
        let chunk_strategy = 
            Box::new(SpeedBasedChunkStrategy::from_config(&config)) as Box<dyn ChunkStrategy + Send + Sync>;
        let mut context = DownloadWorkerContext {
            chunk_strategy,
        };

        // 执行任务
        let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let task = RangeTask::Range {
            url: test_url.to_string(),
            range,
            retry_count: 0,
            cancel_rx,
        };

        let result = executor.execute(0, task, &mut context, &stats).await;

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
        let (total_bytes, _, ranges) = stats.get_summary();
        assert_eq!(total_bytes, test_data.len() as u64);
        assert_eq!(ranges, 1);
    }

    #[tokio::test]
    async fn test_download_worker_executor_failure() {
        use std::num::NonZeroU64;
        
        let test_url = "http://example.com/file.bin";

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, mut allocator) = MmapWriter::new(save_path, NonZeroU64::new(100).unwrap()).unwrap();

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

        // 创建执行器（直接 move writer）
        let executor = DownloadWorkerExecutor::new(client, writer);

        let stats = crate::utils::stats::WorkerStats::default();
        let config = crate::config::DownloadConfig::default();
        let chunk_strategy = 
            Box::new(SpeedBasedChunkStrategy::from_config(&config)) as Box<dyn ChunkStrategy + Send + Sync>;
        let mut context = DownloadWorkerContext {
            chunk_strategy,
        };

        let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let task = RangeTask::Range {
            url: test_url.to_string(),
            range,
            retry_count: 0,
            cancel_rx,
        };

        let result = executor.execute(0, task, &mut context, &stats).await;

        // 验证结果
        match result {
            RangeResult::DownloadFailed { worker_id, error, .. } => {
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

