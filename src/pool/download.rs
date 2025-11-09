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
use crate::Result;
use crate::pool::common::WorkerHandle;
use crate::task::{RangeResult, WorkerTask as RangeTask};
use crate::utils::{
    chunk_strategy::{ChunkStrategy, SpeedBasedChunkStrategy},
    fetch::{RangeFetcher, FetchRangeResult},
    io_traits::HttpClient,
    stats::{TaskStats, WorkerStats},
    writer::MmapWriter,
};
use async_trait::async_trait;
use log::{debug, error, info};
use std::sync::Arc;

// ==================== Task 和 Result 实现 ====================

impl WorkerTask for RangeTask {}

impl WorkerResult for RangeResult {}

impl super::common::WorkerStats for WorkerStats {}

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
impl<C> WorkerExecutor for DownloadWorkerExecutor<C>
where
    C: HttpClient,
{
    type Task = RangeTask;
    type Result = RangeResult;
    type Context = DownloadWorkerContext;
    type Stats = WorkerStats;
    
    async fn execute(
        &self,
        worker_id: u64,
        task: Self::Task,
        context: &mut Self::Context,
        stats: &Self::Stats,
    ) -> Self::Result {
        let RangeTask::Range { url, range, retry_count, cancel_rx } = task;
        
        let (start, end) = range.as_range_tuple();
        debug!(
            "Worker #{} 执行 Range 任务: {} (range {}..{}, retry {})",
            worker_id, url, start, end, retry_count
        );

        // 下载数据（在下载过程中会实时更新 stats）
        let fetch_result = self.fetch_range(&url, &range, stats, cancel_rx).await;

        match fetch_result {
            Ok(FetchRangeResult::Complete(data)) => {
                self.handle_download_complete(worker_id, range, data, context, stats)
            }
            Ok(FetchRangeResult::Cancelled { data, bytes_downloaded }) => {
                self.handle_download_cancelled(worker_id, range, data, bytes_downloaded, retry_count)
            }
            Err(e) => {
                self.handle_download_failed(worker_id, range, retry_count, e)
            }
        }
    }
}

// ==================== 辅助方法 ====================

impl<C: HttpClient> DownloadWorkerExecutor<C> {
    /// 执行 Range 下载
    async fn fetch_range(
        &self,
        url: &str,
        range: &ranged_mmap::AllocatedRange,
        stats: &WorkerStats,
        cancel_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> std::result::Result<FetchRangeResult, crate::utils::fetch::FetchError> {
        use crate::utils::fetch::FetchRange;
        let fetch_range = FetchRange::from_allocated_range(range)
            .expect("AllocatedRange 应该总是有效的");
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
        context: &mut DownloadWorkerContext,
        stats: &WorkerStats,
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
        stats.record_range_complete();
        
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
                    debug!("Worker #{} 成功写入已下载的 {} bytes", worker_id, bytes_downloaded);
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
        error!("Worker #{} Range {}..{} {}", worker_id, start, end, error_msg);
        
        RangeResult::DownloadFailed {
            worker_id,
            range,
            error: error_msg,
            retry_count,
        }
    }

    /// 根据当前速度更新分块大小
    fn update_chunk_size(
        &self,
        context: &mut DownloadWorkerContext,
        stats: &WorkerStats,
    ) {
        let (instant_speed, valid) = stats.get_instant_speed();
        if !valid || instant_speed <= 0.0 {
            return;
        }

        let avg_speed = stats.get_speed();
        let current_chunk_size = stats.get_current_chunk_size();
        let new_chunk_size = context.chunk_strategy.calculate_chunk_size(
            current_chunk_size,
            instant_speed,
            avg_speed,
        );
        stats.set_current_chunk_size(new_chunk_size);
    }
}

// ==================== 下载 Worker 句柄 ====================

/// 下载 Worker 句柄
///
/// 封装单个下载 worker 的操作接口，提供下载特定的便捷方法
///
/// # 示例
///
/// ```ignore
/// let handle = pool.get_worker(0).ok_or(...)?;
/// handle.send_task(range_task).await?;
/// let speed = handle.instant_speed();
/// ```
pub(crate) struct DownloadWorkerHandle<C: HttpClient> {
    /// 底层通用 worker 句柄
    handle: WorkerHandle<DownloadWorkerExecutor<C>>,
}

impl<C: HttpClient> Clone for DownloadWorkerHandle<C> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl<C: HttpClient> DownloadWorkerHandle<C> {
    /// 创建新的下载 worker 句柄
    ///
    /// # Arguments
    ///
    /// - `handle`: 底层通用 worker 句柄
    pub(crate) fn new(handle: WorkerHandle<DownloadWorkerExecutor<C>>) -> Self {
        Self { handle }
    }

    /// 获取 worker ID
    ///
    /// # Returns
    ///
    /// Worker 的 ID
    #[inline]
    pub fn worker_id(&self) -> u64 {
        self.handle.worker_id()
    }

    /// 提交下载任务给该 worker
    ///
    /// # Arguments
    ///
    /// - `task`: 要执行的下载任务
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，如果 worker 不存在或已关闭则返回 `Err`
    pub async fn send_task(&self, task: RangeTask) -> Result<()> {
        self.handle.send_task(task).await
    }

    /// 获取该 worker 的统计数据
    ///
    /// # Returns
    ///
    /// Worker 统计数据的 Arc 引用
    #[inline]
    pub fn stats(&self) -> Arc<WorkerStats> {
        self.handle.stats()
    }

    /// 获取该 worker 的实时速度
    ///
    /// # Returns
    ///
    /// (实时速度 bytes/s, 是否有效)
    pub fn instant_speed(&self) -> (f64, bool) {
        let stats = self.handle.stats();
        stats.get_instant_speed()
    }

    /// 获取该 worker 的窗口平均速度
    ///
    /// # Returns
    ///
    /// (窗口平均速度 bytes/s, 是否有效)
    pub fn window_avg_speed(&self) -> (f64, bool) {
        let stats = self.handle.stats();
        stats.get_window_avg_speed()
    }

    /// 获取该 worker 的当前分块大小
    ///
    /// # Returns
    ///
    /// 当前分块大小 bytes
    pub fn chunk_size(&self) -> u64 {
        let stats = self.handle.stats();
        stats.get_current_chunk_size()
    }
}

// ==================== 下载 Worker 协程池 ====================

/// 下载 Worker 协程池
///
/// 封装通用 WorkerPool，提供下载特定的便捷方法
pub(crate) struct DownloadWorkerPool<C: HttpClient> {
    /// 底层通用协程池
    pool: WorkerPool<DownloadWorkerExecutor<C>>,
    /// 全局统计管理器（聚合所有 worker 的数据）
    global_stats: Arc<TaskStats>,
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
    /// 返回新创建的 DownloadWorkerPool、所有初始 worker 的句柄以及结果接收器
    pub(crate) fn new(
        client: C,
        initial_worker_count: u64,
        writer: MmapWriter,
        config: Arc<crate::config::DownloadConfig>,
        global_stats: Arc<TaskStats>,
    ) -> Result<(Self, Vec<DownloadWorkerHandle<C>>, tokio::sync::mpsc::Receiver<RangeResult>)> {
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

        // 创建通用协程池并获取 worker 句柄和 result_receiver
        let (pool, worker_handles, result_receiver) = WorkerPool::new(executor, contexts_with_stats)?;

        info!("创建下载协程池，{} 个初始 workers", initial_worker_count);

        // 将通用句柄转换为下载特定句柄
        let download_handles = worker_handles
            .into_iter()
            .map(DownloadWorkerHandle::new)
            .collect();

        Ok((Self {
            pool,
            global_stats,
            config,
        }, download_handles, result_receiver))
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `count`: 要添加的 worker 数量
    ///
    /// # Returns
    ///
    /// 成功时返回新添加的所有 worker 的句柄，失败时返回错误信息
    pub(crate) async fn add_workers(&mut self, count: u64) -> Result<Vec<DownloadWorkerHandle<C>>> {
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

        // 添加新 workers（使用现有的执行器）并获取句柄
        let worker_handles = self.pool.add_workers(contexts_with_stats).await?;
        
        // 将通用句柄转换为下载特定句柄
        let download_handles = worker_handles
            .into_iter()
            .map(DownloadWorkerHandle::new)
            .collect();
        
        Ok(download_handles)
    }

    /// 获取当前活跃 worker 总数
    pub(crate) fn worker_count(&self) -> u64 {
        self.pool.worker_count()
    }

    /// 获取所有 worker 的总体窗口平均速度（O(1)，无需遍历）
    ///
    /// # Returns
    ///
    /// `(窗口平均速度 bytes/s, 是否有效)`
    #[allow(dead_code)]
    pub(crate) fn get_total_window_avg_speed(&self) -> (f64, bool) {
        self.global_stats.get_window_avg_speed()
    }

    /// 优雅关闭所有 workers
    ///
    /// 发送关闭信号到所有活跃的 worker，让它们停止接收新任务并自动退出清理
    /// 
    /// # Note
    ///
    /// 此方法会等待 workers 退出
    pub(crate) async fn shutdown(&mut self) {
        self.pool.shutdown().await;
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
        let global_stats = Arc::new(TaskStats::from_config(config.speed()));
        let (pool, _handles, _result_receiver) = DownloadWorkerPool::new(client, worker_count, writer, config, global_stats).unwrap();

        assert_eq!(pool.worker_count(), 4);
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
        let global_stats = Arc::new(TaskStats::from_config(config.speed()));
        let (pool, _handles, _result_receiver) = DownloadWorkerPool::new(client, worker_count, writer, config, global_stats).unwrap();

        // 初始统计应该都是 0
        let (total_bytes, total_secs, ranges) = pool.global_stats.get_summary();
        assert_eq!(total_bytes, 0);
        assert!(total_secs >= 0.0);
        assert_eq!(ranges, 0);

        let speed = pool.global_stats.get_speed();
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
        let global_stats = Arc::new(TaskStats::from_config(config.speed()));
        let (mut pool, _handles, _result_receiver) = DownloadWorkerPool::new(client.clone(), 2, writer, config, global_stats).unwrap();

        // 关闭 workers
        pool.shutdown().await;

        // 验证所有 worker 都已被移除
        assert_eq!(pool.pool.worker_count(), 0);
        assert!(pool.pool.slots.is_empty());
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

