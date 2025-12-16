//! 下载专用 Worker 协程池
//!
//! 基于通用 WorkerPool 实现的下载专用协程池，提供下载任务的并发处理能力。
//!
//! # 核心组件
//!
//! - **DownloadWorkerContext**: 下载 worker 的上下文，包含分块策略
//! - **DownloadTaskExecutor**: 下载任务执行器，封装核心下载逻辑
//! - **DownloadWorkerFactory**: 下载任务工厂，处理 Range 下载和文件写入
//! - **DownloadWorkerPool**: 下载协程池，封装通用 WorkerPool 并提供下载特定方法
//!
//! # 模块结构
//!
//! - `executor`: 核心下载协程逻辑，独立封装方便测试
//! - `retry_scheduler`: 重试调度器，管理待重试任务队列
//! - `task_allocator`: 任务分配器，统一管理新任务分配和重试调度
//!
//! # 设计说明
//!
//! 此模块包含所有下载特定的类型（Task、Result、Context、Stats）。
//! 通用协程池 (common.rs) 仅负责协程生命周期管理。

mod executor;
mod local_health_checker;
mod stats_updater;

pub use executor::state::TaskState;
use local_health_checker::{LocalHealthChecker, LocalHealthCheckerConfig};
use stats_updater::StatsUpdater;
pub use stats_updater::{
    ExecutorBroadcast, ExecutorStats, PendingExecutorStats, RunningExecutorStats,
    StoppedExecutorStats, TaggedBroadcast, WorkerBroadcaster,
};

use super::common::{WorkerFactory, WorkerId, WorkerPool};
use crate::utils::cancel_channel::cancel_channel;
use crate::utils::{
    chunk_strategy::{ChunkStrategy, SpeedBasedChunkStrategy},
    io_traits::HttpClient,
    stats::WorkerStatsRecording,
    writer::MmapWriter,
};
use executor::task_allocator::TaskAllocator;
pub(crate) use executor::{
    DownloadTaskExecutor, DownloadWorkerContext, ExecutorInput, ExecutorResult,
};
use log::info;
use ranged_mmap::allocator::concurrent::Allocator as ConcurrentAllocator;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use std::sync::Arc;

use crate::download::download_stats::AggregatedStats;
use smr_swap::SmrReader;

// ==================== Worker 输入参数 ====================

/// 下载 Worker 的输入参数
///
/// 包含 spawn_worker 所需的所有下载特定数据
pub(crate) struct DownloadWorkerInput {
    /// 上下文（包含 task_allocator、url）
    pub(crate) context: DownloadWorkerContext,
    /// 统计数据
    pub(crate) stats: WorkerStatsRecording,
    /// 分块策略（传递给 StatsUpdater）
    pub(crate) chunk_strategy: Box<dyn ChunkStrategy + Send>,
    /// 初始 chunk size
    pub(crate) initial_chunk_size: u64,
    /// 结果发送通道（oneshot，仅在 worker 完成时发送一次）
    pub(crate) result_tx: tokio::sync::oneshot::Sender<ExecutorResult>,
    /// 外部广播发送器（spawn_worker 中创建 WorkerBroadcaster）
    pub(crate) broadcast_tx: broadcast::Sender<TaggedBroadcast>,
    /// 本地健康检查器配置
    pub(crate) local_health_config: LocalHealthCheckerConfig,
    /// 聚合统计读取器
    pub(crate) aggregated_stats: SmrReader<AggregatedStats>,
}

// ==================== 下载任务工厂 ====================

/// 下载任务工厂
///
/// 实现 WorkerFactory trait，采用 Composite Worker 模式。
/// 包含：
/// - 协程 0: 主下载循环 (接收任务 -> HTTP请求)
/// - 协程 1: Stats Updater (统计更新)
/// - 协程 2: Local Health Checker (本地健康检查)
///
/// # 泛型参数
///
/// - `C`: HTTP 客户端类型
#[derive(Clone)]
pub(crate) struct DownloadWorkerFactory<C> {
    /// HTTP 客户端
    client: C,
    /// 共享的文件写入器
    writer: MmapWriter,
}

impl<C: Clone> DownloadWorkerFactory<C> {
    /// 创建新的下载任务工厂
    pub(crate) fn new(client: C, writer: MmapWriter) -> Self {
        Self { client, writer }
    }
}

impl<C> WorkerFactory for DownloadWorkerFactory<C>
where
    C: HttpClient,
{
    type Input = DownloadWorkerInput;

    /// 启动 Worker 协程组
    fn spawn_worker(
        &self,
        worker_id: WorkerId,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        input: Self::Input,
    ) -> Vec<JoinHandle<()>> {
        let DownloadWorkerInput {
            context,
            stats,
            chunk_strategy,
            initial_chunk_size,
            result_tx,
            broadcast_tx,
            local_health_config,
            aggregated_stats,
        } = input;

        // --- 创建取消通道 ---
        // 在 spawn_worker 内创建，确保每个 worker 实例有独立的取消通道
        let (cancel_tx, cancel_rx) = cancel_channel();

        // --- 创建 WorkerBroadcaster ---
        // 在此处创建，因为这里才知道真正的 worker_id
        let broadcaster = WorkerBroadcaster::new(worker_id, broadcast_tx);

        // --- 协程 1: Stats Updater 辅助协程 ---
        // chunk_strategy 和 initial_chunk_size 传递给 StatsUpdater
        let (stats_updater, stats_handle) = StatsUpdater::new(
            worker_id,
            broadcaster.clone(),
            chunk_strategy,
            initial_chunk_size,
            cancel_tx.clone(),
            None,
        );
        let stats_updater_handle = tokio::spawn(stats_updater.run());

        // --- 协程 2: 本地健康检查协程 ---
        let local_health_checker = LocalHealthChecker::new(
            worker_id,
            &broadcaster,
            cancel_tx,
            local_health_config,
            aggregated_stats.local(),
        );
        let local_health_handle = tokio::spawn(local_health_checker.run());

        // --- 创建 Executor ---
        let executor =
            DownloadTaskExecutor::new(worker_id, self.client.clone(), self.writer.clone());

        // --- 协程 0: 主下载循环（worker 自主从 allocator 分配任务） ---
        let executor_input = ExecutorInput {
            context,
            stats,
            stats_handle,
            result_tx,
            shutdown_rx,
            cancel_rx,
        };
        let main_handle = tokio::spawn(executor.run_loop(executor_input));

        vec![main_handle, stats_updater_handle, local_health_handle]
    }
}

// ==================== 下载 Worker 协程池 ====================

/// 下载 Worker 协程池
///
/// 封装通用 WorkerPool，提供下载特定的便捷方法
///
/// # 泛型参数
///
/// - `C`: HTTP 客户端类型
pub(crate) struct DownloadWorkerPool<C: HttpClient> {
    /// 底层通用协程池
    pool: WorkerPool<DownloadWorkerFactory<C>>,
    /// 下载配置（用于创建新 worker 的分块策略）
    config: Arc<crate::config::DownloadConfig>,
    /// 并发范围分配器（所有 worker 共享）
    allocator: Arc<ConcurrentAllocator>,
    /// 下载 URL
    url: String,
    /// 广播发送器
    broadcast_tx: broadcast::Sender<TaggedBroadcast>,
    /// 聚合统计读取器
    aggregated_stats: SmrReader<AggregatedStats>,
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
    /// - `allocator`: 并发范围分配器，所有 worker 共享
    /// - `url`: 下载 URL
    /// - `config`: 下载配置，用于创建分块策略和设置速度窗口
    /// - `broadcast_tx`: 广播发送器，用于发送 ExecutorStats 更新和关闭信号
    /// - `aggregated_stats`: 聚合统计读取器
    ///
    /// # Returns
    ///
    /// 返回新创建的 DownloadWorkerPool、所有初始 worker 的结果接收器
    pub(crate) fn new(
        client: C,
        initial_worker_count: u64,
        writer: MmapWriter,
        allocator: ConcurrentAllocator,
        url: String,
        config: Arc<crate::config::DownloadConfig>,
        broadcast_tx: broadcast::Sender<TaggedBroadcast>,
        aggregated_stats: SmrReader<AggregatedStats>,
    ) -> (Self, Vec<tokio::sync::oneshot::Receiver<ExecutorResult>>) {
        let allocator = Arc::new(allocator);

        // 创建工厂
        let factory = DownloadWorkerFactory::new(client, writer);

        // 为每个 worker 创建输入参数
        let mut inputs = Vec::with_capacity(initial_worker_count as usize);
        let mut result_rxs = Vec::with_capacity(initial_worker_count as usize);

        for _ in 0..initial_worker_count {
            let (input, result_rx) = Self::create_worker_input(
                &config,
                allocator.clone(),
                url.clone(),
                broadcast_tx.clone(),
                aggregated_stats.clone(),
            );
            inputs.push(input);
            result_rxs.push(result_rx);
        }

        // 创建通用协程池
        let pool = WorkerPool::new(factory, inputs);

        info!("创建下载协程池，{} 个初始 workers", initial_worker_count);

        (
            Self {
                pool,
                config,
                allocator,
                url,
                broadcast_tx,
                aggregated_stats,
            },
            result_rxs,
        )
    }

    /// 创建单个 worker 的输入参数
    ///
    /// 返回 (input, cancel_tx, result_rx)，其中 cancel_tx 用于稍后创建 DownloadWorkerHandle
    fn create_worker_input(
        config: &crate::config::DownloadConfig,
        allocator: Arc<ConcurrentAllocator>,
        url: String,
        broadcast_tx: broadcast::Sender<TaggedBroadcast>,
        aggregated_stats: SmrReader<AggregatedStats>,
    ) -> (
        DownloadWorkerInput,
        tokio::sync::oneshot::Receiver<ExecutorResult>,
    ) {
        // 创建统计数据
        let stats = WorkerStatsRecording::from_config(config.speed());

        // 创建任务分配器
        let task_allocator = TaskAllocator::new(allocator, config.retry().clone());

        // 创建上下文（chunk_strategy 已移至 StatsUpdater）
        let context = DownloadWorkerContext {
            task_allocator,
            url,
        };

        // 创建分块策略（传递给 StatsUpdater）
        let chunk_strategy =
            Box::new(SpeedBasedChunkStrategy::from_config(config)) as Box<dyn ChunkStrategy + Send>;
        let initial_chunk_size = config.chunk().initial_size();

        // 创建结果通道（oneshot）
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        // 创建本地健康检查器配置
        let local_health_config = LocalHealthCheckerConfig::from_download_config(config);

        let input = DownloadWorkerInput {
            context,
            stats,
            chunk_strategy,
            initial_chunk_size,
            result_tx,
            broadcast_tx,
            local_health_config,
            aggregated_stats,
        };

        (input, result_rx)
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `count`: 要添加的 worker 数量
    ///
    /// # Returns
    ///
    /// 成功时返回新添加的所有 worker 的结果接收器
    pub(crate) fn add_workers(
        &mut self,
        count: u64,
    ) -> Vec<tokio::sync::oneshot::Receiver<ExecutorResult>> {
        let mut inputs = Vec::with_capacity(count as usize);
        let mut result_rxs = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (input, result_rx) = Self::create_worker_input(
                &self.config,
                self.allocator.clone(),
                self.url.clone(),
                self.broadcast_tx.clone(),
                self.aggregated_stats.clone(),
            );
            inputs.push(input);
            result_rxs.push(result_rx);
        }

        result_rxs
    }

    /// 获取当前活跃 worker 总数
    pub(crate) fn worker_count(&self) -> u64 {
        self.pool.worker_count()
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

    #[tokio::test]
    async fn test_download_worker_pool_creation() {
        use crate::download::download_stats::AggregatedStats;
        use smr_swap::SmrSwap;
        use std::num::NonZeroU64;

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, allocator) =
            MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();
        let url = "http://example.com/file.bin".to_string();

        let worker_count = 4;
        let config = Arc::new(crate::config::DownloadConfig::default());
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(128);

        let stats = SmrSwap::new(AggregatedStats::Pending);
        let aggregated_stats = stats.reader();

        let (pool, _result_rx) = DownloadWorkerPool::new(
            client,
            worker_count,
            writer,
            allocator,
            url,
            config,
            broadcast_tx,
            aggregated_stats,
        );

        assert_eq!(pool.worker_count(), 4);
    }

    #[tokio::test]
    async fn test_download_worker_pool_shutdown() {
        use crate::download::download_stats::AggregatedStats;
        use smr_swap::SmrSwap;
        use std::num::NonZeroU64;

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, allocator) =
            MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();
        let url = "http://example.com/file.bin".to_string();

        let config = Arc::new(crate::config::DownloadConfig::default());
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(128);

        let stats = SmrSwap::new(AggregatedStats::Pending);
        let aggregated_stats = stats.reader();

        let (mut pool, _result_rx) = DownloadWorkerPool::new(
            client.clone(),
            2,
            writer,
            allocator,
            url,
            config,
            broadcast_tx,
            aggregated_stats,
        );

        // 关闭 workers
        pool.shutdown().await;

        // 验证所有 worker 都已被移除
        assert_eq!(pool.pool.worker_count(), 0);
        assert!(pool.pool.slots.is_empty());
    }
}
