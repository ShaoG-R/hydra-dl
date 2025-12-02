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

pub use stats_updater::{ExecutorBroadcast, ExecutorCurrentStats, ExecutorStats, TaggedBroadcast, WorkerBroadcaster};
use local_health_checker::{LocalHealthChecker, LocalHealthCheckerConfig};
use stats_updater::StatsUpdater;

use super::common::{WorkerFactory, WorkerPool};
pub(crate) use executor::{DownloadTaskExecutor, DownloadWorkerContext, ExecutorResult};
use executor::task_allocator::TaskAllocator;
use crate::utils::{
    cancel_channel::CancelHandle, chunk_strategy::{ChunkStrategy, SpeedBasedChunkStrategy}, io_traits::HttpClient, stats::{WorkerStats}, writer::MmapWriter
};
use log::info;
use net_bytes::{SizeStandard};
use ranged_mmap::allocator::concurrent::Allocator as ConcurrentAllocator;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use crate::utils::cancel_channel::{cancel_channel, CancelReceiver, CancelSender};

use std::sync::Arc;

// ==================== Worker 输入参数 ====================

/// 下载 Worker 的输入参数
///
/// 包含 spawn_worker 所需的所有下载特定数据
pub(crate) struct DownloadWorkerInput {
    /// 上下文（包含 allocator、url、分块策略等）
    pub(crate) context: DownloadWorkerContext,
    /// 统计数据
    pub(crate) stats: WorkerStats,
    /// 结果发送通道（oneshot，仅在 worker 完成时发送一次）
    pub(crate) result_tx: tokio::sync::oneshot::Sender<ExecutorResult>,
    /// 取消信号接收器（用于健康检查触发的重试）
    pub(crate) cancel_rx: CancelReceiver,
    /// 取消信号发送器（用于本地健康检查器）
    pub(crate) cancel_tx: CancelSender,
    /// 外部广播发送器（spawn_worker 中创建 WorkerBroadcaster）
    pub(crate) broadcast_tx: broadcast::Sender<TaggedBroadcast>,
    /// 本地健康检查器配置
    pub(crate) local_health_config: LocalHealthCheckerConfig,
}

// ==================== 下载任务工厂 ====================

/// 下载任务工厂
///
/// 实现 WorkerFactory trait，采用 Composite Worker 模式。
/// 目前包含：
/// - 协程 0: 主下载循环 (接收任务 -> HTTP请求 -> 写入)
///
/// # 泛型参数
///
/// - `C`: HTTP 客户端类型
#[derive(Clone)]
pub(crate) struct DownloadWorkerFactory<C> {
    /// 下载任务执行器
    executor: DownloadTaskExecutor<C>,
}

impl<C: Clone> DownloadWorkerFactory<C> {
    /// 创建新的下载任务工厂
    pub(crate) fn new(
        client: C,
        writer: MmapWriter,
        size_standard: SizeStandard,
    ) -> Self {
        Self {
            executor: DownloadTaskExecutor::new(client, writer, size_standard),
        }
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
        worker_id: u64,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        input: Self::Input,
    ) -> Vec<JoinHandle<()>> {
        let executor = self.executor.clone();

        let DownloadWorkerInput {
            context,
            stats,
            result_tx,
            cancel_rx,
            cancel_tx,
            broadcast_tx,
            local_health_config,
        } = input;

        // --- 创建 WorkerBroadcaster ---
        // 在此处创建，因为这里才知道真正的 worker_id
        let broadcaster = WorkerBroadcaster::new(worker_id, broadcast_tx, 16);

        // --- 协程 1: Stats Updater 辅助协程 ---
        let (stats_updater, stats_handle) = StatsUpdater::new(worker_id, broadcaster.clone(), None);
        let stats_updater_handle = tokio::spawn(stats_updater.run());

        // --- 协程 2: 本地健康检查协程 ---
        let local_health_checker = LocalHealthChecker::new(
            worker_id,
            &broadcaster,
            cancel_tx,
            local_health_config,
        );
        let local_health_handle = tokio::spawn(local_health_checker.run());

        // --- 协程 0: 主下载循环（worker 自主从 allocator 分配任务） ---
        let main_handle = tokio::spawn(
            executor.run_loop(worker_id, context, stats, stats_handle, result_tx, shutdown_rx, cancel_rx),
        );

        vec![main_handle, stats_updater_handle, local_health_handle]
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
#[derive(Clone)]
pub(crate) struct DownloadWorkerHandle {
    /// Worker ID
    worker_id: u64,
    /// 取消信号发送器（用于健康检查触发的重试）
    cancel_tx: CancelSender,
}

impl DownloadWorkerHandle {
    /// 创建新的下载 worker 句柄
    pub(crate) fn new(
        worker_id: u64,
        cancel_tx: CancelSender,
    ) -> Self {
        Self { worker_id, cancel_tx }
    }

    /// 获取 worker ID
    #[inline]
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    // 获取用于取消的句柄
    pub fn cancel_handle(&self) -> CancelHandle {
        self.cancel_tx.get_handle()
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
    ///
    /// # Returns
    ///
    /// 返回新创建的 DownloadWorkerPool、所有初始 worker 的句柄以及结果接收器
    pub(crate) fn new(
        client: C,
        initial_worker_count: u64,
        writer: MmapWriter,
        allocator: ConcurrentAllocator,
        url: String,
        config: Arc<crate::config::DownloadConfig>,
        broadcast_tx: broadcast::Sender<TaggedBroadcast>,
    ) -> (
        Self,
        Vec<DownloadWorkerHandle>,
        Vec<tokio::sync::oneshot::Receiver<ExecutorResult>>,
    ) {
        let allocator = Arc::new(allocator);

        // 创建工厂
        let factory = DownloadWorkerFactory::new(
            client,
            writer,
            config.speed().size_standard(),
        );

        // 为每个 worker 创建输入参数和句柄
        let mut inputs = Vec::with_capacity(initial_worker_count as usize);
        let mut handles = Vec::with_capacity(initial_worker_count as usize);
        let mut result_rxs = Vec::with_capacity(initial_worker_count as usize);

        for _ in 0..initial_worker_count {
            let (input, handle, result_rx) = Self::create_worker_input_and_handle(
                &config,
                allocator.clone(),
                url.clone(),
                broadcast_tx.clone(),
            );
            inputs.push(input);
            handles.push(handle);
            result_rxs.push(result_rx);
        }

        // 创建通用协程池
        let (pool, worker_ids) = WorkerPool::new(factory, inputs);

        // 更新句柄的 worker_id
        for (handle, worker_id) in handles.iter_mut().zip(worker_ids.iter()) {
            handle.worker_id = *worker_id;
        }

        info!("创建下载协程池，{} 个初始 workers", initial_worker_count);

        (
            Self {
                pool,
                config,
                allocator,
                url,
                broadcast_tx,
            },
            handles,
            result_rxs,
        )
    }

    /// 创建单个 worker 的输入参数和句柄
    fn create_worker_input_and_handle(
        config: &crate::config::DownloadConfig,
        allocator: Arc<ConcurrentAllocator>,
        url: String,
        broadcast_tx: broadcast::Sender<TaggedBroadcast>,
    ) -> (DownloadWorkerInput, DownloadWorkerHandle, tokio::sync::oneshot::Receiver<ExecutorResult>) {
        // 创建统计数据
        let mut stats = WorkerStats::from_config(config.speed());
        stats.set_current_chunk_size(config.chunk().initial_size());

        // 创建任务分配器
        let task_allocator = TaskAllocator::new(allocator, config.retry().clone());

        // 创建上下文
        let chunk_strategy = Box::new(SpeedBasedChunkStrategy::from_config(config))
            as Box<dyn ChunkStrategy + Send + Sync>;
        let context = DownloadWorkerContext {
            chunk_strategy,
            task_allocator,
            url,
        };

        // 创建取消通道（用于健康检查触发重试）
        let (cancel_tx, cancel_rx) = cancel_channel();

        // 创建结果通道（oneshot）
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        // 创建本地健康检查器配置
        let local_health_config = LocalHealthCheckerConfig::from_download_config(config);

        let input = DownloadWorkerInput {
            context,
            stats,
            result_tx,
            cancel_rx,
            cancel_tx: cancel_tx.clone(),
            broadcast_tx,
            local_health_config,
        };

        let handle = DownloadWorkerHandle::new(
            0, // worker_id 稍后由 WorkerPool::new 返回的 ID 更新
            cancel_tx,
        );

        (input, handle, result_rx)
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `count`: 要添加的 worker 数量
    ///
    /// # Returns
    ///
    /// 成功时返回新添加的所有 worker 的句柄和结果接收器
    pub(crate) fn add_workers(&mut self, count: u64) -> (Vec<DownloadWorkerHandle>, Vec<tokio::sync::oneshot::Receiver<ExecutorResult>>) {
        let mut inputs = Vec::with_capacity(count as usize);
        let mut handles = Vec::with_capacity(count as usize);
        let mut result_rxs = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (input, handle, result_rx) = Self::create_worker_input_and_handle(
                &self.config,
                self.allocator.clone(),
                self.url.clone(),
                self.broadcast_tx.clone(),
            );
            inputs.push(input);
            handles.push(handle);
            result_rxs.push(result_rx);
        }

        let worker_ids = self.pool.add_workers(inputs);

        // 更新句柄的 worker_id
        for (handle, worker_id) in handles.iter_mut().zip(worker_ids.iter()) {
            handle.worker_id = *worker_id;
        }

        (handles, result_rxs)
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
        use std::num::NonZeroU64;

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, allocator) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();
        let url = "http://example.com/file.bin".to_string();

        let worker_count = 4;
        let config = Arc::new(crate::config::DownloadConfig::default());
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(128);
        let (pool, _handles, _result_receiver) =
            DownloadWorkerPool::new(client, worker_count, writer, allocator, url, config, broadcast_tx);

        assert_eq!(pool.worker_count(), 4);
    }

    #[tokio::test]
    async fn test_download_worker_pool_shutdown() {
        use std::num::NonZeroU64;

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, allocator) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();
        let url = "http://example.com/file.bin".to_string();

        let config = Arc::new(crate::config::DownloadConfig::default());
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(128);
        let (mut pool, _handles, _result_receiver) =
            DownloadWorkerPool::new(client.clone(), 2, writer, allocator, url, config, broadcast_tx);

        // 关闭 workers
        pool.shutdown().await;

        // 验证所有 worker 都已被移除
        assert_eq!(pool.pool.worker_count(), 0);
        assert!(pool.pool.slots.is_empty());
    }
}
