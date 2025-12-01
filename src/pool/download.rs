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
//!
//! # 设计说明
//!
//! 此模块包含所有下载特定的类型（Task、Result、Context、Stats）。
//! 通用协程池 (common.rs) 仅负责协程生命周期管理。

mod executor;

use super::common::{WorkerFactory, WorkerPool};
use crate::Result;
pub(crate) use executor::{DownloadTaskExecutor, DownloadWorkerContext};
use crate::task::{RangeResult, WorkerTask as RangeTask};
use crate::utils::{
    chunk_strategy::{ChunkStrategy, SpeedBasedChunkStrategy},
    io_traits::HttpClient,
    stats::{TaskStats, WorkerStats},
    writer::MmapWriter,
};
use crate::DownloadError;
use log::info;
use net_bytes::{DownloadSpeed, SizeStandard};
use smr_swap::{LocalReader, ReadGuard, SmrSwap};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use std::sync::Arc;

// ==================== Worker 输入参数 ====================

/// 下载 Worker 的输入参数
///
/// 包含 spawn_worker 所需的所有下载特定数据
pub(crate) struct DownloadWorkerInput {
    /// 上下文（分块策略等）
    pub(crate) context: DownloadWorkerContext,
    /// 统计数据（SmrSwap 包装）
    pub(crate) stats: SmrSwap<WorkerStats>,
    /// 任务接收通道
    pub(crate) task_rx: Receiver<RangeTask>,
    /// 结果发送通道
    pub(crate) result_tx: Sender<RangeResult>,
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
            task_rx,
            result_tx,
        } = input;

        // --- 协程 0: 主下载循环 ---
        let main_handle = tokio::spawn(
            executor.run_loop(worker_id, context, stats, task_rx, result_tx, shutdown_rx),
        );

        // --- 协程 1+: 辅助协程 (暂未实现，预留位置) ---
        vec![main_handle]
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
    /// 向 worker 发送任务的通道
    task_tx: Arc<Sender<RangeTask>>,
    /// 该 worker 的统计数据（SwapReader 包装以便外部访问）
    stats: LocalReader<WorkerStats>,
}

impl DownloadWorkerHandle {
    /// 创建新的下载 worker 句柄
    pub(crate) fn new(
        worker_id: u64,
        task_tx: Arc<Sender<RangeTask>>,
        stats: LocalReader<WorkerStats>,
    ) -> Self {
        Self { worker_id, task_tx, stats }
    }

    /// 获取 worker ID
    #[inline]
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    /// 提交下载任务给该 worker
    pub fn send_task(
        &self,
        task: RangeTask,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let sender = self.task_tx.clone();
        async move {
            sender
                .send(task)
                .await
                .map_err(|e| DownloadError::TaskSend(e.to_string()))?;
            Ok(())
        }
    }

    /// 获取该 worker 的统计数据
    #[inline]
    pub fn stats<'a>(&'a self) -> ReadGuard<'a, WorkerStats> {
        self.stats.load()
    }

    /// 获取该 worker 的实时速度
    pub fn instant_speed(&self) -> Option<DownloadSpeed> {
        self.stats.load().get_instant_speed()
    }

    /// 获取该 worker 的当前分块大小
    pub fn chunk_size(&self) -> u64 {
        self.stats.load().get_current_chunk_size()
    }
}

// ==================== 下载 Worker 协程池 ====================

/// 下载 Worker 协程池
///
/// 封装通用 WorkerPool，提供下载特定的便捷方法
pub(crate) struct DownloadWorkerPool<C: HttpClient> {
    /// 底层通用协程池
    pool: WorkerPool<DownloadWorkerFactory<C>>,
    /// 结果发送器（用于创建新 worker 时克隆）
    result_tx: Sender<RangeResult>,
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
    /// 返回新创建的 DownloadWorkerPool、所有初始 worker 的句柄以及结果接收器
    pub(crate) fn new(
        client: C,
        initial_worker_count: u64,
        writer: MmapWriter,
        config: Arc<crate::config::DownloadConfig>,
        global_stats: TaskStats,
    ) -> (
        Self,
        Vec<DownloadWorkerHandle>,
        Receiver<RangeResult>,
    ) {
        // 创建工厂
        let factory = DownloadWorkerFactory::new(
            client,
            writer,
            config.speed().size_standard(),
        );

        // 创建结果通道（所有 worker 共享）
        let (result_tx, result_rx) = mpsc::channel::<RangeResult>(100);

        // 为每个 worker 创建输入参数和句柄
        let mut inputs = Vec::with_capacity(initial_worker_count as usize);
        let mut handles = Vec::with_capacity(initial_worker_count as usize);

        for _ in 0..initial_worker_count {
            let (input, handle) = Self::create_worker_input_and_handle(
                &global_stats,
                &config,
                result_tx.clone(),
            );
            inputs.push(input);
            handles.push(handle);
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
                result_tx,
                global_stats,
                config,
            },
            handles,
            result_rx,
        )
    }

    /// 创建单个 worker 的输入参数和句柄
    fn create_worker_input_and_handle(
        global_stats: &TaskStats,
        config: &crate::config::DownloadConfig,
        result_tx: Sender<RangeResult>,
    ) -> (DownloadWorkerInput, DownloadWorkerHandle) {
        // 创建任务通道
        let (task_tx, task_rx) = mpsc::channel::<RangeTask>(100);

        // 创建统计数据
        let mut worker_stats = global_stats.create_child();
        worker_stats.set_current_chunk_size(config.chunk().initial_size());
        let stats = SmrSwap::new(worker_stats);
        let stats_reader = stats.local();

        // 创建上下文
        let chunk_strategy = Box::new(SpeedBasedChunkStrategy::from_config(config))
            as Box<dyn ChunkStrategy + Send + Sync>;
        let context = DownloadWorkerContext { chunk_strategy };

        let input = DownloadWorkerInput {
            context,
            stats,
            task_rx,
            result_tx,
        };

        let handle = DownloadWorkerHandle::new(
            0, // worker_id 稍后由 WorkerPool::new 返回的 ID 更新
            Arc::new(task_tx),
            stats_reader,
        );

        (input, handle)
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `count`: 要添加的 worker 数量
    ///
    /// # Returns
    ///
    /// 成功时返回新添加的所有 worker 的句柄
    pub(crate) fn add_workers(&mut self, count: u64) -> Vec<DownloadWorkerHandle> {
        let mut inputs = Vec::with_capacity(count as usize);
        let mut handles = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (input, handle) = Self::create_worker_input_and_handle(
                &self.global_stats,
                &self.config,
                self.result_tx.clone(),
            );
            inputs.push(input);
            handles.push(handle);
        }

        let worker_ids = self.pool.add_workers(inputs);

        // 更新句柄的 worker_id
        for (handle, worker_id) in handles.iter_mut().zip(worker_ids.iter()) {
            handle.worker_id = *worker_id;
        }

        handles
    }

    /// 获取当前活跃 worker 总数
    pub(crate) fn worker_count(&self) -> u64 {
        self.pool.worker_count()
    }

    /// 获取所有 worker 的总体窗口平均速度（O(1)，无需遍历）
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    #[allow(dead_code)]
    pub(crate) fn get_total_window_avg_speed(&self) -> Option<DownloadSpeed> {
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

    #[tokio::test]
    async fn test_download_worker_pool_creation() {
        use std::num::NonZeroU64;

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, _) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();

        let worker_count = 4;
        let config = Arc::new(crate::config::DownloadConfig::default());
        let global_stats = TaskStats::from_config(config.clone());
        let (pool, _handles, _result_receiver) =
            DownloadWorkerPool::new(client, worker_count, writer, config, global_stats);

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
        let global_stats = TaskStats::from_config(config.clone());
        let (pool, _handles, _result_receiver) =
            DownloadWorkerPool::new(client, worker_count, writer, config, global_stats);

        // 初始统计应该都是 0
        let (total_bytes, total_secs, ranges) = pool.global_stats.get_summary();
        assert_eq!(total_bytes, 0);
        assert!(total_secs >= 0.0);
        assert_eq!(ranges, 0);

        let speed = pool.global_stats.get_speed();
        assert_eq!(speed, None);
    }

    #[tokio::test]
    async fn test_download_worker_pool_shutdown() {
        use std::num::NonZeroU64;

        let client = MockHttpClient::new();
        let dir = tempfile::tempdir().unwrap();
        let save_path = dir.path().join("test.bin");

        let (writer, _) = MmapWriter::new(save_path, NonZeroU64::new(1000).unwrap()).unwrap();

        let config = Arc::new(crate::config::DownloadConfig::default());
        let global_stats = TaskStats::from_config(config.clone());
        let (mut pool, _handles, _result_receiver) =
            DownloadWorkerPool::new(client.clone(), 2, writer, config, global_stats);

        // 关闭 workers
        pool.shutdown().await;

        // 验证所有 worker 都已被移除
        assert_eq!(pool.pool.worker_count(), 0);
        assert!(pool.pool.slots.is_empty());
    }
}
