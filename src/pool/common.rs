//! 通用 Worker 协程池
//!
//! 提供轻量级的 Worker 协程池实现，专注于协程生命周期管理。
//! 支持 "Composite Worker" 模式，即一个 Worker 可由多个协作的协程组成。
//!
//! # 核心概念
//!
//! - **WorkerFactory**: 工厂 trait，负责 spawn 协程组
//! - **WorkerSlot**: 单个 Worker 的槽位（关闭信号 + JoinHandles）
//! - **WorkerPool**: 协程池，管理多个 Worker 的生命周期
//!
//! # Composite Worker 模式
//!
//! 每个 Worker 可以由多个协程组成。
//! `WorkerFactory::spawn_worker()` 返回 `Vec<JoinHandle<()>>`，
//! Pool 会等待所有协程退出才认为该 Worker 已关闭。
//!
//! # 设计原则
//!
//! 此模块**不**包含任务类型（Task）、结果类型（Result）、上下文（Context）、
//! 统计（Stats）等概念。这些属于具体的业务模块（如下载模块）。
//! 此模块仅负责：
//! - Worker 的 spawn 和 shutdown
//! - JoinHandle 的管理和等待

use deferred_map::DeferredMap;
use log::{debug, error, info};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

/// Worker 标识符
///
/// 封装两种 ID：
/// - `slot_id`: DeferredMap 内部分配的 ID，用于槽位管理
/// - `pool_id`: 池内唯一 ID，单调递增，用于日志和外部标识
///
/// # 设计说明
///
/// - `slot_id` 可能被复用（当 Worker 被移除后，其槽位可能分配给新 Worker）
/// - `pool_id` 在池内永不复用，保证池内唯一性
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId {
    /// DeferredMap 槽位 ID（用于内部管理）
    slot_id: u64,
    /// 池内唯一 ID（单调递增，用于日志和外部标识）
    pool_id: u64,
}

impl WorkerId {
    /// 创建新的 WorkerId
    ///
    /// # Arguments
    ///
    /// - `slot_id`: DeferredMap 分配的槽位 ID
    /// - `pool_id`: 池内唯一 ID（由 WorkerPool 分配）
    pub fn new(slot_id: u64, pool_id: u64) -> Self {
        Self { slot_id, pool_id }
    }

    /// 获取 DeferredMap 槽位 ID
    #[inline]
    pub fn slot_id(&self) -> u64 {
        self.slot_id
    }

    /// 获取池内唯一 ID
    #[inline]
    pub fn pool_id(&self) -> u64 {
        self.pool_id
    }
}

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.pool_id)
    }
}

/// Worker 工厂 trait
///
/// 定义了如何启动 Worker 协程组。此 trait 是轻量级的，
/// 仅负责 spawn 协程，不包含任务/结果/上下文/统计等业务类型。
///
/// # 关联类型
///
/// - `Input`: spawn_worker 所需的输入参数类型（由具体实现定义）
pub trait WorkerFactory: Send + Sync + 'static {
    /// spawn_worker 所需的输入参数类型
    ///
    /// 例如下载模块可能包含 (Context, Stats, Channels) 等
    type Input: Send + 'static;

    /// 启动 Worker 协程组
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `shutdown_rx`: 关闭信号接收通道
    /// - `input`: 具体实现所需的输入参数
    ///
    /// # Returns
    ///
    /// 返回该 Worker 的所有协程句柄。Pool 会等待所有协程退出才认为该 Worker 已关闭。
    fn spawn_worker(
        &self,
        worker_id: WorkerId,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        input: Self::Input,
    ) -> Vec<JoinHandle<()>>;
}

/// 单个 Worker 的槽位
///
/// 封装了与单个 worker 交互所需的所有信息。
/// 支持 Composite Worker 模式，一个 Worker 可包含多个协程。
pub struct WorkerSlot {
    /// 关闭通道的发送端（用于向 worker 发送关闭信号）
    pub(crate) shutdown_sender: tokio::sync::oneshot::Sender<()>,
    /// Worker 组的所有协程句柄（等待所有协程退出）
    pub(crate) join_handles: Vec<JoinHandle<()>>,
}

/// 通用 Worker 协程池
///
/// 管理多个 worker 协程的生命周期，专注于 spawn 和 shutdown。
/// 不包含任务分发、结果收集等业务逻辑。
///
/// # 泛型参数
///
/// - `F`: 工厂类型
///
/// # 核心功能
///
/// - 创建协程池
/// - 动态添加 worker
/// - 优雅关闭所有 worker
pub struct WorkerPool<F: WorkerFactory> {
    /// Worker 槽位存储（使用 DeferredMap）
    pub(crate) slots: DeferredMap<WorkerSlot>,
    /// 工厂（用于 spawn 新 worker）
    factory: F,
    /// 池内 Worker ID 计数器（每个池独立）
    next_worker_id: AtomicU64,
}

impl<F: WorkerFactory> WorkerPool<F> {
    /// 启动单个 worker（内部辅助方法）
    fn spawn_worker_internal(&self, worker_id: WorkerId, input: F::Input) -> WorkerSlot {
        // 创建关闭通道
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();

        // 让工厂启动协程组
        let join_handles = self
            .factory
            .spawn_worker(worker_id, shutdown_receiver, input);

        WorkerSlot {
            shutdown_sender,
            join_handles,
        }
    }

    /// 创建新的协程池
    ///
    /// # Arguments
    ///
    /// - `factory`: Worker 工厂
    /// - `inputs`: 每个 worker 的输入参数列表（数量决定初始 worker 数）
    ///
    /// # Returns
    ///
    /// 返回 (WorkerPool, 所有 worker 的 ID 列表)
    pub fn new(factory: F, inputs: Vec<F::Input>) -> (Self, Vec<WorkerId>) {
        let worker_count = inputs.len();
        let slots = DeferredMap::with_capacity(worker_count);

        let mut pool = Self {
            slots,
            factory,
            next_worker_id: AtomicU64::new(0),
        };

        let mut worker_ids = Vec::with_capacity(worker_count);
        for input in inputs {
            let deferred_handle = pool.slots.allocate_handle();
            let slot_id = deferred_handle.key();
            let pool_id = pool.next_worker_id.fetch_add(1, Ordering::Relaxed);
            let worker_id = WorkerId::new(slot_id, pool_id);

            let slot = pool.spawn_worker_internal(worker_id, input);
            pool.slots.insert(deferred_handle, slot);

            worker_ids.push(worker_id);
        }

        (pool, worker_ids)
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `inputs`: 要添加的 worker 的输入参数列表
    ///
    /// # Returns
    ///
    /// 返回新添加的 worker 的 ID 列表
    pub fn add_workers(&mut self, inputs: Vec<F::Input>) -> Vec<WorkerId> {
        let mut worker_ids = Vec::with_capacity(inputs.len());
        for input in inputs {
            let deferred_handle = self.slots.allocate_handle();
            let slot_id = deferred_handle.key();
            let pool_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
            let worker_id = WorkerId::new(slot_id, pool_id);

            let slot = self.spawn_worker_internal(worker_id, input);
            self.slots.insert(deferred_handle, slot);

            worker_ids.push(worker_id);
            debug!("新 worker 添加: {}", worker_id);
        }
        worker_ids
    }

    /// 获取当前 worker 总数
    pub fn worker_count(&self) -> u64 {
        self.slots.len() as u64
    }

    /// 优雅关闭所有 workers
    ///
    /// 通过 oneshot channel 向所有活跃的 workers 发送关闭信号
    /// 并异步等待所有 workers 真正退出
    pub async fn shutdown(&mut self) {
        info!("发送关闭信号到所有活跃 workers");

        let slot_ids: Vec<u64> = self.slots.iter().map(|(key, _)| key).collect();
        let closed_count = slot_ids.len();

        let mut all_join_handles = Vec::with_capacity(closed_count);

        for slot_id in slot_ids {
            if let Some(slot) = self.slots.remove(slot_id) {
                let _ = slot.shutdown_sender.send(());
                debug!("Worker (slot={}) 关闭信号已发送", slot_id);
                all_join_handles.push((slot_id, slot.join_handles));
            }
        }

        info!("等待 {} 个 workers 退出...", closed_count);

        for (slot_id, handles) in all_join_handles {
            for handle in handles {
                if let Err(e) = handle.await {
                    error!("Worker (slot={}) 协程退出错误: {:?}", slot_id, e);
                }
            }
            debug!("Worker (slot={}) 已退出", slot_id);
        }

        info!("所有 workers 已完全退出");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    /// 测试用的简单工厂
    struct TestFactory;

    impl WorkerFactory for TestFactory {
        type Input = Arc<AtomicUsize>;

        fn spawn_worker(
            &self,
            worker_id: WorkerId,
            mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
            counter: Self::Input,
        ) -> Vec<JoinHandle<()>> {
            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            debug!("Worker {} 收到关闭信号", worker_id);
                            break;
                        }
                        _ = sleep(Duration::from_millis(10)) => {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            });
            vec![handle]
        }
    }

    #[tokio::test]
    async fn test_create_worker_pool() {
        let factory = TestFactory;
        let inputs: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();

        let (mut pool, worker_ids) = WorkerPool::new(factory, inputs);
        assert_eq!(pool.worker_count(), 4);
        assert_eq!(worker_ids.len(), 4);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_empty_pool() {
        let factory = TestFactory;
        let (mut pool, worker_ids) = WorkerPool::new(factory, vec![]);

        assert_eq!(pool.worker_count(), 0);
        assert_eq!(worker_ids.len(), 0);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn test_add_workers() {
        let factory = TestFactory;
        let inputs: Vec<_> = (0..2).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let (mut pool, _) = WorkerPool::new(factory, inputs);

        assert_eq!(pool.worker_count(), 2);

        let new_inputs: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let new_ids = pool.add_workers(new_inputs);

        assert_eq!(pool.worker_count(), 5);
        assert_eq!(new_ids.len(), 3);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn test_worker_ids_unique() {
        let factory = TestFactory;
        let inputs: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let (mut pool, worker_ids) = WorkerPool::new(factory, inputs);

        // 验证 ID 是唯一的
        for i in 0..worker_ids.len() {
            for j in (i + 1)..worker_ids.len() {
                assert_ne!(worker_ids[i], worker_ids[j]);
            }
        }

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let factory = TestFactory;
        let counters: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let inputs = counters.clone();
        let (mut pool, _) = WorkerPool::new(factory, inputs);

        // 让 workers 运行一段时间
        sleep(Duration::from_millis(50)).await;

        // 关闭协程池
        pool.shutdown().await;

        // 验证所有 workers 已退出
        assert_eq!(pool.worker_count(), 0);

        // 验证 workers 确实执行了一些工作
        let total: usize = counters.iter().map(|c| c.load(Ordering::SeqCst)).sum();
        assert!(total > 0, "Workers 应该执行了一些工作");
    }
}
