//! 通用 Worker 协程池
//!
//! 提供完全泛型化的 Worker 协程池实现，支持任意任务类型和执行逻辑。
//!
//! # 核心概念
//!
//! - **WorkerTask**: 任务类型 trait，定义了 worker 处理的任务
//! - **WorkerResult**: 结果类型 trait，定义了 worker 返回的结果
//! - **WorkerContext**: 上下文类型 trait，每个 worker 独立持有的上下文（如统计、策略等）
//! - **WorkerExecutor**: 执行器 trait，定义了如何处理任务
//!
//! # 设计特点
//!
//! - **完全解耦**: 不依赖任何具体业务逻辑
//! - **类型安全**: 通过 trait bound 保证编译期正确性
//! - **灵活扩展**: 可用于任何需要并发处理任务的场景
//! - **优雅关闭**: 支持通过 oneshot channel 发送关闭信号
//!
//! # 使用示例
//!
//! ```ignore
//! // 1. 定义任务类型
//! #[derive(Debug)]
//! struct MyTask {
//!     data: String,
//! }
//! impl WorkerTask for MyTask {}
//!
//! // 2. 定义结果类型
//! #[derive(Debug)]
//! enum MyResult {
//!     Success { worker_id: usize },
//!     Failed { worker_id: usize, error: String },
//! }
//! impl WorkerResult for MyResult {
//!     fn worker_id(&self) -> usize {
//!         match self {
//!             MyResult::Success { worker_id } => *worker_id,
//!             MyResult::Failed { worker_id, .. } => *worker_id,
//!         }
//!     }
//!     fn is_success(&self) -> bool {
//!         matches!(self, MyResult::Success { .. })
//!     }
//! }
//!
//! // 3. 定义上下文（可选）
//! struct MyContext;
//! impl WorkerContext for MyContext {}
//!
//! // 4. 定义执行器
//! struct MyExecutor;
//! #[async_trait::async_trait]
//! impl WorkerExecutor<MyTask, MyResult, MyContext> for MyExecutor {
//!     async fn execute(&self, worker_id: usize, task: MyTask, _context: &MyContext) -> MyResult {
//!         // 处理任务...
//!         MyResult::Success { worker_id }
//!     }
//! }
//!
//! // 5. 创建协程池
//! let executor = Arc::new(MyExecutor);
//! let contexts = vec![MyContext; 4];
//! let pool = WorkerPool::new(executor, contexts);
//!
//! // 6. 发送任务
//! pool.send_task(MyTask { data: "test".to_string() }, 0).await?;
//!
//! // 7. 接收结果
//! if let Some(result) = pool.result_receiver().recv().await {
//!     println!("收到结果: {:?}", result);
//! }
//! ```

use async_trait::async_trait;
use arc_swap::ArcSwap;
use log::{debug, error, info};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Notify;

use crate::config::ConcurrencyDefaults;
use crate::{DownloadError, Result};
use super::worker_mask::WorkerMask;

/// Worker 槽位的包装类型，简化复杂类型定义
type WorkerSlotArc<T, S> = Arc<ArcSwap<Option<WorkerSlot<T, S>>>>;

/// Worker 任务 trait
///
/// 定义了 worker 处理的任务类型必须满足的约束
///
/// # 要求
///
/// - `Send`: 任务可以在线程间传递
/// - `Debug`: 任务可以被调试输出
pub trait WorkerTask: Send + Debug + 'static {}

/// Worker 结果 trait
///
/// 定义了 worker 返回的结果类型必须满足的约束
///
/// # 要求
///
/// - `Send`: 结果可以在线程间传递
/// - `Debug`: 结果可以被调试输出
pub trait WorkerResult: Send + Debug + 'static {}

/// Worker 统计 trait
///
/// 外部可访问的只读统计数据，支持多线程并发访问
///
/// # 要求
///
/// - `Send + Sync`: 统计数据可以在线程间安全共享（通过 Arc）
pub trait WorkerStats: Send + Sync + 'static {}

/// Worker 上下文 trait
///
/// 每个 worker 独立持有的上下文数据（如分块策略等）
/// Context 完全归 worker 所有，外部无法访问
///
/// # 要求
///
/// - `Send`: 上下文可以在线程间传递（worker 独占所有权）
pub trait WorkerContext: Send + 'static {}

/// Worker 执行器 trait
///
/// 定义了如何处理任务的具体逻辑
///
/// # 泛型参数
///
/// - `T`: 任务类型
/// - `R`: 结果类型
/// - `C`: 上下文类型
/// - `S`: 统计类型
///
/// # 要求
///
/// - `Send + Sync`: 执行器可以在多个 worker 间共享
#[async_trait]
pub trait WorkerExecutor<T: WorkerTask, R: WorkerResult, C: WorkerContext, S: WorkerStats>: Send + Sync + 'static {
    /// 执行任务
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `task`: 要处理的任务
    /// - `context`: Worker 的上下文（可用于访问策略等，worker 独占）
    /// - `stats`: Worker 的统计数据（外部可访问，只读）
    ///
    /// # Returns
    ///
    /// 任务执行结果
    async fn execute(&self, worker_id: usize, task: T, context: &mut C, stats: &S) -> R;
}

/// Worker 配置
///
/// 封装启动单个 worker 所需的所有参数
pub(crate) struct WorkerConfig<T: WorkerTask, R: WorkerResult, C: WorkerContext, S: WorkerStats, E: WorkerExecutor<T, R, C, S> + ?Sized> {
    pub id: usize,
    pub executor: Arc<E>,
    pub task_receiver: Receiver<T>,
    pub result_sender: Sender<R>,
    pub context: C,
    pub stats: Arc<S>,
    pub shutdown_receiver: tokio::sync::oneshot::Receiver<()>,
}

/// Worker 协程主循环
///
/// 每个 worker 持有一个独立的 channel receiver，循环等待任务
/// 通过 executor 处理任务，并将结果发送到统一的 result channel
/// 当 task channel 关闭或收到关闭信号时自动退出
///
/// # 泛型参数
///
/// - `T`: 任务类型
/// - `R`: 结果类型
/// - `C`: 上下文类型
/// - `S`: 统计类型
/// - `E`: 执行器类型
pub(crate) async fn run_worker<T, R, C, S, E>(config: WorkerConfig<T, R, C, S, E>)
where
    T: WorkerTask,
    R: WorkerResult,
    C: WorkerContext,
    S: WorkerStats,
    E: WorkerExecutor<T, R, C, S> + ?Sized,
{
    let WorkerConfig {
        id,
        executor,
        mut task_receiver,
        result_sender,
        mut context,
        stats,
        mut shutdown_receiver,
    } = config;

    info!("Worker #{} 启动", id);

    // 使用 select! 同时监听任务通道和关闭通道
    loop {
        tokio::select! {
            _ = &mut shutdown_receiver => {
                info!("Worker #{} 收到关闭信号，立即退出", id);
                break;
            }
            Some(task) = task_receiver.recv() => {
                debug!("Worker #{} 接收到任务: {:?}", id, task);

                // 执行任务
                let result = executor.execute(id, task, &mut context, &stats).await;

                // 发送结果
                if let Err(e) = result_sender.send(result).await {
                    error!("Worker #{} 发送结果失败: {:?}", id, e);
                }
            }
            
            else => {
                // task channel 关闭，正常退出
                info!("Worker #{} 任务通道关闭，退出", id);
                break;
            }
        }
    }

    info!("Worker #{} 退出", id);
}

/// 单个 Worker 的槽位
///
/// 封装了与单个 worker 交互所需的所有信息
pub struct WorkerSlot<T: WorkerTask, S: WorkerStats> {
    /// 向 worker 发送任务的通道
    pub(crate) task_sender: Sender<T>,
    /// 该 worker 的统计数据（Arc 包装以便外部访问）
    pub(crate) stats: Arc<S>,
    /// 关闭通道的发送端（用于向 worker 发送关闭信号）
    pub(crate) shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}


/// Worker 自动清理器
/// 
/// 封装 worker 退出时的资源清理逻辑
pub(crate) struct WorkerCleanup<T: WorkerTask, S: WorkerStats> {
    /// Worker ID
    worker_id: usize,
    /// 该 worker 的 slot 引用（而非整个 workers 数组）
    slot: Arc<ArcSwap<Option<WorkerSlot<T, S>>>>,
    /// Worker 槽位空闲位掩码
    free_mask: Arc<WorkerMask>,
    /// Worker 全部退出通知器
    shutdown_notify: Arc<Notify>,
}

impl<T: WorkerTask, S: WorkerStats> WorkerCleanup<T, S> {
    /// 执行清理逻辑
    pub(crate) fn cleanup(self) {
        let worker_id = self.worker_id;
        info!("Worker #{} 开始自动清理资源", worker_id);
        
        // 清空自己的 slot
        self.slot.swap(Arc::new(None));
        
        // 释放位掩码
        if let Err(e) = self.free_mask.free(worker_id) {
            error!("Worker #{} 释放位掩码失败: {:?}", worker_id, e);
        } else {
            // 检查是否所有 worker 都已退出
            if self.free_mask.is_empty() {
                info!("所有 workers 已完成清理");
                self.shutdown_notify.notify_waiters();
            } else {
                debug!("Worker #{} 资源清理完成，剩余 {} 个运行中的 workers", 
                    worker_id, self.free_mask.count());
            }
        }
    }
}

/// 通用 Worker 协程池
///
/// 管理多个 worker 协程的生命周期，支持任务分发、结果收集和动态扩展
///
/// # 泛型参数
///
/// - `T`: 任务类型
/// - `R`: 结果类型
/// - `C`: 上下文类型
/// - `S`: 统计类型
///
/// # 核心功能
///
/// - 创建协程池（初始 worker 数量）
/// - 发送任务到指定 worker
/// - 接收结果（统一 result channel）
/// - 动态添加 worker
/// - 访问 worker 统计数据
/// - Worker 自动清理（退出时自动释放资源）
pub struct WorkerPool<T: WorkerTask, R: WorkerResult, C: WorkerContext, S: WorkerStats> {
    /// Worker 槽位数组（索引即为 worker_id，None 表示该位置空闲）
    /// 使用固定大小数组和 ArcSwap 实现无锁并发访问
    /// ArcSwap<T> = ArcSwapAny<Arc<T>>，所以这里是 Arc<Option<WorkerSlot>>
    /// 外层 Arc 允许在 tokio::spawn 闭包中访问（worker 自动清理需要）
    pub(crate) workers: [WorkerSlotArc<T, S>; ConcurrencyDefaults::MAX_WORKER_COUNT],
    /// 统一的结果接收器（所有 worker 共享）
    result_receiver: Receiver<R>,
    /// 结果发送器的克隆（用于创建新 worker）
    result_sender: Sender<R>,
    /// 执行器（所有 worker 共享）
    pub(crate) executor: Arc<dyn WorkerExecutor<T, R, C, S>>,
    /// Worker 槽位空闲位掩码（1 表示已占用，0 表示空闲）
    /// 用于快速查找空闲槽位和判断是否所有 worker 都已退出
    free_mask: Arc<WorkerMask>,
    /// Worker 全部退出通知器
    shutdown_notify: Arc<Notify>,
}

impl<T, R, C, S> WorkerPool<T, R, C, S>
where
    T: WorkerTask,
    R: WorkerResult,
    C: WorkerContext,
    S: WorkerStats,
{
    /// 启动单个 worker（内部辅助方法）
    /// 
    /// 封装创建和启动 worker 的通用逻辑，避免在 new 和 add_workers 中重复
    fn spawn_worker(
        &self,
        worker_id: usize,
        context: C,
        stats_arc: Arc<S>,
    ) {
        // 创建任务和关闭通道
        let (task_sender, task_receiver) = mpsc::channel::<T>(100);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        
        // 克隆 worker 需要的资源（直接从 self 获取，减少一层克隆）
        let executor_clone = Arc::clone(&self.executor);
        let result_sender_clone = self.result_sender.clone();
        let stats_for_worker = Arc::clone(&stats_arc);
        
        // 创建 WorkerSlot 并存入 workers 数组
        let slot = WorkerSlot {
            task_sender,
            stats: stats_arc,
            shutdown_sender: Some(shutdown_sender),
        };
        self.workers[worker_id].store(Arc::new(Some(slot)));
        
        // 创建清理器（只持有单个 slot 的引用）
        let slot_ref = Arc::clone(&self.workers[worker_id]);
        let cleanup = WorkerCleanup {
            worker_id,
            slot: slot_ref,
            free_mask: Arc::clone(&self.free_mask),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
        };
        
        // 启动 worker 协程（包含自动清理逻辑）
        tokio::spawn(async move {
            // 执行 worker 主循环（context 直接 move，stats 通过 Arc 共享）
            run_worker(WorkerConfig {
                id: worker_id,
                executor: executor_clone,
                task_receiver,
                result_sender: result_sender_clone,
                context,
                stats: stats_for_worker,
                shutdown_receiver,
            })
            .await;
            
            // worker 退出后自动清理
            cleanup.cleanup();
        });
    }

    /// 创建新的协程池
    ///
    /// # Arguments
    ///
    /// - `executor`: 任务执行器（所有 worker 共享）
    /// - `contexts_with_stats`: 每个 worker 的独立上下文和统计数据（数量决定初始 worker 数）
    ///
    /// # Returns
    ///
    /// 新创建的 WorkerPool
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = Arc::new(MyExecutor);
    /// let contexts_with_stats = vec![(MyContext::new(), Arc::new(MyStats::new())); 4];
    /// let pool = WorkerPool::new(executor, contexts_with_stats);
    /// ```
    pub fn new<E>(executor: Arc<E>, contexts_with_stats: Vec<(C, Arc<S>)>) -> Result<Self>
    where
        E: WorkerExecutor<T, R, C, S>,
    {
        let worker_count = contexts_with_stats.len();
        
        // 验证 worker 数量不超过最大限制
        if worker_count > ConcurrencyDefaults::MAX_WORKER_COUNT {
            return Err(crate::DownloadError::WorkerCountExceeded(worker_count, ConcurrencyDefaults::MAX_WORKER_COUNT));
        }
        
        // 创建统一的 result channel（所有 worker 共享同一个 sender）
        let (result_sender, result_receiver) = mpsc::channel::<R>(100);
        
        // 将具体类型的 executor 转换为 trait object
        let executor_trait_obj: Arc<dyn WorkerExecutor<T, R, C, S>> = executor;
        
        // 初始化空闲位掩码
        let free_mask = Arc::new(WorkerMask::new(worker_count)?);
        
        // 初始化通知器
        let shutdown_notify = Arc::new(Notify::new());
        
        // 使用 array::from_fn 初始化固定大小数组（暂时全部为 None）
        let workers: [WorkerSlotArc<T, S>; ConcurrencyDefaults::MAX_WORKER_COUNT] = 
            std::array::from_fn(|_| Arc::new(ArcSwap::new(Arc::new(None))));
        
        // 创建 pool 实例（先不启动 workers）
        let pool = Self {
            workers,
            result_receiver,
            result_sender: result_sender.clone(),
            executor: executor_trait_obj,
            free_mask: Arc::clone(&free_mask),
            shutdown_notify: Arc::clone(&shutdown_notify),
        };
        
        // 为每个 worker 创建独立的 task channel、上下文和统计，并启动协程
        for (id, (context, stats_arc)) in contexts_with_stats.into_iter().enumerate() {
            pool.spawn_worker(id, context, stats_arc);
        }
        
        info!("创建协程池，{} 个初始 workers", worker_count);

        Ok(pool)
    }

    /// 动态添加新的 worker
    ///
    /// 优先填充空缺的 worker_id 位置（索引小的优先），如果没有空位则返回错误
    ///
    /// # Arguments
    ///
    /// - `contexts_with_stats`: 要添加的 worker 的上下文和统计数据列表
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，失败时返回错误信息
    ///
    /// # Example
    ///
    /// ```ignore
    /// let new_contexts = vec![(MyContext::new(), Arc::new(MyStats::new())); 2];
    /// pool.add_workers(new_contexts)?;
    /// ```
    pub async fn add_workers(&mut self, contexts_with_stats: Vec<(C, Arc<S>)>) -> Result<()> {
        let count = contexts_with_stats.len();
        let current_active = self.worker_count();
        
        info!("动态添加 {} 个新 workers (当前活跃 {} 个)", count, current_active);
        
        for (context, stats_arc) in contexts_with_stats.into_iter() {
            // 使用位掩码快速查找第一个空位
            let worker_id = self.free_mask.allocate()?;
            
            // 启动 worker（使用统一的方法）
            self.spawn_worker(worker_id, context, stats_arc);
            
            debug!("新 worker 添加到位置 #{}", worker_id);
        }
        
        let new_active = self.worker_count();
        info!("成功添加 {} 个新 workers，当前活跃 {} 个", count, new_active);
        Ok(())
    }
    
    /// 关闭指定的 worker
    ///
    /// 通过 oneshot channel 向 worker 发送关闭信号，worker 收到信号后立即退出并自动清理
    ///
    /// # Arguments
    ///
    /// - `worker_id`: 要关闭的 worker ID
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，如果 worker 不存在则返回 `Err(DownloadError::WorkerNotFound)`
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.shutdown_worker(0)?;
    /// ```
    ///
    /// # Note
    ///
    /// 此方法不会等待 worker 退出，worker 会在收到关闭信号后异步自动清理
    #[allow(dead_code)]
    pub fn shutdown_worker(&self, worker_id: usize) -> Result<()> {
        info!("开始关闭 Worker #{}", worker_id);
        
        // 检查 worker_id 是否在范围内
        if worker_id >= ConcurrencyDefaults::MAX_WORKER_COUNT {
            return Err(crate::DownloadError::WorkerNotFound(worker_id));
        }
        
        // 原子性地取出 worker slot（swap 为 None）
        let old_slot_arc = self.workers[worker_id].swap(Arc::new(None));
        
        // 检查 worker 是否存在并发送关闭信号
        if let Some(mut slot) = Arc::try_unwrap(old_slot_arc).ok().and_then(|s| s) {
            // 取出 shutdown_sender 并发送关闭信号
            if let Some(sender) = slot.shutdown_sender.take() {
                // 忽略发送错误（worker 可能已经退出）
                let _ = sender.send(());
                info!("Worker #{} 关闭信号已发送", worker_id);
            } else {
                info!("Worker #{} 的关闭信号已被消费", worker_id);
            }
        } else {
            return Err(crate::DownloadError::WorkerNotFound(worker_id));
        }
        
        Ok(())
    }
    
    /// 获取当前活跃 worker 总数
    /// 
    /// 使用位掩码快速计算，O(1) 时间复杂度
    pub fn worker_count(&self) -> usize {
        self.free_mask.count()
    }

    /// 提交任务给指定的 worker
    ///
    /// # Arguments
    ///
    /// - `task`: 要执行的任务
    /// - `worker_id`: 目标 worker 的 ID
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，如果 worker 不存在则返回 `Err(DownloadError::WorkerNotFound)`
    pub async fn send_task(&self, task: T, worker_id: usize) -> Result<()> {
        // 检查 worker_id 是否在范围内
        if worker_id >= ConcurrencyDefaults::MAX_WORKER_COUNT {
            return Err(crate::DownloadError::WorkerNotFound(worker_id));
        }
        
        // 获取 worker slot 并发送任务
        // load() 返回 Arc<Option<WorkerSlot>>
        let slot_arc = self.workers[worker_id].load();
        let slot = slot_arc
            .as_ref()
            .as_ref()
            .ok_or(crate::DownloadError::WorkerNotFound(worker_id))?;
        
        slot.task_sender
            .send(task)
            .await
            .map_err(|e| DownloadError::TaskSend(e.to_string()))?;
        Ok(())
    }

    /// 获取结果接收器的可变引用
    ///
    /// 允许调用者接收 worker 返回的结果
    ///
    /// # Returns
    ///
    /// 结果接收器的可变引用
    pub fn result_receiver(&mut self) -> &mut Receiver<R> {
        &mut self.result_receiver
    }

    /// 获取指定 worker 的统计数据引用
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    ///
    /// # Returns
    ///
    /// Worker 统计数据的 Arc 引用，如果 worker 不存在则返回 `None`
    pub fn worker_stats(&self, worker_id: usize) -> Option<Arc<S>> {
        if worker_id >= ConcurrencyDefaults::MAX_WORKER_COUNT {
            return None;
        }
        
        // load() 返回 Arc<Option<WorkerSlot>>
        let slot_arc = self.workers[worker_id].load();
        slot_arc.as_ref().as_ref().map(|slot| Arc::clone(&slot.stats))
    }

    /// 优雅关闭所有 workers
    ///
    /// 通过 oneshot channel 向所有活跃的 workers 发送关闭信号
    /// Workers 收到信号后立即退出并自动清理
    /// 
    /// # Note
    ///
    /// 此方法不会等待 workers 退出，workers 会异步自动清理
    /// 使用 `wait_for_shutdown()` 方法等待所有 workers 完成清理
    pub fn shutdown(&mut self) {
        info!("发送关闭信号到所有活跃 workers");
        
        let mut closed_count = 0;
        
        for id in 0..ConcurrencyDefaults::MAX_WORKER_COUNT {
            // 原子性地取出 worker slot（swap 为 None）
            let old_slot_arc = self.workers[id].swap(Arc::new(None));
            
            // 检查 worker 是否存在并发送关闭信号
            if let Some(mut slot) = Arc::try_unwrap(old_slot_arc).ok().and_then(|s| s) {
                // 取出 shutdown_sender 并发送关闭信号
                if let Some(sender) = slot.shutdown_sender.take() {
                    // 忽略发送错误（worker 可能已经退出）
                    let _ = sender.send(());
                    debug!("Worker #{} 关闭信号已发送", id);
                    closed_count += 1;
                }
            }
        }
        
        info!("已向 {} 个 workers 发送关闭信号，它们将自动退出并清理", closed_count);
    }
    
    /// 等待所有 workers 完成清理
    ///
    /// 此方法会阻塞直到所有运行中的 workers 都完成了自动清理
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.shutdown();
    /// pool.wait_for_shutdown().await;
    /// ```
    pub async fn wait_for_shutdown(&self) {
        // 如果已经没有运行中的 worker，直接返回
        if self.free_mask.is_empty() {
            debug!("没有运行中的 workers，无需等待");
            return;
        }
        
        info!("等待所有 workers 完成清理...");
        self.shutdown_notify.notified().await;
        info!("所有 workers 已完成清理");
    }
}

pub mod test_utils {
    //! 公共测试套件
    //! 
    //! 提供可复用的测试类型、执行器和辅助函数，供 common.rs 的单元测试
    //! 和 tests/ 目录下的集成测试使用
    
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    /// 测试用的任务类型
    #[derive(Debug, Clone)]
    pub struct TestTask {
        pub id: usize,
        pub data: String,
    }
    impl WorkerTask for TestTask {}

    /// 测试用的结果类型
    #[derive(Debug)]
    pub enum TestResult {
        Success { worker_id: usize, task_id: usize },
        Failed { worker_id: usize, error: String },
    }
    impl WorkerResult for TestResult {}

    /// 测试用的上下文
    pub struct TestContext {
        pub processed_count: AtomicUsize,
    }
    impl WorkerContext for TestContext {}
    
    impl Default for TestContext {
        fn default() -> Self {
            Self { processed_count: AtomicUsize::new(0) }
        }
    }
    
    impl TestContext {
        pub fn new() -> Self {
            Self::default()
        }
    }
    
    /// 测试用的统计
    pub struct TestStats {
        pub task_count: AtomicUsize,
    }
    impl WorkerStats for TestStats {}
    
    impl Default for TestStats {
        fn default() -> Self {
            Self { task_count: AtomicUsize::new(0) }
        }
    }
    
    impl TestStats {
        pub fn new() -> Self {
            Self::default()
        }
    }

    /// 测试用的成功执行器
    pub struct TestExecutor;
    
    #[async_trait]
    impl WorkerExecutor<TestTask, TestResult, TestContext, TestStats> for TestExecutor {
        async fn execute(&self, worker_id: usize, task: TestTask, context: &mut TestContext, stats: &TestStats) -> TestResult {
            // 模拟处理任务
            sleep(Duration::from_millis(10)).await;
            context.processed_count.fetch_add(1, Ordering::SeqCst);
            stats.task_count.fetch_add(1, Ordering::SeqCst);
            TestResult::Success {
                worker_id,
                task_id: task.id,
            }
        }
    }

    /// 测试用的失败执行器
    pub struct FailingExecutor;
    
    #[async_trait]
    impl WorkerExecutor<TestTask, TestResult, TestContext, TestStats> for FailingExecutor {
        async fn execute(&self, worker_id: usize, _task: TestTask, _context: &mut TestContext, _stats: &TestStats) -> TestResult {
            TestResult::Failed {
                worker_id,
                error: "intentional failure".to_string(),
            }
        }
    }
    
    /// 创建测试用的上下文和统计数据
    pub fn create_context_with_stats() -> (TestContext, Arc<TestStats>) {
        (TestContext::new(), Arc::new(TestStats::new()))
    }
    
    /// 创建指定数量的测试用上下文和统计数据
    pub fn create_contexts_with_stats(count: usize) -> Vec<(TestContext, Arc<TestStats>)> {
        (0..count).map(|_| create_context_with_stats()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;
    use std::sync::atomic::Ordering;

    // ==================== 基础单元测试 ====================
    // 这些测试验证 WorkerPool 的核心功能
    
    #[tokio::test]
    async fn test_create_pool() {
        let executor = Arc::new(TestExecutor);
        let contexts_with_stats = create_contexts_with_stats(2);
        let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();
        assert_eq!(pool.worker_count(), 2);
    }

    #[tokio::test]
    async fn test_send_and_receive_task() {
        let executor = Arc::new(TestExecutor);
        let contexts_with_stats = create_contexts_with_stats(1);
        let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

        let task = TestTask { id: 1, data: "test".to_string() };
        pool.send_task(task, 0).await.unwrap();

        let result = pool.result_receiver().recv().await;
        assert!(result.is_some());
        
        match result.unwrap() {
            TestResult::Success { worker_id, task_id } => {
                assert_eq!(worker_id, 0);
                assert_eq!(task_id, 1);
            }
            TestResult::Failed { .. } => panic!("任务不应该失败"),
        }
    }

    #[tokio::test]
    async fn test_add_workers() {
        let executor = Arc::new(TestExecutor);
        let contexts_with_stats = create_contexts_with_stats(1);
        let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

        assert_eq!(pool.worker_count(), 1);

        let new_contexts_with_stats = create_contexts_with_stats(2);
        pool.add_workers(new_contexts_with_stats).await.unwrap();

        assert_eq!(pool.worker_count(), 3);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let executor = Arc::new(TestExecutor);
        let contexts_with_stats = create_contexts_with_stats(2);
        let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

        pool.shutdown();
        pool.wait_for_shutdown().await;

        // 验证所有 worker 都已被移除
        for worker_slot in pool.workers.iter() {
            assert!(worker_slot.load().is_none());
        }
    }

    #[tokio::test]
    async fn test_worker_stats() {
        let executor = Arc::new(TestExecutor);
        let contexts_with_stats = create_contexts_with_stats(1);
        let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

        let task = TestTask { id: 1, data: "test".to_string() };
        pool.send_task(task, 0).await.unwrap();
        let _ = pool.result_receiver().recv().await;

        let stats = pool.worker_stats(0).unwrap();
        assert_eq!(stats.task_count.load(Ordering::SeqCst), 1);
    }

}

