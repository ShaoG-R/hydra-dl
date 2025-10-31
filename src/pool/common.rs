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
//! #[derive(Debug, Clone)]
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
use log::{debug, error, info, warn};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::config::MAX_WORKER_COUNT;
use crate::{DownloadError, Result};

/// Worker 任务 trait
///
/// 定义了 worker 处理的任务类型必须满足的约束
///
/// # 要求
///
/// - `Send`: 任务可以在线程间传递
/// - `Clone`: 任务可以被克隆（用于重试等场景）
/// - `Debug`: 任务可以被调试输出
pub trait WorkerTask: Send + Clone + Debug + 'static {}

/// Worker 结果 trait
///
/// 定义了 worker 返回的结果类型必须满足的约束
///
/// # 要求
///
/// - `Send`: 结果可以在线程间传递
/// - `Debug`: 结果可以被调试输出
pub trait WorkerResult: Send + Debug + 'static {}

/// Worker 上下文 trait
///
/// 每个 worker 独立持有的上下文数据（如统计信息、策略等）
///
/// # 要求
///
/// - `Send + Sync`: 上下文可以在线程间安全共享
pub trait WorkerContext: Send + Sync + 'static {}

/// Worker 执行器 trait
///
/// 定义了如何处理任务的具体逻辑
///
/// # 泛型参数
///
/// - `T`: 任务类型
/// - `R`: 结果类型
/// - `C`: 上下文类型
///
/// # 要求
///
/// - `Send + Sync`: 执行器可以在多个 worker 间共享
#[async_trait]
pub trait WorkerExecutor<T: WorkerTask, R: WorkerResult, C: WorkerContext>: Send + Sync + 'static {
    /// 执行任务
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `task`: 要处理的任务
    /// - `context`: Worker 的上下文（可用于访问统计、策略等）
    ///
    /// # Returns
    ///
    /// 任务执行结果
    async fn execute(&self, worker_id: usize, task: T, context: &C) -> R;
}

/// Worker 协程主循环
///
/// 每个 worker 持有一个独立的 channel receiver，循环等待任务
/// 通过 executor 处理任务，并将结果发送到统一的 result channel
/// 支持通过 oneshot channel 接收高优先级关闭信号
/// 当 channel 关闭或收到关闭信号时退出
///
/// # 泛型参数
///
/// - `T`: 任务类型
/// - `R`: 结果类型
/// - `C`: 上下文类型
/// - `E`: 执行器类型
pub(crate) async fn run_worker<T, R, C, E>(
    id: usize,
    executor: Arc<E>,
    mut task_receiver: Receiver<T>,
    result_sender: Sender<R>,
    mut shutdown_receiver: oneshot::Receiver<()>,
    context: Arc<C>,
)
where
    T: WorkerTask,
    R: WorkerResult,
    C: WorkerContext,
    E: WorkerExecutor<T, R, C> + ?Sized,
{
    info!("Worker #{} 启动", id);

    // 循环接收并处理任务，同时监听关闭信号
    loop {
        tokio::select! {
            // 高优先级：关闭信号（使用 biased 确保优先处理）
            _ = &mut shutdown_receiver => {
                info!("Worker #{} 收到关闭信号，准备退出", id);
                break;
            }

            // 正常任务处理
            task = task_receiver.recv() => {
                match task {
                    Some(task) => {
                        debug!("Worker #{} 接收到任务: {:?}", id, task);

                        // 执行任务
                        let result = executor.execute(id, task, &context).await;

                        // 发送结果
                        if let Err(e) = result_sender.send(result).await {
                            error!("Worker #{} 发送结果失败: {:?}", id, e);
                        }
                    }
                    None => {
                        // task channel 关闭，正常退出
                        info!("Worker #{} 任务通道关闭，退出", id);
                        break;
                    }
                }
            }
        }
    }

    info!("Worker #{} 退出", id);
}

/// 单个 Worker 的槽位
///
/// 封装了与单个 worker 交互所需的所有信息
pub struct WorkerSlot<T: WorkerTask, C: WorkerContext> {
    /// 向 worker 发送任务的通道
    pub(crate) task_sender: Sender<T>,
    /// 向 worker 发送关闭信号的 oneshot channel
    pub(crate) shutdown_sender: Option<oneshot::Sender<()>>,
    /// 该 worker 的独立上下文（Arc 包装以便在 worker 协程中共享）
    pub(crate) context: Arc<C>,
    /// Worker 协程的句柄
    pub(crate) handle: JoinHandle<()>,
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
///
/// # 核心功能
///
/// - 创建协程池（初始 worker 数量）
/// - 发送任务到指定 worker
/// - 接收结果（统一 result channel）
/// - 动态添加 worker
/// - 优雅关闭（oneshot channel）
/// - 访问 worker 上下文
/// - 关闭单个 worker
pub struct WorkerPool<T: WorkerTask, R: WorkerResult, C: WorkerContext> {
    /// Worker 槽位数组（索引即为 worker_id，None 表示该位置空闲）
    /// 使用固定大小数组和 ArcSwap 实现无锁并发访问
    /// ArcSwap<T> = ArcSwapAny<Arc<T>>，所以这里是 Arc<Option<WorkerSlot>>
    pub(crate) workers: [ArcSwap<Option<WorkerSlot<T, C>>>; MAX_WORKER_COUNT],
    /// 统一的结果接收器（所有 worker 共享）
    result_receiver: Receiver<R>,
    /// 结果发送器的克隆（用于创建新 worker）
    result_sender: Sender<R>,
    /// 执行器（所有 worker 共享）
    pub(crate) executor: Arc<dyn WorkerExecutor<T, R, C>>,
}

impl<T, R, C> WorkerPool<T, R, C>
where
    T: WorkerTask,
    R: WorkerResult,
    C: WorkerContext,
{
    /// 创建新的协程池
    ///
    /// # Arguments
    ///
    /// - `executor`: 任务执行器（所有 worker 共享）
    /// - `contexts`: 每个 worker 的独立上下文（数量决定初始 worker 数）
    ///
    /// # Returns
    ///
    /// 新创建的 WorkerPool
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = Arc::new(MyExecutor);
    /// let contexts = vec![MyContext::new(); 4];
    /// let pool = WorkerPool::new(executor, contexts);
    /// ```
    pub fn new<E>(executor: Arc<E>, contexts: Vec<C>) -> Result<Self>
    where
        E: WorkerExecutor<T, R, C>,
    {
        let worker_count = contexts.len();
        
        // 验证 worker 数量不超过最大限制
        if worker_count > MAX_WORKER_COUNT {
            return Err(crate::DownloadError::WorkerCountExceeded(worker_count, MAX_WORKER_COUNT));
        }
        
        // 创建统一的 result channel（所有 worker 共享同一个 sender）
        let (result_sender, result_receiver) = mpsc::channel::<R>(100);
        
        // 将具体类型的 executor 转换为 trait object
        let executor_trait_obj: Arc<dyn WorkerExecutor<T, R, C>> = executor;
        
        // 预先创建所有 worker slots
        let mut worker_slots: Vec<Option<WorkerSlot<T, C>>> = Vec::with_capacity(worker_count);
        
        // 为每个 worker 创建独立的 task channel、shutdown channel 和上下文
        for (id, context) in contexts.into_iter().enumerate() {
            let (task_sender, task_receiver) = mpsc::channel::<T>(100);
            let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
            
            // 将上下文包装在 Arc 中
            let context_arc = Arc::new(context);
            
            // 克隆共享资源给每个 worker
            let executor_clone = Arc::clone(&executor_trait_obj);
            let result_sender_clone = result_sender.clone();
            let context_for_worker = Arc::clone(&context_arc);
            
            // 启动 worker 协程
            let handle = tokio::spawn(async move {
                run_worker(id, executor_clone, task_receiver, result_sender_clone, shutdown_receiver, context_for_worker).await;
            });

            worker_slots.push(Some(WorkerSlot {
                task_sender,
                shutdown_sender: Some(shutdown_sender),
                context: context_arc,
                handle,
            }));
        }

        // 使用 array::from_fn 初始化固定大小数组
        let workers = std::array::from_fn(|i| {
            if i < worker_count {
                ArcSwap::new(Arc::new(worker_slots[i].take()))
            } else {
                ArcSwap::new(Arc::new(None))
            }
        });

        info!("创建协程池，{} 个初始 workers", worker_count);

        Ok(Self {
            workers,
            result_receiver,
            result_sender,
            executor: executor_trait_obj,
        })
    }

    /// 动态添加新的 worker
    ///
    /// 优先填充空缺的 worker_id 位置（索引小的优先），如果没有空位则返回错误
    ///
    /// # Arguments
    ///
    /// - `contexts`: 要添加的 worker 的上下文列表
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，失败时返回错误信息
    ///
    /// # Example
    ///
    /// ```ignore
    /// let new_contexts = vec![MyContext::new(); 2];
    /// pool.add_workers(new_contexts)?;
    /// ```
    pub async fn add_workers(&mut self, contexts: Vec<C>) -> Result<()> {
        let count = contexts.len();
        let current_active = self.worker_count();
        
        info!("动态添加 {} 个新 workers (当前活跃 {} 个)", count, current_active);
        
        for context in contexts.into_iter() {
            // 查找第一个空位
            let worker_id = (0..MAX_WORKER_COUNT)
                .find(|&i| self.workers[i].load().is_none())
                .ok_or_else(|| {
                    DownloadError::WorkerPoolFull(MAX_WORKER_COUNT)
                })?;
            
            let (task_sender, task_receiver) = mpsc::channel::<T>(100);
            let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
            
            // 将上下文包装在 Arc 中
            let context_arc = Arc::new(context);
            
            // 克隆共享资源给新 worker
            let executor_clone = Arc::clone(&self.executor);
            let result_sender_clone = self.result_sender.clone();
            let context_for_worker = Arc::clone(&context_arc);
            
            // 启动 worker 协程
            let handle = tokio::spawn(async move {
                run_worker(worker_id, executor_clone, task_receiver, result_sender_clone, shutdown_receiver, context_for_worker).await;
            });
            
            // 将新 worker 放入找到的位置
            // ArcSwap::store 会自动包装成 Arc
            let new_slot = Some(WorkerSlot {
                task_sender,
                shutdown_sender: Some(shutdown_sender),
                context: context_arc,
                handle,
            });
            
            self.workers[worker_id].store(Arc::new(new_slot));
            
            debug!("新 worker 添加到位置 #{}", worker_id);
        }
        
        let new_active = self.worker_count();
        info!("成功添加 {} 个新 workers，当前活跃 {} 个", count, new_active);
        Ok(())
    }
    
    /// 关闭指定的 worker
    ///
    /// 发送关闭信号并等待 worker 完全退出后移除其资源
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
    /// pool.shutdown_worker(0).await?;
    /// ```
    #[allow(dead_code)]
    pub async fn shutdown_worker(&self, worker_id: usize) -> Result<()> {
        info!("开始关闭 Worker #{}", worker_id);
        
        // 检查 worker_id 是否在范围内
        if worker_id >= MAX_WORKER_COUNT {
            return Err(crate::DownloadError::WorkerNotFound(worker_id));
        }
        
        // 原子性地取出 worker slot（swap 为 None）
        let old_slot_arc = self.workers[worker_id].swap(Arc::new(None));
        
        // 拆包 Arc: Arc<Option<WorkerSlot>> -> Option<WorkerSlot>
        let slot_option = Arc::try_unwrap(old_slot_arc)
            .map_err(|_| crate::DownloadError::WorkerNotFound(worker_id))?;
        
        let mut slot = slot_option
            .ok_or(crate::DownloadError::WorkerNotFound(worker_id))?;
        
        // 发送关闭信号
        if let Some(shutdown_sender) = slot.shutdown_sender.take() {
            let _ = shutdown_sender.send(());
            debug!("已发送关闭信号到 Worker #{}", worker_id);
        }
        
        // 等待 worker 退出
        if let Err(e) = slot.handle.await {
            error!("Worker #{} 退出失败: {:?}", worker_id, e);
            return Err(crate::DownloadError::WorkerExit(worker_id));
        }
        
        info!("Worker #{} 已成功关闭并移除", worker_id);
        Ok(())
    }
    
    /// 获取当前活跃 worker 总数
    pub fn worker_count(&self) -> usize {
        self.workers.iter()
            .filter(|w| w.load().is_some())
            .count()
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
        if worker_id >= MAX_WORKER_COUNT {
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

    /// 获取指定 worker 的上下文引用
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    ///
    /// # Returns
    ///
    /// Worker 上下文的 Arc 引用，如果 worker 不存在则返回 `None`
    pub fn worker_context(&self, worker_id: usize) -> Option<Arc<C>> {
        if worker_id >= MAX_WORKER_COUNT {
            return None;
        }
        
        // load() 返回 Arc<Option<WorkerSlot>>
        let slot_arc = self.workers[worker_id].load();
        slot_arc.as_ref().as_ref().map(|slot| Arc::clone(&slot.context))
    }

    /// 优雅关闭所有 workers
    ///
    /// 发送关闭信号到所有活跃的 worker，让它们停止接收新任务并退出
    /// 
    /// 注意：由于 WorkerSlot 在 Arc 中，我们通过 swap 来取出所有权
    /// 这会清空所有 worker 槽位
    /// 优雅关闭所有 workers
    ///
    /// 发送关闭信号到所有活跃的 worker，让它们停止接收新任务并退出
    /// 
    /// 注意：由于 WorkerSlot 在 Arc 中，我们通过 swap 来取出所有权
    /// 这会清空所有 worker 槽位
    pub async fn shutdown(&mut self) {
        info!("发送关闭信号到所有活跃 workers");
        
        // 收集所有需要等待的 JoinHandle
        let mut handles = Vec::new();
        
        for (id, _worker) in self.workers.iter().enumerate() {
            // 直接 swap 取出 worker slot，不要提前 load（避免增加引用计数）
            let old_slot_arc = self.workers[id].swap(Arc::new(None));
            
            // 检查是否为 Some
            if old_slot_arc.is_some() {
                debug!("Worker #{} 存在，准备关闭", id);
                
                // 检查 Arc 引用计数
                let ref_count = Arc::strong_count(&old_slot_arc);
                debug!("Worker #{} slot Arc 引用计数: {}", id, ref_count);
                
                // 尝试拆包 Arc<Option<WorkerSlot>>
                match Arc::try_unwrap(old_slot_arc) {
                    Ok(slot_option) => {
                        debug!("Worker #{} Arc unwrap 成功", id);
                        if let Some(mut worker_slot) = slot_option {
                            if let Some(shutdown_sender) = worker_slot.shutdown_sender.take() {
                                // 忽略发送失败（worker 可能已经退出）
                                let _ = shutdown_sender.send(());
                                debug!("已发送关闭信号到 Worker #{}", id);
                            }
                            // 收集 JoinHandle 以便后续等待
                            handles.push((id, worker_slot.handle));
                        }
                    }
                    Err(arc) => {
                        let remaining_refs = Arc::strong_count(&arc);
                        warn!("Worker #{} Arc unwrap 失败，剩余引用计数: {}", id, remaining_refs);
                    }
                }
            }
        }
        
        // 等待所有 worker 协程退出
        info!("等待 {} 个 workers 退出", handles.len());
        for (id, handle) in handles {
            debug!("等待 Worker #{} 退出...", id);
            match handle.await {
                Ok(_) => debug!("Worker #{} 已退出", id),
                Err(e) => error!("Worker #{} 退出时出错: {:?}", id, e),
            }
        }
        info!("所有 workers 已退出")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    // 测试用的任务类型
    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct TestTask {
        id: usize,
        data: String,
    }
    impl WorkerTask for TestTask {}

    // 测试用的结果类型
    #[derive(Debug)]
    #[allow(dead_code)]
    enum TestResult {
        Success { worker_id: usize, task_id: usize },
        Failed { worker_id: usize, error: String },
    }
    
    impl WorkerResult for TestResult {}

    // 测试用的上下文
    struct TestContext {
        processed_count: AtomicUsize,
    }
    impl WorkerContext for TestContext {}

    // 测试用的执行器
    struct TestExecutor;
    
    #[async_trait]
    impl WorkerExecutor<TestTask, TestResult, TestContext> for TestExecutor {
        async fn execute(&self, worker_id: usize, task: TestTask, context: &TestContext) -> TestResult {
            // 模拟处理任务
            sleep(Duration::from_millis(10)).await;
            context.processed_count.fetch_add(1, Ordering::SeqCst);
            TestResult::Success {
                worker_id,
                task_id: task.id,
            }
        }
    }

    #[tokio::test]
    async fn test_worker_pool_creation() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let pool = WorkerPool::new(executor, contexts).unwrap();

        assert_eq!(pool.worker_count(), 2);
    }

    #[tokio::test]
    async fn test_worker_pool_send_and_receive() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let mut pool = WorkerPool::new(executor, contexts).unwrap();

        let task = TestTask {
            id: 1,
            data: "test".to_string(),
        };

        // 发送任务
        pool.send_task(task, 0).await.unwrap();

        // 接收结果
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
    async fn test_worker_pool_add_workers() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let mut pool = WorkerPool::new(executor, contexts).unwrap();

        assert_eq!(pool.worker_count(), 1);

        // 添加新 worker
        let new_contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        pool.add_workers(new_contexts).await.unwrap();

        assert_eq!(pool.worker_count(), 3);
    }

    #[tokio::test]
    async fn test_worker_pool_shutdown() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let mut pool = WorkerPool::new(executor, contexts).unwrap();

        // 关闭 workers
        pool.shutdown().await;

        // 验证所有 worker 都已被移除（slot 为 None）
        for worker_slot in pool.workers.iter() {
            assert!(worker_slot.load().is_none());
        }
    }

    #[tokio::test]
    async fn test_worker_context_access() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let mut pool = WorkerPool::new(executor, contexts).unwrap();

        // 发送任务
        let task = TestTask { id: 1, data: "test".to_string() };
        pool.send_task(task, 0).await.unwrap();

        // 等待处理
        let _ = pool.result_receiver().recv().await;

        // 验证上下文
        let context = pool.worker_context(0).unwrap();
        assert_eq!(context.processed_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_shutdown_single_worker() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let pool = WorkerPool::new(executor, contexts).unwrap();

        // 验证初始 worker 数量
        assert_eq!(pool.worker_count(), 3);

        // 关闭 worker #1
        pool.shutdown_worker(1).await.unwrap();

        // 验证 worker 数量减少
        assert_eq!(pool.worker_count(), 2);

        // 验证该 worker 不可用
        let task = TestTask { id: 1, data: "test".to_string() };
        let result = pool.send_task(task, 1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::DownloadError::WorkerNotFound(1)));

        // 验证其他 worker 仍然正常工作
        let task = TestTask { id: 2, data: "test".to_string() };
        assert!(pool.send_task(task, 0).await.is_ok());
        
        let task = TestTask { id: 3, data: "test".to_string() };
        assert!(pool.send_task(task, 2).await.is_ok());
    }

    #[tokio::test]
    async fn test_add_workers_fills_gaps() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let mut pool = WorkerPool::new(executor.clone(), contexts).unwrap();

        // 验证初始状态
        assert_eq!(pool.worker_count(), 3);

        // 关闭 worker #1
        pool.shutdown_worker(1).await.unwrap();
        assert_eq!(pool.worker_count(), 2);

        // 关闭 worker #0
        pool.shutdown_worker(0).await.unwrap();
        assert_eq!(pool.worker_count(), 1);

        // 添加 2 个新 worker，应该填充到 #0 和 #1
        let new_contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        pool.add_workers(new_contexts).await.unwrap();
        assert_eq!(pool.worker_count(), 3);

        // 验证所有位置都可用
        let task0 = TestTask { id: 1, data: "test".to_string() };
        assert!(pool.send_task(task0, 0).await.is_ok());

        let task1 = TestTask { id: 2, data: "test".to_string() };
        assert!(pool.send_task(task1, 1).await.is_ok());

        let task2 = TestTask { id: 3, data: "test".to_string() };
        assert!(pool.send_task(task2, 2).await.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_nonexistent_worker() {
        let executor = Arc::new(TestExecutor);
        let contexts = vec![
            TestContext { processed_count: AtomicUsize::new(0) },
        ];
        let pool = WorkerPool::new(executor, contexts).unwrap();

        // 尝试关闭不存在的 worker
        let result = pool.shutdown_worker(5).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::DownloadError::WorkerNotFound(5)));
    }
}

