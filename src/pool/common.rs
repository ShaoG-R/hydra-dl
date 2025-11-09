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
//!     Success { worker_id: u64 },
//!     Failed { worker_id: u64, error: String },
//! }
//! impl WorkerResult for MyResult {
//!     fn worker_id(&self) -> u64 {
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
//!     async fn execute(&self, worker_id: u64, task: MyTask, _context: &MyContext) -> MyResult {
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
use log::{debug, error, info};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use deferred_map::DeferredMap;

use crate::{DownloadError, Result};

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
/// # 关联类型
///
/// - `Task`: 任务类型
/// - `Result`: 结果类型
/// - `Context`: 上下文类型
/// - `Stats`: 统计类型
///
/// # 要求
///
/// - `Send + Sync`: 执行器可以在多个 worker 间共享
#[async_trait]
pub trait WorkerExecutor: Send + Sync + Clone + 'static {
    /// 任务类型
    type Task: WorkerTask;
    /// 结果类型
    type Result: WorkerResult;
    /// 上下文类型
    type Context: WorkerContext;
    /// 统计类型
    type Stats: WorkerStats;
    
    /// 执行任务
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID (完整的 u64 key)
    /// - `task`: 要处理的任务
    /// - `context`: Worker 的上下文（可用于访问策略等，worker 独占）
    /// - `stats`: Worker 的统计数据（外部可访问，只读）
    ///
    /// # Returns
    ///
    /// 任务执行结果
    async fn execute(&self, worker_id: u64, task: Self::Task, context: &mut Self::Context, stats: &Self::Stats) -> Self::Result;
}

/// Worker 配置
///
/// 封装启动单个 worker 所需的所有参数
pub(crate) struct WorkerConfig<E: WorkerExecutor> {
    pub id: u64,
    pub executor: E,
    pub task_receiver: Receiver<E::Task>,
    pub result_sender: Sender<E::Result>,
    pub context: E::Context,
    pub stats: Arc<E::Stats>,
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
/// - `E`: 执行器类型
pub(crate) async fn run_worker<E: WorkerExecutor>(config: WorkerConfig<E>) {
    let WorkerConfig {
        id,
        executor,
        mut task_receiver,
        result_sender,
        mut context,
        stats,
        mut shutdown_receiver,
    } = config;

    debug!("Worker #{} 启动", id);

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
                debug!("Worker #{} 任务通道关闭，退出", id);
                break;
            }
        }
    }
}

/// 单个 Worker 的槽位
///
/// 封装了与单个 worker 交互所需的所有信息
pub struct WorkerSlot {
    /// 关闭通道的发送端（用于向 worker 发送关闭信号）
    pub(crate) shutdown_sender: tokio::sync::oneshot::Sender<()>,
    /// Worker 协程的 JoinHandle（用于等待 worker 退出）
    pub(crate) join_handle: tokio::task::JoinHandle<()>,
}

/// Worker 句柄
///
/// 封装单个 worker 的操作接口，可以被克隆和共享
///
/// # 特点
///
/// - **轻量级克隆**: 只包含 Arc 引用，克隆成本低
/// - **线程安全**: 可以在多个线程间共享
/// - **优雅降级**: Worker 退出后操作返回 None/Err
///
/// # 示例
///
/// ```ignore
/// let handle = pool.get_worker(0).ok_or(...)?;
/// handle.send_task(task).await?;
/// let stats = handle.stats().unwrap();
/// handle.shutdown()?;
/// ```
pub struct WorkerHandle<E: WorkerExecutor> {
    /// Worker ID (完整的 u64 key)
    worker_id: u64,
    /// 向 worker 发送任务的通道
    task_sender: Arc<Sender<E::Task>>,
    /// 该 worker 的统计数据（Arc 包装以便外部访问）
    stats: Arc<E::Stats>,
}

impl<E: WorkerExecutor> Clone for WorkerHandle<E> {
    fn clone(&self) -> Self {
        Self {
            worker_id: self.worker_id,
            task_sender: Arc::clone(&self.task_sender),
            stats: Arc::clone(&self.stats),
        }
    }
}

impl<E: WorkerExecutor> WorkerHandle<E> {
    /// 创建新的 worker 句柄
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID (完整的 u64 key)
    /// - `task_sender`: 向 worker 发送任务的通道
    /// - `stats`: 该 worker 的统计数据（Arc 包装以便外部访问）
    pub(crate) fn new(
        worker_id: u64,
        task_sender: Arc<Sender<E::Task>>,
        stats: Arc<E::Stats>,
    ) -> Self {
        Self {
            worker_id,
            task_sender,
            stats,
        }
    }

    /// 获取 worker ID
    ///
    /// # Returns
    ///
    /// Worker 的 ID (完整的 u64 key)
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    /// 提交任务给该 worker
    ///
    /// # Arguments
    ///
    /// - `task`: 要执行的任务
    ///
    /// # Returns
    ///
    /// 成功时返回 `Ok(())`，如果 worker 不存在或已关闭则返回 `Err`
    pub async fn send_task(&self, task: E::Task) -> Result<()> {
        self.task_sender
            .send(task)
            .await
            .map_err(|e| DownloadError::TaskSend(e.to_string()))?;
        Ok(())
    }

    /// 获取该 worker 的统计数据
    ///
    /// # Returns
    ///
    /// Worker 统计数据的 Arc 引用
    pub fn stats(&self) -> Arc<E::Stats> {
        Arc::clone(&self.stats)
    }
}



/// 通用 Worker 协程池
///
/// 管理多个 worker 协程的生命周期，支持任务分发、结果收集和动态扩展
///
/// # 泛型参数
///
/// - `E`: 执行器类型（通过关联类型提供任务、结果、上下文、统计类型）
///
/// # 核心功能
///
/// - 创建协程池（初始 worker 数量）
/// - 发送任务到指定 worker
/// - 接收结果（统一 result channel）
/// - 动态添加 worker
/// - 访问 worker 统计数据
pub struct WorkerPool<E: WorkerExecutor> {
    /// Worker 槽位存储（使用 DeferredMap）
    pub(crate) slots: DeferredMap<WorkerSlot>,
    /// 结果发送器的克隆（用于创建新 worker）
    result_sender: Sender<E::Result>,
    /// 执行器
    pub(crate) executor: E,
}

impl<E: WorkerExecutor> WorkerPool<E> {
    /// 启动单个 worker（内部辅助方法）
    /// 
    /// 封装创建和启动 worker 的通用逻辑，避免在 new 和 add_workers 中重复
    /// 
    /// # Returns
    /// 
    /// 返回新创建的 worker slot 和句柄
    fn spawn_worker(
        &mut self,
        key: u64,
        context: E::Context,
        stats_arc: Arc<E::Stats>,
    ) -> (WorkerSlot, WorkerHandle<E>) {
        // 创建任务和关闭通道
        let (task_sender, task_receiver) = mpsc::channel::<E::Task>(100);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
        
        // 克隆 worker 需要的资源
        let result_sender_clone = self.result_sender.clone();
        let stats_for_worker = Arc::clone(&stats_arc);
        let executor_clone = self.executor.clone();
        
        // 启动 worker 协程（不再包含自动清理逻辑）
        // 直接使用完整的 key 作为 worker_id
        let join_handle = tokio::spawn(async move {
            // 执行 worker 主循环
            run_worker(WorkerConfig {
                id: key,
                executor: executor_clone,
                task_receiver,
                result_sender: result_sender_clone,
                context,
                stats: stats_for_worker,
                shutdown_receiver,
            })
            .await;
            
            debug!("Worker #{} 退出", key);
        });
        
        let task_sender = Arc::new(task_sender);
        
        // 创建 WorkerSlot
        let slot = WorkerSlot {
            shutdown_sender,
            join_handle,
        };

        // 创建 worker 句柄
        let handle = WorkerHandle::new(key, task_sender, stats_arc);
        
        (slot, handle)
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
    /// 返回新创建的 WorkerPool、所有初始 worker 的句柄以及结果接收器
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = MyExecutor;
    /// let contexts_with_stats = vec![(MyContext::new(), Arc::new(MyStats::new())); 4];
    /// let (pool, handles, result_receiver) = WorkerPool::new(executor, contexts_with_stats)?;
    /// ```
    pub fn new(executor: E, contexts_with_stats: Vec<(E::Context, Arc<E::Stats>)>) -> Result<(Self, Vec<WorkerHandle<E>>, Receiver<E::Result>)> {
        let worker_count = contexts_with_stats.len();
        
        // 创建 DeferredMap
        let slots = DeferredMap::with_capacity(worker_count);
        
        // 创建统一的 result channel（所有 worker 共享同一个 sender）
        let (result_sender, result_receiver) = mpsc::channel::<E::Result>(100);
        
        // 创建 pool 实例（先不启动 workers）
        let mut pool = Self {
            slots,
            result_sender: result_sender.clone(),
            executor,
        };
        
        // 为每个 worker 创建独立的 task channel、上下文和统计，并启动协程
        let mut handles = Vec::with_capacity(worker_count);
        for (context, stats_arc) in contexts_with_stats.into_iter() {
            // 先分配 handle
            let deferred_handle = pool.slots.allocate_handle();
            let key = deferred_handle.key();
            
            // spawn_worker 返回 slot 和 handle
            let (slot, worker_handle) = pool.spawn_worker(key, context, stats_arc);
            
            // 将 slot 插入 DeferredMap
            pool.slots.insert(deferred_handle, slot)
                .map_err(|e| DownloadError::Other(format!("插入 slot 失败: {:?}", e)))?;
            
            handles.push(worker_handle);
        }

        Ok((pool, handles, result_receiver))
    }

    /// 动态添加新的 worker
    ///
    /// # Arguments
    ///
    /// - `contexts_with_stats`: 要添加的 worker 的上下文和统计数据列表
    ///
    /// # Returns
    ///
    /// 成功时返回新添加的所有 worker 的句柄，失败时返回错误信息
    ///
    /// # Example
    ///
    /// ```ignore
    /// let new_contexts = vec![(MyContext::new(), Arc::new(MyStats::new())); 2];
    /// let handles = pool.add_workers(new_contexts).await?;
    /// ```
    pub async fn add_workers(&mut self, contexts_with_stats: Vec<(E::Context, Arc<E::Stats>)>) -> Result<Vec<WorkerHandle<E>>> {
        let mut handles = Vec::with_capacity(contexts_with_stats.len());
        for (context, stats_arc) in contexts_with_stats.into_iter() {
            // 分配新的 handle
            let deferred_handle = self.slots.allocate_handle();
            let key = deferred_handle.key();
            
            // 启动 worker（spawn_worker 返回 slot 和 handle）
            let (slot, worker_handle) = self.spawn_worker(key, context, stats_arc);
            
            // 将 slot 插入 DeferredMap
            self.slots.insert(deferred_handle, slot)
                .map_err(|e| DownloadError::Other(format!("插入 slot 失败: {:?}", e)))?;
            
            handles.push(worker_handle);
            
            debug!("新 worker 添加到位置 #{}", key);
        }
        
        Ok(handles)
    }
    
    
    /// 获取当前活跃 worker 总数
    pub fn worker_count(&self) -> u64 {
        self.slots.len() as u64
    }

    /// 优雅关闭所有 workers
    ///
    /// 通过 oneshot channel 向所有活跃的 workers 发送关闭信号
    /// 并异步等待所有 workers 真正退出
    ///
    /// # 异步行为
    ///
    /// 此方法会阻塞直到所有 workers 完全退出
    pub async fn shutdown(&mut self) {
        info!("发送关闭信号到所有活跃 workers");
        
        // 收集所有需要关闭的 workers 的 key
        let keys: Vec<u64> = self.slots.iter().map(|(key, _)| key).collect();
        let closed_count = keys.len();
        
        // 收集所有的 JoinHandle
        let mut join_handles = Vec::with_capacity(closed_count);
        
        // 从 map 中移除每个 slot，获取所有权，并发送关闭信号
        for key in keys {
            if let Some(slot) = self.slots.remove(key) {
                // 发送关闭信号（忽略发送错误，worker 可能已经退出）
                let _ = slot.shutdown_sender.send(());
                debug!("Worker #{} 关闭信号已发送", key);
                
                // 收集 JoinHandle
                join_handles.push((key, slot.join_handle));
            }
        }
        
        info!("已向 {} 个 workers 发送关闭信号，等待退出...", closed_count);
        
        // 等待所有 workers 退出
        for (key, handle) in join_handles {
            match handle.await {
                Ok(_) => {
                    debug!("Worker #{} 已成功退出", key);
                }
                Err(e) => {
                    error!("Worker #{} 退出时发生错误: {:?}", key, e);
                }
            }
        }
        
        info!("所有 {} 个 workers 已完全退出", closed_count);
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
        Success { worker_id: u64, task_id: usize },
        Failed { worker_id: u64, error: String },
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
    #[derive(Clone)]
    pub struct TestExecutor;
    
    #[async_trait]
    impl WorkerExecutor for TestExecutor {
        type Task = TestTask;
        type Result = TestResult;
        type Context = TestContext;
        type Stats = TestStats;
        
        async fn execute(&self, worker_id: u64, task: Self::Task, context: &mut Self::Context, stats: &Self::Stats) -> Self::Result {
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
    #[derive(Clone)]
    pub struct FailingExecutor;
    
    #[async_trait]
    impl WorkerExecutor for FailingExecutor {
        type Task = TestTask;
        type Result = TestResult;
        type Context = TestContext;
        type Stats = TestStats;
        
        async fn execute(&self, worker_id: u64, _task: Self::Task, _context: &mut Self::Context, _stats: &Self::Stats) -> Self::Result {
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
    use super::test_utils::*;
    use std::sync::atomic::Ordering;
    use tokio::time::{sleep, Duration};
    
    /// 测试创建协程池
    #[tokio::test]
    async fn test_create_worker_pool() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(4);
        
        let result = WorkerPool::new(executor, contexts);
        assert!(result.is_ok());
        
        let (mut pool, handles, _result_receiver) = result.unwrap();
        assert_eq!(pool.worker_count(), 4);
        assert_eq!(handles.len(), 4);
        
        // 清理
        pool.shutdown().await;
    }
    
    /// 测试空协程池创建
    #[tokio::test]
    async fn test_create_empty_pool() {
        let executor = TestExecutor;
        let contexts = vec![];
        
        let result = WorkerPool::new(executor, contexts);
        assert!(result.is_ok());
        
        let (mut pool, handles, _result_receiver) = result.unwrap();
        assert_eq!(pool.worker_count(), 0);
        assert_eq!(handles.len(), 0);
        
        pool.shutdown().await;
    }
    
    /// 测试发送任务并接收结果
    #[tokio::test]
    async fn test_send_and_receive() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(2);
        let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        // 向第一个 worker 发送任务
        let task = TestTask {
            id: 1,
            data: "test".to_string(),
        };
        
        handles[0].send_task(task).await.unwrap();
        
        // 接收结果
        if let Some(result) = result_receiver.recv().await {
            match result {
                TestResult::Success { worker_id: _, task_id } => {
                    assert_eq!(task_id, 1);
                }
                _ => panic!("Expected success result"),
            }
        } else {
            panic!("No result received");
        }
        
        pool.shutdown().await;
    }
    
    /// 测试 worker 统计数据
    #[tokio::test]
    async fn test_worker_stats() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(1);
        let stats_ref = contexts[0].1.clone();
        let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        // 发送多个任务
        for i in 0..5 {
            let task = TestTask {
                id: i,
                data: format!("task-{}", i),
            };
            handles[0].send_task(task).await.unwrap();
        }
        
        // 接收所有结果
        for _ in 0..5 {
            result_receiver.recv().await;
        }
        
        // 验证统计数据
        let count = stats_ref.task_count.load(Ordering::SeqCst);
        assert_eq!(count, 5);
        
        pool.shutdown().await;
    }
    
    /// 测试动态添加 worker
    #[tokio::test]
    async fn test_add_workers() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(2);
        let (mut pool, _, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        assert_eq!(pool.worker_count(), 2);
        
        // 动态添加 3 个新 workers
        let new_contexts = create_contexts_with_stats(3);
        let new_handles = pool.add_workers(new_contexts).await.unwrap();
        
        assert_eq!(pool.worker_count(), 5);
        assert_eq!(new_handles.len(), 3);
        
        pool.shutdown().await;
    }
    
    /// 测试 worker 句柄的 worker_id
    #[tokio::test]
    async fn test_worker_handle_id() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(3);
        let (mut pool, handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        // 验证每个 handle 的 worker_id 是唯一的
        let ids: Vec<u64> = handles.iter().map(|h| h.worker_id()).collect();
        assert_eq!(ids.len(), 3);
        
        // 验证 ID 是唯一的
        for i in 0..ids.len() {
            for j in (i+1)..ids.len() {
                assert_ne!(ids[i], ids[j]);
            }
        }
        
        pool.shutdown().await;
    }
    
    /// 测试优雅关闭
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(4);
        let (mut pool, handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        // 发送一些任务
        for i in 0..10 {
            let task = TestTask {
                id: i,
                data: format!("task-{}", i),
            };
            // 轮流发送到不同 worker
            handles[i % 4].send_task(task).await.unwrap();
        }
        
        // 给 workers 一些时间处理任务
        sleep(Duration::from_millis(50)).await;
        
        // 关闭协程池
        pool.shutdown().await;
        
        // 验证所有 workers 已退出
        assert_eq!(pool.worker_count(), 0);
    }
    
    /// 测试失败执行器
    #[tokio::test]
    async fn test_failing_executor() {
        let executor = FailingExecutor;
        let contexts = create_contexts_with_stats(2);
        let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        // 发送任务
        let task = TestTask {
            id: 1,
            data: "test".to_string(),
        };
        handles[0].send_task(task).await.unwrap();
        
        // 接收结果
        if let Some(result) = result_receiver.recv().await {
            match result {
                TestResult::Failed { worker_id: _, error } => {
                    assert_eq!(error, "intentional failure");
                }
                _ => panic!("Expected failed result"),
            }
        } else {
            panic!("No result received");
        }
        
        pool.shutdown().await;
    }
    
    /// 测试 WorkerHandle 克隆
    #[tokio::test]
    async fn test_worker_handle_clone() {
        let executor = TestExecutor;
        let contexts = create_contexts_with_stats(1);
        let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
        
        // 克隆句柄
        let handle1 = handles[0].clone();
        let handle2 = handle1.clone();
        
        // 两个句柄都应该有相同的 worker_id
        assert_eq!(handle1.worker_id(), handle2.worker_id());
        
        // 两个句柄都能发送任务
        handle1.send_task(TestTask { id: 1, data: "test1".to_string() }).await.unwrap();
        handle2.send_task(TestTask { id: 2, data: "test2".to_string() }).await.unwrap();
        
        // 接收结果
        result_receiver.recv().await;
        result_receiver.recv().await;
        
        pool.shutdown().await;
    }
}