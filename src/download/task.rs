use std::path::PathBuf;
use std::sync::Arc;
use log::{error, info};
use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use crate::{download::{
    progress_reporter::ProgressReporter,
    progressive::{ProgressiveLauncher, WorkerLaunchRequest},
    task_allocator::{CompletionResult, FailedRange, TaskAllocatorActor, TaskAllocatorHandle},
    worker_health_checker::WorkerHealthChecker,
}, pool::download::DownloadWorkerHandle};
use crate::utils::writer::MmapWriter;
use crate::DownloadError;
use crate::pool::download::DownloadWorkerPool;
use crate::utils::io_traits::HttpClient;
use crate::download::DownloadTaskParams;

/// 下载任务执行器
///
/// 封装了下载任务的执行逻辑，使用辅助结构体管理任务分配和进度报告
pub struct DownloadTask<C: HttpClient> {
    /// Worker 协程池
    pool: DownloadWorkerPool<C>,
    /// 文件写入器
    writer: MmapWriter,
    /// 任务分配器句柄
    task_allocator: TaskAllocatorHandle,
    /// 进度报告器
    progress_reporter: ProgressReporter<C>,
    /// 下载配置
    config: Arc<crate::config::DownloadConfig>,
    /// 渐进式启动管理器
    progressive_launcher: ProgressiveLauncher<C>,
    /// Worker 启动请求接收器（由 progressive_launcher 发送）
    launch_request_rx: mpsc::Receiver<WorkerLaunchRequest>,
    /// 任务完成通知接收器（由 task_allocator 发送）
    completion_rx: tokio::sync::oneshot::Receiver<CompletionResult>,
    /// Worker 句柄缓存（worker_id -> handle）
    /// 使用 ArcSwap + im::HashMap 实现无锁原子更新和共享
    worker_handles: Arc<ArcSwap<im::HashMap<u64, DownloadWorkerHandle<C>>>>,
    /// 健康检查器（actor 模式）
    health_checker: WorkerHealthChecker<C>,
}

impl<C: HttpClient + Clone> DownloadTask<C> {
    /// 创建新的下载任务
    pub(super) async fn new(params: DownloadTaskParams<C>) -> crate::Result<Self> {
        // 解构参数
        let DownloadTaskParams {
            client,
            progress_sender,
            writer,
            allocator,
            url,
            total_size,
            timer_service,
            config,
        } = params;

        // 获取 global_stats
        let global_stats = Arc::new(crate::utils::stats::TaskStats::from_config(config.speed()));
        
        // worker_handles 占位，稍后填充
        let worker_handles = Arc::new(ArcSwap::from_pointee(im::HashMap::new()));
        
        // 创建渐进式启动管理器（actor 模式）
        let mut progressive_launcher = ProgressiveLauncher::new(
            Arc::clone(&config),
            Arc::clone(&worker_handles),
            writer.total_size(),
            writer.written_bytes_ref(),
            Arc::clone(&global_stats),
            config.speed().instant_speed_window(),
        );
        
        // 取出启动请求接收器
        let launch_request_rx = progressive_launcher.take_launch_request_rx()
            .expect("launch_request_rx should be available");
        
        // 创建 active_workers 共享状态（使用 RwLock 确保 TaskAllocator 和 HealthChecker 同步）
        let active_workers = Arc::new(RwLock::new(FxHashMap::default()));
        
        // 创建健康检查器（actor 模式）
        let mut health_checker = WorkerHealthChecker::new(
            Arc::clone(&config),
            Arc::clone(&worker_handles),
            config.speed().instant_speed_window(),
            Arc::clone(&active_workers),
        );
        
        // 取出取消请求接收器
        let cancel_request_rx = health_checker.take_cancel_request_rx()
            .expect("cancel_request_rx should be available");

        // 第一批 worker 数量
        let initial_worker_count = progressive_launcher.initial_worker_count();

        info!("初始启动 {} 个 workers", initial_worker_count);

        // 创建 DownloadWorkerPool（只启动第一批 worker）
        let (pool, initial_handles, result_receiver) = DownloadWorkerPool::new(
            client.clone(),
            initial_worker_count,
            writer.clone(),
            Arc::clone(&config),
            Arc::clone(&global_stats),
        )?;

        // 使用实际的 worker_id（从 handle 获取）填充 worker_handles
        let initial_worker_handles: im::HashMap<u64, _> = initial_handles.into_iter()
            .map(|handle| (handle.worker_id(), handle))
            .collect();
        worker_handles.store(Arc::new(initial_worker_handles));
        
        // 创建并启动任务分配器 Actor
        let (task_allocator_handle, completion_rx) = TaskAllocatorActor::new(
            allocator,
            url,
            timer_service,
            result_receiver,
            cancel_request_rx,
            Arc::clone(&config),
            Arc::clone(&worker_handles),
            Arc::clone(&active_workers),
        );
        
        // 创建进度报告器（使用配置的统计窗口作为更新间隔）
        let progress_reporter = ProgressReporter::new(
            progress_sender,
            total_size,
            Arc::clone(&worker_handles),
            Arc::clone(&global_stats),
            config.speed().instant_speed_window(),
        );

        // 将第一批 workers 加入空闲队列（使用实际的 worker_id）
        for &worker_id in worker_handles.load().keys() {
            task_allocator_handle.mark_worker_idle(worker_id).await;
        }

        Ok(Self {
            pool,
            writer,
            task_allocator: task_allocator_handle,
            progress_reporter,
            config,
            progressive_launcher,
            launch_request_rx,
            completion_rx,
            worker_handles,
            health_checker,
        })
    }


    /// 等待所有 range 完成
    ///
    /// 动态分配任务，支持失败重试
    /// 如果有任务达到最大重试次数，将终止下载并返回错误
    pub(super) async fn wait_for_completion(&mut self) -> crate::Result<Vec<FailedRange>> {
        // 分配初始任务
        self.task_allocator.allocate_initial_tasks().await;

        // 事件循环：分发各种事件到对应的处理器
        let completion_result = loop {
            tokio::select! {
                Some(request) = self.launch_request_rx.recv() => {
                    self.handle_launch_request(request).await;
                },
                result = &mut self.completion_rx => {
                    match result {
                        Ok(completion) => {
                            info!("收到任务完成通知: {:?}", completion);
                            break completion;
                        },
                        Err(_) => {
                            error!("完成通知 channel 已关闭");
                            return Err(DownloadError::Other("TaskAllocator 异常终止".to_string()));
                        }
                    }
                },
            }
        };

        // 根据完成结果返回相应的值
        match completion_result {
            CompletionResult::Success => Ok(Vec::new()),
            CompletionResult::PermanentFailure { failures } => {
                let error_details: Vec<String> = failures
                    .iter()
                    .map(|(range, error)| {
                        let (start, end) = range.as_range_tuple();
                        format!("range {}..{}: {}", start, end, error)
                    })
                    .collect();

                let error_msg = format!(
                    "有 {} 个任务达到最大重试次数后失败:\n  {}",
                    failures.len(),
                    error_details.join("\n  ")
                );

                Err(DownloadError::Other(error_msg))
            }
        }
    }

    
    /// 处理 worker 启动请求（由 progressive_launcher actor 发送）
    async fn handle_launch_request(&mut self, request: WorkerLaunchRequest) {
        let WorkerLaunchRequest { count, stage } = request;
        
        if let Err(e) = self.execute_worker_launch(count, stage).await {
            error!("渐进式启动失败: {:?}", e);
        }
    }

    /// 关闭并清理资源
    pub(super) async fn shutdown_and_cleanup(mut self, error_msg: Option<String>) -> crate::Result<()> {
        // 发送错误事件
        if let Some(ref msg) = error_msg {
            self.progress_reporter.send_error(msg);
        }

        // 关闭 workers（发送关闭信号）
        self.pool.shutdown().await;
        
        // 关闭 progress reporter actor
        self.progress_reporter.shutdown();
        
        // 关闭 progressive launcher actor 并等待其完全停止
        self.progressive_launcher.shutdown_and_wait().await;
        
        // 关闭 health checker actor 并等待其完全停止
        self.health_checker.shutdown_and_wait().await;
        
        // 关闭 TaskAllocator Actor 并等待其完全停止
        self.task_allocator.shutdown_and_wait().await;

        if let Some(msg) = error_msg {
            return Err(DownloadError::Other(msg));
        }

        Ok(())
    }

    /// 完成并清理资源
    pub(super) async fn finalize_and_cleanup(self, save_path: PathBuf) -> crate::Result<()> {
        // 发送完成统计（Actor 会自动从共享数据源获取统计）
        self.progress_reporter.send_completion();

        // 优雅关闭所有 workers（发送关闭信号，workers 会异步自动清理）
        let mut pool = self.pool;
        pool.shutdown().await;

        // 释放 pool（它持有 executor 的引用）
        drop(pool);
        
        // 关闭 progress reporter actor
        self.progress_reporter.shutdown();
        
        // 关闭 progressive launcher actor 并等待其完全停止
        // 这很重要，因为 actor 持有 written_bytes 的 Arc 引用
        // 必须等待它释放后才能 finalize writer
        self.progressive_launcher.shutdown_and_wait().await;
        
        // 关闭 health checker actor 并等待其完全停止
        self.health_checker.shutdown_and_wait().await;
        
        // 关闭 TaskAllocator Actor 并等待其完全停止
        self.task_allocator.shutdown_and_wait().await;

        // 完成写入
        self.writer.finalize()?;

        info!("Range 下载任务完成: {:?}", save_path);

        Ok(())
    }

    /// 获取当前活跃 worker 数量
    #[inline]
    pub fn worker_count(&self) -> u64 {
        self.pool.worker_count()
    }

    /// 获取指定 worker 的句柄（从缓存中）
    ///
    /// # Arguments
    ///
    /// * `worker_id` - Worker ID
    ///
    /// # Returns
    ///
    /// Worker 句柄的克隆，如果 worker 不存在则返回 `None`
    #[inline]
    fn get_worker_handle(&self, worker_id: u64) -> Option<crate::pool::download::DownloadWorkerHandle<C>> {
        self.worker_handles.load().get(&worker_id).cloned()
    }

    /// 获取指定 worker 的当前分块大小
    #[inline]
    pub fn get_worker_chunk_size(&self, worker_id: u64) -> u64 {
        self.get_worker_handle(worker_id)
            .map(|h| h.chunk_size())
            .unwrap_or(self.config.chunk().initial_size())
    }

    /// 获取进度报告器
    #[inline]
    pub(super) fn progress_reporter(&self) -> &ProgressReporter<C> {
        &self.progress_reporter
    }

    /// 检查是否下载完成
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.writer.is_complete()
    }

    /// 执行 worker 启动（内部方法）
    async fn execute_worker_launch(
        &mut self,
        count: u64,
        stage: usize,
    ) -> crate::Result<()> {
        let current_worker_count = self.pool.worker_count();
        let next_target = current_worker_count + count;

        info!(
            "渐进式启动 - 第{}批: 启动 {} 个新 workers (总计 {} 个)",
            stage + 1,
            count,
            next_target
        );

        // 动态添加新 worker，收集实际的 worker_id
        let new_worker_ids: Vec<u64> = match self.pool.add_workers(count).await {
            Ok(handles) => {
                let mut ids = Vec::new();
                // load() 返回 Guard，解引用两次得到 im::HashMap，然后 clone (O(1) 操作)
                let mut new_handles = (*self.worker_handles.load_full()).clone();
                for handle in handles {
                    let worker_id = handle.worker_id();
                    ids.push(worker_id);
                    new_handles = new_handles.update(worker_id, handle);
                }
                self.worker_handles.store(Arc::new(new_handles));
                ids
            },
            Err(e) => {
                error!("添加新 workers 失败: {:?}", e);
                return Err(e);
            }
        };

        // 为新启动的worker加入队列（使用实际的 worker_id）
        self.task_allocator.register_new_workers(new_worker_ids).await;

        Ok(())
    }
}