use crate::DownloadError;
use crate::download::DownloadTaskParams;
use crate::pool::download::DownloadWorkerPool;
use crate::utils::io_traits::HttpClient;
use crate::utils::writer::MmapWriter;
use crate::{
    download::{
        progress_reporter::{ProgressReporter, ProgressReporterParams},
        progressive::{ProgressiveLauncher, ProgressiveLauncherParams, WorkerLaunchRequest},
        task_allocator::{
            CompletionResult, FailedRange, TaskAllocatorActor, TaskAllocatorHandle,
            TaskAllocatorParams,
        },
        worker_health_checker::{WorkerHealthChecker, WorkerHealthCheckerParams},
    },
    pool::download::DownloadWorkerHandle,
};
use log::{error, info};
use rustc_hash::FxHashMap;
use smr_swap::SmrSwap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

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
    worker_handles: SmrSwap<FxHashMap<u64, DownloadWorkerHandle<C>>>,
    /// 健康检查器（actor 模式）
    health_checker: WorkerHealthChecker<C>,
}

impl<C: HttpClient + Clone> DownloadTask<C> {
    /// 创建新的下载任务
    pub(super) async fn new(params: DownloadTaskParams<C>) -> Self {
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
        let global_stats = crate::utils::stats::TaskStats::from_config(config.speed());

        // worker_handles 占位，稍后填充
        let mut swap = SmrSwap::new(FxHashMap::default());

        // 基准时间间隔：50ms
        let base_offset = std::time::Duration::from_millis(50);

        // 创建渐进式启动管理器（actor 模式） - 偏移 0ms
        let (progressive_launcher, launch_request_rx) = ProgressiveLauncher::new(
            ProgressiveLauncherParams {
                config: Arc::clone(&config),
                worker_handles: swap.local(),
                total_size: writer.total_size(),
                written_bytes: writer.written_bytes_ref(),
                global_stats: global_stats.clone(),
                start_offset: std::time::Duration::ZERO,
            },
        );

        // 创建健康检查器（actor 模式） - 偏移 50ms
        let (health_checker, cancel_request_rx) = WorkerHealthChecker::new(
            WorkerHealthCheckerParams {
                config: Arc::clone(&config),
                worker_handles: swap.local(),
                check_interval: config.speed().instant_speed_window(),
                start_offset: base_offset,
            },
        );

        // 第一批 worker 数量
        let initial_worker_count = progressive_launcher.initial_worker_count();

        info!("初始启动 {} 个 workers", initial_worker_count);

        // 创建 DownloadWorkerPool（只启动第一批 worker）
        let (pool, initial_handles, result_receiver) = DownloadWorkerPool::new(
            client.clone(),
            initial_worker_count,
            writer.clone(),
            Arc::clone(&config),
            global_stats.clone(),
        );

        // 创建进度报告器（使用配置的统计窗口作为更新间隔） - 偏移 100ms
        let progress_reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender,
            total_size,
            worker_handles: swap.local(),
            global_stats: global_stats.clone(),
            update_interval: config.speed().instant_speed_window(),
            start_offset: base_offset * 2,
        });

        // 使用实际的 worker_id（从 handle 获取）填充 worker_handles
        let initial_worker_handles: FxHashMap<u64, _> = initial_handles
            .into_iter()
            .map(|handle| (handle.worker_id(), handle))
            .collect();
        swap.store(initial_worker_handles);

        // 创建并启动任务分配器 Actor
        let (task_allocator_handle, completion_rx) = TaskAllocatorActor::new(
            TaskAllocatorParams {
                allocator,
                url,
                timer_service,
                result_rx: result_receiver,
                cancel_rx: cancel_request_rx,
                config: Arc::clone(&config),
                worker_handles: swap.local(),
            },
        );

        // 注册第一批 workers（会自动触发任务分配）
        let worker_ids = {
            let guard = swap.load();
            guard.keys().cloned().collect()
        };
        task_allocator_handle.register_new_workers(worker_ids).await;

        Self {
            pool,
            writer,
            task_allocator: task_allocator_handle,
            progress_reporter,
            config,
            progressive_launcher,
            launch_request_rx,
            completion_rx,
            worker_handles: swap,
            health_checker,
        }
    }

    /// 等待所有 range 完成
    ///
    /// 动态分配任务，支持失败重试
    /// 如果有任务达到最大重试次数，将终止下载并返回错误
    pub(super) async fn wait_for_completion(&mut self) -> crate::Result<Vec<FailedRange>> {
        // 注意：任务分配已在注册 workers 时自动触发，这里直接进入事件循环

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

        self.execute_worker_launch(count, stage).await
    }

    /// 关闭并清理资源
    pub(super) async fn shutdown_and_cleanup(
        mut self,
        error_msg: Option<String>,
    ) -> crate::Result<()> {
        // 发送错误事件
        if let Some(ref msg) = error_msg {
            self.progress_reporter.send_error(msg).await;
        }

        // 关闭 workers（发送关闭信号）
        self.pool.shutdown().await;

        // 关闭 progress reporter actor
        self.progress_reporter.shutdown().await;

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
        self.progress_reporter.send_completion().await;

        // 优雅关闭所有 workers（发送关闭信号，workers 会异步自动清理）
        let mut pool = self.pool;
        pool.shutdown().await;

        // 释放 pool（它持有 writer 的共享）
        drop(pool);

        // 关闭 progress reporter actor
        self.progress_reporter.shutdown().await;

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
    fn get_worker_handle(
        &self,
        worker_id: u64,
    ) -> Option<crate::pool::download::DownloadWorkerHandle<C>> {
        let guard = self.worker_handles.load();
        guard.get(&worker_id).cloned()
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
    async fn execute_worker_launch(&mut self, count: u64, stage: usize) {
        let current_worker_count = self.pool.worker_count();
        let next_target = current_worker_count + count;

        info!(
            "渐进式启动 - 第{}批: 启动 {} 个新 workers (总计 {} 个)",
            stage + 1,
            count,
            next_target
        );

        // 动态添加新 worker，收集实际的 worker_id
        let new_worker_ids: Vec<u64> =  {
            let new_handles = self.pool.add_workers(count).await;
            let mut ids = Vec::new();
            self.worker_handles.update(|handles| {
                let mut handles = handles.clone();
                for handle in new_handles {
                    let worker_id = handle.worker_id();
                    ids.push(worker_id);
                    handles.insert(worker_id, handle);
                }
                handles
            });
            ids
        };

        // 为新启动的worker加入队列（使用实际的 worker_id）
        self.task_allocator
            .register_new_workers(new_worker_ids)
            .await;
    }
}
