use std::path::PathBuf;
use std::sync::Arc;
use log::{debug, error, info, warn};
use kestrel_timer::{TaskId, TimerService};
use ranged_mmap::AllocatedRange;
use rustc_hash::FxHashMap;
use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use crate::{download::{
    progress_reporter::ProgressReporter,
    progressive::{ProgressiveLauncher, WorkerLaunchRequest},
    task_allocator::{FailedRange, TaskAllocator},
    worker_health_checker::{WorkerHealthChecker, WorkerSpeed},
}, pool::download::DownloadWorkerHandle};
use crate::utils::writer::MmapWriter;
use crate::DownloadError;
use crate::pool::download::DownloadWorkerPool;
use crate::task::RangeResult;
use crate::utils::io_traits::HttpClient;
use crate::download::DownloadTaskParams;

/// 下载循环控制流
///
/// 用于控制下载事件循环的行为
enum LoopControl {
    /// 继续循环
    Continue,
    /// 正常完成，退出循环
    Break,
}


/// 下载任务执行器
///
/// 封装了下载任务的执行逻辑，使用辅助结构体管理任务分配和进度报告
pub struct DownloadTask<C: HttpClient> {
    /// Worker 协程池
    pool: DownloadWorkerPool<C>,
    /// 文件写入器
    writer: MmapWriter,
    /// 任务分配器
    task_allocator: TaskAllocator,
    /// 进度报告器
    progress_reporter: ProgressReporter<C>,
    /// 下载配置
    config: Arc<crate::config::DownloadConfig>,
    /// 渐进式启动管理器
    progressive_launcher: ProgressiveLauncher<C>,
    /// Worker 启动请求接收器（由 progressive_launcher 发送）
    launch_request_rx: mpsc::Receiver<WorkerLaunchRequest>,
    /// 定时器服务（用于管理失败任务的重试定时器）
    timer_service: TimerService,
    /// 任务取消 sender（worker_id -> cancel_sender）
    ///
    /// 用于内部管理每个 worker 当前任务的取消功能
    /// 当需要中止某个 worker 的任务时，可以通过发送取消信号来实现
    cancel_senders: FxHashMap<u64, tokio::sync::oneshot::Sender<()>>,
    /// Worker 句柄缓存（worker_id -> handle）
    /// 使用 ArcSwap + im::HashMap 实现无锁原子更新和共享
    worker_handles: Arc<ArcSwap<im::HashMap<u64, DownloadWorkerHandle<C>>>>,
}

impl<C: HttpClient + Clone> DownloadTask<C> {
    /// 创建新的下载任务
    pub(super) fn new(params: DownloadTaskParams<C>) -> crate::Result<Self> {
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

        // 第一批 worker 数量
        let initial_worker_count = progressive_launcher.initial_worker_count();

        info!("初始启动 {} 个 workers", initial_worker_count);

        // 创建 DownloadWorkerPool（只启动第一批 worker）
        let (pool, initial_handles) = DownloadWorkerPool::new(
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
        
        let mut task_allocator = TaskAllocator::new(allocator, url);
        
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
            task_allocator.mark_worker_idle(worker_id);
        }

        Ok(Self {
            pool,
            writer,
            task_allocator,
            progress_reporter,
            config,
            progressive_launcher,
            launch_request_rx,
            timer_service,
            cancel_senders: FxHashMap::default(),
            worker_handles,
        })
    }


    /// 等待所有 range 完成
    ///
    /// 动态分配任务，支持失败重试
    /// 如果有任务达到最大重试次数，将终止下载并返回错误
    pub(super) async fn wait_for_completion(&mut self) -> crate::Result<Vec<FailedRange>> {
        // 获取定时器超时接收器
        let timeout_rx = self.timer_service.take_receiver()
            .ok_or_else(|| DownloadError::Other("无法获取定时器接收器".to_string()))?;

        // 创建定时器，用于健康检查和渐进式启动
        let mut management_timer = tokio::time::interval(self.config.speed().instant_speed_window());
        management_timer.tick().await; // 跳过首次立即触发

        // 分配初始任务
        self.allocate_initial_tasks().await?;

        // 事件循环：分发各种事件到对应的处理器
        loop {
            let control = tokio::select! {
                _ = management_timer.tick() => self.handle_management_tick().await,
                Some(notification) = timeout_rx.recv() => self.handle_retry_timeout(notification.task_id()).await,
                result = self.pool.result_receiver().recv() => self.handle_worker_result(result).await,
                Some(request) = self.launch_request_rx.recv() => self.handle_launch_request(request).await,
            };

            match control {
                LoopControl::Continue => continue,
                LoopControl::Break => break,
            }
        }

        // 检查最终状态并返回结果
        self.check_final_status()
    }


    /// 处理管理定时器触发（仅进行健康检查）
    async fn handle_management_tick(&mut self) -> LoopControl {
        // 执行健康检查，检测并终止异常下载线程
        self.check_and_handle_unhealthy_workers();

        LoopControl::Continue
    }
    
    /// 处理 worker 启动请求（由 progressive_launcher actor 发送）
    async fn handle_launch_request(&mut self, request: WorkerLaunchRequest) -> LoopControl {
        let WorkerLaunchRequest { count, stage } = request;
        
        match self.execute_worker_launch(count, stage).await {
            Ok(new_cancel_senders) => {
                // 保存新分配任务的取消通道
                for (worker_id, cancel_tx) in new_cancel_senders {
                    self.cancel_senders.insert(worker_id, cancel_tx);
                }
            }
            Err(e) => {
                error!("渐进式启动失败: {:?}", e);
            }
        }
        
        LoopControl::Continue
    }

    /// 处理重试超时事件
    async fn handle_retry_timeout(&mut self, timer_id: TaskId) -> LoopControl {
        // 根据 timer_id 获取失败任务信息
        let Some(info) = self.task_allocator.pop_failed_task(timer_id) else {
            return LoopControl::Continue;
        };

        let (start, end) = info.range.as_range_tuple();
        info!(
            "定时器触发，重试任务 range {}..{}, 重试次数 {}",
            start,
            end,
            info.retry_count
        );

        // 从空闲队列获取 worker
        if let Some(worker_id) = self.task_allocator.idle_workers.pop_front() {
            // 有空闲 worker，创建任务和取消通道
            let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
            let task = crate::task::WorkerTask::Range {
                url: self.task_allocator.url().to_string(),
                range: info.range,
                retry_count: info.retry_count,
                cancel_rx,
            };

            if let Some(handle) = self.get_worker_handle(worker_id) {
                if let Err(e) = handle.send_task(task).await {
                    error!("分配重试任务失败: {:?}", e);
                    self.task_allocator.mark_worker_idle(worker_id);
                    // 任务发送失败，放回就绪队列
                    self.task_allocator.push_ready_retry_task(info);
                } else {
                    // 保存取消 sender
                    self.cancel_senders.insert(worker_id, cancel_tx);
                }
            } else {
                error!("Worker #{} 不存在", worker_id);
                self.task_allocator.mark_worker_idle(worker_id);
                self.task_allocator.push_ready_retry_task(info);
            }
        } else {
            // 没有空闲 worker，推入就绪队列等待下次分配
            debug!("定时器触发但没有空闲 worker，推入就绪队列");
            self.task_allocator.push_ready_retry_task(info);
        }

        LoopControl::Continue
    }

    /// 处理 worker 结果
    async fn handle_worker_result(&mut self, result: Option<RangeResult>) -> LoopControl {
        let Some(result) = result else {
            // 所有 worker 的 result_sender 都已关闭
            info!("所有 worker 已退出");
            return LoopControl::Break;
        };

        match result {
            RangeResult::Complete { worker_id } => self.handle_complete(worker_id).await,
            RangeResult::DownloadFailed { worker_id, range, error, retry_count } => {
                self.handle_failed(worker_id, range, error, retry_count).await
            }
            RangeResult::WriteFailed { worker_id, range, error, .. } => {
                // 写入失败通常是致命的（磁盘满、权限问题等），直接终止下载
                let (start, end) = range.as_range_tuple();
                error!(
                    "Worker #{} 写入失败，终止下载 (range: {}..{}): {}",
                    worker_id, start, end, error
                );
                LoopControl::Break
            }
        }
    }

    /// 处理任务完成事件
    async fn handle_complete(&mut self, worker_id: u64) -> LoopControl {
        self.progress_reporter.record_range_complete();

        // 移除该 worker 的取消 sender（任务已完成）
        self.cancel_senders.remove(&worker_id);

        // 将完成的 worker 标记为空闲
        self.task_allocator.mark_worker_idle(worker_id);

        // 尝试为空闲 worker 分配新任务
        self.try_allocate_next_task(worker_id).await;

        // 检查是否所有任务已完成
        self.check_completion_status()
    }

    /// 处理任务失败事件
    async fn handle_failed(
        &mut self,
        worker_id: u64,
        range: AllocatedRange,
        error: String,
        retry_count: usize,
    ) -> LoopControl {
        let (start, end) = range.as_range_tuple();
        warn!(
            "Worker #{} Range {}..{} 失败 (重试 {}): {}",
            worker_id,
            start,
            end,
            retry_count,
            error
        );

        // 移除该 worker 的取消 sender（任务已失败）
        self.cancel_senders.remove(&worker_id);

        // 将失败的 worker 标记为空闲
        self.task_allocator.mark_worker_idle(worker_id);

        // 调度重试任务
        self.schedule_retry_task(range, retry_count);

        // 尝试为空闲 worker 分配新任务
        self.try_allocate_next_task(worker_id).await;

        // 检查是否所有任务已完成
        let status = self.check_completion_status();
        if matches!(status, LoopControl::Break) {
            // 所有任务完成或出现永久失败，关闭 workers 以释放 result_sender
            self.pool.shutdown().await;
        }
        status
    }


    /// 分配初始任务给所有空闲的 worker
    async fn allocate_initial_tasks(&mut self) -> crate::Result<()> {
        let current_worker_count = self.pool.worker_count();
        info!(
            "渐进式启动 - 第1批: 已启动 {} 个 workers",
            current_worker_count
        );

        // 尝试为所有空闲 worker 分配初始任务
        while let Some(&worker_id) = self.task_allocator.idle_workers.front() {
            let chunk_size = self.get_worker_handle(worker_id)
                .map(|h| h.chunk_size())
                .unwrap_or(self.config.chunk().initial_size());

            if let Some(allocated) = self.task_allocator.try_allocate_task_to_idle_worker(chunk_size) {
                let (task, worker_id, cancel_tx) = allocated.into_parts();
                info!("为 Worker #{} 分配初始任务，分块大小 {} bytes", worker_id, chunk_size);

                if let Some(handle) = self.get_worker_handle(worker_id) {
                    if let Err(e) = handle.send_task(task).await {
                        error!("初始任务分配失败: {:?}", e);
                        // 失败了，将 worker 放回队列
                        self.task_allocator.mark_worker_idle(worker_id);
                    } else {
                        // 保存取消 sender
                        self.cancel_senders.insert(worker_id, cancel_tx);
                    }
                } else {
                    error!("Worker #{} 不存在", worker_id);
                    self.task_allocator.mark_worker_idle(worker_id);
                }
            } else {
                info!("没有足够的数据为空闲 workers 分配更多任务");
                break;
            }
        }

        Ok(())
    }

    /// 尝试为指定 worker 分配下一个任务
    ///
    /// 获取该 worker 的当前分块大小，并尝试分配新任务
    async fn try_allocate_next_task(&mut self, worker_id: u64) {
        let chunk_size = self.get_worker_handle(worker_id)
            .map(|h| h.chunk_size())
            .unwrap_or(self.config.chunk().initial_size());

        if let Some(allocated) = self.task_allocator.try_allocate_task_to_idle_worker(chunk_size) {
            let (task, target_worker, cancel_tx) = allocated.into_parts();
            debug!(
                "Worker #{} 分配新任务到空闲 Worker #{}，分块大小 {} bytes",
                worker_id, target_worker, chunk_size
            );

            if let Some(handle) = self.get_worker_handle(target_worker) {
                if let Err(e) = handle.send_task(task).await {
                    error!("分配新任务失败: {:?}", e);
                    // 失败了，将 worker 放回队列
                    self.task_allocator.mark_worker_idle(target_worker);
                } else {
                    // 保存取消通道，稍后由调用者保存
                    self.cancel_senders.insert(target_worker, cancel_tx);
                }
            } else {
                error!("Worker #{} 不存在", target_worker);
                self.task_allocator.mark_worker_idle(target_worker);
            }
        } else {
            debug!("Worker #{} 完成任务，但没有更多任务可分配", worker_id);
        }
    }

    /// 检查是否所有任务已完成
    ///
    /// 返回相应的 LoopControl 来控制主循环行为
    fn check_completion_status(&self) -> LoopControl {
        // 检查是否所有任务完成（包括新任务和重试任务）
        if self.task_allocator.remaining() == 0
            && self.task_allocator.pending_retry_count() == 0
            && self.writer.is_complete() {
            info!("所有任务已完成");
            return LoopControl::Break;
        }

        // 检查是否有永久失败的任务
        if self.task_allocator.has_permanent_failures() {
            error!("检测到永久失败的任务，准备终止下载");
            return LoopControl::Break;
        }

        LoopControl::Continue
    }

    /// 检查最终状态并返回结果
    ///
    /// 如果有永久失败的任务，返回错误；否则返回成功
    fn check_final_status(&self) -> crate::Result<Vec<FailedRange>> {
        if self.task_allocator.has_permanent_failures() {
            let failures = self.task_allocator.get_permanent_failures();
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

            return Err(DownloadError::Other(error_msg));
        }

        // 返回空的失败列表（所有失败都已重试或已处理）
        Ok(Vec::new())
    }

    /// 调度重试任务
    ///
    /// 根据重试次数计算延迟并注册定时器
    fn schedule_retry_task(
        &mut self,
        range: AllocatedRange,
        retry_count: usize,
    ) {
        let max_retry = self.config.retry().max_retry_count();
        let (start, end) = range.as_range_tuple();
        if retry_count < max_retry {
            // 还可以重试，计算延迟时间
            let retry_delays = self.config.retry().retry_delays();
            let delay = if retry_count < retry_delays.len() {
                retry_delays[retry_count]
            } else {
                // 如果重试次数超过序列长度，使用最后一个值
                *retry_delays.last().unwrap_or(&std::time::Duration::from_secs(3))
            };

            info!(
                "任务 range {}..{} 将在 {:.1}s 后进行第 {} 次重试",
                start,
                end,
                delay.as_secs_f64(),
                retry_count + 1
            );

            // 记录失败任务以便稍后重试
            if let Err(e) = self.task_allocator.record_failed_task(
                range,
                retry_count + 1,  // 下次重试的次数
                delay,
                &self.timer_service,
            ) {
                error!("注册重试定时器失败: {:?}", e);
                // 失败则标记为永久失败
                self.task_allocator.record_permanent_failure(
                    range,
                    format!("注册定时器失败: {}", e)
                );
                self.progress_reporter.record_range_complete();
            }
        } else {
            // 已达到最大重试次数，记录为永久失败
            error!(
                "任务 range {}..{} 已达到最大重试次数 {}，标记为永久失败",
                start,
                end,
                max_retry
            );
            self.task_allocator.record_permanent_failure(range, "达到最大重试次数".to_string());
            self.progress_reporter.record_range_complete();
        }
    }

    /// 关闭并清理资源
    pub(super) async fn shutdown_and_cleanup(mut self, error_msg: Option<String>) -> crate::Result<()> {
        // 发送错误事件
        if let Some(ref msg) = error_msg {
            self.progress_reporter.send_error(msg);
        }

        // 清理所有取消 sender
        self.cancel_senders.clear();

        // 关闭 workers（发送关闭信号）
        self.pool.shutdown().await;
        
        // 关闭 progress reporter actor
        self.progress_reporter.shutdown();
        
        // 关闭 progressive launcher actor 并等待其完全停止
        self.progressive_launcher.shutdown_and_wait().await;

        if let Some(msg) = error_msg {
            return Err(DownloadError::Other(msg));
        }

        Ok(())
    }

    /// 取消指定 worker 的当前任务（内部方法，不暴露给外部）
    ///
    /// 通过发送取消信号来中止 worker 正在执行的下载任务
    ///
    /// # Arguments
    ///
    /// * `worker_id` - 要取消任务的 worker ID
    ///
    /// # Returns
    ///
    /// 如果成功发送取消信号返回 `true`，否则返回 `false`（worker 可能没有正在执行的任务）
    fn cancel_worker_task(&mut self, worker_id: u64) -> bool {
        if let Some(cancel_tx) = self.cancel_senders.remove(&worker_id) {
            // 发送取消信号（忽略发送失败，因为接收端可能已关闭）
            let _ = cancel_tx.send(());
            debug!("已向 Worker #{} 发送取消信号", worker_id);
            true
        } else {
            debug!("Worker #{} 没有正在执行的任务", worker_id);
            false
        }
    }

    /// 检查并处理不健康的 worker
    ///
    /// 使用健康检查器来识别速度异常的 worker 并取消其任务
    fn check_and_handle_unhealthy_workers(&mut self) {
        // 检查是否启用健康检查
        if !self.config.health_check().enabled() {
            return;
        }

        let current_worker_count = self.pool.worker_count();
        let min_workers = self.config.health_check().min_workers_for_check();

        // worker 数量不足，跳过检查
        if current_worker_count < min_workers {
            return;
        }

        // 收集正在执行任务的 worker 的速度信息
        // 只检查有正在执行任务的 worker（通过 cancel_senders 判断）
        // 避免对空闲 worker 的旧速度数据反复警告
        let mut worker_speeds: Vec<WorkerSpeed> = Vec::new();

        // 遍历所有实际的 worker_id（不能假设从 0 开始）
        let handles = self.worker_handles.load();
        for &worker_id in handles.keys() {
            // 只检查正在执行任务的 worker
            if !self.cancel_senders.contains_key(&worker_id) {
                continue;
            }
            
            if let Some(handle) = handles.get(&worker_id) {
                let (speed, valid) = handle.window_avg_speed();
                // 只考虑有效的速度数据
                if valid && speed > 0.0 {
                    worker_speeds.push(WorkerSpeed { worker_id, speed });
                }
            }
        }

        // 至少需要 min_workers 个有效速度数据才能进行比较
        if (worker_speeds.len() as u64) < min_workers {
            return;
        }

        // 创建健康检查器
        let absolute_threshold = self.config.health_check().absolute_speed_threshold() as f64;
        let checker = WorkerHealthChecker::new(absolute_threshold, 0.5);

        // 执行健康检查
        let Some(result) = checker.check(&worker_speeds) else {
            return;
        };

        // 终止不健康的 worker
        for (worker_id, speed) in result.unhealthy_workers {
            warn!(
                "检测到不健康的 Worker #{}: 速度 {:.2} KB/s (基准: {:.2} KB/s, 阈值: {:.2} KB/s)，准备终止",
                worker_id,
                speed / 1024.0,
                result.health_baseline / 1024.0,
                absolute_threshold / 1024.0
            );

            if self.cancel_worker_task(worker_id) {
                info!("已取消 Worker #{} 的任务，将重新分配", worker_id);
                // worker 会在任务取消后返回 DownloadFailed 结果，
                // 触发失败处理流程，自动将该 worker 标记为空闲并重新分配任务
            }
        }
    }

    /// 完成并清理资源
    pub(super) async fn finalize_and_cleanup(mut self, save_path: PathBuf) -> crate::Result<()> {
        // 发送完成统计（Actor 会自动从共享数据源获取统计）
        self.progress_reporter.send_completion();

        // 清理所有取消 sender
        self.cancel_senders.clear();

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
    ) -> crate::Result<Vec<(u64, tokio::sync::oneshot::Sender<()>)>> {
        let current_worker_count = self.pool.worker_count();
        let next_target = current_worker_count + count;
        let mut new_cancel_senders = Vec::new();

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

        // 为新启动的worker加入队列并分配任务（使用实际的 worker_id）
        for worker_id in new_worker_ids {
            // 将新 worker 加入空闲队列
            self.task_allocator.mark_worker_idle(worker_id);

            let chunk_size = self.get_worker_handle(worker_id)
                .map(|h| h.chunk_size())
                .unwrap_or(self.config.chunk().initial_size());

            if let Some(allocated) = self.task_allocator.try_allocate_task_to_idle_worker(chunk_size) {
                let (task, assigned_worker, cancel_tx) = allocated.into_parts();
                info!("为新启动的 Worker #{} 分配任务，分块大小 {} bytes", assigned_worker, chunk_size);
                if let Some(handle) = self.get_worker_handle(assigned_worker) {
                    if let Err(e) = handle.send_task(task).await {
                        error!("为新 worker 分配任务失败: {:?}", e);
                        // 失败了，将 worker 放回队列
                        self.task_allocator.mark_worker_idle(assigned_worker);
                    } else {
                        // 收集取消通道，稍后由调用者保存
                        new_cancel_senders.push((assigned_worker, cancel_tx));
                    }
                } else {
                    error!("Worker #{} 不存在", assigned_worker);
                    self.task_allocator.mark_worker_idle(assigned_worker);
                }
            } else {
                debug!("没有足够的数据为新 worker #{} 分配任务", worker_id);
                break;
            }
        }

        Ok(new_cancel_senders)
    }
}