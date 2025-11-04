use crate::{DownloadError, Result};
use log::{debug, error, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use kestrel_timer::{TimerService, TaskId};
use crate::pool::download::DownloadWorkerPool;
use crate::utils::io_traits::{AsyncFile, FileSystem, HttpClient};
use crate::utils::range_writer::{RangeAllocator, RangeWriter, AllocatedRange};
use crate::task::RangeResult;

mod progressive;
mod task_allocator;
mod progress_reporter;

use progressive::ProgressiveLauncher;
use task_allocator::{TaskAllocator, FailedRange};
use progress_reporter::ProgressReporter;

/// 下载循环控制流
/// 
/// 用于控制下载事件循环的行为
enum LoopControl {
    /// 继续循环
    Continue,
    /// 正常完成，退出循环
    Break,
}

/// Worker 统计信息
#[derive(Debug, Clone)]
pub struct WorkerStatSnapshot {
    /// Worker ID
    pub worker_id: usize,
    /// 该 worker 下载的字节数
    pub bytes: u64,
    /// 该 worker 完成的 range 数量
    pub ranges: usize,
    /// 该 worker 平均速度 (bytes/s)
    pub avg_speed: f64,
    /// 该 worker 实时速度 (bytes/s)，如果无效则为 None
    pub instant_speed: Option<f64>,
    /// 该 worker 当前的分块大小 (bytes)
    pub current_chunk_size: u64,
}

/// 下载进度更新信息
#[derive(Debug, Clone)]
pub enum DownloadProgress {
    /// 下载已开始
    Started { 
        /// 文件总大小（bytes）
        total_size: u64,
        /// Worker 数量
        worker_count: usize,
        /// 初始分块大小（bytes）
        initial_chunk_size: u64,
    },
    /// 下载进度更新（包含总体统计和所有 worker 的统计）
    Progress {
        /// 已下载字节数
        bytes_downloaded: u64,
        /// 文件总大小（bytes）
        total_size: u64,
        /// 下载百分比 (0.0 ~ 100.0)
        percentage: f64,
        /// 平均速度 (bytes/s)
        avg_speed: f64,
        /// 实时速度 (bytes/s)，如果无效则为 None
        instant_speed: Option<f64>,
        /// 所有 worker 的统计信息（包含各自的分块大小）
        worker_stats: Vec<WorkerStatSnapshot>,
    },
    /// 下载已完成（包含最终的 worker 统计）
    Completed {
        /// 总下载字节数
        total_bytes: u64,
        /// 总耗时（秒）
        total_time: f64,
        /// 平均速度 (bytes/s)
        avg_speed: f64,
        /// 所有 worker 的最终统计信息
        worker_stats: Vec<WorkerStatSnapshot>,
    },
    /// 下载出错
    Error {
        /// 错误消息
        message: String,
    },
}

/// 下载任务句柄
/// 
/// 封装了正在进行的下载任务，提供进度监听和等待完成的接口
pub struct DownloadHandle {
    /// 接收进度更新的 channel
    progress_rx: Receiver<DownloadProgress>,
    /// 等待下载完成的 handle
    completion_handle: JoinHandle<Result<()>>,
}

impl DownloadHandle {
    /// 等待下载完成
    /// 
    /// 此方法会消费 handle 并等待下载任务完成
    /// 
    /// # Returns
    /// 
    /// 成功时返回 `Ok(())`，失败时返回错误信息
    pub async fn wait(self) -> Result<()> {
        self.completion_handle
            .await
            .map_err(|e| DownloadError::TaskPanic(e.to_string()))?
    }
    
    /// 获取进度接收器的可变引用
    /// 
    /// 使用此方法可以循环接收进度更新
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// # use hydra_dl::{download_ranged, DownloadProgress, DownloadConfig};
    /// # use kestrel_timer::{TimerWheel, config::ServiceConfig};
    /// # use std::path::PathBuf;
    /// # #[tokio::main]
    /// # async fn main() {
    /// let config = DownloadConfig::default();
    /// let timer = TimerWheel::with_defaults();
    /// let timer_service = timer.create_service(ServiceConfig::default());
    /// let (mut handle, save_path) = download_ranged("http://example.com/file", PathBuf::from("."), config, timer_service).await.unwrap();
    /// 
    /// while let Some(progress) = handle.progress_receiver().recv().await {
    ///     match progress {
    ///         DownloadProgress::Progress { percentage, avg_speed, worker_stats, .. } => {
    ///             // 每个 worker 有各自的分块大小，可从 worker_stats 中获取
    ///             println!("进度: {:.1}%, 速度: {:.2} MB/s, {} workers", 
    ///                 percentage, avg_speed / 1024.0 / 1024.0, worker_stats.len());
    ///         }
    ///         DownloadProgress::Completed { .. } => {
    ///             println!("下载完成！");
    ///         }
    ///         _ => {}
    ///     }
    /// }
    /// 
    /// handle.wait().await.unwrap();
    /// # }
    /// ```
    pub fn progress_receiver(&mut self) -> &mut Receiver<DownloadProgress> {
        &mut self.progress_rx
    }
    
    /// 同时接收进度并等待完成
    /// 
    /// 这是一个便捷方法，会持续接收进度更新直到下载完成
    /// 
    /// # Arguments
    /// 
    /// * `callback` - 每次收到进度更新时调用的回调函数
    /// 
    /// # Returns
    /// 
    /// 成功时返回 `Ok(())`，失败时返回错误信息
    pub async fn wait_with_progress<F>(mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(DownloadProgress),
    {
        loop {
            tokio::select! {
                progress = self.progress_rx.recv() => {
                    match progress {
                        Some(p) => callback(p),
                        None => break, // channel 关闭，下载任务结束
                    }
                }
                result = &mut self.completion_handle => {
                    // 下载任务完成，继续接收剩余的进度消息
                    while let Ok(progress) = self.progress_rx.try_recv() {
                        callback(progress);
                    }
                    return result.map_err(|e| DownloadError::TaskPanic(e.to_string()))?;
                }
            }
        }
        
        // channel 关闭后等待任务完成
        self.completion_handle.await.map_err(|e| DownloadError::TaskPanic(e.to_string()))?
    }
}

/// 下载任务执行器
/// 
/// 封装了下载任务的执行逻辑，使用辅助结构体管理任务分配和进度报告
struct DownloadTask<C: HttpClient, F: AsyncFile> {
    /// HTTP 客户端（用于动态添加 worker）
    client: C,
    /// Worker 协程池
    pool: DownloadWorkerPool<F>,
    /// 文件写入器
    writer: Arc<RangeWriter<F>>,
    /// 任务分配器
    task_allocator: TaskAllocator,
    /// 进度报告器
    progress_reporter: ProgressReporter,
    /// 下载配置
    config: Arc<crate::config::DownloadConfig>,
    /// 渐进式启动管理器
    progressive_launcher: ProgressiveLauncher,
    /// 定时器服务（用于管理失败任务的重试定时器）
    timer_service: TimerService,
}

impl<C: HttpClient + Clone, F: AsyncFile> DownloadTask<C, F> {
    /// 创建新的下载任务
    fn new(
        client: C,
        progress_sender: Option<Sender<DownloadProgress>>,
        writer: Arc<RangeWriter<F>>,
        allocator: RangeAllocator,
        url: String,
        total_size: u64,
        timer_service: TimerService,
        config: Arc<crate::config::DownloadConfig>,
    ) -> Result<Self> {
        // 创建渐进式启动管理器
        let progressive_launcher = ProgressiveLauncher::new(&config);
        
        // 第一批 worker 数量
        let initial_worker_count = progressive_launcher.initial_worker_count();
        
        info!("初始启动 {} 个 workers", initial_worker_count);
        
        // 创建 DownloadWorkerPool（只启动第一批 worker）
        let pool = DownloadWorkerPool::new(
            client.clone(),
            initial_worker_count,
            Arc::clone(&writer),
            Arc::clone(&config),
        )?;
        
        let mut task_allocator = TaskAllocator::new(allocator, url);
        let progress_reporter = ProgressReporter::new(progress_sender, total_size);
        
        // 将第一批 workers 加入空闲队列
        for worker_id in 0..initial_worker_count {
            task_allocator.mark_worker_idle(worker_id);
        }
        
        Ok(Self {
            client,
            pool,
            writer,
            task_allocator,
            progress_reporter,
            config,
            progressive_launcher,
            timer_service,
        })
    }
    
    
    /// 等待所有 range 完成
    /// 
    /// 动态分配任务，支持失败重试，定期发送进度更新和调整分块大小
    /// 如果有任务达到最大重试次数，将终止下载并返回错误
    async fn wait_for_completion(&mut self) -> Result<Vec<FailedRange>> {
        // 获取定时器超时接收器
        let timeout_rx = self.timer_service.take_receiver()
            .ok_or_else(|| DownloadError::Other("无法获取定时器接收器".to_string()))?;
        
        // 创建定时器，用于定期更新进度和调整分块大小
        let mut progress_timer = tokio::time::interval(self.config.speed().instant_speed_window());
        progress_timer.tick().await; // 跳过首次立即触发
        
        // 分配初始任务
        self.allocate_initial_tasks().await?;
        
        // 事件循环：分发各种事件到对应的处理器
        loop {
            let control = tokio::select! {
                _ = progress_timer.tick() => self.handle_progress_tick().await,
                Some(notification) = timeout_rx.recv() => self.handle_retry_timeout(notification.task_id()).await,
                result = self.pool.result_receiver().recv() => self.handle_worker_result(result).await,
            };
            
            match control {
                LoopControl::Continue => continue,
                LoopControl::Break => break,
            }
        }
        
        // 检查最终状态并返回结果
        self.check_final_status()
    }
    
    /// 处理进度更新定时器触发
    async fn handle_progress_tick(&mut self) -> LoopControl {
        self.send_progress().await;
        
        // 检查是否可以启动下一批worker（渐进式启动）
        if self.progressive_launcher.should_check_next_stage() {
            if let Err(e) = self.progressive_launcher.check_and_launch_next_stage(
                &mut self.pool,
                self.client.clone(),
                &self.config,
                &mut self.task_allocator,
            ).await {
                error!("渐进式启动下一批 worker 失败: {:?}", e);
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
        
        info!(
            "定时器触发，重试任务 range {}..{}, 重试次数 {}",
            info.range.start(),
            info.range.end(),
            info.retry_count
        );
        
        // 从空闲队列获取 worker
        if let Some(worker_id) = self.task_allocator.idle_workers.pop_front() {
            // 有空闲 worker，立即分配任务
            let task = crate::task::WorkerTask::Range {
                url: self.task_allocator.url().to_string(),
                range: info.range,
                retry_count: info.retry_count,
            };
            
            if let Err(e) = self.pool.send_task(task, worker_id).await {
                error!("分配重试任务失败: {:?}", e);
                self.task_allocator.mark_worker_idle(worker_id);
                // 任务发送失败，放回就绪队列
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
            RangeResult::Failed { worker_id, range, error, retry_count } => {
                self.handle_failed(worker_id, range, error, retry_count).await
            }
        }
    }
    
    /// 处理任务完成事件
    async fn handle_complete(&mut self, worker_id: usize) -> LoopControl {
        self.progress_reporter.record_range_complete();
        
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
        worker_id: usize,
        range: AllocatedRange,
        error: String,
        retry_count: usize,
    ) -> LoopControl {
        warn!(
            "Worker #{} Range {}..{} 失败 (重试 {}): {}",
            worker_id,
            range.start(),
            range.end(),
            retry_count,
            error
        );
        
        // 将失败的 worker 标记为空闲
        self.task_allocator.mark_worker_idle(worker_id);
        
        // 调度重试任务
        self.schedule_retry_task(range, retry_count);
        
        // 尝试为空闲 worker 分配新任务
        self.try_allocate_next_task(worker_id).await;
        
        // 检查是否所有任务已完成
        self.check_completion_status()
    }
    
    /// 发送进度更新
    /// 
    /// 定期发送进度更新（分块大小由各 worker 独立调整）
    async fn send_progress(&mut self) {
        self.progress_reporter.send_progress_update(&self.pool).await;
    }
    
    /// 分配初始任务给所有空闲的 worker
    async fn allocate_initial_tasks(&mut self) -> Result<()> {
        let current_worker_count = self.pool.worker_count();
        info!(
            "渐进式启动 - 第1批: 已启动 {} 个 workers",
            current_worker_count
        );
        
        // 尝试为所有空闲 worker 分配初始任务
        while let Some(&worker_id) = self.task_allocator.idle_workers.front() {
            let chunk_size = self.pool.get_worker_chunk_size(worker_id);
            
            if let Some((task, assigned_worker)) = self.task_allocator.try_allocate_task_to_idle_worker(chunk_size) {
                info!("为 Worker #{} 分配初始任务，分块大小 {} bytes", assigned_worker, chunk_size);
                if let Err(e) = self.pool.send_task(task, assigned_worker).await {
                    error!("初始任务分配失败: {:?}", e);
                    // 失败了，将 worker 放回队列
                    self.task_allocator.mark_worker_idle(assigned_worker);
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
    async fn try_allocate_next_task(&mut self, worker_id: usize) {
        let chunk_size = self.pool.get_worker_chunk_size(worker_id);
        
        if let Some((task, target_worker)) = self.task_allocator.try_allocate_task_to_idle_worker(chunk_size) {
            debug!(
                "Worker #{} 分配新任务到空闲 Worker #{}，分块大小 {} bytes",
                worker_id, target_worker, chunk_size
            );
            if let Err(e) = self.pool.send_task(task, target_worker).await {
                error!("分配新任务失败: {:?}", e);
                // 失败了，将 worker 放回队列
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
            info!(
                "所有任务已完成，总共完成 {} 个 range",
                self.progress_reporter.total_ranges_completed()
            );
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
    fn check_final_status(&self) -> Result<Vec<FailedRange>> {
        if self.task_allocator.has_permanent_failures() {
            let failures = self.task_allocator.get_permanent_failures();
            let error_details: Vec<String> = failures
                .iter()
                .map(|(range, error)| {
                    format!("range {}..{}: {}", range.start(), range.end(), error)
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
                range.start(),
                range.end(),
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
                range.start(),
                range.end(),
                max_retry
            );
            self.task_allocator.record_permanent_failure(range, "达到最大重试次数".to_string());
            self.progress_reporter.record_range_complete();
        }
    }
    
    /// 关闭并清理资源
    async fn shutdown_and_cleanup(mut self, error_msg: Option<String>) -> Result<()> {
        // 发送错误事件
        if let Some(ref msg) = error_msg {
            self.progress_reporter.send_error(msg).await;
        }
        
        // 关闭 workers（发送关闭信号，workers 会异步自动清理）
        self.pool.shutdown();
        
        // 等待所有 workers 完成自动清理
        self.pool.wait_for_shutdown().await;
        
        if let Some(msg) = error_msg {
            return Err(DownloadError::Other(msg));
        }
        
        Ok(())
    }
    
    /// 完成并清理资源
    async fn finalize_and_cleanup(self, save_path: PathBuf) -> Result<()> {
        // 发送完成统计
        self.progress_reporter.send_completion_stats(&self.pool).await;
        
        // 优雅关闭所有 workers（发送关闭信号，workers 会异步自动清理）
        let mut pool = self.pool;
        pool.shutdown();
        
        // 等待所有 workers 完成自动清理
        // 这确保所有对 executor（含 writer）的引用都已释放
        pool.wait_for_shutdown().await;
        
        // 释放 pool（它持有 writer 的引用）
        drop(pool);
        
        // 完成写入（从 Arc 中提取 writer）
        let writer = Arc::try_unwrap(self.writer)
            .map_err(|_| DownloadError::WriterOwnership)?;
        writer.finalize().await?;
        
        info!("Range 下载任务完成: {:?}", save_path);
        
        Ok(())
    }
}

/// 使用 Range 请求下载单个文件（内部泛型实现）
/// 
/// 为此下载任务创建独立的协程池，下载完成后销毁
/// Workers 直接写入共享的 RangeWriter，减少内存拷贝
/// 使用动态分块机制，根据实时速度自动调整分块大小
/// 
/// # Arguments
/// 
/// * `config` - 下载配置，包含动态分块策略和并发控制参数
/// * `progress_sender` - 可选的进度更新发送器，通过 channel 发送进度信息
async fn download_ranged_generic<C, FS>(
    client: C,
    fs: FS,
    url: &str,
    save_path: PathBuf,
    config: &crate::config::DownloadConfig,
    progress_sender: Option<Sender<DownloadProgress>>,
    timer_service: TimerService,
) -> Result<()>
where
    C: HttpClient + Clone + Send + 'static,
    FS: FileSystem,
    FS::File: Send + 'static,
{
    let worker_count = config.concurrency().worker_count();
    
    info!("准备 Range 下载: {} ({} 个 workers, 动态分块)", url, worker_count);

    // 获取文件元数据
    let metadata = crate::utils::fetch::fetch_file_metadata(&client, url).await?;

    if !metadata.range_supported {
        warn!("服务器不支持 Range 请求，回退到普通下载");
        let task = crate::task::FileTask {
            url: url.to_string(),
            save_path: save_path.clone(),
        };
        return Ok(crate::utils::fetch::fetch_file(&client, task, &fs).await?);
    }

    let content_length = metadata.content_length.ok_or_else(|| DownloadError::Other("无法获取文件大小".to_string()))?;
    info!("文件大小: {} bytes ({:.2} MB)", content_length, content_length as f64 / 1024.0 / 1024.0);
    info!(
        "动态分块配置: 初始 {} bytes, 范围 {} ~ {} bytes",
        config.chunk().initial_size(),
        config.chunk().min_size(),
        config.chunk().max_size()
    );

    // 创建 RangeWriter 和 RangeAllocator（会预分配文件）
    let (writer, allocator) = RangeWriter::new(&fs, save_path.clone(), content_length).await?;

    // 将 writer 包装在 Arc 中
    let writer = Arc::new(writer);
    let config = Arc::new(config.clone());

    // 创建下载任务（内部会创建 WorkerPool 并启动第一批 worker）
    let mut task = DownloadTask::new(
        client,
        progress_sender,
        writer,
        allocator,
        url.to_string(),
        content_length,
        timer_service,
        Arc::clone(&config),
    )?;
    
    // 发送开始事件（使用第一个 worker 的初始分块大小）
    let current_worker_count = task.pool.worker_count();
    let initial_chunk_size = task.pool.get_worker_chunk_size(0);
    task.progress_reporter.send_started_event(current_worker_count, initial_chunk_size).await;

    // 等待所有任务完成（内部会动态分配任务）
    let failed_ranges = task.wait_for_completion().await?;

    // 处理失败的任务
    if !failed_ranges.is_empty() {
        let error_msg = format!("有 {} 个 Range 下载失败", failed_ranges.len());
        return task.shutdown_and_cleanup(Some(error_msg)).await;
    }

    // 验证完成状态
    if !task.writer.is_complete() {
        let error_msg = "下载未完成，但所有任务已处理".to_string();
        return task.shutdown_and_cleanup(Some(error_msg)).await;
    }

    // 完成并清理
    task.finalize_and_cleanup(save_path).await
}

/// 使用 Range 请求下载单个文件（公共API）
/// 
/// 为此下载任务创建独立的协程池，下载完成后销毁
/// Workers 直接写入共享的 RangeWriter，减少内存拷贝
/// 使用动态分块机制，根据实时下载速度自动调整分块大小
/// 
/// **破坏性变更**：此函数现在接受目录路径而非文件路径，并自动从服务器或 URL 提取文件名
/// 
/// # Arguments
/// * `url` - 下载 URL
/// * `save_dir` - 保存目录路径
/// * `config` - 下载配置（包含动态分块参数、worker数等）
/// 
/// # Returns
/// 
/// 返回 `(DownloadHandle, PathBuf)`，其中：
/// - `DownloadHandle` - 可以通过它监听下载进度并等待完成
/// - `PathBuf` - 实际保存的文件路径（目录 + 自动检测的文件名）
/// 
/// # 文件名检测优先级
/// 
/// 1. Content-Disposition header（服务器建议的文件名）
/// 2. 重定向后 URL 中的文件名
/// 3. 原始 URL 中的文件名
/// 4. 时间戳文件名 `file_{unix_timestamp}`
/// 
/// # Example
/// 
/// ```no_run
/// # use hydra_dl::{download_ranged, DownloadConfig, DownloadProgress};
/// # use hydra_dl::timer::{TimerWheel, TimerService, ServiceConfig};
/// # use std::path::PathBuf;
/// # #[tokio::main]
/// # async fn main() -> Result<(), hydra_dl::DownloadError> {
/// // 使用默认配置（推荐）
/// let config = DownloadConfig::default();
/// let timer = TimerWheel::with_defaults();
/// let service = timer.create_service(ServiceConfig::default());
/// let (mut handle, save_path) = download_ranged(
///     "http://example.com/file.bin",
///     PathBuf::from("."),  // 保存到当前目录
///     config,
///     service,
/// ).await?;
/// 
/// println!("文件将保存到: {:?}", save_path);
/// 
/// // 监听进度
/// while let Some(progress) = handle.progress_receiver().recv().await {
///     match progress {
///         DownloadProgress::Progress { percentage, avg_speed, worker_stats, .. } => {
///             // 每个 worker 有各自的分块大小，可从 worker_stats 中获取
///             println!("进度: {:.1}%, 速度: {:.2} MB/s, {} workers", 
///                 percentage, 
///                 avg_speed / 1024.0 / 1024.0,
///                 worker_stats.len());
///         }
///         DownloadProgress::Completed { total_bytes, total_time, worker_stats, .. } => {
///             println!("下载完成！{:.2} MB in {:.2}s, {} workers", 
///                 total_bytes as f64 / 1024.0 / 1024.0, total_time, worker_stats.len());
///         }
///         _ => {}
///     }
/// }
/// 
/// // 等待下载完成
/// handle.wait().await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_ranged(
    url: &str,
    save_dir: impl AsRef<std::path::Path>,
    config: crate::config::DownloadConfig,
    timer_service: TimerService,
) -> Result<(DownloadHandle, std::path::PathBuf)> {
    use crate::utils::io_traits::TokioFileSystem;
    use reqwest::Client;
    
    // 创建带超时设置的 HTTP 客户端
    let client = Client::builder()
        .timeout(config.network().timeout())
        .connect_timeout(config.network().connect_timeout())
        .build()?;
    
    let fs = TokioFileSystem::default();
    
    // 获取文件元数据以确定文件名
    info!("正在获取文件元数据: {}", url);
    let metadata = crate::utils::fetch::fetch_file_metadata(&client, url).await?;
    
    // 确定文件名
    let filename = metadata.suggested_filename
        .ok_or_else(|| DownloadError::Other("无法确定文件名".to_string()))?;
    
    // 组合完整路径
    let save_path = save_dir.as_ref().join(&filename);
    
    info!("自动检测到文件名: {}", filename);
    info!("保存路径: {:?}", save_path);
    
    // 创建进度 channel
    let (progress_tx, progress_rx) = mpsc::channel(100);
    
    // 克隆必要的参数给后台任务
    let url = url.to_string();
    let save_path_clone = save_path.clone();
    
    // 启动后台下载任务
    let completion_handle = tokio::spawn(async move {
        download_ranged_generic(
            client, 
            fs, 
            &url, 
            save_path_clone, 
            &config,
            Some(progress_tx),
            timer_service,
        ).await
    });
    
    Ok((
        DownloadHandle {
            progress_rx,
            completion_handle,
        },
        save_path,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::io_traits::mock::{MockHttpClient, MockFileSystem};
    use reqwest::{header::HeaderMap, StatusCode};
    use bytes::Bytes;
    use std::path::PathBuf;
    use kestrel_timer::{TimerWheel, config::ServiceConfig};

    fn create_timer_service() -> (TimerWheel, TimerService) {
        let timer = TimerWheel::with_defaults();
        let service = timer.create_service(ServiceConfig::default());
        (timer, service)
    }

    #[tokio::test]
    async fn test_download_ranged_basic() {
        let (_timer, timer_service) = create_timer_service();

        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 36 bytes
        let save_path = PathBuf::from("/tmp/test_download.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应（检查 Range 支持）
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置 Range 请求响应
        // 假设分成 3 个 range：0-11, 12-23, 24-35
        let range_count = 3;
        let chunk_size = test_data.len() / range_count;

        for i in 0..range_count {
            let start = i * chunk_size;
            let end = if i == range_count - 1 {
                test_data.len()
            } else {
                (i + 1) * chunk_size
            };

            let chunk = &test_data[start..end];
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, test_data.len())
                    .parse()
                    .unwrap(),
            );

            client.set_range_response(
                test_url,
                start as u64,
                (end - 1) as u64,
                StatusCode::PARTIAL_CONTENT,
                headers,
                Bytes::copy_from_slice(chunk),
            );
        }

        // 执行下载（使用 1 个 worker 以简化测试）
        let chunk_size = chunk_size as u64;
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(1))
            .chunk(|c| c
                .initial_size(chunk_size)
                .min_size(1)  // 设置为 1 以允许小文件测试
                .max_size(chunk_size))  // 固定分块大小以便测试
            .build()
            .unwrap();
        
        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None, // 测试中不需要进度更新
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "下载应该成功: {:?}", result);

        // 验证文件已创建
        let file = fs.get_file(&save_path);
        assert!(file.is_some(), "文件应该已创建");

        // 注意：由于 MockFileSystem 的限制，我们无法直接验证文件内容
        // 但可以验证请求日志
        let log = client.get_request_log();
        assert!(log.len() > 0, "应该有请求记录");
    }

    #[tokio::test]
    async fn test_download_ranged_fallback_to_normal() {
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/file.bin";
        let test_data = b"Test data without range support";
        let save_path = PathBuf::from("/tmp/test_fallback.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应（不支持 Range）
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "none".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置普通 GET 请求响应
        let mut get_headers = HeaderMap::new();
        get_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_response(
            test_url,
            StatusCode::OK,
            get_headers,
            Bytes::from_static(test_data),
        );

        // 执行下载
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(2))
            .chunk(|c| c
                .initial_size(test_data.len() as u64)  // 单次分块完成
                .min_size(1)
                .max_size(test_data.len() as u64))
            .build()
            .unwrap();
        
        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None, // 测试中不需要进度更新
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "应该回退到普通下载: {:?}", result);

        // 验证使用了 HEAD 和 GET 请求
        let log = client.get_request_log();
        assert!(log.len() >= 2, "应该有 HEAD 和 GET 请求");
        assert!(log.iter().any(|s| s.starts_with("HEAD")), "应该有 HEAD 请求");
        assert!(log.iter().any(|s| s.starts_with("GET http://example.com/file.bin")), "应该有 GET 请求");
    }

    #[tokio::test]
    async fn test_download_ranged_multiple_workers() {
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/file.bin";
        let test_data: Vec<u8> = (0..100).collect(); // 100 bytes
        let save_path = PathBuf::from("/tmp/test_multi_workers.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置 Range 请求响应（4 个 range）
        let range_count = 4;
        let chunk_size = test_data.len() / range_count;

        for i in 0..range_count {
            let start = i * chunk_size;
            let end = if i == range_count - 1 {
                test_data.len()
            } else {
                (i + 1) * chunk_size
            };

            let chunk = &test_data[start..end];
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, test_data.len())
                    .parse()
                    .unwrap(),
            );

            client.set_range_response(
                test_url,
                start as u64,
                (end - 1) as u64,
                StatusCode::PARTIAL_CONTENT,
                headers,
                Bytes::from(chunk.to_vec()),
            );
        }

        // 使用 2 个 workers 下载
        let chunk_size = chunk_size as u64;
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(2))
            .chunk(|c| c
                .initial_size(chunk_size)
                .min_size(1)
                .max_size(chunk_size))  // 固定分块大小以便测试
            .build()
            .unwrap();
        
        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None, // 测试中不需要进度更新
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "多 worker 下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_dynamic_chunking_small_file() {
        // 测试小文件（< 2MB）自动调整为 1 个分块
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/small_file.bin";
        let test_data: Vec<u8> = vec![0; 1024 * 1024]; // 1 MB 文件
        let save_path = PathBuf::from("/tmp/test_small_file.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置单个 Range 请求（因为会被调整为 1 个分块）
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-{}/{}", test_data.len() - 1, test_data.len())
                .parse()
                .unwrap(),
        );
        client.set_range_response(
            test_url,
            0,
            (test_data.len() - 1) as u64,
            StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from(test_data.clone()),
        );

        // 使用 1 MB 的初始分块大小下载 1 MB 文件
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(4))
            .chunk(|c| c
                .initial_size(1 * 1024 * 1024)  // 1 MB
                .min_size(512 * 1024)  // 512 KB
                .max_size(2 * 1024 * 1024))  // 2 MB
            .build()
            .unwrap();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "小文件下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_dynamic_chunking_medium_file() {
        // 测试中等文件正确计算分块数
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/medium_file.bin";
        let file_size = 10 * 1024 * 1024; // 10 MB 文件
        let test_data: Vec<u8> = vec![0; file_size];
        let save_path = PathBuf::from("/tmp/test_medium_file.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", file_size.to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 使用 2MB 的分块大小
        let chunk_size = 2 * 1024 * 1024;
        let expected_chunks = (file_size + chunk_size - 1) / chunk_size;

        for i in 0..expected_chunks {
            let start = i * chunk_size;
            let end = if i == expected_chunks - 1 {
                file_size
            } else {
                (i + 1) * chunk_size
            };

            let chunk = &test_data[start..end];
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, file_size)
                    .parse()
                    .unwrap(),
            );

            client.set_range_response(
                test_url,
                start as u64,
                (end - 1) as u64,
                StatusCode::PARTIAL_CONTENT,
                headers,
                Bytes::from(chunk.to_vec()),
            );
        }

        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(3))
            .chunk(|c| c
                .initial_size(chunk_size as u64)
                .min_size(chunk_size as u64)
                .max_size(chunk_size as u64))  // 固定 2 MB 分块
            .build()
            .unwrap();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "中等文件下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_dynamic_chunking_large_file() {
        // 测试大文件不受最小分块限制影响
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/large_file.bin";
        let file_size = 100 * 1024 * 1024; // 100 MB 文件
        let save_path = PathBuf::from("/tmp/test_large_file.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", file_size.to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 使用 10MB 的分块大小
        let chunk_size = 10 * 1024 * 1024;
        let expected_chunks = (file_size + chunk_size - 1) / chunk_size;

        for i in 0..expected_chunks {
            let start = i * chunk_size;
            let end = if i == expected_chunks - 1 {
                file_size
            } else {
                (i + 1) * chunk_size
            };

            let chunk = vec![0u8; end - start];
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, file_size)
                    .parse()
                    .unwrap(),
            );

            client.set_range_response(
                test_url,
                start as u64,
                (end - 1) as u64,
                StatusCode::PARTIAL_CONTENT,
                headers,
                Bytes::from(chunk),
            );
        }

        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(4))
            .chunk(|c| c
                .initial_size(chunk_size as u64)
                .min_size(chunk_size as u64)
                .max_size(chunk_size as u64))  // 固定 10 MB 分块
            .build()
            .unwrap();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "大文件下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_progressive_worker_launch() {
        // 测试渐进式启动配置
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/file.bin";
        let test_data: Vec<u8> = (0..100).collect(); // 100 bytes
        let save_path = PathBuf::from("/tmp/test_progressive.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置足够多的 Range 请求响应
        let chunk_size = 10;
        let range_count = (test_data.len() + chunk_size - 1) / chunk_size;

        for i in 0..range_count {
            let start = i * chunk_size;
            let end = if i == range_count - 1 {
                test_data.len()
            } else {
                (i + 1) * chunk_size
            };

            let chunk = &test_data[start..end];
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, test_data.len())
                    .parse()
                    .unwrap(),
            );

            client.set_range_response(
                test_url,
                start as u64,
                (end - 1) as u64,
                StatusCode::PARTIAL_CONTENT,
                headers,
                Bytes::from(chunk.to_vec()),
            );
        }

        // 配置渐进式启动：[0.5, 1.0] 表示先启动2个worker，再启动剩余2个
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(4))
            .chunk(|c| c
                .initial_size(chunk_size as u64)
                .min_size(chunk_size as u64)
                .max_size(chunk_size as u64))
            .progressive(|p| p
                .worker_ratios(vec![0.5, 1.0])
                .min_speed_threshold(0))  // 设置为0以便立即启动下一批
            .build()
            .unwrap();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "渐进式启动下载应该成功: {:?}", result);
    }

    #[test]
    fn test_progressive_config() {
        // 测试渐进式启动配置的正确性
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(12))
            .progressive(|p| p
                .worker_ratios(vec![0.25, 0.5, 0.75, 1.0])
                .min_speed_threshold(5 * 1024 * 1024))  // 5 MB/s
            .build()
            .unwrap();

        assert_eq!(config.concurrency().worker_count(), 12);
        assert_eq!(config.progressive().worker_ratios(), &[0.25, 0.5, 0.75, 1.0]);
        assert_eq!(config.progressive().min_speed_threshold(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_retry_config() {
        // 测试重试配置的正确性
        let config = crate::config::DownloadConfig::builder()
            .retry(|r| r
                .max_retry_count(5)
                .retry_delays(vec![
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(2),
                    std::time::Duration::from_secs(5),
                ])
            )
            .build()
            .unwrap();

        assert_eq!(config.retry().max_retry_count(), 5);
        assert_eq!(config.retry().retry_delays().len(), 3);
        assert_eq!(config.retry().retry_delays()[0], std::time::Duration::from_secs(1));
        assert_eq!(config.retry().retry_delays()[1], std::time::Duration::from_secs(2));
        assert_eq!(config.retry().retry_delays()[2], std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_retry_config_default() {
        // 测试默认重试配置
        let config = crate::config::DownloadConfig::default();
        
        assert_eq!(config.retry().max_retry_count(), 3);
        assert_eq!(config.retry().retry_delays().len(), 3);
        assert_eq!(config.retry().retry_delays()[0], std::time::Duration::from_secs(1));
        assert_eq!(config.retry().retry_delays()[1], std::time::Duration::from_secs(2));
        assert_eq!(config.retry().retry_delays()[2], std::time::Duration::from_secs(3));
    }

    #[test]
    fn test_retry_delays_empty_uses_default() {
        // 测试空延迟序列使用默认值
        let config = crate::config::DownloadConfig::builder()
            .retry(|r| { r
                .retry_delays(vec![])
            })
            .build()
            .unwrap();
            

        assert_eq!(config.retry().retry_delays().len(), 3);
        assert_eq!(config.retry().retry_delays()[0], std::time::Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_download_with_retry_success() {
        // 测试失败任务重试成功
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 36 bytes
        let save_path = PathBuf::from("/tmp/test_retry_success.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置 Range 请求响应
        let chunk_size = 12;
        let range_count = (test_data.len() + chunk_size - 1) / chunk_size;

        for i in 0..range_count {
            let start = i * chunk_size;
            let end = if i == range_count - 1 {
                test_data.len()
            } else {
                (i + 1) * chunk_size
            };

            let chunk = &test_data[start..end];
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, test_data.len())
                    .parse()
                    .unwrap(),
            );

            // 第一个 range 第一次失败，第二次成功（模拟重试）
            if i == 0 {
                // 第一次请求失败
                client.set_range_response(
                    test_url,
                    start as u64,
                    (end - 1) as u64,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    Bytes::new(),
                );
                // 第二次请求成功（重试）
                client.set_range_response(
                    test_url,
                    start as u64,
                    (end - 1) as u64,
                    StatusCode::PARTIAL_CONTENT,
                    headers,
                    Bytes::copy_from_slice(chunk),
                );
            } else {
                client.set_range_response(
                    test_url,
                    start as u64,
                    (end - 1) as u64,
                    StatusCode::PARTIAL_CONTENT,
                    headers,
                    Bytes::copy_from_slice(chunk),
                );
            }
        }

        // 配置：1 个 worker，最大重试 3 次，快速重试（100ms）
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(1))
            .chunk(|c| c.initial_size(chunk_size as u64).min_size(1).max_size(chunk_size as u64))
            .retry(|r| r
                .max_retry_count(3)
                .retry_delays(vec![
                    std::time::Duration::from_millis(100),
                    std::time::Duration::from_millis(200),
                    std::time::Duration::from_millis(300),
                ]))
            .build()
            .unwrap();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
            timer_service,
        )
        .await;

        assert!(result.is_ok(), "下载应该成功（经过重试）: {:?}", result);
    }

    #[tokio::test]
    async fn test_download_with_retry_permanent_failure() {
        // 测试达到最大重试次数后失败
        let (_timer, timer_service) = create_timer_service();
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 36 bytes
        let save_path = PathBuf::from("/tmp/test_retry_failure.bin");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 HEAD 请求响应
        let mut head_headers = HeaderMap::new();
        head_headers.insert("accept-ranges", "bytes".parse().unwrap());
        head_headers.insert("content-length", test_data.len().to_string().parse().unwrap());
        client.set_head_response(
            test_url,
            StatusCode::OK,
            head_headers,
        );

        // 设置 Range 请求响应：第一个 range 始终失败
        let chunk_size = 12;
        let range_count = (test_data.len() + chunk_size - 1) / chunk_size;

        for i in 0..range_count {
            let start = i * chunk_size;
            let end = if i == range_count - 1 {
                test_data.len()
            } else {
                (i + 1) * chunk_size
            };

            if i == 0 {
                // 第一个 range 始终失败（模拟多次重试都失败）
                for _ in 0..5 {
                    client.set_range_response(
                        test_url,
                        start as u64,
                        (end - 1) as u64,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HeaderMap::new(),
                        Bytes::new(),
                    );
                }
            } else {
                let chunk = &test_data[start..end];
                let mut headers = HeaderMap::new();
                headers.insert(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end - 1, test_data.len())
                        .parse()
                        .unwrap(),
                );
                client.set_range_response(
                    test_url,
                    start as u64,
                    (end - 1) as u64,
                    StatusCode::PARTIAL_CONTENT,
                    headers,
                    Bytes::copy_from_slice(chunk),
                );
            }
        }

        // 配置：1 个 worker，最大重试 2 次，快速重试（50ms）
        let config = crate::config::DownloadConfig::builder()
            .concurrency(|c| c.worker_count(1))
            .chunk(|c| c.initial_size(chunk_size as u64).min_size(1).max_size(chunk_size as u64))
            .retry(|r| r
                .max_retry_count(2)
                .retry_delays(vec![
                    std::time::Duration::from_millis(50),
                    std::time::Duration::from_millis(50),
                ]))
            .build()
            .unwrap();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
            timer_service,
        )
        .await;

        // 应该失败，因为达到最大重试次数
        assert!(result.is_err(), "下载应该失败（达到最大重试次数）");
        
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("达到最大重试次数"), "错误消息应该包含重试信息");
    }

    #[tokio::test]
    async fn test_retry_delay_sequence() {
        // 测试重试延迟序列正确使用
        let config = crate::config::DownloadConfig::builder()
            .retry(|r| r
                .max_retry_count(5)
                .retry_delays(vec![
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(2),
                ]))
            .build()
            .unwrap();

        let delays = config.retry().retry_delays();
        
        // 第 0 次重试使用第一个延迟
        assert_eq!(delays[0.min(delays.len() - 1)], std::time::Duration::from_secs(1));
        
        // 第 1 次重试使用第二个延迟
        assert_eq!(delays[1.min(delays.len() - 1)], std::time::Duration::from_secs(2));
        
        // 第 2 次及以后重试使用最后一个延迟
        assert_eq!(delays[2.min(delays.len() - 1)], std::time::Duration::from_secs(2));
        assert_eq!(delays[10.min(delays.len() - 1)], std::time::Duration::from_secs(2));
    }
}

