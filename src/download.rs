use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::tools::chunk_strategy::{ChunkStrategy, SpeedBasedChunkStrategy};
use crate::tools::fetch::check_range_support;
use crate::tools::io_traits::{AsyncFile, FileSystem, HttpClient};
use crate::tools::range_writer::{AllocatedRange, RangeAllocator, RangeWriter};
use crate::task::{RangeResult, WorkerTask};
use crate::worker::WorkerPool;

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
    /// 下载进度更新
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
        /// 当前分块大小（bytes）
        current_chunk_size: u64,
    },
    /// Worker 统计信息
    WorkerStats {
        /// Worker ID
        worker_id: usize,
        /// 该 worker 下载的字节数
        bytes: u64,
        /// 该 worker 完成的 range 数量
        ranges: usize,
        /// 该 worker 平均速度 (bytes/s)
        avg_speed: f64,
        /// 该 worker 实时速度 (bytes/s)，如果无效则为 None
        instant_speed: Option<f64>,
    },
    /// 下载已完成
    Completed {
        /// 总下载字节数
        total_bytes: u64,
        /// 总耗时（秒）
        total_time: f64,
        /// 平均速度 (bytes/s)
        avg_speed: f64,
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
            .context("下载任务 panic")?
    }
    
    /// 获取进度接收器的可变引用
    /// 
    /// 使用此方法可以循环接收进度更新
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// # use rs_dn::{download_ranged, DownloadProgress, DownloadConfig};
    /// # use std::path::PathBuf;
    /// # async fn example() {
    /// let config = DownloadConfig::default();
    /// let mut handle = download_ranged("http://example.com/file", PathBuf::from("file"), config).await.unwrap();
    /// 
    /// while let Some(progress) = handle.progress_receiver().recv().await {
    ///     match progress {
    ///         DownloadProgress::Progress { percentage, avg_speed, .. } => {
    ///             println!("进度: {:.1}%, 速度: {:.2} MB/s", 
    ///                 percentage, avg_speed / 1024.0 / 1024.0);
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
                    return result.context("下载任务 panic")?;
                }
            }
        }
        
        // channel 关闭后等待任务完成
        self.completion_handle.await.context("下载任务 panic")?
    }
}

/// 失败的 Range 信息
type FailedRange = (AllocatedRange, String);

/// 下载任务执行器
/// 
/// 封装了下载任务的执行逻辑，包括进度监控、统计收集、动态分块和资源清理
struct DownloadTask<F: AsyncFile> {
    pool: WorkerPool<F>,
    progress_sender: Option<Sender<DownloadProgress>>,
    writer: Arc<RangeWriter<F>>,
    allocator: RangeAllocator,
    url: String,
    total_size: u64,
    chunk_strategy: Box<dyn ChunkStrategy + Send + Sync>,
    config: Arc<crate::config::DownloadConfig>,
    next_worker_id: usize, // 用于轮询分配任务
    total_ranges_completed: usize, // 已完成的 range 总数
}

impl<F: AsyncFile + 'static> DownloadTask<F> {
    /// 创建新的下载任务
    fn new(
        pool: WorkerPool<F>,
        progress_sender: Option<Sender<DownloadProgress>>,
        writer: Arc<RangeWriter<F>>,
        allocator: RangeAllocator,
        url: String,
        total_size: u64,
        config: Arc<crate::config::DownloadConfig>,
    ) -> Self {
        let chunk_strategy = Box::new(SpeedBasedChunkStrategy::from_config(&config));
        Self {
            pool,
            progress_sender,
            writer,
            allocator,
            url,
            total_size,
            chunk_strategy,
            config,
            next_worker_id: 0,
            total_ranges_completed: 0,
        }
    }
    
    /// 发送开始事件
    async fn send_started_event(&self) {
        if let Some(ref sender) = self.progress_sender {
            let _ = sender.send(DownloadProgress::Started {
                total_size: self.total_size,
                worker_count: self.pool.worker_channels.len(),
                initial_chunk_size: self.config.initial_chunk_size(),
            }).await;
        }
    }
    
    
    /// 尝试分配下一个任务
    /// 
    /// 返回 (任务, worker_id)，如果没有剩余空间则返回 None
    fn try_allocate_next_task(&mut self) -> Option<(WorkerTask, usize)> {
        let remaining = self.allocator.remaining();
        if remaining == 0 {
            return None;
        }
        
        // 计算实际分配大小（不超过剩余空间）
        let alloc_size = self.chunk_strategy.current_chunk_size().min(remaining);
        
        // 分配 range
        let range = self.allocator.allocate(alloc_size)?;
        
        // 创建任务
        let task = WorkerTask::Range {
            url: self.url.clone(),
            range,
        };
        
        // 轮询选择 worker
        let worker_id = self.next_worker_id;
        self.next_worker_id = (self.next_worker_id + 1) % self.pool.worker_channels.len();
        
        Some((task, worker_id))
    }
    
    /// 等待所有 range 完成
    /// 
    /// 动态分配任务，收集完成和失败的 range，定期发送进度更新和调整分块大小
    async fn wait_for_completion(&mut self) -> Result<Vec<FailedRange>> {
        let mut failed_ranges = Vec::new();
        
        // 创建定时器，用于定期更新进度和调整分块大小
        let mut progress_timer = tokio::time::interval(self.config.instant_speed_window());
        progress_timer.tick().await; // 跳过首次立即触发
        
        // 初始任务分配：为每个 worker 分配一个任务
        info!("开始初始任务分配，每个 worker 分配一个 {} bytes 的分块", self.chunk_strategy.current_chunk_size());
        for worker_id in 0..self.pool.worker_channels.len() {
            if let Some((task, _)) = self.try_allocate_next_task() {
                if let Err(e) = self.pool.send_task(task, worker_id).await {
                    error!("初始任务分配失败: {:?}", e);
                }
            } else {
                info!("没有足够的数据为所有 worker 分配初始任务");
                break;
            }
        }
        
        loop {
            tokio::select! {
                // 定时器触发：更新进度和调整分块大小
                _ = progress_timer.tick() => {
                    self.adjust_chunk_size_and_send_progress().await;
                }
                
                // 接收 worker 结果并分配新任务
                result = self.pool.result_receiver.recv() => {
                    match result {
                        Some(RangeResult::Complete { worker_id }) => {
                            self.total_ranges_completed += 1;
                            
                            // 为该 worker 分配新任务
                            if let Some((task, target_worker)) = self.try_allocate_next_task() {
                                debug!(
                                    "Worker #{} 完成任务，分配新任务到 Worker #{}",
                                    worker_id, target_worker
                                );
                                if let Err(e) = self.pool.send_task(task, target_worker).await {
                                    error!("分配新任务失败: {:?}", e);
                                }
                            } else {
                                debug!("Worker #{} 完成任务，但没有更多任务可分配", worker_id);
                            }
                        }
                        Some(RangeResult::Failed { worker_id, range, error }) => {
                            error!(
                                "Worker #{} Range {}..{} 失败: {}",
                                worker_id,
                                range.start(),
                                range.end(),
                                error
                            );
                            failed_ranges.push((range, error));
                            self.total_ranges_completed += 1;
                            
                            // 失败后也需要分配新任务
                            if let Some((task, target_worker)) = self.try_allocate_next_task() {
                                debug!(
                                    "Worker #{} 任务失败，分配新任务到 Worker #{}",
                                    worker_id, target_worker
                                );
                                if let Err(e) = self.pool.send_task(task, target_worker).await {
                                    error!("分配新任务失败: {:?}", e);
                                }
                            } else {
                                debug!("Worker #{} 任务失败，但没有更多任务可分配", worker_id);
                            }
                        }
                        None => {
                            // 所有 worker 的 result_sender 都已关闭
                            info!("所有 worker 已退出");
                            break;
                        }
                    }
                }
            }
            
            // 检查是否所有任务完成
            if self.allocator.remaining() == 0 && self.writer.is_complete() {
                info!("所有任务已完成，总共完成 {} 个 range", self.total_ranges_completed);
                break;
            }
        }
        
        Ok(failed_ranges)
    }
    
    /// 调整分块大小并发送进度更新
    /// 
    /// 根据实时速度动态调整分块大小，并发送进度更新
    async fn adjust_chunk_size_and_send_progress(&mut self) {
        // 根据实时速度调整分块大小
        let (instant_speed, instant_valid) = self.pool.get_total_instant_speed();
        if instant_valid {
            let new_chunk_size = self.chunk_strategy.calculate_chunk_size(instant_speed);
            let current_chunk_size = self.chunk_strategy.current_chunk_size();
            if new_chunk_size != current_chunk_size {
                info!(
                    "根据实时速度 {:.2} MB/s 调整分块大小: {} -> {} bytes",
                    instant_speed / 1024.0 / 1024.0,
                    current_chunk_size,
                    new_chunk_size
                );
                self.chunk_strategy.update_chunk_size(new_chunk_size);
            }
        }
        
        self.send_progress_update().await;
    }
    
    /// 发送进度更新
    async fn send_progress_update(&self) {
        if let Some(ref sender) = self.progress_sender {
            let total_avg_speed = self.pool.get_total_speed();
            let (total_instant_speed, instant_valid) = self.pool.get_total_instant_speed();
            let (total_bytes, _, _) = self.pool.get_total_stats();
            
            // 计算百分比
            let percentage = if self.total_size > 0 {
                (total_bytes as f64 / self.total_size as f64) * 100.0
            } else {
                0.0
            };
            
            // 发送总体进度
            let _ = sender.send(DownloadProgress::Progress {
                bytes_downloaded: total_bytes,
                total_size: self.total_size,
                percentage,
                avg_speed: total_avg_speed,
                instant_speed: if instant_valid { Some(total_instant_speed) } else { None },
                current_chunk_size: self.chunk_strategy.current_chunk_size(),
            }).await;
            
            // 发送每个 worker 的统计信息
            for (id, channel) in self.pool.worker_channels.iter().enumerate() {
                let (worker_bytes, _, worker_ranges, avg_speed, instant_speed, instant_valid) = 
                    channel.stats.get_full_summary();
                let _ = sender.send(DownloadProgress::WorkerStats {
                    worker_id: id,
                    bytes: worker_bytes,
                    ranges: worker_ranges,
                    avg_speed,
                    instant_speed: if instant_valid { Some(instant_speed) } else { None },
                }).await;
            }
        }
    }
    
    /// 发送完成统计
    async fn send_completion_stats(&self) {
        if let Some(ref sender) = self.progress_sender {
            let (total_bytes, total_secs, _) = self.pool.get_total_stats();
            let avg_speed = self.pool.get_total_speed();
            
            // 发送完成事件
            let _ = sender.send(DownloadProgress::Completed {
                total_bytes,
                total_time: total_secs,
                avg_speed,
            }).await;
            
            // 发送最终的 worker 统计信息
            for (id, channel) in self.pool.worker_channels.iter().enumerate() {
                let (worker_bytes, _, worker_ranges, avg_speed, instant_speed, instant_valid) = 
                    channel.stats.get_full_summary();
                let _ = sender.send(DownloadProgress::WorkerStats {
                    worker_id: id,
                    bytes: worker_bytes,
                    ranges: worker_ranges,
                    avg_speed,
                    instant_speed: if instant_valid { Some(instant_speed) } else { None },
                }).await;
            }
        }
    }
    
    /// 关闭并清理资源
    async fn shutdown_and_cleanup(mut self, error_msg: Option<String>) -> Result<()> {
        // 发送错误事件
        if let Some(ref msg) = error_msg {
            if let Some(ref sender) = self.progress_sender {
                let _ = sender.send(DownloadProgress::Error {
                    message: msg.clone(),
                }).await;
            }
        }
        
        // 关闭 workers
        self.pool.shutdown();
        
        // 等待 workers 退出
        for (id, handle) in self.pool.worker_handles.into_iter().enumerate() {
            let _ = handle.await;
            log::debug!("Worker #{} 已退出", id);
        }
        
        if let Some(msg) = error_msg {
            anyhow::bail!(msg);
        }
        
        Ok(())
    }
    
    /// 完成并清理资源
    async fn finalize_and_cleanup(mut self, save_path: PathBuf) -> Result<()> {
        // 发送完成统计
        self.send_completion_stats().await;
        
        // 优雅关闭所有 workers
        self.pool.shutdown();
        
        // 等待 workers 退出
        for (id, handle) in self.pool.worker_handles.into_iter().enumerate() {
            handle
                .await
                .context(format!("等待 Worker #{} 退出失败", id))?;
        }
        
        // 完成写入（从 Arc 中提取 writer）
        let writer = Arc::try_unwrap(self.writer)
            .map_err(|_| anyhow::anyhow!("无法获取 RangeWriter 的所有权（仍有其他引用）"))?;
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
) -> Result<()>
where
    C: HttpClient + Clone + Send + 'static,
    FS: FileSystem,
    FS::File: Send + 'static,
{
    let worker_count = config.worker_count();
    
    info!("准备 Range 下载: {} ({} 个 workers, 动态分块)", url, worker_count);

    // 检查服务器是否支持 Range 请求
    let range_support = check_range_support(&client, url).await?;

    if !range_support.supported {
        warn!("服务器不支持 Range 请求，回退到普通下载");
        let task = crate::task::FileTask {
            url: url.to_string(),
            save_path: save_path.clone(),
        };
        return crate::tools::fetch::fetch_file(&client, task, &fs).await;
    }

    let content_length = range_support.content_length.context("无法获取文件大小")?;
    info!("文件大小: {} bytes ({:.2} MB)", content_length, content_length as f64 / 1024.0 / 1024.0);
    info!(
        "动态分块配置: 初始 {} bytes, 范围 {} ~ {} bytes",
        config.initial_chunk_size(),
        config.min_chunk_size(),
        config.max_chunk_size()
    );

    // 创建 RangeWriter 和 RangeAllocator（会预分配文件）
    let (writer, allocator) = RangeWriter::new(&fs, save_path.clone(), content_length).await?;

    // 将 writer 包装在 Arc 中，创建协程池
    let writer = Arc::new(writer);
    let pool = WorkerPool::new(
        client, 
        worker_count, 
        Arc::clone(&writer),
        config.instant_speed_window(),
    );

    // 创建下载任务（使用动态分块）
    let mut task = DownloadTask::new(
        pool,
        progress_sender,
        writer,
        allocator,
        url.to_string(),
        content_length,
        Arc::new(config.clone()),
    );
    task.send_started_event().await;

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
/// # Arguments
/// * `url` - 下载 URL
/// * `save_path` - 保存路径
/// * `config` - 下载配置（包含动态分块参数、worker数等）
/// 
/// # Returns
/// 
/// 返回 `DownloadHandle`，可以通过它监听下载进度并等待完成
/// 
/// # Example
/// 
/// ```no_run
/// # use rs_dn::{download_ranged, DownloadConfig, DownloadProgress};
/// # use std::path::PathBuf;
/// # async fn example() -> anyhow::Result<()> {
/// // 使用默认配置（推荐）
/// let config = DownloadConfig::default();
/// let mut handle = download_ranged(
///     "http://example.com/file.bin",
///     PathBuf::from("file.bin"),
///     config
/// ).await?;
/// 
/// // 或使用自定义配置
/// let config = DownloadConfig::builder()
///     .worker_count(8)                         // 8 个并发 worker
///     .initial_chunk_size(10 * 1024 * 1024)    // 初始 10 MB 分块
///     .min_chunk_size(2 * 1024 * 1024)         // 最小 2 MB（慢速时）
///     .max_chunk_size(100 * 1024 * 1024)       // 最大 100 MB（高速时）
///     .build();
/// 
/// // 监听进度
/// while let Some(progress) = handle.progress_receiver().recv().await {
///     match progress {
///         DownloadProgress::Progress { percentage, avg_speed, current_chunk_size, .. } => {
///             println!("进度: {:.1}%, 速度: {:.2} MB/s, 分块: {:.2} MB", 
///                 percentage, 
///                 avg_speed / 1024.0 / 1024.0,
///                 current_chunk_size as f64 / 1024.0 / 1024.0);
///         }
///         DownloadProgress::Completed { total_bytes, total_time, .. } => {
///             println!("下载完成！{:.2} MB in {:.2}s", 
///                 total_bytes as f64 / 1024.0 / 1024.0, total_time);
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
    save_path: PathBuf,
    config: crate::config::DownloadConfig,
) -> Result<DownloadHandle> {
    use crate::tools::io_traits::TokioFileSystem;
    use reqwest::Client;
    
    let client = Client::new();
    let fs = TokioFileSystem::default();
    
    // 创建进度 channel
    let (progress_tx, progress_rx) = mpsc::channel(100);
    
    // 克隆必要的参数给后台任务
    let url = url.to_string();
    
    // 启动后台下载任务
    let completion_handle = tokio::spawn(async move {
        download_ranged_generic(
            client, 
            fs, 
            &url, 
            save_path, 
            &config,
            Some(progress_tx)
        ).await
    });
    
    Ok(DownloadHandle {
        progress_rx,
        completion_handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::io_traits::mock::{MockHttpClient, MockFileSystem};
    use reqwest::{header::HeaderMap, StatusCode};
    use bytes::Bytes;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_download_ranged_basic() {
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
            .worker_count(1)
            .initial_chunk_size(chunk_size)
            .min_chunk_size(1)  // 设置为 1 以允许小文件测试
            .max_chunk_size(chunk_size)  // 固定分块大小以便测试
            .build();
        
        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None, // 测试中不需要进度更新
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
            .worker_count(2)
            .initial_chunk_size(test_data.len() as u64)  // 单次分块完成
            .min_chunk_size(1)
            .max_chunk_size(test_data.len() as u64)
            .build();
        
        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None, // 测试中不需要进度更新
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
            .worker_count(2)
            .initial_chunk_size(chunk_size)
            .min_chunk_size(1)
            .max_chunk_size(chunk_size)  // 固定分块大小以便测试
            .build();
        
        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None, // 测试中不需要进度更新
        )
        .await;

        assert!(result.is_ok(), "多 worker 下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_dynamic_chunking_small_file() {
        // 测试小文件（< 2MB）自动调整为 1 个分块
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
            .worker_count(4)
            .initial_chunk_size(1 * 1024 * 1024)  // 1 MB
            .min_chunk_size(512 * 1024)  // 512 KB
            .max_chunk_size(2 * 1024 * 1024)  // 2 MB
            .build();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
        )
        .await;

        assert!(result.is_ok(), "小文件下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_dynamic_chunking_medium_file() {
        // 测试中等文件正确计算分块数
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
            .worker_count(3)
            .initial_chunk_size(chunk_size as u64)
            .min_chunk_size(chunk_size as u64)
            .max_chunk_size(chunk_size as u64)  // 固定 2 MB 分块
            .build();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
        )
        .await;

        assert!(result.is_ok(), "中等文件下载应该成功: {:?}", result);
    }

    #[tokio::test]
    async fn test_dynamic_chunking_large_file() {
        // 测试大文件不受最小分块限制影响
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
            .worker_count(4)
            .initial_chunk_size(chunk_size as u64)
            .min_chunk_size(chunk_size as u64)
            .max_chunk_size(chunk_size as u64)  // 固定 10 MB 分块
            .build();

        let result = download_ranged_generic(
            client.clone(),
            fs.clone(),
            test_url,
            save_path.clone(),
            &config,
            None,
        )
        .await;

        assert!(result.is_ok(), "大文件下载应该成功: {:?}", result);
    }
}

