use anyhow::{Context, Result};
use log::{error, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::tools::fetch::check_range_support;
use crate::tools::io_traits::{AsyncFile, FileSystem, HttpClient};
use crate::tools::range_writer::{AllocatedRange, RangeWriter};
use crate::task::{RangeResult, WorkerTask};
use crate::worker::WorkerPool;

/// 下载进度更新信息
#[derive(Debug, Clone)]
pub enum DownloadProgress {
    /// 下载已开始
    Started { 
        /// 文件总大小（bytes）
        total_size: u64, 
        /// Range 分段数量
        range_count: usize,
        /// Worker 数量
        worker_count: usize,
    },
    /// Range 完成更新
    RangeComplete {
        /// 已完成的 range 数量
        completed: usize,
        /// 总 range 数量
        total: usize,
        /// 已下载字节数
        bytes_downloaded: u64,
        /// 平均速度 (bytes/s)
        avg_speed: f64,
        /// 实时速度 (bytes/s)，如果无效则为 None
        instant_speed: Option<f64>,
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
    /// # use rs_dn::{download_ranged, DownloadProgress};
    /// # use std::path::PathBuf;
    /// # async fn example() {
    /// let mut handle = download_ranged("http://example.com/file", PathBuf::from("file"), 10, 4).await.unwrap();
    /// 
    /// while let Some(progress) = handle.progress_receiver().recv().await {
    ///     match progress {
    ///         DownloadProgress::RangeComplete { completed, total, .. } => {
    ///             println!("进度: {}/{}", completed, total);
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
/// 封装了下载任务的执行逻辑，包括进度监控、统计收集和资源清理
struct DownloadTask<F: AsyncFile> {
    pool: WorkerPool<F>,
    progress_sender: Option<Sender<DownloadProgress>>,
    range_count: usize,
    writer: Arc<RangeWriter<F>>,
}

impl<F: AsyncFile + 'static> DownloadTask<F> {
    /// 创建新的下载任务
    fn new(
        pool: WorkerPool<F>,
        progress_sender: Option<Sender<DownloadProgress>>,
        range_count: usize,
        writer: Arc<RangeWriter<F>>,
    ) -> Self {
        Self {
            pool,
            progress_sender,
            range_count,
            writer,
        }
    }
    
    /// 发送开始事件
    async fn send_started_event(&self, total_size: u64, worker_count: usize) {
        if let Some(ref sender) = self.progress_sender {
            let _ = sender.send(DownloadProgress::Started {
                total_size,
                range_count: self.range_count,
                worker_count,
            }).await;
        }
    }
    
    /// 等待所有 range 完成
    /// 
    /// 轮询所有 worker 的结果，收集完成和失败的 range
    /// 定期发送进度更新
    async fn wait_for_completion(&mut self) -> Result<Vec<FailedRange>> {
        let mut completed_count = 0;
        let mut failed_ranges = Vec::new();
        let mut should_send_progress = false;
        
        while completed_count < self.range_count {
            let mut found = false;
            
            for (worker_id, channel) in self.pool.worker_channels.iter_mut().enumerate() {
                match channel.result_receiver.try_recv() {
                    Ok(RangeResult::Complete(_)) => {
                        completed_count += 1;
                        found = true;
                        
                        // 每完成 10% 的任务标记需要发送进度
                        if completed_count % (self.range_count / 10).max(1) == 0 {
                            should_send_progress = true;
                        }
                    }
                    Ok(RangeResult::Failed { range, error }) => {
                        error!(
                            "Worker #{} Range {}..{} 失败: {}",
                            worker_id,
                            range.start(),
                            range.end(),
                            error
                        );
                        failed_ranges.push((range, error));
                        completed_count += 1;
                        found = true;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // 该 receiver 暂时没有数据
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // 该 worker 已退出
                    }
                }
            }
            
            // 发送进度更新
            if should_send_progress {
                self.send_progress_update(completed_count).await;
                should_send_progress = false;
            }
            
            if !found {
                // 没有任何信号到达，稍等一下
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
        
        Ok(failed_ranges)
    }
    
    /// 发送进度更新
    async fn send_progress_update(&self, completed: usize) {
        if let Some(ref sender) = self.progress_sender {
            let total_avg_speed = self.pool.get_total_speed();
            let (total_instant_speed, instant_valid) = self.pool.get_total_instant_speed();
            let (total_bytes, _, _) = self.pool.get_total_stats();
            
            // 发送总体进度
            let _ = sender.send(DownloadProgress::RangeComplete {
                completed,
                total: self.range_count,
                bytes_downloaded: total_bytes,
                avg_speed: total_avg_speed,
                instant_speed: if instant_valid { Some(total_instant_speed) } else { None },
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
/// 
/// # Arguments
/// 
/// * `config` - 下载配置，包含分块策略和并发控制参数
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
    let range_count = config.range_count();
    let worker_count = config.worker_count();
    let min_chunk_size = config.min_chunk_size();
    
    info!("准备 Range 下载: {} ({} 个分段, {} 个 workers)", url, range_count, worker_count);

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
    info!("文件大小: {} bytes", content_length);

    // 动态调整 range_count，确保每个分块不小于 min_chunk_size
    let actual_range_count = if (content_length / range_count as u64) < min_chunk_size {
        // 计算满足最小分块大小的最大分块数
        let adjusted = (content_length / min_chunk_size).max(1) as usize;
        info!(
            "根据最小分块大小 ({} bytes) 调整分块数: {} -> {}",
            min_chunk_size, range_count, adjusted
        );
        adjusted
    } else {
        range_count
    };

    // 创建 RangeWriter 和 RangeAllocator（会预分配文件）
    let (writer, mut allocator) = RangeWriter::new(&fs, save_path.clone(), content_length).await?;

    // 预先分配所有 Range（保证不重叠且有效）
    let range_size = content_length / actual_range_count as u64;
    let mut allocated_ranges = Vec::with_capacity(actual_range_count);
    for i in 0..actual_range_count {
        let size = if i == actual_range_count - 1 {
            allocator.remaining()
        } else {
            range_size
        };
        
        let range = allocator
            .allocate(size)
            .context(format!("分配 Range #{} 失败", i))?;
        
        allocated_ranges.push(range);
    }

    // 将 writer 包装在 Arc 中，创建协程池
    let writer = Arc::new(writer);
    let pool = WorkerPool::new(client, worker_count, Arc::clone(&writer));

    // 创建下载任务
    let mut task = DownloadTask::new(pool, progress_sender, actual_range_count, writer);
    task.send_started_event(content_length, worker_count).await;

    // 分发 Range 任务给 workers
    for (i, &range) in allocated_ranges.iter().enumerate() {
        let worker_task = WorkerTask::Range {
            url: url.to_string(),
            range,
        };
        let worker_id = i % worker_count;
        task.pool.send_task(worker_task, worker_id).await?;
    }

    info!("已提交 {} 个 Range 任务", actual_range_count);

    // 等待所有任务完成
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
/// 
/// # Arguments
/// * `url` - 下载 URL
/// * `save_path` - 保存路径
/// * `config` - 下载配置（包含分块数、worker数等参数）
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
/// // 使用默认配置
/// let config = DownloadConfig::default();
/// let mut handle = download_ranged(
///     "http://example.com/file.bin",
///     PathBuf::from("file.bin"),
///     config
/// ).await?;
/// 
/// // 或使用自定义配置
/// let config = DownloadConfig::builder()
///     .range_count(16)
///     .worker_count(8)
///     .min_chunk_size(5 * 1024 * 1024)  // 5 MB
///     .build();
/// 
/// // 监听进度
/// while let Some(progress) = handle.progress_receiver().recv().await {
///     match progress {
///         DownloadProgress::RangeComplete { completed, total, avg_speed, .. } => {
///             println!("进度: {}/{}, 速度: {:.2} MB/s", 
///                 completed, total, avg_speed / 1024.0 / 1024.0);
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
        let config = crate::config::DownloadConfig::builder()
            .range_count(range_count)
            .worker_count(1)
            .min_chunk_size(1)  // 设置为 1 以允许小文件测试
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
            .range_count(3)
            .worker_count(2)
            .min_chunk_size(1)
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

        // 使用 2 个 workers 下载 4 个 ranges
        let config = crate::config::DownloadConfig::builder()
            .range_count(range_count)
            .worker_count(2)
            .min_chunk_size(1)
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

        // 期望 10 个分块，但因为文件只有 1MB，最小分块 2MB，应该调整为 1 个
        let config = crate::config::DownloadConfig::builder()
            .range_count(10)
            .worker_count(4)
            .min_chunk_size(2 * 1024 * 1024)  // 2 MB
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

        // 期望 10 个分块，最小分块 2MB
        // 10MB / 10 = 1MB < 2MB，应该调整为 10MB / 2MB = 5 个分块
        let expected_chunks = 5;
        let chunk_size = file_size / expected_chunks;

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
            .range_count(10)
            .worker_count(3)
            .min_chunk_size(2 * 1024 * 1024)  // 2 MB
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

        // 期望 10 个分块，最小分块 2MB
        // 100MB / 10 = 10MB > 2MB，不需要调整
        let expected_chunks = 10;
        let chunk_size = file_size / expected_chunks;

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
            .range_count(10)
            .worker_count(4)
            .min_chunk_size(2 * 1024 * 1024)  // 2 MB
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

