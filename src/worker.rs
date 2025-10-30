use anyhow::{Context, Result};
use log::{debug, error, info};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::tools::fetch::fetch_range;
use crate::tools::io_traits::{AsyncFile, HttpClient};
use crate::tools::range_writer::RangeWriter;
use crate::tools::stats::{DownloadStats, DownloadStatsParent};
use crate::task::{RangeResult, WorkerTask};

/// Worker 协程主循环
/// 
/// 每个 worker 持有一个独立的 channel receiver，循环等待任务
/// 下载数据后直接写入共享的 RangeWriter
/// 下载过程中实时更新共享的 DownloadStats
/// 完成后通过 result_sender 发送轻量级完成信号（不包含数据）
/// 支持通过 oneshot channel 接收高优先级关闭信号
/// 当 channel 关闭或收到关闭信号时退出
pub(crate) async fn run_worker<C, F>(
    id: usize,
    client: C,
    mut task_receiver: Receiver<WorkerTask>,
    result_sender: Sender<RangeResult>,
    mut shutdown_receiver: oneshot::Receiver<()>,
    writer: Arc<RangeWriter<F>>,
    stats: Arc<DownloadStats>,
)
where
    C: HttpClient,
    F: AsyncFile,
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
                    Some(WorkerTask::Range { url, range }) => {
                        debug!(
                            "Worker #{} 接收到 Range 任务: {} (range {}..{})",
                            id, url, range.start(), range.end()
                        );
                        
                        // 下载数据（在下载过程中会实时更新 stats）
                        let fetch_result = fetch_range(&client, &url, range, Arc::clone(&stats)).await;
                        
                        match fetch_result {
                            Ok(data) => {
                                // 直接写入文件
                                match writer.write_range(range, data).await {
                                    Ok(()) => {
                                        // 记录 range 完成
                                        stats.record_range_complete();
                                        
                                        // 发送完成信号
                                        if let Err(e) = result_sender.send(RangeResult::Complete { worker_id: id }).await {
                                            error!("Worker #{} 发送完成信号失败: {:?}", id, e);
                                        }
                                    }
                                    Err(e) => {
                                        // 写入失败，发送失败信号
                                        let error_msg = format!("写入失败: {:?}", e);
                                        error!("Worker #{} {}", id, error_msg);
                                        if let Err(e) = result_sender.send(RangeResult::Failed { 
                                            worker_id: id,
                                            range, 
                                            error: error_msg 
                                        }).await {
                                            error!("Worker #{} 发送失败信号失败: {:?}", id, e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                // 下载失败，发送失败信号
                                let error_msg = format!("下载失败: {:?}", e);
                                error!("Worker #{} Range {}..{} {}", id, range.start(), range.end(), error_msg);
                                if let Err(e) = result_sender.send(RangeResult::Failed { 
                                    worker_id: id,
                                    range, 
                                    error: error_msg 
                                }).await {
                                    error!("Worker #{} 发送失败信号失败: {:?}", id, e);
                                }
                            }
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

/// 单个 Worker 的通信通道和统计信息
/// 
/// 封装了与单个 worker 交互所需的所有信息
pub(crate) struct WorkerChannel {
    /// 向 worker 发送任务的通道
    pub(crate) task_sender: mpsc::Sender<WorkerTask>,
    /// 向 worker 发送关闭信号的 oneshot channel
    pub(crate) shutdown_sender: Option<oneshot::Sender<()>>,
    /// 该 worker 的独立下载统计
    pub(crate) stats: Arc<DownloadStats>,
}

/// 下载协程池
/// 
/// 为单个下载任务创建的临时协程池
/// 任务完成后自动销毁
pub(crate) struct WorkerPool<F: AsyncFile> {
    /// 每个 worker 的通道和统计信息
    pub(crate) worker_channels: Vec<WorkerChannel>,
    /// Worker 协程的句柄
    pub(crate) worker_handles: Vec<JoinHandle<()>>,
    /// 统一的结果接收器（所有 worker 共享）
    pub(crate) result_receiver: mpsc::Receiver<RangeResult>,
    /// 全局统计管理器（聚合所有 worker 的数据）
    global_stats: DownloadStatsParent,
    _phantom: PhantomData<F>,
}

impl<F: AsyncFile + 'static> WorkerPool<F> {
    /// 创建新的协程池
    /// 
    /// # Arguments
    /// * `client` - HTTP客户端（将被克隆给每个worker）
    /// * `worker_count` - worker 协程数量
    /// * `writer` - 共享的 RangeWriter，所有 worker 将直接写入此文件
    /// * `instant_speed_window` - 实时速度窗口，用于计算瞬时速度
    /// 
    /// 每个 worker 都有独立的 DownloadStats，用于监控单个 worker 的性能
    /// 所有 worker 的统计会自动聚合到 global_stats，实现 O(1) 获取
    pub(crate) fn new<C>(
        client: C, 
        worker_count: usize, 
        writer: Arc<RangeWriter<F>>,
        instant_speed_window: std::time::Duration,
    ) -> Self
    where
        C: HttpClient + Clone + Send + 'static,
    {
        // 创建全局统计管理器（使用配置的时间窗口）
        let global_stats = DownloadStatsParent::with_window(instant_speed_window);
        
        // 创建统一的 result channel（所有 worker 共享同一个 sender）
        let (result_sender, result_receiver) = mpsc::channel::<RangeResult>(100);
        
        let mut worker_channels = Vec::new();
        let mut worker_handles = Vec::new();

        // 为每个 worker 创建独立的 task channel、shutdown channel 和统计信息
        for id in 0..worker_count {
            let (task_sender, task_receiver) = mpsc::channel::<WorkerTask>(100);
            let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
            
            // 通过 parent 创建 child stats 并包装为 Arc
            let worker_stats = Arc::new(global_stats.create_child());
            
            // 克隆 client、writer、result_sender 和 stats Arc 给每个 worker
            let client_clone = client.clone();
            let writer_clone = Arc::clone(&writer);
            let stats_clone = Arc::clone(&worker_stats);
            let result_sender_clone = result_sender.clone();
            
            // 启动 worker 协程
            let handle = tokio::spawn(async move {
                run_worker(id, client_clone, task_receiver, result_sender_clone, shutdown_receiver, writer_clone, stats_clone).await;
            });

            worker_channels.push(WorkerChannel {
                task_sender,
                shutdown_sender: Some(shutdown_sender),
                stats: worker_stats,
            });
            worker_handles.push(handle);
        }
        
        // 释放原始的 result_sender，只保留 worker 持有的克隆
        drop(result_sender);

        info!("创建协程池，{} 个 workers", worker_count);

        Self {
            worker_channels,
            worker_handles,
            result_receiver,
            global_stats,
            _phantom: PhantomData,
        }
    }

    /// 提交任务给指定的 worker
    pub(crate) async fn send_task(&self, task: WorkerTask, worker_id: usize) -> Result<()> {
        self.worker_channels[worker_id].task_sender
            .send(task)
            .await
            .context("发送任务失败")?;
        Ok(())
    }

    /// 获取指定 worker 的统计信息
    #[allow(dead_code)]
    pub(crate) fn worker_stats(&self, worker_id: usize) -> &Arc<DownloadStats> {
        &self.worker_channels[worker_id].stats
    }

    /// 获取所有 worker 的聚合统计（O(1)，无需遍历）
    pub(crate) fn get_total_stats(&self) -> (u64, f64, usize) {
        self.global_stats.get_summary()
    }

    /// 获取所有 worker 的总体下载速度（平均速度，O(1)）
    pub(crate) fn get_total_speed(&self) -> f64 {
        self.global_stats.get_speed()
    }

    /// 获取所有 worker 的总体实时速度（O(1)，无需遍历）
    /// 
    /// # Returns
    /// 
    /// `(实时速度 bytes/s, 是否有效)`
    pub(crate) fn get_total_instant_speed(&self) -> (f64, bool) {
        self.global_stats.get_instant_speed()
    }

    /// 优雅关闭所有 workers
    /// 
    /// 发送关闭信号到所有 worker，让它们停止接收新任务并退出
    pub(crate) fn shutdown(&mut self) {
        info!("发送关闭信号到所有 workers");
        for (id, channel) in self.worker_channels.iter_mut().enumerate() {
            if let Some(shutdown_sender) = channel.shutdown_sender.take() {
                // 忽略发送失败（worker 可能已经退出）
                let _ = shutdown_sender.send(());
                debug!("已发送关闭信号到 Worker #{}", id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::io_traits::mock::{MockHttpClient, MockFileSystem, MockFile};
    use crate::tools::range_writer::RangeWriter;
    use reqwest::{header::HeaderMap, StatusCode};
    use bytes::Bytes;
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_run_worker_single_task() {
        // 准备测试数据
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJ"; // 20 bytes
        
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        // 创建 RangeWriter
        let (writer, mut allocator) = RangeWriter::new(&fs, save_path.clone(), test_data.len() as u64)
            .await
            .unwrap();
        let writer = Arc::new(writer);

        // 分配一个 range
        let range = allocator.allocate(test_data.len() as u64).unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-19/20").parse().unwrap(),
        );
        client.set_range_response(
            test_url,
            0,
            19,
            StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from_static(test_data),
        );

        // 创建通道
        let (task_sender, task_receiver) = mpsc::channel(10);
        let (result_sender, mut result_receiver) = mpsc::channel(10);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let stats = Arc::new(DownloadStats::default());

        // 启动 worker
        let worker_handle = tokio::spawn({
            let client = client.clone();
            let writer = Arc::clone(&writer);
            let stats = Arc::clone(&stats);
            async move {
                run_worker(
                    0,
                    client,
                    task_receiver,
                    result_sender,
                    shutdown_receiver,
                    writer,
                    stats,
                )
                .await;
            }
        });

        // 发送任务
        let task = WorkerTask::Range {
            url: test_url.to_string(),
            range,
        };
        task_sender.send(task).await.unwrap();

        // 接收结果
        let result = result_receiver.recv().await;
        assert!(result.is_some(), "应该收到结果");
        
        match result.unwrap() {
            RangeResult::Complete { worker_id } => {
                assert_eq!(worker_id, 0, "应该是 Worker #0 完成的任务");
            }
            RangeResult::Failed { error, .. } => {
                panic!("任务不应该失败: {}", error);
            }
        }

        // 关闭通道以让 worker 退出
        drop(task_sender);
        
        // 等待 worker 退出
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker_handle).await;

        // 验证统计信息
        let (total_bytes, _, ranges) = stats.get_summary();
        assert_eq!(total_bytes, test_data.len() as u64);
        assert_eq!(ranges, 1);
    }

    #[tokio::test]
    async fn test_run_worker_shutdown_signal() {
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        let (writer, _) = RangeWriter::new(&fs, save_path, 100).await.unwrap();
        let writer = Arc::new(writer);

        let (_task_sender, task_receiver) = mpsc::channel(10);
        let (result_sender, _result_receiver) = mpsc::channel(10);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let stats = Arc::new(DownloadStats::default());

        // 启动 worker
        let worker_handle = tokio::spawn({
            let client = client.clone();
            let writer = Arc::clone(&writer);
            let stats = Arc::clone(&stats);
            async move {
                run_worker(
                    0,
                    client,
                    task_receiver,
                    result_sender,
                    shutdown_receiver,
                    writer,
                    stats,
                )
                .await;
            }
        });

        // 立即发送关闭信号
        shutdown_sender.send(()).unwrap();

        // 等待 worker 退出
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), worker_handle).await;
        assert!(result.is_ok(), "Worker 应该在收到关闭信号后退出");
    }

    #[tokio::test]
    async fn test_worker_pool_creation() {
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        let (writer, _) = RangeWriter::new(&fs, save_path, 1000).await.unwrap();
        let writer = Arc::new(writer);

        let worker_count = 4;
        let pool = WorkerPool::<MockFile>::new(
            client, 
            worker_count, 
            writer,
            std::time::Duration::from_secs(1),
        );

        assert_eq!(pool.worker_channels.len(), worker_count);
        assert_eq!(pool.worker_handles.len(), worker_count);
    }

    #[tokio::test]
    async fn test_worker_pool_send_task() {
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        let (writer, mut allocator) = RangeWriter::new(&fs, save_path, 100).await.unwrap();
        let writer = Arc::new(writer);

        let pool = WorkerPool::<MockFile>::new(
            client, 
            2, 
            writer,
            std::time::Duration::from_secs(1),
        );

        // 分配一个 range
        let range = allocator.allocate(10).unwrap();
        
        let task = WorkerTask::Range {
            url: "http://example.com/file.bin".to_string(),
            range,
        };

        // 发送任务到 worker 0
        let result = pool.send_task(task, 0).await;
        assert!(result.is_ok(), "发送任务应该成功");
    }

    #[tokio::test]
    async fn test_worker_pool_stats() {
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        let (writer, _) = RangeWriter::new(&fs, save_path, 1000).await.unwrap();
        let writer = Arc::new(writer);

        let worker_count = 3;
        let pool = WorkerPool::<MockFile>::new(
            client, 
            worker_count, 
            writer,
            std::time::Duration::from_secs(1),
        );

        // 初始统计应该都是 0
        let (total_bytes, total_secs, ranges) = pool.get_total_stats();
        assert_eq!(total_bytes, 0);
        assert!(total_secs >= 0.0);
        assert_eq!(ranges, 0);

        let speed = pool.get_total_speed();
        assert_eq!(speed, 0.0);
    }

    #[tokio::test]
    async fn test_worker_pool_shutdown() {
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        let (writer, _) = RangeWriter::new(&fs, save_path, 1000).await.unwrap();
        let writer = Arc::new(writer);

        let mut pool = WorkerPool::<MockFile>::new(
            client.clone(), 
            2, 
            writer,
            std::time::Duration::from_secs(1),
        );

        // 关闭 workers
        pool.shutdown();

        // 验证所有 shutdown_sender 都已被消费
        for channel in &pool.worker_channels {
            assert!(channel.shutdown_sender.is_none());
        }

        // 等待所有 workers 退出
        for handle in pool.worker_handles.into_iter() {
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
            assert!(result.is_ok(), "Worker 应该退出");
        }
    }

    #[tokio::test]
    async fn test_run_worker_download_failure() {
        // 测试下载失败的情况
        let test_url = "http://example.com/file.bin";
        
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();
        let save_path = PathBuf::from("/tmp/test.bin");

        let (writer, mut allocator) = RangeWriter::new(&fs, save_path, 100).await.unwrap();
        let writer = Arc::new(writer);

        let range = allocator.allocate(10).unwrap();

        // 设置失败的 Range 响应
        client.set_range_response(
            test_url,
            0,
            9,
            StatusCode::INTERNAL_SERVER_ERROR,
            HeaderMap::new(),
            Bytes::new(),
        );

        let (task_sender, task_receiver) = mpsc::channel(10);
        let (result_sender, mut result_receiver) = mpsc::channel(10);
        let (_shutdown_sender, shutdown_receiver) = oneshot::channel();
        let stats = Arc::new(DownloadStats::default());

        // 启动 worker
        let worker_handle = tokio::spawn({
            let client = client.clone();
            let writer = Arc::clone(&writer);
            let stats = Arc::clone(&stats);
            async move {
                run_worker(
                    0,
                    client,
                    task_receiver,
                    result_sender,
                    shutdown_receiver,
                    writer,
                    stats,
                )
                .await;
            }
        });

        // 发送任务
        let task = WorkerTask::Range {
            url: test_url.to_string(),
            range,
        };
        task_sender.send(task).await.unwrap();

        // 接收结果
        let result = result_receiver.recv().await;
        assert!(result.is_some(), "应该收到结果");
        
        match result.unwrap() {
            RangeResult::Failed { worker_id, error, .. } => {
                // 预期失败
                assert_eq!(worker_id, 0, "应该是 Worker #0 失败的任务");
                assert!(error.contains("下载失败"), "错误消息应该包含'下载失败'");
            }
            RangeResult::Complete { .. } => {
                panic!("任务应该失败");
            }
        }

        // 关闭通道
        drop(task_sender);
        
        // 等待 worker 退出
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker_handle).await;
    }
}
