//! 进度报告器模块（Actor 模式）
//! 
//! 负责管理进度报告和统计信息收集
//! 采用 Actor 模式，完全独立于主下载循环

use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::sync::mpsc;
use log::debug;
use arc_swap::ArcSwap;

/// Worker 统计信息
#[derive(Debug, Clone)]
pub struct WorkerStatSnapshot {
    /// Worker ID
    pub worker_id: u64,
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
        total_size: NonZeroU64,
        /// Worker 数量
        worker_count: u64,
        /// 初始分块大小（bytes）
        initial_chunk_size: u64,
    },
    /// 下载进度更新（包含总体统计和所有 worker 的统计）
    Progress {
        /// 已下载字节数
        bytes_downloaded: u64,
        /// 文件总大小（bytes）
        total_size: NonZeroU64,
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


/// Actor 消息类型
#[derive(Debug)]
enum ActorMessage {
    /// 发送开始事件
    SendStarted {
        worker_count: u64,
        initial_chunk_size: u64,
    },
    /// 发送完成统计（actor 从共享数据源获取所有统计）
    SendCompletion,
    /// 发送错误事件
    SendError {
        message: String,
    },
    /// 关闭 actor
    Shutdown,
}

/// 进度报告器 Actor
/// 
/// 独立运行的 actor，负责管理进度报告和统计信息收集
struct ProgressReporterActor<C: crate::utils::io_traits::HttpClient> {
    /// 进度发送器
    progress_sender: Option<mpsc::Sender<DownloadProgress>>,
    /// 文件总大小
    total_size: NonZeroU64,
    /// 消息接收器（async channel）
    message_rx: mpsc::Receiver<ActorMessage>,
    /// 共享的 worker handles（用于直接获取统计信息）
    worker_handles: Arc<ArcSwap<im::HashMap<u64, crate::pool::download::DownloadWorkerHandle<C>>>>,
    /// 全局统计管理器（用于获取总体统计数据）
    global_stats: Arc<crate::utils::stats::TaskStats>,
    /// 进度更新定时器（内部管理）
    progress_timer: tokio::time::Interval,
}

impl<C: crate::utils::io_traits::HttpClient> ProgressReporterActor<C> {
    /// 创建新的 actor
    async fn new(
        progress_sender: Option<mpsc::Sender<DownloadProgress>>,
        total_size: NonZeroU64,
        message_rx: mpsc::Receiver<ActorMessage>,
        worker_handles: Arc<ArcSwap<im::HashMap<u64, crate::pool::download::DownloadWorkerHandle<C>>>>,
        global_stats: Arc<crate::utils::stats::TaskStats>,
        update_interval: std::time::Duration,
    ) -> Self {
        let mut progress_timer = tokio::time::interval(update_interval);
        progress_timer.tick().await; // 跳过首次立即触发
        
        Self {
            progress_sender,
            total_size,
            message_rx,
            worker_handles,
            global_stats,
            progress_timer,
        }
    }
    
    /// 运行 actor 事件循环（使用 tokio::select!）
    async fn run(mut self) {
        debug!("ProgressReporterActor started");
        
        loop {
            tokio::select! {
                // 内部定时器：自主触发进度更新
                _ = self.progress_timer.tick() => {
                    self.send_progress_update().await;
                }
                // 外部消息
                msg = self.message_rx.recv() => {
                    match msg {
                        Some(ActorMessage::SendStarted { worker_count, initial_chunk_size }) => {
                            self.send_started_event(worker_count, initial_chunk_size).await;
                        }
                        Some(ActorMessage::SendCompletion) => {
                            self.send_completion_stats().await;
                        }
                        Some(ActorMessage::SendError { message }) => {
                            self.send_error(&message).await;
                        }
                        Some(ActorMessage::Shutdown) => {
                            debug!("ProgressReporterActor shutting down");
                            break;
                        }
                        None => {
                            // Channel 已关闭
                            debug!("ProgressReporterActor message channel closed");
                            break;
                        }
                    }
                }
            }
        }
        
        debug!("ProgressReporterActor stopped");
    }
    
    /// 发送开始事件
    async fn send_started_event(&self, worker_count: u64, initial_chunk_size: u64) {
        if let Some(ref sender) = self.progress_sender {
            let _ = sender.send(DownloadProgress::Started {
                total_size: self.total_size,
                worker_count,
                initial_chunk_size,
            }).await;
        }
    }
    
    /// 计算 worker 统计快照（直接从 worker_handles 中获取）
    fn compute_worker_snapshots(&self) -> Vec<WorkerStatSnapshot> {
        let handles = self.worker_handles.load();
        handles.iter().map(|(worker_id, handle)| {
            let stats = handle.stats();
            let (worker_bytes, _, worker_ranges, avg_speed, instant_speed, instant_valid, _, _) = 
                stats.get_full_summary();
            let current_chunk_size = stats.get_current_chunk_size();
            
            WorkerStatSnapshot {
                worker_id: *worker_id,
                bytes: worker_bytes,
                ranges: worker_ranges,
                avg_speed,
                instant_speed: if instant_valid { Some(instant_speed) } else { None },
                current_chunk_size,
            }
        }).collect()
    }
    
    /// 发送进度更新（从 global_stats 获取所有数据）
    async fn send_progress_update(&self) {
        if let Some(ref sender) = self.progress_sender {
            // 从 global_stats 获取总体统计
            let (total_bytes, _, _, total_avg_speed, total_instant_speed, instant_valid, _, _) = 
                self.global_stats.get_full_summary();
            
            // 计算百分比
            let percentage = if self.total_size.get() > 0 {
                (total_bytes as f64 / self.total_size.get() as f64) * 100.0
            } else {
                0.0
            };
            
            // 在 actor 内部从 worker_handles 计算 worker 快照
            let worker_snapshots = self.compute_worker_snapshots();
            
            let _ = sender.send(DownloadProgress::Progress {
                bytes_downloaded: total_bytes,
                total_size: self.total_size,
                percentage,
                avg_speed: total_avg_speed,
                instant_speed: if instant_valid { Some(total_instant_speed) } else { None },
                worker_stats: worker_snapshots,
            }).await;
        }
    }
    
    /// 发送完成统计（从 global_stats 获取所有数据）
    async fn send_completion_stats(&self) {
        if let Some(ref sender) = self.progress_sender {
            // 从 global_stats 获取总体统计
            let (total_bytes, total_secs, _, total_avg_speed, _, _, _, _) = 
                self.global_stats.get_full_summary();
            
            // 在 actor 内部从 worker_handles 计算 worker 快照
            let worker_snapshots = self.compute_worker_snapshots();
            
            let _ = sender.send(DownloadProgress::Completed {
                total_bytes,
                total_time: total_secs,
                avg_speed: total_avg_speed,
                worker_stats: worker_snapshots,
            }).await;
        }
    }
    
    /// 发送错误事件
    async fn send_error(&self, error_msg: &str) {
        if let Some(ref sender) = self.progress_sender {
            let _ = sender.send(DownloadProgress::Error {
                message: error_msg.to_string(),
            }).await;
        }
    }
}

/// 进度报告器 Handle
/// 
/// 提供与 ProgressReporterActor 通信的接口
#[derive(Clone)]
pub(super) struct ProgressReporter<C: crate::utils::io_traits::HttpClient> {
    /// 消息发送器（async channel）
    message_tx: mpsc::Sender<ActorMessage>,
    /// PhantomData 用于持有泛型参数
    _phantom: std::marker::PhantomData<C>,
}

impl<C: crate::utils::io_traits::HttpClient> ProgressReporter<C> {
    /// 创建新的进度报告器（启动 actor）
    pub(super) fn new(
        progress_sender: Option<mpsc::Sender<DownloadProgress>>,
        total_size: NonZeroU64,
        worker_handles: Arc<ArcSwap<im::HashMap<u64, crate::pool::download::DownloadWorkerHandle<C>>>>,
        global_stats: Arc<crate::utils::stats::TaskStats>,
        update_interval: std::time::Duration,
    ) -> Self {
        // 使用有界 channel，容量 100
        let (message_tx, message_rx) = mpsc::channel(100);
        
        // 启动 actor 任务
        tokio::spawn(async move {
            ProgressReporterActor::new(
                progress_sender,
                total_size,
                message_rx,
                worker_handles,
                global_stats,
                update_interval,
            ).await.run().await;
        });
        
        Self { 
            message_tx,
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// 发送开始事件
    pub(super) async fn send_started_event(&self, worker_count: u64, initial_chunk_size: u64) {
        let tx = self.message_tx.clone();
        let _ = tx.send(ActorMessage::SendStarted {
            worker_count,
            initial_chunk_size,
        }).await;
    }
    
    
    /// 发送完成统计
    /// Actor 会从共享数据源直接获取所有统计信息
    pub(super) async fn send_completion(&self) {
        let tx = self.message_tx.clone();
        let _ = tx.send(ActorMessage::SendCompletion).await;
    }
    
    /// 发送错误事件
    pub(super) async fn send_error(&self, error_msg: &str) {
        let tx = self.message_tx.clone();
        let message = error_msg.to_string();
        let _ = tx.send(ActorMessage::SendError { message }).await;
    }
    
    /// 关闭 actor
    pub(super) async fn shutdown(&self) {
        let tx = self.message_tx.clone();
        let _ = tx.send(ActorMessage::Shutdown).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // 辅助函数：创建空的 worker_handles
    fn create_empty_worker_handles<C: crate::utils::io_traits::HttpClient>() -> Arc<ArcSwap<im::HashMap<u64, crate::pool::download::DownloadWorkerHandle<C>>>> {
        Arc::new(ArcSwap::from_pointee(im::HashMap::new()))
    }
    
    // 辅助函数：创建模拟的 global_stats
    fn create_mock_global_stats() -> Arc<crate::utils::stats::TaskStats> {
        // 使用默认的配置来创建 TaskStats
        let config = crate::config::DownloadConfig::default();
        Arc::new(crate::utils::stats::TaskStats::from_config(config.speed()))
    }

    #[tokio::test]
    async fn test_progress_reporter_creation() {
        let (tx, _rx) = mpsc::channel(10);
        let worker_handles = create_empty_worker_handles::<reqwest::Client>();
        let global_stats = create_mock_global_stats();
        let _reporter = ProgressReporter::new(
            Some(tx),
            NonZeroU64::new(1000).unwrap(),
            worker_handles,
            global_stats,
            std::time::Duration::from_secs(1),
        );
        // Actor 已启动，只验证创建成功
    }

    #[tokio::test]
    async fn test_progress_reporter_without_sender() {
        let worker_handles = create_empty_worker_handles::<reqwest::Client>();
        let global_stats = create_mock_global_stats();
        let _reporter = ProgressReporter::new(
            None,
            NonZeroU64::new(1000).unwrap(),
            worker_handles,
            global_stats,
            std::time::Duration::from_secs(1),
        );
        // Actor 已启动，只验证创建成功
    }

    #[tokio::test]
    async fn test_send_started_event() {
        let (tx, mut rx) = mpsc::channel(10);
        let worker_handles = create_empty_worker_handles::<reqwest::Client>();
        let global_stats = create_mock_global_stats();
        let reporter = ProgressReporter::new(
            Some(tx),
            NonZeroU64::new(1000).unwrap(),
            worker_handles,
            global_stats,
            std::time::Duration::from_secs(1),
        );
        
        reporter.send_started_event(4, 256).await;
        
        // 接收事件
        if let Some(progress) = rx.recv().await {
            match progress {
                DownloadProgress::Started { total_size, worker_count, initial_chunk_size } => {
                    assert_eq!(total_size.get(), 1000);
                    assert_eq!(worker_count, 4);
                    assert_eq!(initial_chunk_size, 256);
                }
                _ => panic!("Expected Started event"),
            }
        } else {
            panic!("No event received");
        }
    }

    #[tokio::test]
    async fn test_send_error() {
        let (tx, mut rx) = mpsc::channel(10);
        let worker_handles = create_empty_worker_handles::<reqwest::Client>();
        let global_stats = create_mock_global_stats();
        let reporter = ProgressReporter::new(
            Some(tx),
            NonZeroU64::new(1000).unwrap(),
            worker_handles,
            global_stats,
            std::time::Duration::from_secs(1),
        );
        
        reporter.send_error("Test error").await;
        
        // 接收事件
        if let Some(progress) = rx.recv().await {
            match progress {
                DownloadProgress::Error { message } => {
                    assert_eq!(message, "Test error");
                }
                _ => panic!("Expected Error event"),
            }
        } else {
            panic!("No event received");
        }
    }

    #[tokio::test]
    async fn test_send_events_without_sender() {
        let worker_handles = create_empty_worker_handles::<reqwest::Client>();
        let global_stats = create_mock_global_stats();
        let reporter = ProgressReporter::new(
            None,
            NonZeroU64::new(1000).unwrap(),
            worker_handles,
            global_stats,
            std::time::Duration::from_secs(1),
        );
        
        // 这些调用不应该 panic
        reporter.send_started_event(4, 256).await;
        reporter.send_error("Test error").await;
        
        // 给 actor 一些时间处理消息
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

