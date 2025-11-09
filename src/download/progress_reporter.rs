//! 进度报告器模块（Actor 模式）
//! 
//! 负责管理进度报告和统计信息收集
//! 采用 Actor 模式，完全独立于主下载循环

use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::sync::mpsc;
use log::debug;
use crate::utils::stats::WorkerStats;

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

/// Worker 原始统计数据（用于传递给 actor）
#[derive(Debug, Clone)]
pub(super) struct WorkerStatsRef {
    /// Worker ID
    pub worker_id: u64,
    /// Worker 统计数据
    pub stats: Arc<WorkerStats>,
}

/// Actor 消息类型
#[derive(Debug)]
enum ActorMessage {
    /// 记录 range 完成
    RecordRangeComplete,
    /// 发送开始事件
    SendStarted {
        worker_count: u64,
        initial_chunk_size: u64,
    },
    /// 定期统计更新（传递原始统计数据，actor 内部计算）
    StatsUpdate {
        total_bytes: u64,
        total_avg_speed: f64,
        total_instant_speed: Option<f64>,
        worker_stats: Vec<WorkerStatsRef>,
    },
    /// 发送完成统计
    SendCompletion {
        total_bytes: u64,
        total_avg_speed: f64,
        total_secs: f64,
        worker_stats: Vec<WorkerStatsRef>,
    },
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
struct ProgressReporterActor {
    /// 进度发送器
    progress_sender: Option<mpsc::Sender<DownloadProgress>>,
    /// 已完成的 range 总数
    total_ranges_completed: usize,
    /// 文件总大小
    total_size: NonZeroU64,
    /// 消息接收器（async channel）
    message_rx: mpsc::Receiver<ActorMessage>,
}

impl ProgressReporterActor {
    /// 创建新的 actor
    fn new(
        progress_sender: Option<mpsc::Sender<DownloadProgress>>,
        total_size: NonZeroU64,
        message_rx: mpsc::Receiver<ActorMessage>,
    ) -> Self {
        Self {
            progress_sender,
            total_ranges_completed: 0,
            total_size,
            message_rx,
        }
    }
    
    /// 运行 actor 事件循环（使用 tokio::select!）
    async fn run(mut self) {
        debug!("ProgressReporterActor started");
        
        loop {
            tokio::select! {
                msg = self.message_rx.recv() => {
                    match msg {
                        Some(ActorMessage::RecordRangeComplete) => {
                            self.total_ranges_completed += 1;
                        }
                        Some(ActorMessage::SendStarted { worker_count, initial_chunk_size }) => {
                            self.send_started_event(worker_count, initial_chunk_size).await;
                        }
                        Some(ActorMessage::StatsUpdate { total_bytes, total_avg_speed, total_instant_speed, worker_stats }) => {
                            self.send_progress_update(total_bytes, total_avg_speed, total_instant_speed, worker_stats).await;
                        }
                        Some(ActorMessage::SendCompletion { total_bytes, total_avg_speed, total_secs, worker_stats }) => {
                            self.send_completion_stats(total_bytes, total_avg_speed, total_secs, worker_stats).await;
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
    
    /// 计算 worker 统计快照
    fn compute_worker_snapshots(&self, worker_stats: Vec<WorkerStatsRef>) -> Vec<WorkerStatSnapshot> {
        worker_stats.into_iter().map(|ws| {
            let (worker_bytes, _, worker_ranges, avg_speed, instant_speed, instant_valid, _, _) = 
                ws.stats.get_full_summary();
            let current_chunk_size = ws.stats.get_current_chunk_size();
            
            WorkerStatSnapshot {
                worker_id: ws.worker_id,
                bytes: worker_bytes,
                ranges: worker_ranges,
                avg_speed,
                instant_speed: if instant_valid { Some(instant_speed) } else { None },
                current_chunk_size,
            }
        }).collect()
    }
    
    /// 发送进度更新
    async fn send_progress_update(
        &self,
        total_bytes: u64,
        total_avg_speed: f64,
        total_instant_speed: Option<f64>,
        worker_stats: Vec<WorkerStatsRef>,
    ) {
        if let Some(ref sender) = self.progress_sender {
            // 计算百分比
            let percentage = if self.total_size.get() > 0 {
                (total_bytes as f64 / self.total_size.get() as f64) * 100.0
            } else {
                0.0
            };
            
            // 在 actor 内部计算 worker 快照
            let worker_snapshots = self.compute_worker_snapshots(worker_stats);
            
            let _ = sender.send(DownloadProgress::Progress {
                bytes_downloaded: total_bytes,
                total_size: self.total_size,
                percentage,
                avg_speed: total_avg_speed,
                instant_speed: total_instant_speed,
                worker_stats: worker_snapshots,
            }).await;
        }
    }
    
    /// 发送完成统计
    async fn send_completion_stats(
        &self,
        total_bytes: u64,
        total_avg_speed: f64,
        total_secs: f64,
        worker_stats: Vec<WorkerStatsRef>,
    ) {
        if let Some(ref sender) = self.progress_sender {
            // 在 actor 内部计算 worker 快照
            let worker_snapshots = self.compute_worker_snapshots(worker_stats);
            
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
pub(super) struct ProgressReporter {
    /// 消息发送器（async channel）
    message_tx: mpsc::Sender<ActorMessage>,
}

impl ProgressReporter {
    /// 创建新的进度报告器（启动 actor）
    pub(super) fn new(
        progress_sender: Option<mpsc::Sender<DownloadProgress>>,
        total_size: NonZeroU64,
    ) -> Self {
        // 使用有界 channel，容量 100
        let (message_tx, message_rx) = mpsc::channel(100);
        
        let actor = ProgressReporterActor::new(
            progress_sender,
            total_size,
            message_rx,
        );
        
        // 启动 actor 任务
        tokio::spawn(actor.run());
        
        Self { message_tx }
    }
    
    /// 发送开始事件
    pub(super) fn send_started_event(&self, worker_count: u64, initial_chunk_size: u64) {
        let tx = self.message_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(ActorMessage::SendStarted {
                worker_count,
                initial_chunk_size,
            }).await;
        });
    }
    
    /// 发送统计更新（进度报告）
    pub(super) fn send_stats_update(
        &self,
        total_bytes: u64,
        total_avg_speed: f64,
        total_instant_speed: Option<f64>,
        worker_stats: Vec<WorkerStatsRef>,
    ) {
        let tx = self.message_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(ActorMessage::StatsUpdate {
                total_bytes,
                total_avg_speed,
                total_instant_speed,
                worker_stats,
            }).await;
        });
    }
    
    /// 发送完成统计
    pub(super) fn send_completion(
        &self,
        total_bytes: u64,
        total_avg_speed: f64,
        total_secs: f64,
        worker_stats: Vec<WorkerStatsRef>,
    ) {
        let tx = self.message_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(ActorMessage::SendCompletion {
                total_bytes,
                total_avg_speed,
                total_secs,
                worker_stats,
            }).await;
        });
    }
    
    /// 发送错误事件
    pub(super) fn send_error(&self, error_msg: &str) {
        let tx = self.message_tx.clone();
        let message = error_msg.to_string();
        tokio::spawn(async move {
            let _ = tx.send(ActorMessage::SendError { message }).await;
        });
    }
    
    /// 记录一个 range 完成
    pub(super) fn record_range_complete(&self) {
        let tx = self.message_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(ActorMessage::RecordRangeComplete).await;
        });
    }
    
    /// 关闭 actor
    pub(super) fn shutdown(&self) {
        let tx = self.message_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(ActorMessage::Shutdown).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_progress_reporter_creation() {
        let (tx, _rx) = mpsc::channel(10);
        let _reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        // Actor 已启动，只验证创建成功
    }

    #[tokio::test]
    async fn test_progress_reporter_without_sender() {
        let _reporter = ProgressReporter::new(None, NonZeroU64::new(1000).unwrap());
        // Actor 已启动，只验证创建成功
    }

    #[tokio::test]
    async fn test_send_started_event() {
        let (tx, mut rx) = mpsc::channel(10);
        let reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
        reporter.send_started_event(4, 256);
        
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
        let reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
        reporter.send_error("Test error");
        
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
        let reporter = ProgressReporter::new(None, NonZeroU64::new(1000).unwrap());
        
        // 这些调用不应该 panic
        reporter.send_started_event(4, 256);
        reporter.send_error("Test error");
        
        // 给 actor 一些时间处理消息
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (tx, _rx) = mpsc::channel(10);
        let reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
        reporter.record_range_complete();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        reporter.shutdown();
        
        // 给 actor 一些时间关闭
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

