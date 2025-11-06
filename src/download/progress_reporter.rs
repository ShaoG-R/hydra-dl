//! 进度报告器模块
//! 
//! 负责管理进度报告和统计信息收集

use crate::pool::download::DownloadWorkerPool;
use super::DownloadProgress;
use tokio::sync::mpsc::Sender;
use std::num::NonZeroU64;

/// 进度报告器
/// 
/// 负责管理进度报告和统计信息收集
pub(super) struct ProgressReporter {
    /// 进度发送器
    progress_sender: Option<Sender<DownloadProgress>>,
    /// 已完成的 range 总数
    total_ranges_completed: usize,
    /// 文件总大小
    total_size: NonZeroU64,
}

impl ProgressReporter {
    /// 创建新的进度报告器
    pub(super) fn new(
        progress_sender: Option<Sender<DownloadProgress>>,
        total_size: NonZeroU64,
    ) -> Self {
        Self {
            progress_sender,
            total_ranges_completed: 0,
            total_size,
        }
    }
    
    /// 发送开始事件
    pub(super) async fn send_started_event(&self, worker_count: usize, initial_chunk_size: u64) {
        if let Some(ref sender) = self.progress_sender {
            let _ = sender.send(DownloadProgress::Started {
                total_size: self.total_size,
                worker_count,
                initial_chunk_size,
            }).await;
        }
    }
    
    /// 发送进度更新
    pub(super) async fn send_progress_update(
        &self,
        pool: &DownloadWorkerPool,
    ) {
        if let Some(ref sender) = self.progress_sender {
            let total_avg_speed = pool.get_total_speed();
            let (total_instant_speed, instant_valid) = pool.get_total_instant_speed();
            let (total_bytes, _, _) = pool.get_total_stats();
            
            // 计算百分比
            let percentage = if self.total_size.get() > 0 {
                (total_bytes as f64 / self.total_size.get() as f64) * 100.0
            } else {
                0.0
            };
            
            // 收集所有 worker 的统计信息（包含各自的分块大小）
            let worker_stats = pool.get_worker_snapshots();
            
            // 发送总体进度和所有 worker 统计
            let _ = sender.send(DownloadProgress::Progress {
                bytes_downloaded: total_bytes,
                total_size: self.total_size,
                percentage,
                avg_speed: total_avg_speed,
                instant_speed: if instant_valid { Some(total_instant_speed) } else { None },
                worker_stats,
            }).await;
        }
    }
    
    /// 发送完成统计
    pub(super) async fn send_completion_stats(&self, pool: &DownloadWorkerPool) {
        if let Some(ref sender) = self.progress_sender {
            let (total_bytes, total_secs, _) = pool.get_total_stats();
            let avg_speed = pool.get_total_speed();
            
            // 收集所有 worker 的最终统计信息（包含分块大小）
            let worker_stats = pool.get_worker_snapshots();
            
            // 发送完成事件和最终 worker 统计
            let _ = sender.send(DownloadProgress::Completed {
                total_bytes,
                total_time: total_secs,
                avg_speed,
                worker_stats,
            }).await;
        }
    }
    
    /// 发送错误事件
    pub(super) async fn send_error(&self, error_msg: &str) {
        if let Some(ref sender) = self.progress_sender {
            let _ = sender.send(DownloadProgress::Error {
                message: error_msg.to_string(),
            }).await;
        }
    }
    
    /// 记录一个 range 完成
    pub(super) fn record_range_complete(&mut self) {
        self.total_ranges_completed += 1;
    }
    
    /// 获取已完成的 range 总数
    pub(super) fn total_ranges_completed(&self) -> usize {
        self.total_ranges_completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_progress_reporter_creation() {
        let (tx, _rx) = mpsc::channel(10);
        let reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
        assert_eq!(reporter.total_ranges_completed(), 0);
        assert_eq!(reporter.total_size.get(), 1000);
    }

    #[tokio::test]
    async fn test_progress_reporter_without_sender() {
        let reporter = ProgressReporter::new(None, NonZeroU64::new(1000).unwrap());
        
        assert_eq!(reporter.total_ranges_completed(), 0);
        assert_eq!(reporter.total_size.get(), 1000);
    }

    #[tokio::test]
    async fn test_record_range_complete() {
        let (tx, _rx) = mpsc::channel(10);
        let mut reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
        assert_eq!(reporter.total_ranges_completed(), 0);
        
        reporter.record_range_complete();
        assert_eq!(reporter.total_ranges_completed(), 1);
        
        reporter.record_range_complete();
        reporter.record_range_complete();
        assert_eq!(reporter.total_ranges_completed(), 3);
    }

    #[tokio::test]
    async fn test_send_started_event() {
        let (tx, mut rx) = mpsc::channel(10);
        let reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
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
        let reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(1000).unwrap());
        
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
        let reporter = ProgressReporter::new(None, NonZeroU64::new(1000).unwrap());
        
        // 这些调用不应该 panic
        reporter.send_started_event(4, 256).await;
        reporter.send_error("Test error").await;
    }

    #[tokio::test]
    async fn test_multiple_range_completions() {
        let (tx, _rx) = mpsc::channel(10);
        let mut reporter = ProgressReporter::new(Some(tx), NonZeroU64::new(10000).unwrap());
        
        for i in 1..=100 {
            reporter.record_range_complete();
            assert_eq!(reporter.total_ranges_completed(), i);
        }
    }
}

