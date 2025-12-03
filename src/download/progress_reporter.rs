//! 进度报告器模块（Actor 模式）
//!
//! 负责管理进度报告和统计信息收集
//! 采用 Actor 模式，完全独立于主下载循环

use crate::download::download_stats::AggregatedStats;
use log::debug;
use net_bytes::{DownloadAcceleration, DownloadSpeed};
use smr_swap::LocalReader;
use std::num::NonZeroU64;
use std::time::Instant;
use tokio::sync::mpsc;

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
    /// 下载进度更新（包含总体统计和所有 Executor 的聚合统计）
    Progress {
        /// 已写入磁盘的有效字节数（不包含重试的重复字节，用于进度计算）
        written_bytes: u64,
        /// 已下载的总字节数（包括重试的重复字节，用于速度计算）
        downloaded_bytes: u64,
        /// 文件总大小（bytes）
        total_size: NonZeroU64,
        /// 下载百分比 (0.0 ~ 100.0)
        percentage: f64,
        /// 平均速度 (bytes/s)
        avg_speed: Option<DownloadSpeed>,
        /// 实时速度 (bytes/s)，如果无效则为 None
        instant_speed: Option<DownloadSpeed>,
        /// 窗口平均速度 (bytes/s)，如果无效则为 None
        window_avg_speed: Option<DownloadSpeed>,
        /// 实时加速度 (bytes/s²)，如果无效则为 None
        instant_acceleration: Option<DownloadAcceleration>,
        /// 所有 Executor 的聚合统计信息
        executor_stats: AggregatedStats,
    },
    /// 下载已完成（包含最终的 Executor 聚合统计）
    Completed {
        /// 已写入磁盘的有效字节数（不包含重试的重复字节）
        total_written_bytes: u64,
        /// 总下载字节数（包括重试的重复字节）
        total_downloaded_bytes: u64,
        /// 总耗时（秒）
        total_time: f64,
        /// 平均速度 (bytes/s)
        avg_speed: Option<DownloadSpeed>,
        /// 所有 Executor 的最终聚合统计信息
        executor_stats: AggregatedStats,
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
    SendError { message: String },
    /// 关闭 actor
    Shutdown,
}

/// 进度报告器参数
pub(super) struct ProgressReporterParams {
    /// 进度发送器
    pub progress_sender: Option<mpsc::Sender<DownloadProgress>>,
    /// 文件总大小
    pub total_size: NonZeroU64,
    /// 聚合统计数据读取器（从 DownloadStats 获取）
    pub aggregated_stats: LocalReader<AggregatedStats>,
    /// 更新间隔
    pub update_interval: std::time::Duration,
    /// 启动偏移时间
    pub start_offset: std::time::Duration,
}

/// 进度报告器 Actor
///
/// 独立运行的 actor，负责管理进度报告和统计信息收集
struct ProgressReporterActor {
    /// 进度发送器
    progress_sender: Option<mpsc::Sender<DownloadProgress>>,
    /// 文件总大小
    total_size: NonZeroU64,
    /// 消息接收器（async channel）
    message_rx: mpsc::Receiver<ActorMessage>,
    /// 聚合统计数据读取器（从 DownloadStats 获取）
    aggregated_stats: LocalReader<AggregatedStats>,
    /// 进度更新定时器（内部管理）
    progress_timer: tokio::time::Interval,
    /// 启动时间（用于计算总耗时）
    start_time: std::time::Instant,
    /// 上次实时速度（用于计算加速度）
    last_instant_speed: Option<DownloadSpeed>,
    /// 上次速度记录时间（用于计算加速度）
    last_speed_time: Option<Instant>,
}

impl ProgressReporterActor {
    /// 创建新的 actor
    fn new(params: ProgressReporterParams, message_rx: mpsc::Receiver<ActorMessage>) -> Self {
        let ProgressReporterParams {
            progress_sender,
            total_size,
            aggregated_stats,
            update_interval,
            start_offset,
        } = params;

        debug!("ProgressReporter 启动偏移: {:?}", start_offset);
        let timer_start = tokio::time::Instant::now() + start_offset;
        let progress_timer = tokio::time::interval_at(timer_start, update_interval);

        Self {
            progress_sender,
            total_size,
            message_rx,
            aggregated_stats,
            progress_timer,
            start_time: std::time::Instant::now(),
            last_instant_speed: None,
            last_speed_time: None,
        }
    }

    /// 运行 actor 事件循环（使用 tokio::select!）
    async fn run(mut self) {
        debug!("ProgressReporterActor started");

        // 提取 sender 用于发送，避免在 await 期间持有 &self
        let sender = self.progress_sender.clone();

        loop {
            tokio::select! {
                // 内部定时器：自主触发进度更新
                _ = self.progress_timer.tick() => {
                    if let Some(ref tx) = sender {
                        if let Some(msg) = self.generate_progress_update() {
                            let _ = tx.send(msg).await;
                        }
                    }
                }
                // 外部消息
                msg = self.message_rx.recv() => {
                    match msg {
                        Some(ActorMessage::SendStarted { worker_count, initial_chunk_size }) => {
                            if let Some(ref tx) = sender {
                                let msg = self.generate_started_event(worker_count, initial_chunk_size);
                                let _ = tx.send(msg).await;
                            }
                        }
                        Some(ActorMessage::SendCompletion) => {
                            if let Some(ref tx) = sender {
                                let msg = self.generate_completion_stats();
                                let _ = tx.send(msg).await;
                            }
                        }
                        Some(ActorMessage::SendError { message }) => {
                            if let Some(ref tx) = sender {
                                let msg = self.generate_error(&message);
                                let _ = tx.send(msg).await;
                            }
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

    /// 生成开始事件消息
    fn generate_started_event(
        &self,
        worker_count: u64,
        initial_chunk_size: u64,
    ) -> DownloadProgress {
        DownloadProgress::Started {
            total_size: self.total_size,
            worker_count,
            initial_chunk_size,
        }
    }

    /// 获取聚合统计数据快照
    fn get_aggregated_stats(&self) -> AggregatedStats {
        self.aggregated_stats.load().cloned()
    }

    /// 生成进度更新消息（从 aggregated_stats 获取所有数据）
    fn generate_progress_update(&mut self) -> Option<DownloadProgress> {
        // 只有当有 sender 时才生成消息（虽然调用方也会检查，但双重检查无害）
        if self.progress_sender.is_none() {
            return None;
        }

        // 从 aggregated_stats 获取所有 Executor 的统计
        let executor_stats = self.get_aggregated_stats();

        // 一次性获取所有统计数据
        let summary = executor_stats.get_summary();

        // 计算百分比（基于已写入字节数）
        let percentage = if self.total_size.get() > 0 {
            (summary.written_bytes as f64 / self.total_size.get() as f64) * 100.0
        } else {
            0.0
        };

        let current_instant_speed = summary.instant_speed;

        // 计算实时加速度
        let now = Instant::now();
        let instant_acceleration = self.calculate_acceleration(current_instant_speed, now);

        // 更新上次速度记录
        if current_instant_speed.is_some() {
            self.last_instant_speed = current_instant_speed;
            self.last_speed_time = Some(now);
        }

        Some(DownloadProgress::Progress {
            written_bytes: summary.written_bytes,
            downloaded_bytes: summary.downloaded_bytes,
            total_size: self.total_size,
            percentage,
            avg_speed: summary.avg_speed,
            instant_speed: current_instant_speed,
            window_avg_speed: summary.window_avg_speed,
            instant_acceleration,
            executor_stats,
        })
    }

    /// 计算实时加速度
    ///
    /// 使用上次记录的实时速度和当前实时速度计算加速度
    fn calculate_acceleration(
        &self,
        current_speed: Option<DownloadSpeed>,
        now: Instant,
    ) -> Option<DownloadAcceleration> {
        let current = current_speed?;
        let last_speed = self.last_instant_speed?;
        let last_time = self.last_speed_time?;

        let duration = now.duration_since(last_time);
        if duration.is_zero() {
            return None;
        }

        Some(DownloadAcceleration::new(
            last_speed.as_u64(),
            current.as_u64(),
            duration,
        ))
    }

    /// 生成完成统计消息（从 aggregated_stats 获取所有数据）
    fn generate_completion_stats(&self) -> DownloadProgress {
        // 从 aggregated_stats 获取所有 Executor 的统计
        let executor_stats = self.get_aggregated_stats();
        let summary = executor_stats.get_summary();

        // 计算总耗时
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();

        DownloadProgress::Completed {
            total_written_bytes: summary.written_bytes,
            total_downloaded_bytes: summary.downloaded_bytes,
            total_time: elapsed_secs,
            avg_speed: summary.avg_speed,
            executor_stats,
        }
    }

    /// 生成错误事件消息
    fn generate_error(&self, error_msg: &str) -> DownloadProgress {
        DownloadProgress::Error {
            message: error_msg.to_string(),
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
    pub(super) fn new(params: ProgressReporterParams) -> Self {
        // 使用有界 channel，容量 100
        let (message_tx, message_rx) = mpsc::channel(100);

        // 启动 actor 任务
        tokio::spawn(async move {
            ProgressReporterActor::new(params, message_rx).run().await;
        });

        Self {
            message_tx,
        }
    }

    /// 发送开始事件
    pub(super) async fn send_started_event(&self, worker_count: u64, initial_chunk_size: u64) {
        let tx = self.message_tx.clone();
        let _ = tx
            .send(ActorMessage::SendStarted {
                worker_count,
                initial_chunk_size,
            })
            .await;
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

    // 辅助函数：创建空的 aggregated_stats
    fn create_empty_aggregated_stats() -> LocalReader<AggregatedStats> {
        let smr = smr_swap::SmrSwap::new(AggregatedStats::default());
        smr.local()
    }

    #[tokio::test]
    async fn test_progress_reporter_creation() {
        let (tx, _rx) = mpsc::channel(10);
        let aggregated_stats = create_empty_aggregated_stats();
        let _reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender: Some(tx),
            total_size: NonZeroU64::new(1000).unwrap(),
            aggregated_stats,
            update_interval: std::time::Duration::from_secs(1),
            start_offset: std::time::Duration::ZERO,
        });
        // Actor 已启动，只验证创建成功
    }

    #[tokio::test]
    async fn test_progress_reporter_without_sender() {
        let aggregated_stats = create_empty_aggregated_stats();
        let _reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender: None,
            total_size: NonZeroU64::new(1000).unwrap(),
            aggregated_stats,
            update_interval: std::time::Duration::from_secs(1),
            start_offset: std::time::Duration::ZERO,
        });
        // Actor 已启动，只验证创建成功
    }

    #[tokio::test]
    async fn test_send_started_event() {
        let (tx, mut rx) = mpsc::channel(10);
        let aggregated_stats = create_empty_aggregated_stats();
        let reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender: Some(tx),
            total_size: NonZeroU64::new(1000).unwrap(),
            aggregated_stats,
            update_interval: std::time::Duration::from_secs(1),
            start_offset: std::time::Duration::ZERO,
        });

        reporter.send_started_event(4, 256).await;

        // 接收事件
        if let Some(progress) = rx.recv().await {
            match progress {
                DownloadProgress::Started {
                    total_size,
                    worker_count,
                    initial_chunk_size,
                } => {
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
        let aggregated_stats = create_empty_aggregated_stats();
        let reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender: Some(tx),
            total_size: NonZeroU64::new(1000).unwrap(),
            aggregated_stats,
            update_interval: std::time::Duration::from_secs(1),
            start_offset: std::time::Duration::ZERO,
        });

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
        let aggregated_stats = create_empty_aggregated_stats();
        let reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender: None,
            total_size: NonZeroU64::new(1000).unwrap(),
            aggregated_stats,
            update_interval: std::time::Duration::from_secs(1),
            start_offset: std::time::Duration::ZERO,
        });

        // 这些调用不应该 panic
        reporter.send_started_event(4, 256).await;
        reporter.send_error("Test error").await;

        // 给 actor 一些时间处理消息
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}
