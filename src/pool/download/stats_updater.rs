//! Stats Updater 辅助协程
//!
//! 负责维护 `SmrSwap<WorkerStats>` 的更新，从主下载协程接收消息并更新统计数据。
//!
//! # 设计说明
//!
//! 主下载协程在以下时机发送消息到此协程：
//! - 任务开始时发送 `TaskStarted`
//! - `record_chunk` 返回 `true`（采样成功）时发送 `ChunkSampled`
//! - 任务结束时发送 `TaskEnded`
//!
//! 这样可以将统计更新的开销从热路径中移除，提高下载性能。

use crate::utils::stats::WorkerStats;
use log::debug;
use smr_swap::SmrSwap;
use tokio::sync::mpsc;

/// Stats 更新消息
#[derive(Debug, Clone)]
pub(crate) enum StatsMessage {
    /// 任务开始
    TaskStarted,
    /// Chunk 采样成功，包含当前的 WorkerStats 快照
    ChunkSampled(WorkerStats),
    /// 任务结束
    TaskEnded,
}

/// Stats Updater 配置
pub(crate) struct StatsUpdaterConfig {
    /// 消息通道容量
    pub(crate) channel_capacity: usize,
}

impl Default for StatsUpdaterConfig {
    fn default() -> Self {
        Self {
            // 默认容量为 64，足够缓冲高频采样消息
            channel_capacity: 64,
        }
    }
}

/// Stats Updater 句柄
///
/// 用于向辅助协程发送消息
#[derive(Clone)]
pub(crate) struct StatsUpdaterHandle {
    tx: mpsc::Sender<StatsMessage>,
}

impl StatsUpdaterHandle {
    /// 发送任务开始信号
    #[inline]
    pub(crate) fn send_task_started(&self) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::TaskStarted);
    }

    /// 发送采样成功信号
    #[inline]
    pub(crate) fn send_chunk_sampled(&self, stats: WorkerStats) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::ChunkSampled(stats));
    }

    /// 发送任务结束信号
    #[inline]
    pub(crate) fn send_task_ended(&self) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::TaskEnded);
    }
}

/// Stats Updater 辅助协程
///
/// 持有 `SmrSwap<WorkerStats>` 并根据接收到的消息更新统计数据
pub(crate) struct StatsUpdater {
    /// Worker ID（用于日志）
    worker_id: u64,
    /// 统计数据
    stats: SmrSwap<WorkerStats>,
    /// 消息接收通道
    rx: mpsc::Receiver<StatsMessage>,
}

impl StatsUpdater {
    /// 创建新的 Stats Updater 和对应的句柄
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `stats`: 统计数据的 SmrSwap 包装
    /// - `config`: 配置（可选，使用默认配置）
    ///
    /// # Returns
    ///
    /// 返回 `(StatsUpdater, StatsUpdaterHandle)`
    pub(crate) fn new(
        worker_id: u64,
        stats: SmrSwap<WorkerStats>,
        config: Option<StatsUpdaterConfig>,
    ) -> (Self, StatsUpdaterHandle) {
        let config = config.unwrap_or_default();
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        let updater = Self {
            worker_id,
            stats,
            rx,
        };

        let handle = StatsUpdaterHandle { tx };

        (updater, handle)
    }

    /// 运行 Stats Updater 主循环
    ///
    /// 持续接收消息并更新统计数据，直到通道关闭
    pub(crate) async fn run(mut self) {
        debug!("Worker #{} Stats Updater 启动", self.worker_id);

        while let Some(msg) = self.rx.recv().await {
            match msg {
                StatsMessage::TaskStarted => {
                    self.handle_task_started();
                }
                StatsMessage::ChunkSampled(worker_stats) => {
                    self.handle_chunk_sampled(worker_stats);
                }
                StatsMessage::TaskEnded => {
                    self.handle_task_ended();
                }
            }
        }

        debug!("Worker #{} Stats Updater 退出", self.worker_id);
    }

    /// 处理任务开始
    fn handle_task_started(&mut self) {
        debug!("Worker #{} 任务开始", self.worker_id);
        self.stats.update(|s| {
            let mut s = s.clone();
            s.set_active(true);
            s.clear_samples();
            s
        });
    }

    /// 处理采样成功
    fn handle_chunk_sampled(&mut self, worker_stats: WorkerStats) {
        // 直接存储新的统计数据
        self.stats.store(worker_stats);
    }

    /// 处理任务结束
    fn handle_task_ended(&mut self) {
        debug!("Worker #{} 任务结束", self.worker_id);
        self.stats.update(|s| {
            let mut s = s.clone();
            s.set_active(false);
            s
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stats_updater_task_lifecycle() {
        let stats = SmrSwap::new(WorkerStats::default());
        let local = stats.local();
        let (updater, handle) = StatsUpdater::new(1, stats, None);

        // 启动 updater
        let updater_handle = tokio::spawn(updater.run());

        // 发送任务开始
        handle.send_task_started();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 验证 active 状态
        assert!(local.load().is_active());

        // 发送任务结束
        handle.send_task_ended();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 验证 active 状态
        assert!(!local.load().is_active());

        // 关闭通道
        drop(handle);
        let _ = updater_handle.await;
    }

    #[tokio::test]
    async fn test_stats_updater_chunk_sampled() {
        let stats = SmrSwap::new(WorkerStats::default());
        let local = stats.local();
        let (updater, handle) = StatsUpdater::new(1, stats, None);

        // 启动 updater
        let updater_handle = tokio::spawn(updater.run());

        // 创建一个带有数据的 WorkerStats
        let mut worker_stats = WorkerStats::default();
        worker_stats.set_current_chunk_size(1024 * 1024);

        // 发送采样
        handle.send_chunk_sampled(worker_stats);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 验证 chunk size 被更新
        assert_eq!(local.load().get_current_chunk_size(), 1024 * 1024);

        // 关闭通道
        drop(handle);
        let _ = updater_handle.await;
    }
}
