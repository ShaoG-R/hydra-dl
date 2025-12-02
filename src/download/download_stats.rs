//! 下载统计聚合模块
//!
//! 负责聚合所有 Worker 的下载统计数据，使用 Actor 模式接收来自 broadcast channel 的更新。
//!
//! # 设计说明
//!
//! - 每个 StatsUpdater 通过 broadcast channel 广播 `ExecutorBroadcast` 消息
//! - 聚合器订阅 broadcast channel，接收所有 Executor 的统计更新和关闭信号
//! - 内部使用 `HashMap<u64, ExecutorStats>` 存储所有 Worker 的统计
//! - 提供查询接口获取聚合后的统计数据

use crate::pool::download::{ExecutorBroadcast, ExecutorStats, TaggedBroadcast};
use lite_sync::oneshot::lite;
use log::debug;
use net_bytes::{DownloadAcceleration, DownloadSpeed};
use rustc_hash::FxHashMap;
use smr_swap::{LocalReader, SmrSwap};
use tokio::sync::broadcast;

/// 下载统计聚合器句柄
///
/// 包含生命周期管理（shutdown）和统计数据访问
pub(crate) struct DownloadStatsHandle {
    /// 关闭信号发送器
    shutdown_tx: lite::Sender<()>,
    /// Actor 任务句柄
    actor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DownloadStatsHandle {

    /// 关闭聚合器并等待其完全停止
    ///
    /// 这个方法会消耗 self，确保聚合器完全停止并释放所有引用
    pub(crate) async fn shutdown_and_wait(mut self) {
        // 发送关闭信号
        let _ = self.shutdown_tx.notify(());

        // 等待 actor 任务完成
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
            debug!("DownloadStats actor has fully stopped");
        }
    }
}

/// 聚合后的统计数据
#[derive(Debug, Clone, Default)]
pub struct AggregatedStats {
    /// 所有 Executor 的统计映射
    pub stats_map: FxHashMap<u64, ExecutorStats>,
}

impl AggregatedStats {
    /// 获取指定 Executor 的统计
    pub fn get(&self, worker_id: u64) -> Option<&ExecutorStats> {
        self.stats_map.get(&worker_id)
    }

    /// 获取 Executor 数量
    pub fn len(&self) -> usize {
        self.stats_map.len()
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.stats_map.is_empty()
    }

    /// 迭代所有 Executor 统计
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &ExecutorStats)> {
        self.stats_map.iter()
    }

    /// 获取所有 Worker 的总窗口平均速度
    ///
    /// 将所有 Worker 的窗口平均速度相加
    pub fn get_total_window_avg_speed(&self) -> Option<DownloadSpeed> {
        let mut total: u64 = 0;
        let mut has_any = false;

        for (_, stats) in self.stats_map.iter() {
            if let Some(speed) = stats.get_window_avg_speed() {
                total += speed.as_u64();
                has_any = true;
            }
        }

        if has_any {
            Some(DownloadSpeed::from_raw(total))
        } else {
            None
        }
    }

    /// 获取所有 Worker 的总下载字节数
    pub fn get_total_bytes(&self) -> u64 {
        self.stats_map.values().map(|s| s.total_bytes).sum()
    }

    /// 获取所有 Worker 的总实时速度
    pub fn get_total_instant_speed(&self) -> Option<DownloadSpeed> {
        let mut total: u64 = 0;
        let mut has_any = false;

        for (_, stats) in self.stats_map.iter() {
            if let Some(speed) = stats.get_instant_speed() {
                total += speed.as_u64();
                has_any = true;
            }
        }

        if has_any {
            Some(DownloadSpeed::from_raw(total))
        } else {
            None
        }
    }

    /// 获取所有 Worker 的总平均速度
    pub fn get_total_avg_speed(&self) -> Option<DownloadSpeed> {
        let mut total: u64 = 0;
        let mut has_any = false;

        for (_, stats) in self.stats_map.iter() {
            if let Some(speed) = stats.get_avg_speed() {
                total += speed.as_u64();
                has_any = true;
            }
        }

        if has_any {
            Some(DownloadSpeed::from_raw(total))
        } else {
            None
        }
    }

    /// 获取所有 Worker 的总实时加速度
    pub fn get_total_instant_acceleration(&self) -> Option<DownloadAcceleration> {
        let mut total: i64 = 0;
        let mut has_any = false;

        for (_, stats) in self.stats_map.iter() {
            if let Some(accel) = stats.get_instant_acceleration() {
                total += accel.as_i64();
                has_any = true;
            }
        }

        if has_any {
            Some(DownloadAcceleration::from_raw(total))
        } else {
            None
        }
    }
}

/// 下载统计聚合器
///
/// 订阅 broadcast channel，接收所有 Executor 的统计更新和关闭信号
pub(crate) struct DownloadStats {
    /// 聚合后的统计数据（SmrSwap 包装，支持外部读取）
    stats: SmrSwap<AggregatedStats>,
    /// 广播接收器
    broadcast_rx: broadcast::Receiver<TaggedBroadcast>,
    /// 关闭信号接收器
    shutdown_rx: lite::Receiver<()>,
}

impl DownloadStats {
    /// 创建新的聚合器并启动 actor 任务
    ///
    /// # Arguments
    ///
    /// - `broadcast_rx`: 广播接收器，用于接收 Executor 统计更新和关闭信号
    ///
    /// # Returns
    ///
    /// 返回 `DownloadStatsHandle`（只包含 shutdown 接口）
    pub(crate) fn spawn(broadcast_rx: broadcast::Receiver<TaggedBroadcast>) -> (DownloadStatsHandle, LocalReader<AggregatedStats>) {
        let (shutdown_tx, shutdown_rx) = lite::channel();

        // 使用 SmrSwap 包装统计数据
        let stats = SmrSwap::new(AggregatedStats::default());
        let stats_reader = stats.local();

        let aggregator = Self {
            stats,
            broadcast_rx,
            shutdown_rx,
        };

        // 启动 actor 任务
        let actor_handle = tokio::spawn(aggregator.run());

        (DownloadStatsHandle {
            shutdown_tx,
            actor_handle: Some(actor_handle),
        }, stats_reader)
    }

    /// 运行聚合器主循环
    ///
    /// 持续接收广播消息并更新聚合统计，直到收到关闭信号
    async fn run(mut self) {
        debug!("DownloadStats 启动");

        loop {
            tokio::select! {
                // 处理广播消息
                result = self.broadcast_rx.recv() => {
                    match result {
                        Ok((worker_id, ExecutorBroadcast::Stats(stats))) => {
                            self.handle_update(worker_id, stats);
                        }
                        Ok((worker_id, ExecutorBroadcast::Shutdown)) => {
                            self.handle_remove(worker_id);
                        }
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            // 消息丢失，记录日志但继续运行
                            debug!("DownloadStats 落后 {} 条消息", count);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            // 广播通道已关闭
                            debug!("DownloadStats 广播通道已关闭");
                            break;
                        }
                    }
                }
                // 处理关闭信号
                result = &mut self.shutdown_rx => {
                    match result {
                        Ok(_) => {
                            debug!("DownloadStats 收到关闭信号");
                        }
                        Err(e) => {
                            debug!("DownloadStats 关闭通道已断开: {:?}", e);
                        }
                    }
                    break;
                }
            }
        }

        // 清空统计数据
        self.stats.store(AggregatedStats::default());
        debug!("DownloadStats 退出");
    }

    /// 处理统计更新
    fn handle_update(&mut self, worker_id: u64, stats: ExecutorStats) {
        self.stats.update(|s| {
            let mut s = s.clone();
            s.stats_map.insert(worker_id, stats.clone());
            s
        });
    }

    /// 处理移除
    fn handle_remove(&mut self, worker_id: u64) {
        self.stats.update(|s| {
            let mut s = s.clone();
            s.stats_map.remove(&worker_id);
            s
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::pool::download::WorkerBroadcaster;
    use tokio::sync::broadcast;

    use super::*;

    #[tokio::test]
    async fn test_aggregator_update() {
        // 创建广播通道
        let (broadcast_tx, broadcast_rx) = broadcast::channel(16);
        
        // 启动聚合器
        let (handle, _) = DownloadStats::spawn(broadcast_rx);

        // 创建 WorkerBroadcaster 用于发送消息
        let broadcaster1 = WorkerBroadcaster::new(1, broadcast_tx.clone(), 4);
        let broadcaster2 = WorkerBroadcaster::new(2, broadcast_tx, 4);

        // 发送更新（通过广播）
        let executor_stats = ExecutorStats::default();
        broadcaster1.send_stats(executor_stats.clone());
        broadcaster2.send_stats(executor_stats);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 使用 shutdown_and_wait 正确关闭
        handle.shutdown_and_wait().await;
    }

    #[tokio::test]
    async fn test_aggregator_remove() {
        // 创建广播通道
        let (broadcast_tx, broadcast_rx) = broadcast::channel(16);
        
        // 启动聚合器
        let (handle, _) = DownloadStats::spawn(broadcast_rx);

        // 创建 WorkerBroadcaster 用于发送消息
        let broadcaster1 = WorkerBroadcaster::new(1, broadcast_tx.clone(), 4);
        let broadcaster2 = WorkerBroadcaster::new(2, broadcast_tx, 4);

        // 添加两个 executor
        let executor_stats = ExecutorStats::default();
        broadcaster1.send_stats(executor_stats.clone());
        broadcaster2.send_stats(executor_stats);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 移除一个（通过广播关闭信号）
        broadcaster1.send_shutdown();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 使用 shutdown_and_wait 正确关闭
        handle.shutdown_and_wait().await;
    }

    #[tokio::test]
    async fn test_aggregator_state_transitions() {
        // 创建广播通道
        let (broadcast_tx, broadcast_rx) = broadcast::channel(16);
        
        // 启动聚合器
        let (handle, _) = DownloadStats::spawn(broadcast_rx);

        // 创建 WorkerBroadcaster 用于发送消息
        let broadcaster = WorkerBroadcaster::new(1, broadcast_tx, 4);

        // 发送一些消息确保 actor 在运行
        let executor_stats = ExecutorStats::default();
        broadcaster.send_stats(executor_stats);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 关闭并等待 - 这会触发 Stopped 状态
        handle.shutdown_and_wait().await;
    }
}
