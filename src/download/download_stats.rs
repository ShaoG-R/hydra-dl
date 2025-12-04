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
use net_bytes::DownloadSpeed;
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
        self.shutdown_tx.send_unchecked(());

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
    /// 预计算的下载摘要
    summary: DownloadSummary,
}

/// 字节统计
///
/// 仅包含已写入和已下载的字节数
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSummary {
    /// 已写入磁盘的有效字节数（不包含重试的重复字节，用于进度计算）
    pub written_bytes: u64,
    /// 已下载的总字节数（包括重试的重复字节，用于速度计算）
    pub downloaded_bytes: u64,
}

/// 下载统计摘要
///
/// 单次遍历聚合所有统计数据，用于进度展示
#[derive(Debug, Clone, Default)]
pub struct DownloadSummary {
    /// 已写入磁盘的有效字节数（不包含重试的重复字节，用于进度计算）
    pub written_bytes: u64,
    /// 已下载的总字节数（包括重试的重复字节，用于速度计算）
    pub downloaded_bytes: u64,
    /// 平均速度（从开始到现在）
    pub avg_speed: Option<DownloadSpeed>,
    /// 实时速度（基于短时间窗口）
    pub instant_speed: Option<DownloadSpeed>,
    /// 窗口平均速度（基于较长时间窗口）
    pub window_avg_speed: Option<DownloadSpeed>,
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
        self.stats_map
            .values()
            .filter_map(|s| s.get_window_avg_speed().map(|v| v.as_u64()))
            .reduce(|a, b| a + b)
            .map(DownloadSpeed::from_raw)
    }

    /// 获取所有 Worker 的总实时速度
    pub fn get_total_instant_speed(&self) -> Option<DownloadSpeed> {
        self.stats_map
            .values()
            .filter_map(|s| s.get_instant_speed().map(|v| v.as_u64()))
            .reduce(|a, b| a + b)
            .map(DownloadSpeed::from_raw)
    }

    /// 获取所有 Worker 的总平均速度
    pub fn get_total_avg_speed(&self) -> Option<DownloadSpeed> {
        self.stats_map
            .values()
            .filter_map(|s| s.get_avg_speed().map(|v| v.as_u64()))
            .reduce(|a, b| a + b)
            .map(DownloadSpeed::from_raw)
    }

    /// 获取字节统计
    ///
    /// 仅遍历获取已写入和已下载的字节数，不计算速度
    pub fn get_bytes_summary(&self) -> BytesSummary {
        BytesSummary { written_bytes: self.summary.written_bytes, downloaded_bytes: self.summary.downloaded_bytes }
    }

    /// 获取预计算的下载统计摘要
    #[inline]
    pub fn get_summary(&self) -> &DownloadSummary {
        &self.summary
    }
}

/// 从 stats_map 计算 DownloadSummary
fn compute_summary(stats_map: &FxHashMap<u64, ExecutorStats>) -> DownloadSummary {
    let mut avg_speed: u64 = 0;
    let mut instant_speed: u64 = 0;
    let mut window_avg_speed: u64 = 0;
    let mut written_bytes: u64 = 0;
    let mut downloaded_bytes: u64 = 0;

    for stats in stats_map.values() {
        // 直接从 ExecutorStats 累加（包括已停止的 Executor）
        written_bytes += stats.written_bytes;
        downloaded_bytes += stats.downloaded_bytes;

        // 速度统计只从运行中的 Executor 获取
        if let Some(speed_stats) = stats.get_speed_stats() {
            avg_speed += speed_stats.avg_speed.as_u64();
            instant_speed += speed_stats.instant_speed.as_u64();
            window_avg_speed += speed_stats.window_avg_speed.as_u64();
        }
    }

    DownloadSummary {
        written_bytes,
        downloaded_bytes,
        avg_speed: if avg_speed > 0 { Some(DownloadSpeed::from_raw(avg_speed)) } else { None },
        instant_speed: if instant_speed > 0 { Some(DownloadSpeed::from_raw(instant_speed)) } else { None },
        window_avg_speed: if window_avg_speed > 0 { Some(DownloadSpeed::from_raw(window_avg_speed)) } else { None },
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
                            self.handle_update(worker_id.pool_id(), stats);
                        }
                        Ok((worker_id, ExecutorBroadcast::Shutdown)) => {
                            self.handle_remove(worker_id.pool_id());
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
            s.summary = compute_summary(&s.stats_map);
            s
        });
    }

    /// 处理移除
    fn handle_remove(&mut self, worker_id: u64) {
        self.stats.update(|s| {
            let mut s = s.clone();
            s.stats_map.remove(&worker_id);
            s.summary = compute_summary(&s.stats_map);
            s
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::pool::common::WorkerId;
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
        let broadcaster1 = WorkerBroadcaster::new(WorkerId::new(0, 0), broadcast_tx.clone());
        let broadcaster2 = WorkerBroadcaster::new(WorkerId::new(1, 1), broadcast_tx);

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
        let broadcaster1 = WorkerBroadcaster::new(WorkerId::new(0, 0), broadcast_tx.clone());
        let broadcaster2 = WorkerBroadcaster::new(WorkerId::new(1, 1), broadcast_tx);

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
        let broadcaster = WorkerBroadcaster::new(WorkerId::new(0, 0), broadcast_tx);

        // 发送一些消息确保 actor 在运行
        let executor_stats = ExecutorStats::default();
        broadcaster.send_stats(executor_stats);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // 关闭并等待 - 这会触发 Stopped 状态
        handle.shutdown_and_wait().await;
    }
}
