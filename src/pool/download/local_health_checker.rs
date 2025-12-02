//! 本地健康检查协程
//!
//! 为单个 Worker 提供本地健康监测，监听 WorkerBroadcaster 的本地广播，
//! 检测 stats 更新超时和绝对速度阈值。
//!
//! # 设计说明
//!
//! 与全局 WorkerHealthChecker 的分工：
//! - 本地健康检查器：超时检测、绝对速度检测（每个 Worker 独立）
//! - 全局健康检查器：相对速度检测（max gap 算法，需要所有 Workers 数据）
//!
//! # 工作流程
//!
//! 1. 订阅 WorkerBroadcaster 的本地广播
//! 2. 使用 tokio::time::timeout 监听广播消息
//! 3. 超时时触发取消信号
//! 4. 收到 Stats 更新时检查绝对速度阈值

use super::stats_updater::{ExecutorBroadcast, ExecutorCurrentStats, WorkerBroadcaster};
use crate::utils::cancel_channel::CancelSender;
use log::{debug, warn};
use net_bytes::{DownloadSpeed, FileSizeFormat, SizeStandard};
use std::num::NonZeroU64;
use std::time::Duration;
use tokio::sync::broadcast;

/// 本地健康检查器配置
#[derive(Debug, Clone)]
pub(crate) struct LocalHealthCheckerConfig {
    /// 超时时间（距离上次更新超过此时间则取消）
    pub stale_timeout: Duration,
    /// 绝对速度阈值（bytes/s），低于此值且超过阈值次数则取消
    pub absolute_threshold: Option<NonZeroU64>,
    /// 文件大小单位标准（用于日志）
    pub size_standard: SizeStandard,
    /// 异常历史窗口大小
    pub history_size: usize,
    /// 异常阈值（历史窗口中异常次数达到此值则取消）
    pub anomaly_threshold: usize,
}

impl LocalHealthCheckerConfig {
    /// 从下载配置创建
    pub fn from_download_config(config: &crate::config::DownloadConfig) -> Self {
        // 超时时间：速度统计窗口的 3 倍
        let stale_timeout = config.speed().instant_speed_window() * 3;
        
        // 绝对速度阈值
        let absolute_threshold = config.health_check().absolute_speed_threshold();
        
        // 历史窗口大小：10 次
        let history_size = 10;
        // 异常阈值：历史窗口的 70%
        let anomaly_threshold = (history_size as f64 * 0.7).ceil() as usize;
        
        Self {
            stale_timeout,
            absolute_threshold,
            size_standard: config.speed().size_standard(),
            history_size,
            anomaly_threshold,
        }
    }
}

/// 异常历史追踪器
///
/// 使用滑动窗口记录异常状态，O(1) 增量更新
struct AnomalyTracker {
    /// 异常历史（环形缓冲区）
    history: Vec<bool>,
    /// 当前写入位置
    position: usize,
    /// 当前异常计数
    anomaly_count: usize,
    /// 窗口大小
    size: usize,
    /// 异常阈值
    threshold: usize,
}

impl AnomalyTracker {
    /// 创建新的追踪器
    fn new(size: usize, threshold: usize) -> Self {
        Self {
            history: vec![false; size],
            position: 0,
            anomaly_count: 0,
            size,
            threshold,
        }
    }

    /// 记录一次检查结果（O(1)）
    fn record(&mut self, is_anomaly: bool) {
        // 移除旧记录的计数
        if self.history[self.position] {
            self.anomaly_count -= 1;
        }
        // 添加新记录的计数
        if is_anomaly {
            self.anomaly_count += 1;
        }
        // 更新历史
        self.history[self.position] = is_anomaly;
        self.position = (self.position + 1) % self.size;
    }

    /// 检查是否超过异常阈值
    #[inline]
    fn exceeds_threshold(&self) -> bool {
        self.anomaly_count >= self.threshold
    }

    /// 重置追踪器
    fn reset(&mut self) {
        self.history.fill(false);
        self.position = 0;
        self.anomaly_count = 0;
    }

    /// 获取当前异常计数
    #[inline]
    fn anomaly_count(&self) -> usize {
        self.anomaly_count
    }
}

/// 本地健康检查协程
///
/// 监听 WorkerBroadcaster 的本地广播，检测超时和绝对速度
pub(crate) struct LocalHealthChecker {
    /// Worker ID（用于日志）
    worker_id: u64,
    /// 配置
    config: LocalHealthCheckerConfig,
    /// 本地广播接收器
    broadcast_rx: broadcast::Receiver<ExecutorBroadcast>,
    /// 取消信号发送器
    cancel_tx: CancelSender,
    /// 异常追踪器
    anomaly_tracker: AnomalyTracker,
    /// 是否有任务在运行
    task_running: bool,
}

impl LocalHealthChecker {
    /// 创建新的本地健康检查器
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `broadcaster`: Worker 广播器，用于订阅本地广播
    /// - `cancel_tx`: 取消信号发送器
    /// - `config`: 配置
    pub(crate) fn new(
        worker_id: u64,
        broadcaster: &WorkerBroadcaster,
        cancel_tx: CancelSender,
        config: LocalHealthCheckerConfig,
    ) -> Self {
        let broadcast_rx = broadcaster.subscribe_local();
        let anomaly_tracker = AnomalyTracker::new(config.history_size, config.anomaly_threshold);
        
        Self {
            worker_id,
            config,
            broadcast_rx,
            cancel_tx,
            anomaly_tracker,
            task_running: false,
        }
    }

    /// 运行健康检查主循环
    pub(crate) async fn run(mut self) {
        debug!("Worker #{} LocalHealthChecker 启动", self.worker_id);

        loop {
            // 根据任务状态决定是否使用超时
            let result = if self.task_running {
                // 任务运行中，使用超时监听
                tokio::time::timeout(
                    self.config.stale_timeout,
                    self.broadcast_rx.recv()
                ).await
            } else {
                // 任务未运行，无限等待
                Ok(self.broadcast_rx.recv().await)
            };

            match result {
                // 超时：stats 更新超时
                Err(_timeout) => {
                    self.handle_timeout();
                }
                // 收到消息
                Ok(Ok(msg)) => {
                    if !self.handle_broadcast(msg) {
                        break;
                    }
                }
                // Lagged：落后太多消息
                Ok(Err(broadcast::error::RecvError::Lagged(count))) => {
                    debug!("Worker #{} LocalHealthChecker 落后 {} 条消息", self.worker_id, count);
                }
                // Closed：通道关闭
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    debug!("Worker #{} LocalHealthChecker 广播通道已关闭", self.worker_id);
                    break;
                }
            }
        }

        debug!("Worker #{} LocalHealthChecker 退出", self.worker_id);
    }

    /// 处理超时
    fn handle_timeout(&mut self) {
        warn!(
            "Worker #{} stats 更新超时 (>{:?}), 取消当前任务",
            self.worker_id, self.config.stale_timeout
        );
        
        // 发送取消信号
        let handle = self.cancel_tx.get_handle();
        if handle.cancel() {
            debug!("Worker #{} 取消信号已发送 (超时)", self.worker_id);
        }
        
        // 重置状态
        self.task_running = false;
        self.anomaly_tracker.reset();
    }

    /// 处理广播消息
    ///
    /// 返回 false 表示应该退出循环
    fn handle_broadcast(&mut self, msg: ExecutorBroadcast) -> bool {
        match msg {
            ExecutorBroadcast::Stats(stats) => {
                self.handle_stats_update(&stats);
                true
            }
            ExecutorBroadcast::Shutdown => {
                debug!("Worker #{} LocalHealthChecker 收到关闭信号", self.worker_id);
                false
            }
        }
    }

    /// 处理 Stats 更新
    fn handle_stats_update(&mut self, stats: &super::stats_updater::ExecutorStats) {
        match &stats.current_stats {
            ExecutorCurrentStats::Running { instant_speed, .. } => {
                self.task_running = true;
                
                // 检查绝对速度阈值
                let is_anomaly = self.check_absolute_speed(*instant_speed);
                self.anomaly_tracker.record(is_anomaly);
                
                // 检查是否超过异常阈值
                if self.anomaly_tracker.exceeds_threshold() {
                    let anomaly_count = self.anomaly_tracker.anomaly_count();
                    warn!(
                        "Worker #{} 速度异常次数过多 ({}/{}), 取消当前任务",
                        self.worker_id, anomaly_count, self.config.history_size
                    );
                    
                    // 发送取消信号
                    let handle = self.cancel_tx.get_handle();
                    if handle.cancel() {
                        debug!("Worker #{} 取消信号已发送 (速度异常)", self.worker_id);
                    }
                    
                    // 重置追踪器
                    self.anomaly_tracker.reset();
                }
            }
            ExecutorCurrentStats::Stopped => {
                // 任务停止，重置状态
                self.task_running = false;
                self.anomaly_tracker.reset();
            }
        }
    }

    /// 检查绝对速度是否低于阈值
    ///
    /// 返回 true 表示速度异常（低于阈值）
    fn check_absolute_speed(&self, instant_speed: Option<DownloadSpeed>) -> bool {
        let threshold = match self.config.absolute_threshold {
            Some(t) => t.get(),
            None => return false, // 未配置阈值，不检查
        };

        match instant_speed {
            Some(speed) => {
                let is_slow = speed.as_u64() < threshold;
                if is_slow {
                    debug!(
                        "Worker #{} 速度 {} 低于阈值 {}",
                        self.worker_id,
                        speed.to_formatted(self.config.size_standard),
                        DownloadSpeed::from_raw(threshold).to_formatted(self.config.size_standard)
                    );
                }
                is_slow
            }
            None => {
                // 没有速度数据，视为正常（刚开始下载）
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anomaly_tracker_basic() {
        let mut tracker = AnomalyTracker::new(5, 3);
        
        // 初始状态
        assert_eq!(tracker.anomaly_count(), 0);
        assert!(!tracker.exceeds_threshold());
        
        // 记录 3 次异常
        tracker.record(true);
        tracker.record(true);
        tracker.record(true);
        
        assert_eq!(tracker.anomaly_count(), 3);
        assert!(tracker.exceeds_threshold());
    }

    #[test]
    fn test_anomaly_tracker_sliding_window() {
        let mut tracker = AnomalyTracker::new(3, 2);
        
        // 记录: [true, true, false]
        tracker.record(true);
        tracker.record(true);
        tracker.record(false);
        
        assert_eq!(tracker.anomaly_count(), 2);
        assert!(tracker.exceeds_threshold());
        
        // 滑动: [true, false, false] -> [false, false]
        tracker.record(false);
        assert_eq!(tracker.anomaly_count(), 1);
        assert!(!tracker.exceeds_threshold());
    }

    #[test]
    fn test_anomaly_tracker_reset() {
        let mut tracker = AnomalyTracker::new(5, 3);
        
        tracker.record(true);
        tracker.record(true);
        tracker.record(true);
        
        assert_eq!(tracker.anomaly_count(), 3);
        
        tracker.reset();
        
        assert_eq!(tracker.anomaly_count(), 0);
        assert!(!tracker.exceeds_threshold());
    }
}
