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

use super::stats_updater::{ExecutorBroadcast, ExecutorStats, TaskStats, WorkerBroadcaster};
use crate::pool::common::WorkerId;
use crate::utils::cancel_channel::{CancelHandle, CancelSender};
use log::{debug, warn};
use net_bytes::{DownloadSpeed, FileSizeFormat, SizeStandard};
use std::num::NonZeroU64;
use std::time::Duration;
use tokio::sync::watch;

/// 本地健康检查器配置
#[derive(Debug, Clone)]
pub(crate) struct LocalHealthCheckerConfig {
    /// 超时时间（距离上次更新超过此时间则取消）
    pub stale_timeout: Duration,
    /// 绝对速度阈值（bytes/s），低于此值且超过阈值次数则取消
    pub absolute_threshold: Option<NonZeroU64>,
    /// 文件大小单位标准（用于日志）
    pub size_standard: SizeStandard,
    /// 异常历史窗口大小（来自 HealthCheckConfig）
    pub history_size: usize,
    /// 异常阈值（来自 HealthCheckConfig）
    pub anomaly_threshold: usize,
}

impl LocalHealthCheckerConfig {
    /// 从下载配置创建
    pub fn from_download_config(config: &crate::config::DownloadConfig) -> Self {
        let health_check = config.health_check();
        // 超时时间：速度统计窗口 × 倍数
        let stale_timeout =
            config.speed().instant_speed_window() * health_check.stale_timeout_multiplier();

        Self {
            stale_timeout,
            absolute_threshold: health_check.absolute_speed_threshold(),
            size_standard: config.speed().size_standard(),
            history_size: health_check.history_size(),
            anomaly_threshold: health_check.anomaly_threshold(),
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
/// 监听 WorkerBroadcaster 的本地 watch，检测超时和绝对速度
pub(crate) struct LocalHealthChecker {
    /// Worker ID（用于日志）
    worker_id: WorkerId,
    /// 配置
    config: LocalHealthCheckerConfig,
    /// 本地 watch 接收器
    watch_rx: watch::Receiver<ExecutorBroadcast>,
    /// 取消信号发送器
    cancel_tx: CancelSender,
    /// 当前任务的取消句柄（在任务开始时获取，Some 表示任务运行中）
    cancel_handle: Option<CancelHandle>,
    /// 异常追踪器
    anomaly_tracker: AnomalyTracker,
}

impl LocalHealthChecker {
    /// 创建新的本地健康检查器
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `broadcaster`: Worker 广播器，用于订阅本地 watch
    /// - `cancel_tx`: 取消信号发送器
    /// - `config`: 配置
    pub(crate) fn new(
        worker_id: WorkerId,
        broadcaster: &WorkerBroadcaster,
        cancel_tx: CancelSender,
        config: LocalHealthCheckerConfig,
    ) -> Self {
        let watch_rx = broadcaster.subscribe_local();
        let anomaly_tracker = AnomalyTracker::new(config.history_size, config.anomaly_threshold);
        
        Self {
            worker_id,
            config,
            watch_rx,
            cancel_tx,
            cancel_handle: None,
            anomaly_tracker,
        }
    }

    /// 运行健康检查主循环
    pub(crate) async fn run(mut self) {
        debug!("Worker {} LocalHealthChecker 启动", self.worker_id);

        loop {
            // 根据任务状态决定是否使用超时
            let result = if self.cancel_handle.is_some() {
                // 任务运行中，使用超时监听
                tokio::time::timeout(self.config.stale_timeout, self.watch_rx.changed()).await
            } else {
                // 任务未运行，无限等待
                Ok(self.watch_rx.changed().await)
            };

            match result {
                // 超时：stats 更新超时
                Err(_timeout) => {
                    self.handle_timeout();
                }
                // watch 值已更新
                Ok(Ok(())) => {
                    let msg = self.watch_rx.borrow().clone();
                    if !self.handle_broadcast(msg) {
                        break;
                    }
                }
                // watch 通道关闭
                Ok(Err(_)) => {
                    debug!("Worker {} LocalHealthChecker watch 通道已关闭", self.worker_id);
                    break;
                }
            }
        }

        debug!("Worker {} LocalHealthChecker 退出", self.worker_id);
    }

    /// 处理超时
    fn handle_timeout(&mut self) {
        warn!(
            "Worker {} stats 更新超时 (>{:?}), 取消当前任务",
            self.worker_id, self.config.stale_timeout
        );
        
        // 使用之前获取的句柄发送取消信号（take 已将 cancel_handle 置为 None）
        if let Some(handle) = self.cancel_handle.take() {
            if handle.cancel() {
                debug!("Worker {} 取消信号已发送 (超时)", self.worker_id);
            }
        }

        // 重置异常追踪器
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
                debug!("Worker {} LocalHealthChecker 收到关闭信号", self.worker_id);
                false
            }
        }
    }

    /// 处理 Stats 更新
    fn handle_stats_update(&mut self, stats: &ExecutorStats) {
        match stats {
            ExecutorStats::Pending => {
                // 待命状态，无需处理
            }
            ExecutorStats::Running(TaskStats::Started { .. }) => {
                // 任务刚启动，获取取消句柄
                if self.cancel_handle.is_none() {
                    self.cancel_handle = Some(self.cancel_tx.get_handle());
                }
            }
            ExecutorStats::Running(TaskStats::Running { data, .. }) => {
                // 任务运行中，检查速度
                if self.cancel_handle.is_none() {
                    self.cancel_handle = Some(self.cancel_tx.get_handle());
                }

                // 检查绝对速度阈值
                let is_anomaly = self.check_absolute_speed(data.get_instant_speed());
                self.anomaly_tracker.record(is_anomaly);

                // 检查是否超过异常阈值
                if self.anomaly_tracker.exceeds_threshold() {
                    let anomaly_count = self.anomaly_tracker.anomaly_count();
                    warn!(
                        "Worker {} 速度异常次数过多 ({}/{}), 取消当前任务",
                        self.worker_id, anomaly_count, self.config.history_size
                    );

                    // 使用之前获取的句柄发送取消信号
                    if let Some(handle) = self.cancel_handle.take() {
                        if handle.cancel() {
                            debug!("Worker {} 取消信号已发送 (速度异常)", self.worker_id);
                        }
                    }

                    // 重置状态
                    self.cancel_handle = None;
                    self.anomaly_tracker.reset();
                }
            }
            ExecutorStats::Running(TaskStats::Ended { .. }) | ExecutorStats::Stopped(_) => {
                // 任务结束或 Executor 停止，重置状态
                self.cancel_handle = None;
                self.anomaly_tracker.reset();
            }
        }
    }

    /// 检查绝对速度是否低于阈值
    ///
    /// 返回 true 表示速度异常（低于阈值）
    fn check_absolute_speed(&self, instant_speed: DownloadSpeed) -> bool {
        let threshold = match self.config.absolute_threshold {
            Some(t) => t.get(),
            None => return false, // 未配置阈值，不检查
        };

        let is_slow = instant_speed.as_u64() < threshold;
        if is_slow {
            debug!(
                "Worker {} 速度 {} 低于阈值 {}",
                self.worker_id,
                instant_speed.to_formatted(self.config.size_standard),
                DownloadSpeed::from_raw(threshold).to_formatted(self.config.size_standard)
            );
        }
        is_slow
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
