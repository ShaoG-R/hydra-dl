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

use super::executor::state::TaskState;
use super::stats_updater::{ExecutorBroadcast, ExecutorStats, WorkerBroadcaster};
use crate::config::health_check::anomaly_checker::AnomalyChecker;
use crate::config::health_check::{AbsoluteSpeedConfig, RelativeSpeedConfig};
use crate::download::download_stats::AggregatedStats;
use crate::pool::common::WorkerId;
use crate::utils::cancel_channel::{CancelHandle, CancelSender};
use log::{debug, warn};
use net_bytes::{DownloadSpeed, FileSizeFormat, SizeStandard};
use smr_swap::LocalReader;
use std::time::Duration;
use tokio::sync::watch;

/// 本地健康检查器配置
#[derive(Debug, Clone)]
pub(crate) struct LocalHealthCheckerConfig {
    /// 超时时间（距离上次更新超过此时间则取消）
    pub stale_timeout: Duration,
    /// 绝对速度检测配置
    pub absolute: AbsoluteSpeedConfig,
    /// 相对速度检测配置
    pub relative: RelativeSpeedConfig,
    /// 文件大小单位标准（用于日志）
    pub size_standard: SizeStandard,
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
            absolute: health_check.absolute().clone(),
            relative: health_check.relative().clone(),
            size_standard: config.speed().size_standard(),
        }
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
    /// 绝对速度异常检测器
    absolute_checker: AnomalyChecker,
    /// 相对速度异常检测器
    relative_checker: AnomalyChecker,
    /// 聚合统计读取器
    aggregated_stats: LocalReader<AggregatedStats>,
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
    /// - `aggregated_stats`: 聚合统计读取器
    pub(crate) fn new(
        worker_id: WorkerId,
        broadcaster: &WorkerBroadcaster,
        cancel_tx: CancelSender,
        config: LocalHealthCheckerConfig,
        aggregated_stats: LocalReader<AggregatedStats>,
    ) -> Self {
        let watch_rx = broadcaster.subscribe_local();
        let absolute_checker = AnomalyChecker::new(
            config.absolute.window.history_size,
            config.absolute.window.anomaly_threshold,
        );
        let relative_checker = AnomalyChecker::new(
            config.relative.window.history_size,
            config.relative.window.anomaly_threshold,
        );

        Self {
            worker_id,
            config,
            watch_rx,
            cancel_tx,
            cancel_handle: None,
            absolute_checker,
            relative_checker,
            aggregated_stats,
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
                    debug!(
                        "Worker {} LocalHealthChecker watch 通道已关闭",
                        self.worker_id
                    );
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
        if let Some(handle) = self.cancel_handle.take()
            && handle.cancel()
        {
            debug!("Worker {} 取消信号已发送 (超时)", self.worker_id);
        }

        // 重置检测器
        self.absolute_checker.reset();
        self.relative_checker.reset();
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
            ExecutorStats::Pending(_) => {
                // 待命状态，无需处理
            }
            ExecutorStats::Running(stats) => {
                match &stats.state {
                    TaskState::Started { .. } => {
                        // 任务刚启动，获取取消句柄
                        if self.cancel_handle.is_none() {
                            self.cancel_handle = Some(self.cancel_tx.get_handle());
                        }
                    }
                    TaskState::Running { .. } => {
                        // 任务运行中，检查速度
                        if self.cancel_handle.is_none() {
                            self.cancel_handle = Some(self.cancel_tx.get_handle());
                        }

                        if let Some(speed_stats) = stats.get_speed_stats() {
                            // 检查绝对速度
                            let is_absolute_slow =
                                self.check_absolute_speed(speed_stats.instant_speed);
                            self.absolute_checker.record(is_absolute_slow);

                            if self.absolute_checker.exceeds_threshold() {
                                let anomaly_count = self.absolute_checker.anomaly_count();
                                warn!(
                                    "Worker {} 绝对速度异常次数过多 ({}/{}), 取消当前任务",
                                    self.worker_id,
                                    anomaly_count,
                                    self.config.absolute.window.history_size
                                );
                                self.trigger_cancel("绝对速度异常");
                                return;
                            }

                            // 检查相对速度
                            let is_relative_slow = self.check_relative_speed();
                            self.relative_checker.record(is_relative_slow);

                            if self.relative_checker.exceeds_threshold() {
                                let anomaly_count = self.relative_checker.anomaly_count();
                                warn!(
                                    "Worker {} 相对速度异常次数过多 ({}/{}), 取消当前任务",
                                    self.worker_id,
                                    anomaly_count,
                                    self.config.relative.window.history_size
                                );
                                self.trigger_cancel("相对速度异常");
                                return;
                            }
                        }
                    }
                    TaskState::Ended { .. } => {
                        // 任务结束，重置状态
                        self.cancel_handle = None;
                        self.absolute_checker.reset();
                        self.relative_checker.reset();
                    }
                }
            }
            ExecutorStats::Stopped(_) => {
                // Executor 停止，重置状态
                self.cancel_handle = None;
                self.absolute_checker.reset();
                self.relative_checker.reset();
            }
        }
    }

    /// 触发取消并重置状态
    fn trigger_cancel(&mut self, reason: &str) {
        // 使用之前获取的句柄发送取消信号
        if let Some(handle) = self.cancel_handle.take()
            && handle.cancel()
        {
            debug!("Worker {} 取消信号已发送 ({})", self.worker_id, reason);
        }

        // 重置状态
        self.cancel_handle = None;
        self.absolute_checker.reset();
        self.relative_checker.reset();
    }

    /// 检查绝对速度是否低于阈值
    ///
    /// 返回 true 表示速度异常（低于阈值）
    fn check_absolute_speed(&self, instant_speed: DownloadSpeed) -> bool {
        let threshold = match self.config.absolute.threshold {
            Some(t) => t.get(),
            None => return false, // 未配置阈值，不检查
        };

        let is_slow = instant_speed.as_u64() < threshold;
        if is_slow {
            debug!(
                "Worker {} 速度 {} 低于绝对阈值 {}",
                self.worker_id,
                instant_speed.to_formatted(self.config.size_standard),
                DownloadSpeed::from_raw(threshold).to_formatted(self.config.size_standard)
            );
        }
        is_slow
    }

    /// 检查相对速度是否低于阈值
    ///
    /// 检查当前线程窗口平均速度是否低于 (窗口平均速度 / 下载线程数) * 0.005
    fn check_relative_speed(&self) -> bool {
        let guard = self.aggregated_stats.load();

        let total_window_avg = guard
            .get_summary()
            .map(|s| s.window_avg_speed.as_u64())
            .unwrap_or(0);
        let count = guard.running_count() as u64;

        if count == 0 {
            return false;
        }

        let avg_per_thread = total_window_avg / count;
        // 0.5% = 5 / 1000
        let threshold = avg_per_thread * 5 / 1000;

        // 获取当前线程的窗口平均速度
        let current_window_avg = match guard.get_running(self.worker_id.pool_id()) {
            Some(stats) => stats
                .get_window_avg_speed()
                .map(|s| s.as_u64())
                .unwrap_or(0),
            None => return false,
        };

        let is_slow = current_window_avg < threshold;
        if is_slow {
            debug!(
                "Worker {} 窗口平均速度 {} 低于相对阈值 {} (总平均: {}, 线程数: {})",
                self.worker_id,
                DownloadSpeed::from_raw(current_window_avg).to_formatted(self.config.size_standard),
                DownloadSpeed::from_raw(threshold).to_formatted(self.config.size_standard),
                DownloadSpeed::from_raw(avg_per_thread).to_formatted(self.config.size_standard),
                count
            );
        }
        is_slow
    }
}
