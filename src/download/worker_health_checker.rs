//! Worker 健康检查模块（Actor 模式）
//!
//! 监听 broadcast channel 接收 Executor 统计更新，通过相对速度比较（max gap 算法）
//! 来检测相对于其他 Worker 显著过慢的 Worker。
//!
//! # 设计说明
//!
//! 与 LocalHealthChecker 的分工：
//! - LocalHealthChecker：超时检测、绝对速度检测（每个 Worker 独立）
//! - WorkerHealthChecker：相对速度检测（max gap 算法，需要所有 Workers 数据）
//!
//! # 工作流程
//!
//! - 订阅 broadcast channel 接收 `TaggedBroadcast` 消息
//! - 为每个 Executor 维护健康历史记录（过去 n 次更新的相对速度异常状态）
//! - 异常判定：速度相对于其他 Executor 显著过慢
//! - 取消条件：相对速度异常次数超过阈值

use crate::config::DownloadConfig;
use crate::pool::download::{DownloadWorkerHandle, ExecutorBroadcast, ExecutorStats, TaggedBroadcast};
use lite_sync::oneshot::lite;
use log::{debug, warn};
use net_bytes::{DownloadSpeed, FileSizeFormat, SizeStandard};
use rustc_hash::FxHashMap;
use smr_swap::LocalReader;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::broadcast;

/// 速度类型（字节/秒）
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Speed(u64);

impl From<u64> for Speed {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Deref for Speed {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Speed {
    fn to_formatted(&self, size_standard: SizeStandard) -> String {
        DownloadSpeed::from_raw(self.0)
            .to_formatted(size_standard)
            .to_string()
    }
}

/// Worker 速度信息
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkerSpeed {
    /// Worker ID
    pub worker_id: u64,
    /// 速度（字节/秒）
    pub speed: Speed,
}

/// 健康检查结果
#[derive(Debug, Clone, PartialEq)]
pub struct HealthCheckResult {
    /// 需要终止的 worker 列表（worker_id, speed）
    pub unhealthy_workers: Vec<(u64, Speed)>,
    /// 健康基准速度（字节/秒）
    pub health_baseline: Speed,
    /// 最大间隙值
    pub max_gap: Speed,
}

/// 单个 Executor 的健康追踪器
///
/// 记录 Executor 过去 n 次更新的相对速度异常状态，用于判断是否需要取消下载
#[derive(Debug)]
struct ExecutorHealthTracker {
    /// 最近 n 次更新的异常状态（true = 异常）
    anomaly_history: VecDeque<bool>,
    /// 历史窗口大小
    history_size: usize,
    /// 当前异常计数（增量维护，避免每次遍历）
    anomaly_count: usize,
}

impl ExecutorHealthTracker {
    /// 创建新的健康追踪器
    fn new(history_size: usize) -> Self {
        Self {
            anomaly_history: VecDeque::with_capacity(history_size),
            history_size,
            anomaly_count: 0,
        }
    }

    /// 记录一次更新的异常状态（O(1) 增量更新）
    fn record(&mut self, is_anomaly: bool) {
        // 移除最旧记录时更新计数
        if self.anomaly_history.len() >= self.history_size {
            if self.anomaly_history.pop_front() == Some(true) {
                self.anomaly_count -= 1;
            }
        }
        // 添加新记录时更新计数
        if is_anomaly {
            self.anomaly_count += 1;
        }
        self.anomaly_history.push_back(is_anomaly);
    }

    /// 获取异常次数（O(1)）
    #[inline]
    fn anomaly_count(&self) -> usize {
        self.anomaly_count
    }

    /// 检查是否超过异常阈值
    #[inline]
    fn exceeds_anomaly_threshold(&self, threshold: usize) -> bool {
        self.anomaly_count >= threshold
    }

    /// 重置追踪器
    fn reset(&mut self) {
        self.anomaly_history.clear();
        self.anomaly_count = 0;
    }
}

/// 健康检查器参数
pub(super) struct WorkerHealthCheckerParams {
    /// 配置
    pub config: Arc<DownloadConfig>,
    /// 共享的 worker handles（用于发送取消信号）
    pub worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle>>,
    /// 广播接收器
    pub broadcast_rx: broadcast::Receiver<TaggedBroadcast>,
}

/// Worker 健康检查器（内部逻辑）
///
/// 使用最大间隙检测算法来识别速度异常的 worker：
/// 1. 收集所有 worker 的窗口平均速度并排序
/// 2. 计算相邻元素的间隙
/// 3. 找到最大间隙作为分界线，将 workers 分为慢速簇和快速簇
/// 4. 以快速簇的最小值作为健康基准
/// 5. 同时应用绝对速度阈值，确保不会错误地终止健康的 worker
///
/// # 算法示例
///
/// ```text
/// 速度列表: [10, 30, 5120, 6144] KB/s
/// 间隙: [20, 5090, 1024]
/// 最大间隙: 5090 (在 30 和 5120 之间)
/// 慢速簇: [10, 30]
/// 快速簇: [5120, 6144]
/// 健康基准: 5120 KB/s (快速簇的最小值)
/// ```
struct WorkerHealthCheckerLogic {
    /// 绝对速度阈值（字节/秒）
    /// 低于此阈值的 worker 可能被标记为不健康
    absolute_threshold: Option<Speed>,
    /// 相对速度比例
    /// worker 速度低于健康基准的此比例时被视为显著慢速
    relative_threshold: f64,
    /// 文件大小单位标准
    size_standard: SizeStandard,
}

impl WorkerHealthCheckerLogic {
    /// 创建新的健康检查逻辑
    pub fn new(
        absolute_threshold: Option<Speed>,
        relative_threshold: f64,
        size_standard: SizeStandard,
    ) -> Self {
        assert!(
            relative_threshold > 0.0 && relative_threshold <= 1.0,
            "relative_threshold must be between 0 and 1"
        );
        Self {
            absolute_threshold,
            relative_threshold,
            size_standard,
        }
    }

    /// 执行健康检查（内部方法）
    fn check(&self, worker_speeds: &[WorkerSpeed]) -> Option<HealthCheckResult> {
        // 至少需要 2 个 worker 才能进行比较
        if worker_speeds.len() < 2 {
            return None;
        }

        // 使用索引数组排序，避免克隆整个 WorkerSpeed 数据
        let mut indices: Vec<usize> = (0..worker_speeds.len()).collect();
        indices.sort_unstable_by_key(|&i| worker_speeds[i].speed);

        let (max_gap_idx, gap_value) = self.find_max_gap(&indices, worker_speeds)?;

        // 分界点：最大间隙之后的第一个元素
        let split_idx = max_gap_idx + 1;

        // 如果快速簇为空（所有 worker 速度相近），则提前返回
        if split_idx >= indices.len() {
            return None;
        }

        // 健康基准：快速簇的最小值
        let health_baseline = worker_speeds[indices[split_idx]].speed;

        debug!(
            "健康检查: 最大间隙={} (索引 {}-{}), 基准={}{}",
            gap_value.to_formatted(self.size_standard),
            max_gap_idx,
            split_idx,
            health_baseline.to_formatted(self.size_standard),
            self.absolute_threshold
                .map(|t| format!(", 阈值={}", t.to_formatted(self.size_standard)))
                .unwrap_or_default()
        );

        // 收集需要终止的 worker（慢速簇）
        let unhealthy_workers =
            self.identify_unhealthy_workers(&indices[..split_idx], worker_speeds, health_baseline);

        if unhealthy_workers.is_empty() {
            return None;
        }

        Some(HealthCheckResult {
            unhealthy_workers,
            health_baseline,
            max_gap: gap_value,
        })
    }

    /// 查找最大间隙（一次遍历 O(n)）
    #[inline]
    fn find_max_gap(
        &self,
        indices: &[usize],
        worker_speeds: &[WorkerSpeed],
    ) -> Option<(usize, Speed)> {
        if indices.len() < 2 {
            return None;
        }

        let (max_idx, max_gap) = (0..indices.len() - 1)
            .map(|i| {
                let gap = worker_speeds[indices[i + 1]]
                    .speed
                    .saturating_sub(*worker_speeds[indices[i]].speed);
                (i, gap)
            })
            .max_by_key(|&(_, gap)| gap)?;

        // 间隙为 0 表示所有速度相同
        (max_gap > 0).then(|| (max_idx, Speed(max_gap)))
    }

    /// 识别不健康的 worker
    ///
    /// worker 同时满足以下条件才被标记为不健康：
    /// 1. 速度低于绝对阈值（如果设置）
    /// 2. 速度明显低于健康基准（例如：低于 50%）
    #[inline]
    fn identify_unhealthy_workers(
        &self,
        slow_indices: &[usize],
        worker_speeds: &[WorkerSpeed],
        health_baseline: Speed,
    ) -> Vec<(u64, Speed)> {
        // 使用整数计算避免浮点运算: 0.5 -> 50/100
        let threshold_percent = (self.relative_threshold * 100.0) as u64;
        let threshold_speed = health_baseline.saturating_mul(threshold_percent) / 100;
        let abs_threshold = self.absolute_threshold;

        slow_indices
            .iter()
            .map(|&idx| &worker_speeds[idx])
            .filter(|w| {
                *w.speed < threshold_speed && abs_threshold.map_or(true, |t| w.speed < t)
            })
            .map(|w| (w.worker_id, w.speed))
            .collect()
    }
}

/// 健康检查 Actor
///
/// 独立运行的 actor，监听 broadcast channel 接收 Executor 统计更新，
/// 通过相对速度比较（max gap 算法）检测显著过慢的 Worker。
struct WorkerHealthCheckerActor {
    /// 内部逻辑管理器（用于判断相对速度是否异常）
    logic: WorkerHealthCheckerLogic,
    /// 配置
    config: Arc<DownloadConfig>,
    /// 关闭信号接收器
    shutdown_rx: lite::Receiver<()>,
    /// 共享的 worker handles（用于发送取消信号）
    worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle>>,
    /// 广播接收器
    broadcast_rx: broadcast::Receiver<TaggedBroadcast>,
    /// 每个 Executor 的健康追踪器（相对速度异常）
    health_trackers: FxHashMap<u64, ExecutorHealthTracker>,
    /// 当前所有 Executor 的最新统计（用于计算相对速度）
    current_stats: FxHashMap<u64, ExecutorStats>,
    /// 异常历史窗口大小
    history_size: usize,
    /// 异常阈值（超过此次数则取消）
    anomaly_threshold: usize,
}

impl WorkerHealthCheckerActor {
    /// 创建新的 actor
    fn new(params: WorkerHealthCheckerParams, shutdown_rx: lite::Receiver<()>) -> Self {
        let WorkerHealthCheckerParams {
            config,
            worker_handles,
            broadcast_rx,
        } = params;

        let health_check = config.health_check();

        // 绝对速度阈值仍然用于相对速度检测的安全过滤
        // (不会取消绝对速度达标但相对较慢的 worker)
        let absolute_threshold = health_check
            .absolute_speed_threshold()
            .map(|v| Speed(v.get()));
        let logic = WorkerHealthCheckerLogic::new(
            absolute_threshold,
            health_check.relative_threshold(),
            config.speed().size_standard(),
        );

        // 从配置获取健康检查参数
        let history_size = health_check.history_size();
        let anomaly_threshold = health_check.anomaly_threshold();

        debug!(
            "WorkerHealthChecker 启动: history_size={}, anomaly_threshold={}",
            history_size, anomaly_threshold
        );

        Self {
            logic,
            config,
            shutdown_rx,
            worker_handles,
            broadcast_rx,
            health_trackers: FxHashMap::default(),
            current_stats: FxHashMap::default(),
            history_size,
            anomaly_threshold,
        }
    }

    /// 运行 actor 事件循环
    ///
    /// 监听 broadcast channel 接收统计更新
    async fn run(mut self) {
        debug!("WorkerHealthCheckerActor started");

        loop {
            tokio::select! {
                // 监听广播消息
                result = self.broadcast_rx.recv() => {
                    match result {
                        Ok(msg) => {
                            self.handle_broadcast(msg);
                        }
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            debug!("WorkerHealthChecker 落后 {} 条消息", count);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            debug!("WorkerHealthChecker 广播通道已关闭");
                            break;
                        }
                    }
                }
                // 外部关闭信号
                _ = &mut self.shutdown_rx => {
                    debug!("WorkerHealthCheckerActor shutting down");
                    break;
                }
            }
        }

        debug!("WorkerHealthCheckerActor stopped");
    }

    /// 处理广播消息
    fn handle_broadcast(&mut self, msg: TaggedBroadcast) {
        // 检查是否启用健康检查
        if !self.config.health_check().enabled() {
            return;
        }

        let (worker_id, broadcast) = msg;
        match broadcast {
            ExecutorBroadcast::Stats(stats) => {
                self.handle_stats_update(worker_id, stats);
            }
            ExecutorBroadcast::Shutdown => {
                self.handle_executor_shutdown(worker_id);
            }
        }
    }

    /// 处理统计更新
    fn handle_stats_update(&mut self, worker_id: u64, stats: ExecutorStats) {
        // 更新当前统计
        self.current_stats.insert(worker_id, stats);

        // 判断当前 Executor 是否相对速度异常（基于 max gap 算法）
        let is_anomaly = self.check_if_anomaly(worker_id);

        // 确保有健康追踪器并记录异常状态
        let history_size = self.history_size;
        let tracker = self
            .health_trackers
            .entry(worker_id)
            .or_insert_with(|| ExecutorHealthTracker::new(history_size));
        tracker.record(is_anomaly);

        // 检查是否需要取消（相对速度异常次数过多）
        if let Some(tracker) = self.health_trackers.get(&worker_id) {
            if tracker.exceeds_anomaly_threshold(self.anomaly_threshold) {
                let anomaly_count = tracker.anomaly_count();
                warn!(
                    "Worker #{} 相对速度异常次数过多 ({}/{}), 取消当前下载",
                    worker_id, anomaly_count, self.history_size
                );
                self.cancel_worker(worker_id, format!(
                    "相对速度异常 ({}/{})",
                    anomaly_count, self.history_size
                ));
                
                // 重置该 worker 的健康追踪器
                self.reset_worker_tracking(worker_id);
            }
        }
    }

    /// 重置 worker 的追踪器
    fn reset_worker_tracking(&mut self, worker_id: u64) {
        if let Some(tracker) = self.health_trackers.get_mut(&worker_id) {
            tracker.reset();
        }
    }

    /// 处理 Executor 关闭
    fn handle_executor_shutdown(&mut self, worker_id: u64) {
        // 清理追踪数据
        self.health_trackers.remove(&worker_id);
        self.current_stats.remove(&worker_id);
        debug!("Worker #{} 已关闭，移除健康追踪", worker_id);
    }

    /// 判断指定 Executor 是否异常
    ///
    /// 使用最大间隙检测算法，判断该 Executor 是否在慢速簇中
    fn check_if_anomaly(&self, target_worker_id: u64) -> bool {
        // 收集所有活跃 Executor 的速度
        let worker_speeds: Vec<WorkerSpeed> = self
            .current_stats
            .iter()
            .filter_map(|(&worker_id, stats)| {
                stats.get_instant_speed().map(|speed| WorkerSpeed {
                    worker_id,
                    speed: Speed(speed.as_u64()),
                })
            })
            .collect();

        // 使用现有逻辑检查
        if let Some(result) = self.logic.check(&worker_speeds) {
            // 检查目标 worker 是否在不健康列表中
            result
                .unhealthy_workers
                .iter()
                .any(|(id, _)| *id == target_worker_id)
        } else {
            false
        }
    }

    /// 取消指定 worker 的当前下载
    fn cancel_worker(&self, worker_id: u64, reason: String) {
        let handles = self.worker_handles.load();
        if let Some(handle) = handles.get(&worker_id) {
            if handle.cancel_handle().cancel() {
                debug!("Worker #{} 已取消: {}", worker_id, reason);
            } else {
                warn!("Worker #{} 发送取消信号失败 (task_id 不匹配或 channel 已关闭)", worker_id);
            }
        }
    }
}

/// Worker 健康检查器 Handle
///
/// 提供与 WorkerHealthCheckerActor 通信的接口
pub(super) struct WorkerHealthChecker {
    /// 关闭信号发送器
    shutdown_tx: lite::Sender<()>,
    /// Actor 任务句柄
    actor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WorkerHealthChecker {
    /// 创建新的健康检查器（启动 actor）
    pub(super) fn new(params: WorkerHealthCheckerParams) -> Self {
        let (shutdown_tx, shutdown_rx) = lite::channel();

        // 启动 actor 任务
        let actor_handle = tokio::spawn(async move {
            WorkerHealthCheckerActor::new(params, shutdown_rx).run().await;
        });

        Self {
            shutdown_tx,
            actor_handle: Some(actor_handle),
        }
    }

    /// 关闭 actor 并等待其完全停止
    pub(super) async fn shutdown_and_wait(mut self) {
        // 发送关闭消息
        let _ = self.shutdown_tx.notify(());

        // 等待 actor 任务完成
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
            debug!("WorkerHealthChecker actor has fully stopped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_speed(value: u64) -> Speed {
        Speed::from(value)
    }

    #[test]
    fn test_normal_workers_no_unhealthy() {
        // 所有 worker 速度相近
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(10240)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(5000),
            },
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(5100),
            },
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(5200),
            },
        ];

        let result = checker.check(&speeds);
        assert!(result.is_none());
    }

    #[test]
    fn test_one_slow_worker_detected() {
        // 一个 worker 明显慢于其他
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(1024 * 10)), 0.5, SizeStandard::IEC); // 10 KB/s threshold
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(5120),
            }, // 5 KB/s - slow
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(512000),
            }, // 500 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(614400),
            }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert_eq!(*result.health_baseline, 512000);
    }

    #[test]
    fn test_multiple_slow_workers() {
        // 多个 worker 慢速
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(20480)), 0.5, SizeStandard::IEC); // 20 KB/s threshold
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(10240),
            }, // 10 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(15360),
            }, // 15 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(512000),
            }, // 500 KB/s
            WorkerSpeed {
                worker_id: 3,
                speed: create_speed(614400),
            }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 2);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert_eq!(result.unhealthy_workers[1].0, 1);
    }

    #[test]
    fn test_slow_but_above_absolute_threshold() {
        // worker 相对慢但高于绝对阈值
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(1024 * 100)), 0.5, SizeStandard::IEC); // 100 KB/s threshold
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(150000),
            }, // 146 KB/s - slow but above threshold
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(512000),
            }, // 500 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(614400),
            }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        // 应该没有不健康的 worker，因为都高于绝对阈值
        assert!(result.is_none());
    }

    #[test]
    fn test_below_absolute_but_not_significantly_slow() {
        // worker 低于绝对阈值但不显著慢于健康基准
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(250000)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(200000),
            }, // 195 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(300000),
            }, // 293 KB/s (baseline)
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(350000),
            }, // 342 KB/s
        ];

        let result = checker.check(&speeds);
        // worker 0 低于绝对阈值，但是 200000 > 300000 * 0.5
        // 所以不会被标记为不健康
        assert!(result.is_none());
    }

    #[test]
    fn test_insufficient_workers() {
        // 只有一个 worker
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(10240)), 0.5, SizeStandard::IEC);
        let speeds = vec![WorkerSpeed {
            worker_id: 0,
            speed: create_speed(5120),
        }];

        let result = checker.check(&speeds);
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_workers() {
        // 空列表
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(10240)), 0.5, SizeStandard::IEC);
        let speeds = vec![];

        let result = checker.check(&speeds);
        assert!(result.is_none());
    }

    #[test]
    fn test_typical_scenario() {
        // 典型场景：10 KB/s、30 KB/s 慢速，5 MB/s、6 MB/s 正常
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(1024 * 40)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(10240),
            }, // 10 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(30720),
            }, // 30 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(5242880),
            }, // 5 MB/s
            WorkerSpeed {
                worker_id: 3,
                speed: create_speed(6291456),
            }, // 6 MB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        // 两个慢速 worker 都应该被检测到
        assert_eq!(result.unhealthy_workers.len(), 2);

        // 验证间隙是在 30 KB/s 和 5 MB/s 之间
        assert_eq!(*result.max_gap, 5242880 - 30720);

        // 验证健康基准是 5 MB/s
        assert_eq!(*result.health_baseline, 5242880);
    }

    #[test]
    fn test_all_workers_slow() {
        // 所有 worker 都很慢
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(10240)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(1024),
            }, // 1 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(2048),
            }, // 2 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(3072),
            }, // 3 KB/s
        ];

        let result = checker.check(&speeds);
        // 因为没有明显的间隙分界（最快的簇为空或所有都慢），应该没有检测到
        // 或者如果有间隙，快速簇的基准也会很低，不会触发
        // 根据算法，最大间隙在 2KB 和 3KB 之间（1024），快速簇是 [3KB]
        // 慢速簇 [1KB, 2KB]，它们都低于 3KB * 0.5 = 1.5KB 吗？
        // 1KB < 10KB && 1KB < 1.5KB ✓
        // 2KB < 10KB && 2KB < 1.5KB ✗
        // 所以只有 worker 0 会被检测到
        if let Some(result) = result {
            assert_eq!(result.unhealthy_workers.len(), 1);
            assert_eq!(result.unhealthy_workers[0].0, 0);
        }
    }

    #[test]
    fn test_custom_relative_threshold() {
        // 测试不同的相对阈值
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(1024 * 100)), 0.8, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(50000),
            }, // 49 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(500000),
            }, // 488 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(600000),
            }, // 586 KB/s
        ];

        let result = checker.check(&speeds);
        // 50KB < 100KB ✓
        // 50KB < 500KB * 0.8 = 400KB ✓
        // 应该检测到 worker 0
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
    }

    #[test]
    fn test_zero_speed_worker() {
        // worker 速度为 0
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(10240)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(0),
            }, // 0 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(512000),
            }, // 500 KB/s
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(614400),
            }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert_eq!(*result.unhealthy_workers[0].1, 0);
    }

    #[test]
    fn test_large_number_of_workers() {
        // 测试大量 worker 的性能
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(50000)), 0.5, SizeStandard::IEC);
        let mut speeds = Vec::new();

        // 10 个慢速 worker
        for i in 0..10 {
            speeds.push(WorkerSpeed {
                worker_id: i,
                speed: create_speed(10000 + i * 1000),
            });
        }

        // 90 个快速 worker
        for i in 10..100 {
            speeds.push(WorkerSpeed {
                worker_id: i as u64,
                speed: create_speed(500000 + (i * 1000) as u64),
            });
        }

        let result = checker.check(&speeds);
        assert!(result.is_some());
        let result = result.unwrap();

        // 所有慢速 worker 都应该被检测到
        assert_eq!(result.unhealthy_workers.len(), 10);
    }

    #[test]
    fn test_unordered_input() {
        // 测试输入顺序不影响结果
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(20480)), 0.5, SizeStandard::IEC);

        // 乱序输入
        let speeds = vec![
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(512000),
            }, // 500 KB/s
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(10240),
            }, // 10 KB/s
            WorkerSpeed {
                worker_id: 3,
                speed: create_speed(614400),
            }, // 600 KB/s
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(15360),
            }, // 15 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 2);

        // 验证检测到的是 worker 0 和 1
        let mut worker_ids: Vec<u64> = result.unhealthy_workers.iter().map(|&(id, _)| id).collect();
        worker_ids.sort();
        assert_eq!(worker_ids, vec![0, 1]);
    }

    #[test]
    fn test_identical_speeds() {
        // 所有 worker 速度完全相同
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(10240)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(100000),
            },
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(100000),
            },
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(100000),
            },
        ];

        let result = checker.check(&speeds);
        // 没有间隙，应该返回 None
        assert!(result.is_none());
    }

    #[test]
    fn test_edge_case_threshold() {
        // 测试边界情况：worker 速度恰好等于健康基准 * relative_threshold
        // health_baseline = max speed = 100000
        // threshold_speed = 100000 * 0.5 = 50000
        // worker 0: 50000 < 50000? No (等于，不满足 < 条件)
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(100000)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(50000),
            }, // 恰好等于 100000 * 0.5 = 50000
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(100000),
            }, // 基准 (max speed)
        ];

        let result = checker.check(&speeds);
        // worker 0: 50000 < 100000 (绝对阈值) ✓, 但 50000 < 50000 ✗ (等于不算)
        // 不应该被检测到
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_equal_gaps() {
        // 多个相等的间隙
        let checker =
            WorkerHealthCheckerLogic::new(Some(create_speed(15000)), 0.5, SizeStandard::IEC);
        let speeds = vec![
            WorkerSpeed {
                worker_id: 0,
                speed: create_speed(10000),
            },
            WorkerSpeed {
                worker_id: 1,
                speed: create_speed(20000),
            }, // gap = 10000
            WorkerSpeed {
                worker_id: 2,
                speed: create_speed(30000),
            }, // gap = 10000
            WorkerSpeed {
                worker_id: 3,
                speed: create_speed(100000),
            }, // gap = 70000 (最大)
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        // 只有 worker 0 应该被检测到（低于 15KB 且低于 100KB * 0.5）
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert_eq!(*result.health_baseline, 100000);
    }
}
