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

mod logic;

use crate::config::DownloadConfig;
use crate::pool::download::{DownloadWorkerHandle, ExecutorBroadcast, ExecutorStats, TaggedBroadcast};
use lite_sync::oneshot::lite;
use log::{debug, warn};
use rustc_hash::FxHashMap;
use smr_swap::LocalReader;
use std::sync::Arc;
use tokio::sync::broadcast;

use logic::{ExecutorHealthTracker, Speed, WorkerHealthCheckerLogic};
pub use logic::WorkerSpeed;

/// 健康检查器参数
pub(super) struct WorkerHealthCheckerParams {
    /// 配置
    pub config: Arc<DownloadConfig>,
    /// 共享的 worker handles（用于发送取消信号）
    pub worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle>>,
    /// 广播接收器
    pub broadcast_rx: broadcast::Receiver<TaggedBroadcast>,
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

