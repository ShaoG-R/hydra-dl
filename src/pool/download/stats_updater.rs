//! Stats Updater 辅助协程
//!
//! 负责维护统计数据的更新，从主下载协程接收消息并更新统计数据。
//!
//! # 设计说明
//!
//! 主下载协程在以下时机发送消息到此协程：
//! - 任务开始时发送 `TaskStarted`
//! - `record_chunk` 返回 `true`（采样成功）时发送 `ChunkSampled`
//! - 任务结束时发送 `TaskEnded`
//!
//! 这样可以将统计更新的开销从热路径中移除，提高下载性能。
//!
//! 统计更新通过 broadcast channel 广播 `ExecutorBroadcast` 消息到所有订阅者。
//!
//! # chunk_size 维护
//!
//! `current_chunk_size` 由 `Arc<AtomicU64>` 维护，Executor 通过原子操作读取。
//! 当 `ChunkSampled` 消息到达时，使用 `ChunkStrategy` 计算新的 chunk size 并更新。

use crate::pool::common::WorkerId;
use crate::utils::cancel_channel::CancelSender;
use crate::utils::chunk_strategy::ChunkStrategy;
use log::{debug, warn};
use net_bytes::DownloadSpeed;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};

use super::executor::state::TaskState;

/// 等待中的 Executor 统计信息
#[derive(Debug, Clone)]
pub struct PendingExecutorStats {
    /// 取消信号发送器
    pub cancel_tx: CancelSender,
}

/// 已停止的 Executor 统计信息
///
/// 记录已停止的 Executor 的总运行时长和总下载字节数
#[derive(Debug, Clone)]
pub struct StoppedExecutorStats {
    /// 总运行时长
    pub total_duration: Duration,
    /// 总下载字节数（包括重试的重复字节，用于速度计算）
    pub downloaded_bytes: u64,
    /// 已写入磁盘的有效字节数（不包含重试的重复字节，用于进度计算）
    pub written_bytes: u64,
}

/// Executor 统计信息（枚举）
///
/// 表示 Executor 的生命周期，状态单向转换：Pending -> Running -> Stopped
/// Executor 运行时的统计数据
#[derive(Debug, Clone)]
pub struct RunningExecutorStats {
    /// 当前任务状态
    pub state: TaskState,
    /// 已写入磁盘的有效字节数（不包含重试的重复字节）
    pub written_bytes: u64,
    /// 所有任务总下载字节数
    pub total_downloaded_bytes: u64,
    /// 所有任务总耗时
    pub total_consumed_time: Duration,
    /// 取消信号发送器
    pub cancel_tx: CancelSender,
}

impl RunningExecutorStats {
    /// 获取当前任务状态
    pub fn as_task_state(&self) -> &TaskState {
        &self.state
    }

    /// 获取平均速度
    pub fn get_avg_speed(&self) -> Option<DownloadSpeed> {
        self.state.stats().map(|s| s.get_speed_stats(0).avg_speed)
    }

    /// 获取实时速度
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        self.state
            .stats()
            .map(|s| s.get_speed_stats(0).instant_speed)
    }

    /// 获取窗口平均速度
    pub fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        self.state
            .stats()
            .map(|s| s.get_speed_stats(0).window_avg_speed)
    }

    /// 获取速度统计
    pub fn get_speed_stats(&self) -> Option<crate::utils::stats::SpeedStats> {
        self.state.stats().map(|s| s.get_speed_stats(0))
    }
}

/// Executor 统计数据
///
/// 包含 Executor 的当前状态和相关统计指标
#[derive(Debug, Clone)]
pub enum ExecutorStats {
    /// 等待任务中
    Pending(PendingExecutorStats),
    /// 正在执行任务
    Running(RunningExecutorStats),
    /// 已停止（包含最终统计）
    Stopped(StoppedExecutorStats),
}

impl ExecutorStats {
    /// 获取实时速度（仅 Running 且 TaskState::Running 有效）
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        match self {
            ExecutorStats::Running(stats) => stats.get_instant_speed(),
            _ => None,
        }
    }
}

/// Stats 更新消息（内部使用）
#[derive(Debug, Clone)]
pub(crate) enum StatsMessage {
    /// 任务状态更新（包含 Started, Running, Ended）
    StateUpdate {
        state: TaskState,
        total_downloaded_bytes: u64,
        total_consumed_time: Duration,
    },
    /// 成功写入磁盘的字节数
    BytesWritten(u64),
    /// Executor 关闭
    ExecutorShutdown,
}

/// Executor 广播消息
///
/// 通过 broadcast channel 发送给所有订阅者
/// 本地广播直接发送此消息，外部广播发送 `(worker_id, ExecutorBroadcast)`
#[derive(Debug, Clone)]
pub enum ExecutorBroadcast {
    /// Executor 统计更新
    Stats(ExecutorStats),
    /// Executor 关闭信号
    Shutdown,
}

/// 带标签的广播消息（外部广播用）
///
/// 包含 worker_id 和广播消息，用于外部订阅者区分不同 Worker
pub type TaggedBroadcast = (WorkerId, ExecutorBroadcast);

/// Worker 本地广播发送器
///
/// 封装 Worker 内部的广播分发，包含：
/// - 外部广播：发送 `(worker_id, ExecutorBroadcast)` 到 Executor 级别的订阅者
/// - 本地 Watch：发送最新的 `ExecutorBroadcast` 到 Worker 本地的协程（如健康检测）
#[derive(Clone)]
pub struct WorkerBroadcaster {
    /// Worker ID
    worker_id: WorkerId,
    /// 外部广播发送器（Executor 级别）
    executor_tx: broadcast::Sender<TaggedBroadcast>,
    /// 本地 watch 发送器（Worker 内部，只保留最新消息）
    local_watch_tx: watch::Sender<ExecutorBroadcast>,
}

impl WorkerBroadcaster {
    /// 创建新的 Worker 广播器
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `executor_tx`: 外部广播发送器
    pub fn new(worker_id: WorkerId, executor_tx: broadcast::Sender<TaggedBroadcast>) -> Self {
        // 初始值为 Shutdown，表示尚未开始
        let (local_watch_tx, _) = watch::channel(ExecutorBroadcast::Shutdown);
        Self {
            worker_id,
            executor_tx,
            local_watch_tx,
        }
    }

    /// 订阅本地 watch（用于健康检测协程）
    pub fn subscribe_local(&self) -> watch::Receiver<ExecutorBroadcast> {
        self.local_watch_tx.subscribe()
    }

    /// 发送统计更新
    /// - 外部广播：`(worker_id, ExecutorBroadcast::Stats)`
    /// - 本地 watch：更新最新 `ExecutorBroadcast::Stats`
    #[inline]
    pub fn send_stats(&self, stats: ExecutorStats) {
        let msg = ExecutorBroadcast::Stats(stats);
        let _ = self.executor_tx.send((self.worker_id, msg.clone()));
        let _ = self.local_watch_tx.send(msg);
    }

    /// 发送关闭信号
    /// - 外部广播：`(worker_id, ExecutorBroadcast::Shutdown)`
    /// - 本地 watch：发送 `ExecutorBroadcast::Shutdown`
    #[inline]
    pub fn send_shutdown(&self) {
        let msg = ExecutorBroadcast::Shutdown;
        let _ = self.executor_tx.send((self.worker_id, msg.clone()));
        let _ = self.local_watch_tx.send(msg);
    }
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
/// 用于向辅助协程发送消息，并提供 `current_chunk_size` 的读取接口
#[derive(Clone)]
pub(crate) struct StatsUpdaterHandle {
    tx: mpsc::Sender<StatsMessage>,
    /// chunk size 读取器（通过 Arc<AtomicU64> 实现无锁读取）
    chunk_size: Arc<AtomicU64>,
}

impl StatsUpdaterHandle {
    /// 读取当前 chunk size
    ///
    /// 通过 `AtomicU64` 无锁读取
    #[inline]
    pub(crate) fn read_chunk_size(&self) -> u64 {
        self.chunk_size.load(Ordering::Relaxed)
    }

    /// 发送任务状态更新
    #[inline]
    pub(crate) fn send_state_update(
        &self,
        state: TaskState,
        total_downloaded_bytes: u64,
        total_consumed_time: Duration,
    ) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::StateUpdate {
            state,
            total_downloaded_bytes,
            total_consumed_time,
        });
    }

    /// 发送写入成功信号
    #[inline]
    pub(crate) fn send_bytes_written(&self, bytes: u64) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::BytesWritten(bytes));
    }

    /// 发送 Executor 关闭信号
    #[inline]
    pub(crate) fn send_executor_shutdown(&self) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::ExecutorShutdown);
    }
}

/// Stats Updater 辅助协程
///
/// 根据接收到的消息实时更新统计数据。
/// 直接维护 `ExecutorStats` 状态，状态转换只在消息到达时发生。
/// 状态转换是单向的：Pending -> Running -> Stopped
///
/// # chunk_size 维护
///
/// 使用 `Arc<AtomicU64>` 维护 `current_chunk_size`，Executor 通过原子操作读取。
pub(crate) struct StatsUpdater {
    /// Worker ID（用于日志）
    worker_id: WorkerId,
    /// 当前状态（包含所有统计数据和启动时间）
    state: ExecutorStats,
    /// 消息接收通道
    rx: mpsc::Receiver<StatsMessage>,
    /// Worker 广播器（封装外部和本地广播）
    broadcaster: WorkerBroadcaster,
    /// 分块策略（用于计算 chunk size）
    chunk_strategy: Box<dyn ChunkStrategy + Send>,
    /// 当前 chunk size（Arc<AtomicU64> 维护）
    chunk_size: Arc<AtomicU64>,
}

impl StatsUpdater {
    /// 创建新的 Stats Updater 和对应的句柄
    pub(crate) fn new(
        worker_id: WorkerId,
        broadcaster: WorkerBroadcaster,
        chunk_strategy: Box<dyn ChunkStrategy + Send>,
        initial_chunk_size: u64,
        cancel_tx: CancelSender,
        config: Option<StatsUpdaterConfig>,
    ) -> (Self, StatsUpdaterHandle) {
        let config = config.unwrap_or_default();
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        // 创建 Arc<AtomicU64> 用于维护 chunk_size
        let chunk_size = Arc::new(AtomicU64::new(initial_chunk_size));

        let updater = Self {
            worker_id,
            state: ExecutorStats::Pending(PendingExecutorStats { cancel_tx }),
            rx,
            broadcaster,
            chunk_strategy,
            chunk_size: chunk_size.clone(),
        };

        let handle = StatsUpdaterHandle { tx, chunk_size };

        (updater, handle)
    }

    /// 运行 Stats Updater 主循环
    pub(crate) async fn run(mut self) {
        debug!("Worker {} Stats Updater 启动", self.worker_id);

        while let Some(msg) = self.rx.recv().await {
            match msg {
                StatsMessage::StateUpdate {
                    state,
                    total_downloaded_bytes,
                    total_consumed_time,
                } => self.handle_state_update(state, total_downloaded_bytes, total_consumed_time),
                StatsMessage::BytesWritten(bytes) => self.handle_bytes_written(bytes),
                StatsMessage::ExecutorShutdown => self.handle_executor_shutdown(),
            }
        }

        debug!("Worker {} Stats Updater 退出", self.worker_id);
    }

    /// 处理状态更新
    fn handle_state_update(
        &mut self,
        new_state: TaskState,
        total_downloaded_bytes: u64,
        total_consumed_time: Duration,
    ) {
        // 更新 chunk size
        if let Some(stats) = new_state.stats() {
            // 计算新的 chunk size
            let current_chunk_size = self.chunk_size.load(Ordering::Relaxed);
            let speed_stats = stats.get_speed_stats(current_chunk_size);
            let new_chunk_size = self.chunk_strategy.calculate_chunk_size(
                current_chunk_size,
                speed_stats.instant_speed,
                speed_stats.window_avg_speed,
            );

            // 更新 atomic chunk size
            self.chunk_size.store(new_chunk_size, Ordering::Relaxed);
        }

        // 更新 ExecutorStats
        match &mut self.state {
            ExecutorStats::Pending(pending) => {
                // Pending -> Running
                self.state = ExecutorStats::Running(RunningExecutorStats {
                    state: new_state,
                    written_bytes: 0,
                    total_downloaded_bytes,
                    total_consumed_time,
                    cancel_tx: pending.cancel_tx.clone(),
                });
            }
            ExecutorStats::Running(stats) => {
                // 更新 Running 状态
                stats.state = new_state;
                stats.total_downloaded_bytes = total_downloaded_bytes;
                stats.total_consumed_time = total_consumed_time;
            }
            ExecutorStats::Stopped(_) => {
                warn!(
                    "Worker {} StatsUpdater 收到状态更新，但当前状态为 Stopped",
                    self.worker_id
                );
            }
        }

        // 广播更新
        self.broadcaster.send_stats(self.state.clone());
    }

    /// 处理写入字节数更新
    fn handle_bytes_written(&mut self, bytes: u64) {
        if let ExecutorStats::Running(stats) = &mut self.state {
            stats.written_bytes += bytes;
            // 广播更新
            self.broadcaster.send_stats(self.state.clone());
        }
    }

    /// 处理 Executor 关闭
    fn handle_executor_shutdown(&mut self) {
        // 如果当前是 Running，转换为 Stopped
        if let ExecutorStats::Running(stats) = &self.state {
            let stopped_stats = StoppedExecutorStats::from_running(stats);
            self.state = ExecutorStats::Stopped(stopped_stats);
            // 广播更新
            self.broadcaster.send_stats(self.state.clone());
        }

        // 发送关闭信号
        self.broadcaster.send_shutdown();
    }
}

impl StoppedExecutorStats {
    /// 从 Running 状态创建 Stopped 状态
    fn from_running(running: &RunningExecutorStats) -> Self {
        Self {
            total_duration: running.total_consumed_time,
            downloaded_bytes: running.total_downloaded_bytes,
            written_bytes: running.written_bytes,
        }
    }
}
