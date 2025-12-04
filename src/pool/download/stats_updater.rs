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
use crate::utils::chunk_strategy::ChunkStrategy;
use crate::utils::stats::{SpeedStats, WorkerStatsActive};
use log::{debug, warn};
use net_bytes::DownloadSpeed;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, watch};

/// 运行中的任务统计信息
///
/// 记录正在运行的任务的当前运行时长、下载字节数和速度统计
#[derive(Debug, Clone)]
pub struct RunningTaskData {
    /// 当前运行时长（从本次启动到现在）
    pub current_duration: Duration,
    /// 当前下载字节数（包括重试的重复字节，用于速度计算）
    pub downloaded_bytes: u64,
    /// 已写入磁盘的有效字节数（不包含重试的重复字节，用于进度计算）
    pub written_bytes: u64,
    /// 实时速度统计
    pub speed_stats: SpeedStats,
}

impl RunningTaskData {
    /// 获取速度统计
    #[inline]
    pub fn get_speed_stats(&self) -> SpeedStats {
        self.speed_stats
    }

    /// 获取实时速度
    #[inline]
    pub fn get_instant_speed(&self) -> DownloadSpeed {
        self.speed_stats.instant_speed
    }

    /// 获取窗口平均速度
    #[inline]
    pub fn get_window_avg_speed(&self) -> DownloadSpeed {
        self.speed_stats.window_avg_speed
    }

    /// 获取平均速度
    #[inline]
    pub fn get_avg_speed(&self) -> DownloadSpeed {
        self.speed_stats.avg_speed
    }
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

/// 任务统计信息（枚举）
///
/// 表示单个任务的生命周期，状态单向循环：Started -> Running -> Ended -> Started -> ...
#[derive(Debug, Clone)]
pub enum TaskStats {
    /// 任务已启动（收到 TaskStarted，等待第一个 ChunkSampled）
    /// 携带启动时间和已写入字节数
    Started {
        start_time: Instant,
        written_bytes: u64,
    },
    /// 任务运行中，携带启动时间和统计数据
    Running {
        start_time: Instant,
        data: RunningTaskData,
    },
    /// 任务已结束，包含总运行时长和总字节数
    Ended {
        total_duration: Duration,
        downloaded_bytes: u64,
        written_bytes: u64,
    },
}

impl TaskStats {
    /// 获取统计信息的字符串表示
    pub fn stats_str(&self) -> &'static str {
        match self {
            TaskStats::Started { .. } => "Started",
            TaskStats::Running { .. } => "Running",
            TaskStats::Ended { .. } => "Ended",
        }
    }

    /// 获取启动时间（Started 或 Running 状态）
    pub fn start_time(&self) -> Option<Instant> {
        match self {
            TaskStats::Started { start_time, .. } | TaskStats::Running { start_time, .. } => {
                Some(*start_time)
            }
            TaskStats::Ended { .. } => None,
        }
    }

    /// 获取已下载字节数
    pub fn downloaded_bytes(&self) -> u64 {
        match self {
            TaskStats::Started { .. } => 0,
            TaskStats::Running { data, .. } => data.downloaded_bytes,
            TaskStats::Ended {
                downloaded_bytes, ..
            } => *downloaded_bytes,
        }
    }

    /// 获取已写入字节数
    pub fn written_bytes(&self) -> u64 {
        match self {
            TaskStats::Started { written_bytes, .. } => *written_bytes,
            TaskStats::Running { data, .. } => data.written_bytes,
            TaskStats::Ended { written_bytes, .. } => *written_bytes,
        }
    }

    /// 检查是否已启动
    pub fn is_started(&self) -> bool {
        matches!(self, TaskStats::Started { .. })
    }

    /// 检查是否正在运行
    pub fn is_running(&self) -> bool {
        matches!(self, TaskStats::Running { .. })
    }

    /// 检查是否已结束
    pub fn is_ended(&self) -> bool {
        matches!(self, TaskStats::Ended { .. })
    }

    /// 获取运行中的统计（如果正在运行）
    pub fn as_running(&self) -> Option<&RunningTaskData> {
        match self {
            TaskStats::Running { data, .. } => Some(data),
            _ => None,
        }
    }

    /// 获取速度统计（仅运行中有效）
    pub fn get_speed_stats(&self) -> Option<SpeedStats> {
        match self {
            TaskStats::Running { data, .. } => Some(data.speed_stats),
            _ => None,
        }
    }

    /// 获取实时速度（仅运行中有效）
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        match self {
            TaskStats::Running { data, .. } => Some(data.speed_stats.instant_speed),
            _ => None,
        }
    }

    /// 获取窗口平均速度（仅运行中有效）
    pub fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        match self {
            TaskStats::Running { data, .. } => Some(data.speed_stats.window_avg_speed),
            _ => None,
        }
    }

    /// 获取平均速度（仅运行中有效）
    pub fn get_avg_speed(&self) -> Option<DownloadSpeed> {
        match self {
            TaskStats::Running { data, .. } => Some(data.speed_stats.avg_speed),
            _ => None,
        }
    }
}

/// Executor 统计信息（枚举）
///
/// 表示 Executor 的生命周期，状态单向转换：Pending -> Running -> Stopped
/// Running 状态包含 TaskStats，TaskStats 内部单向循环
#[derive(Debug, Clone)]
pub enum ExecutorStats {
    /// Executor 待命中（尚未开始运行）
    Pending,
    /// Executor 运行中，包含任务统计
    Running(TaskStats),
    /// Executor 已停止，包含总运行时长和总字节数
    Stopped(StoppedExecutorStats),
}

impl ExecutorStats {
    /// 获取统计信息的字符串表示
    pub fn stats_str(&self) -> &'static str {
        match self {
            ExecutorStats::Pending => "Pending",
            ExecutorStats::Running(task) => task.stats_str(),
            ExecutorStats::Stopped(_) => "Stopped",
        }
    }

    /// 获取已下载字节数
    pub fn downloaded_bytes(&self) -> u64 {
        match self {
            ExecutorStats::Pending => 0,
            ExecutorStats::Running(task) => task.downloaded_bytes(),
            ExecutorStats::Stopped(stats) => stats.downloaded_bytes,
        }
    }

    /// 获取已写入字节数
    pub fn written_bytes(&self) -> u64 {
        match self {
            ExecutorStats::Pending => 0,
            ExecutorStats::Running(task) => task.written_bytes(),
            ExecutorStats::Stopped(stats) => stats.written_bytes,
        }
    }

    /// 检查是否待命
    pub fn is_pending(&self) -> bool {
        matches!(self, ExecutorStats::Pending)
    }

    /// 检查是否正在运行
    pub fn is_running(&self) -> bool {
        matches!(self, ExecutorStats::Running(_))
    }

    /// 检查是否已停止
    pub fn is_stopped(&self) -> bool {
        matches!(self, ExecutorStats::Stopped(_))
    }

    /// 获取任务统计（如果正在运行）
    pub fn as_task_stats(&self) -> Option<&TaskStats> {
        match self {
            ExecutorStats::Running(task) => Some(task),
            _ => None,
        }
    }

    /// 获取已停止的统计（如果已停止）
    pub fn as_stopped(&self) -> Option<&StoppedExecutorStats> {
        match self {
            ExecutorStats::Stopped(stats) => Some(stats),
            _ => None,
        }
    }

    /// 获取速度统计（仅 Running 且 TaskStats::Running 有效）
    pub fn get_speed_stats(&self) -> Option<SpeedStats> {
        match self {
            ExecutorStats::Running(task) => task.get_speed_stats(),
            _ => None,
        }
    }

    /// 获取实时速度（仅 Running 且 TaskStats::Running 有效）
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        match self {
            ExecutorStats::Running(task) => task.get_instant_speed(),
            _ => None,
        }
    }

    /// 获取窗口平均速度（仅 Running 且 TaskStats::Running 有效）
    pub fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        match self {
            ExecutorStats::Running(task) => task.get_window_avg_speed(),
            _ => None,
        }
    }

    /// 获取平均速度（仅 Running 且 TaskStats::Running 有效）
    pub fn get_avg_speed(&self) -> Option<DownloadSpeed> {
        match self {
            ExecutorStats::Running(task) => task.get_avg_speed(),
            _ => None,
        }
    }
}

/// Stats 更新消息（内部使用）
#[derive(Debug, Clone)]
pub(crate) enum StatsMessage {
    /// 任务开始
    TaskStarted,
    /// Chunk 采样成功，包含激活状态的 WorkerStats 快照
    ChunkSampled(WorkerStatsActive),
    /// 成功写入磁盘的字节数
    BytesWritten(u64),
    /// 任务结束
    TaskEnded,
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

    /// 发送任务开始信号
    #[inline]
    pub(crate) fn send_task_started(&self) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::TaskStarted);
    }

    /// 发送采样成功信号
    #[inline]
    pub(crate) fn send_chunk_sampled(&self, stats: WorkerStatsActive) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::ChunkSampled(stats));
    }

    /// 发送写入成功信号
    #[inline]
    pub(crate) fn send_bytes_written(&self, bytes: u64) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::BytesWritten(bytes));
    }

    /// 发送任务结束信号
    #[inline]
    pub(crate) fn send_task_ended(&self) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::TaskEnded);
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
        config: Option<StatsUpdaterConfig>,
    ) -> (Self, StatsUpdaterHandle) {
        let config = config.unwrap_or_default();
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        // 创建 Arc<AtomicU64> 用于维护 chunk_size
        let chunk_size = Arc::new(AtomicU64::new(initial_chunk_size));

        let updater = Self {
            worker_id,
            state: ExecutorStats::Pending,
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
                StatsMessage::TaskStarted => self.handle_task_started(),
                StatsMessage::ChunkSampled(worker_stats) => self.handle_chunk_sampled(worker_stats),
                StatsMessage::BytesWritten(bytes) => self.handle_bytes_written(bytes),
                StatsMessage::TaskEnded => self.handle_task_ended(),
                StatsMessage::ExecutorShutdown => self.handle_executor_shutdown(),
            }
        }

        debug!("Worker {} Stats Updater 退出", self.worker_id);
    }

    /// 处理任务开始
    /// - Pending 状态：Pending -> Running(TaskStats::Started)
    /// - Running(TaskStats::Ended) 状态：TaskStats::Ended -> TaskStats::Started（循环）
    fn handle_task_started(&mut self) {
        match &self.state {
            ExecutorStats::Pending => {
                debug!("Worker {} 任务开始 (Pending -> Running)", self.worker_id);
                self.state = ExecutorStats::Running(TaskStats::Started {
                    start_time: Instant::now(),
                    written_bytes: 0,
                });
                self.broadcast_stats();
            }
            ExecutorStats::Running(TaskStats::Ended { written_bytes, .. }) => {
                debug!("Worker {} 新任务开始 (Ended -> Started)", self.worker_id);
                self.state = ExecutorStats::Running(TaskStats::Started {
                    start_time: Instant::now(),
                    written_bytes: *written_bytes,
                });
                self.broadcast_stats();
            }
            _ => {
                warn!(
                    "Worker {} 在 {} 状态时接收到任务开始信号",
                    self.worker_id,
                    self.state.stats_str()
                );
            }
        }
    }

    /// 处理采样成功（TaskStats::Started -> TaskStats::Running 或更新 TaskStats::Running）
    fn handle_chunk_sampled(&mut self, worker_stats: WorkerStatsActive) {
        let ExecutorStats::Running(task_stats) = &self.state else {
            return;
        };

        // 获取启动时间（从 Started 或 Running 状态）
        let Some(start_time) = task_stats.start_time() else {
            return;
        };

        let current_chunk_size = self.chunk_size.load(Ordering::Relaxed);
        let speed_stats = worker_stats.get_speed_stats(current_chunk_size);
        let downloaded_bytes = worker_stats.get_total_bytes();
        let written_bytes = task_stats.written_bytes();

        // 使用 ChunkStrategy 计算新的 chunk size
        let instant_speed = worker_stats.get_instant_speed();
        let window_avg_speed = worker_stats.get_window_avg_speed();
        let new_chunk_size = self.chunk_strategy.calculate_chunk_size(
            current_chunk_size,
            instant_speed,
            window_avg_speed,
        );

        // 更新 AtomicU64 中的 chunk_size
        if new_chunk_size != current_chunk_size {
            self.chunk_size.store(new_chunk_size, Ordering::Relaxed);
            debug!(
                "Worker {} chunk_size 更新: {} -> {}",
                self.worker_id, current_chunk_size, new_chunk_size
            );
        }

        // 更新状态并广播
        self.state = ExecutorStats::Running(TaskStats::Running {
            start_time,
            data: RunningTaskData {
                current_duration: start_time.elapsed(),
                downloaded_bytes,
                written_bytes,
                speed_stats,
            },
        });
        self.broadcast_stats();
    }

    /// 处理写入成功（Running 状态有效，Pending/Stopped 状态无效）
    fn handle_bytes_written(&mut self, bytes: u64) {
        match &self.state {
            ExecutorStats::Running(TaskStats::Started {
                start_time,
                written_bytes,
            }) => {
                self.state = ExecutorStats::Running(TaskStats::Started {
                    start_time: *start_time,
                    written_bytes: written_bytes + bytes,
                });
                self.broadcast_stats();
            }
            ExecutorStats::Running(TaskStats::Running { start_time, data }) => {
                self.state = ExecutorStats::Running(TaskStats::Running {
                    start_time: *start_time,
                    data: RunningTaskData {
                        current_duration: start_time.elapsed(),
                        downloaded_bytes: data.downloaded_bytes,
                        written_bytes: data.written_bytes + bytes,
                        speed_stats: data.speed_stats,
                    },
                });
                self.broadcast_stats();
            }
            ExecutorStats::Running(TaskStats::Ended {
                total_duration,
                downloaded_bytes,
                written_bytes,
            }) => {
                self.state = ExecutorStats::Running(TaskStats::Ended {
                    total_duration: *total_duration,
                    downloaded_bytes: *downloaded_bytes,
                    written_bytes: written_bytes + bytes,
                });
                self.broadcast_stats();
            }
            ExecutorStats::Pending | ExecutorStats::Stopped(_) => {
                warn!(
                    "Worker {} 在 {} 状态时接收到写入成功信号",
                    self.worker_id,
                    self.state.stats_str()
                );
            }
        }
    }

    /// 处理任务结束（TaskStats::Started/Running -> TaskStats::Ended）
    fn handle_task_ended(&mut self) {
        let ExecutorStats::Running(task_stats) = &self.state else {
            return;
        };

        // 获取启动时间
        let Some(start_time) = task_stats.start_time() else {
            return;
        };

        debug!("Worker {} 任务结束", self.worker_id);

        self.state = ExecutorStats::Running(TaskStats::Ended {
            total_duration: start_time.elapsed(),
            downloaded_bytes: task_stats.downloaded_bytes(),
            written_bytes: task_stats.written_bytes(),
        });

        self.broadcast_stats();
    }

    /// 广播 ExecutorStats
    #[inline]
    fn broadcast_stats(&self) {
        self.broadcaster.send_stats(self.state.clone());
    }

    /// 处理 Executor 关闭
    ///
    /// 从当前状态提取统计数据，转换到 Stopped 状态并广播，然后发送 Shutdown 信号
    fn handle_executor_shutdown(&mut self) {
        debug!("Worker {} Executor 关闭", self.worker_id);

        // 从当前状态提取统计数据
        let stopped_stats = match &self.state {
            ExecutorStats::Pending => StoppedExecutorStats {
                total_duration: Duration::ZERO,
                downloaded_bytes: 0,
                written_bytes: 0,
            },
            ExecutorStats::Running(task_stats) => {
                let total_duration = task_stats
                    .start_time()
                    .map(|t| t.elapsed())
                    .unwrap_or(Duration::ZERO);
                StoppedExecutorStats {
                    total_duration,
                    downloaded_bytes: task_stats.downloaded_bytes(),
                    written_bytes: task_stats.written_bytes(),
                }
            }
            ExecutorStats::Stopped(stats) => stats.clone(),
        };

        // 转换到 Stopped 状态并广播
        self.state = ExecutorStats::Stopped(stopped_stats);
        self.broadcast_stats();

        // 发送 Shutdown 信号
        self.broadcaster.send_shutdown();
    }
}
