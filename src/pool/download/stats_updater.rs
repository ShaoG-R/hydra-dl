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
//!
//! 统计更新通过 broadcast channel 广播 `ExecutorBroadcast` 消息到所有订阅者。

use crate::utils::stats::{SpeedStats, WorkerStats};
use log::debug;
use net_bytes::DownloadSpeed;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};

/// Executor 当前运行状态统计
///
/// 表示 Executor 的两种状态：停止或运行中
#[derive(Debug, Clone, Default)]
pub enum ExecutorCurrentStats {
    /// Executor 已停止
    #[default]
    Stopped,
    /// Executor 运行中，包含实时统计数据
    Running(SpeedStats),
}

/// Executor 统计信息
///
/// 记录 Executor 的总运行时长、总下载字节数，以及当前运行状态的详细统计
#[derive(Debug, Clone)]
pub struct ExecutorStats {
    /// 总运行时长
    pub total_duration: Duration,
    /// 总下载字节数（包括重试的重复字节，用于速度计算）
    pub downloaded_bytes: u64,
    /// 已写入磁盘的有效字节数（不包含重试的重复字节，用于进度计算）
    pub written_bytes: u64,
    /// 当前运行状态统计
    pub current_stats: ExecutorCurrentStats,
    /// 内部：运行开始时间（用于计算总运行时长）
    pub run_start_time: Option<Instant>,
}

impl Default for ExecutorStats {
    fn default() -> Self {
        Self {
            total_duration: Duration::ZERO,
            downloaded_bytes: 0,
            written_bytes: 0,
            current_stats: ExecutorCurrentStats::Stopped,
            run_start_time: None,
        }
    }
}

impl ExecutorStats {
    /// 创建新的 ExecutorStats
    pub fn new() -> Self {
        Self::default()
    }

    /// 标记 Executor 开始运行
    pub(crate) fn start_running(&mut self, current_chunk_size: u64) {
        if self.run_start_time.is_none() {
            self.run_start_time = Some(Instant::now());
        }
        // 初始化为运行状态，但还没有速度数据
        self.current_stats = ExecutorCurrentStats::Running(SpeedStats {
            current_chunk_size,
            ..SpeedStats::empty()
        });
    }

    /// 标记 Executor 停止运行
    pub(crate) fn stop_running(&mut self) {
        // 累加运行时长
        if let Some(start) = self.run_start_time.take() {
            self.total_duration += start.elapsed();
        }
        self.current_stats = ExecutorCurrentStats::Stopped;
    }

    /// 更新统计数据（从 WorkerStats 同步）
    /// 
    /// 注意：written_bytes 由 BytesWritten 消息单独管理，这里不覆盖
    pub(crate) fn update_from_worker_stats(&mut self, worker_stats: &WorkerStats) {
        self.downloaded_bytes = worker_stats.get_total_bytes();
        // written_bytes 不从 WorkerStats 同步，由 add_written_bytes 单独累加
        
        if matches!(self.current_stats, ExecutorCurrentStats::Running(_)) {
            self.current_stats = ExecutorCurrentStats::Running(worker_stats.get_speed_stats());
        }
    }

    /// 添加已写入的字节数
    pub(crate) fn add_written_bytes(&mut self, bytes: u64) {
        self.written_bytes += bytes;
    }

    /// 获取速度统计（如果正在运行）
    pub fn get_speed_stats(&self) -> Option<SpeedStats> {
        match &self.current_stats {
            ExecutorCurrentStats::Running(stats) => Some(*stats),
            ExecutorCurrentStats::Stopped => None,
        }
    }

    /// 获取实时速度
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        match &self.current_stats {
            ExecutorCurrentStats::Running(stats) => stats.instant_speed,
            ExecutorCurrentStats::Stopped => None,
        }
    }

    /// 获取窗口平均速度
    pub fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        match &self.current_stats {
            ExecutorCurrentStats::Running(stats) => stats.window_avg_speed,
            ExecutorCurrentStats::Stopped => None,
        }
    }

    /// 获取平均速度
    pub fn get_avg_speed(&self) -> Option<DownloadSpeed> {
        match &self.current_stats {
            ExecutorCurrentStats::Running(stats) => stats.avg_speed,
            ExecutorCurrentStats::Stopped => None,
        }
    }
}

/// Stats 更新消息（内部使用）
#[derive(Debug, Clone)]
pub(crate) enum StatsMessage {
    /// 任务开始
    TaskStarted{
        initial_chunk_size: u64
    },
    /// Chunk 采样成功，包含当前的 WorkerStats 快照
    ChunkSampled(WorkerStats),
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
pub type TaggedBroadcast = (u64, ExecutorBroadcast);

/// Worker 本地广播发送器
///
/// 封装 Worker 内部的广播分发，包含：
/// - 外部广播：发送 `(worker_id, ExecutorBroadcast)` 到 Executor 级别的订阅者
/// - 本地广播：发送 `ExecutorBroadcast` 到 Worker 本地的协程（如健康检测）
#[derive(Clone)]
pub struct WorkerBroadcaster {
    /// Worker ID
    worker_id: u64,
    /// 外部广播发送器（Executor 级别）
    executor_tx: broadcast::Sender<TaggedBroadcast>,
    /// 本地广播发送器（Worker 内部，不带 worker_id）
    local_tx: broadcast::Sender<ExecutorBroadcast>,
}

impl WorkerBroadcaster {
    /// 创建新的 Worker 广播器
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `executor_tx`: 外部广播发送器
    /// - `local_capacity`: 本地广播通道容量
    pub fn new(worker_id: u64, executor_tx: broadcast::Sender<TaggedBroadcast>, local_capacity: usize) -> Self {
        let (local_tx, _) = broadcast::channel(local_capacity);
        Self {
            worker_id,
            executor_tx,
            local_tx,
        }
    }

    /// 订阅本地广播（用于健康检测协程）
    pub fn subscribe_local(&self) -> broadcast::Receiver<ExecutorBroadcast> {
        self.local_tx.subscribe()
    }

    /// 发送统计更新
    /// - 外部广播：`(worker_id, ExecutorBroadcast::Stats)`
    /// - 本地广播：`ExecutorBroadcast::Stats`
    #[inline]
    pub fn send_stats(&self, stats: ExecutorStats) {
        let msg = ExecutorBroadcast::Stats(stats);
        let _ = self.executor_tx.send((self.worker_id, msg.clone()));
        let _ = self.local_tx.send(msg);
    }

    /// 发送关闭信号
    /// - 外部广播：`(worker_id, ExecutorBroadcast::Shutdown)`
    /// - 本地广播：`ExecutorBroadcast::Shutdown`
    #[inline]
    pub fn send_shutdown(&self) {
        let msg = ExecutorBroadcast::Shutdown;
        let _ = self.executor_tx.send((self.worker_id, msg.clone()));
        let _ = self.local_tx.send(msg);
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
/// 用于向辅助协程发送消息
#[derive(Clone)]
pub(crate) struct StatsUpdaterHandle {
    tx: mpsc::Sender<StatsMessage>,
}

impl StatsUpdaterHandle {
    /// 发送任务开始信号
    #[inline]
    pub(crate) fn send_task_started(&self, initial_chunk_size: u64) {
        // 使用 try_send 避免阻塞，如果通道满了就跳过
        let _ = self.tx.try_send(StatsMessage::TaskStarted {
            initial_chunk_size
        });
    }

    /// 发送采样成功信号
    #[inline]
    pub(crate) fn send_chunk_sampled(&self, stats: WorkerStats) {
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
/// 持有 `SmrSwap<WorkerStats>` 并根据接收到的消息实时更新统计数据。
/// 内部维护 `ExecutorStats` 用于记录总运行时长和总下载字节数。
/// 通过 broadcast channel 广播 `ExecutorStats` 更新和关闭信号。
pub(crate) struct StatsUpdater {
    /// Worker ID（用于日志）
    worker_id: u64,
    /// Executor 统计数据（内部维护）
    executor_stats: ExecutorStats,
    /// 消息接收通道
    rx: mpsc::Receiver<StatsMessage>,
    /// Worker 广播器（封装外部和本地广播）
    broadcaster: WorkerBroadcaster,
}

impl StatsUpdater {
    /// 创建新的 Stats Updater 和对应的句柄
    ///
    /// # Arguments
    ///
    /// - `worker_id`: Worker ID
    /// - `broadcaster`: Worker 广播器，用于发送 ExecutorStats 更新和关闭信号
    /// - `config`: 配置（可选，使用默认配置）
    ///
    /// # Returns
    ///
    /// 返回 `(StatsUpdater, StatsUpdaterHandle)`
    pub(crate) fn new(
        worker_id: u64,
        broadcaster: WorkerBroadcaster,
        config: Option<StatsUpdaterConfig>,
    ) -> (Self, StatsUpdaterHandle) {
        let config = config.unwrap_or_default();
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        let updater = Self {
            worker_id,
            executor_stats: ExecutorStats::new(),
            rx,
            broadcaster,
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
                StatsMessage::TaskStarted{ initial_chunk_size } => {
                    self.handle_task_started(initial_chunk_size);
                }
                StatsMessage::ChunkSampled(worker_stats) => {
                    self.handle_chunk_sampled(worker_stats);
                }
                StatsMessage::BytesWritten(bytes) => {
                    self.handle_bytes_written(bytes);
                }
                StatsMessage::TaskEnded => {
                    self.handle_task_ended();
                }
                StatsMessage::ExecutorShutdown => {
                    self.handle_executor_shutdown();
                }
            }
        }

        debug!("Worker #{} Stats Updater 退出", self.worker_id);
    }

    /// 处理任务开始
    fn handle_task_started(&mut self, initial_chunk_size: u64) {
        debug!("Worker #{} 任务开始", self.worker_id);
        
        // 更新 ExecutorStats：标记开始运行
        self.executor_stats.start_running(initial_chunk_size);
        
        // 广播到订阅者
        self.broadcast_stats();
    }

    /// 处理采样成功
    fn handle_chunk_sampled(&mut self, worker_stats: WorkerStats) {
        // 更新 ExecutorStats：同步速度数据
        self.executor_stats.update_from_worker_stats(&worker_stats);
        
        // 广播到订阅者
        self.broadcast_stats();
    }

    /// 处理写入成功
    fn handle_bytes_written(&mut self, bytes: u64) {
        // 更新 ExecutorStats：累加已写入字节数
        self.executor_stats.add_written_bytes(bytes);
        
        // 广播到订阅者
        self.broadcast_stats();
    }

    /// 处理任务结束
    fn handle_task_ended(&mut self) {
        debug!("Worker #{} 任务结束", self.worker_id);
        
        // 更新 ExecutorStats：标记停止运行
        self.executor_stats.stop_running();
        
        // 广播到订阅者
        self.broadcast_stats();
    }
    
    /// 广播 ExecutorStats
    #[inline]
    fn broadcast_stats(&self) {
        self.broadcaster.send_stats(self.executor_stats.clone());
    }

    /// 处理 Executor 关闭
    #[inline]
    fn handle_executor_shutdown(&self) {
        debug!("Worker #{} Executor 关闭", self.worker_id);
        self.broadcaster.send_shutdown();
    }
}