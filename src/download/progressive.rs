//! 渐进式 Worker 启动模块（Actor 模式）
//!
//! 负责管理 Worker 的分阶段启动逻辑，根据当前 Worker 的速度表现决定何时启动下一批 Worker
//! 采用 Actor 模式，完全独立于主下载循环

use crate::config::DownloadConfig;
use crate::download::download_stats::AggregatedStats;
use lite_sync::oneshot::lite;
use log::{debug, error, info};
use net_bytes::{DownloadSpeed, FileSizeFormat};
use smr_swap::LocalReader;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Worker 启动请求
#[derive(Debug, Clone)]
pub(super) struct WorkerLaunchRequest {
    /// 要启动的 worker 数量
    pub count: u64,
    /// 当前阶段索引
    pub stage: usize,
}

/// 启动决策结果
#[derive(Debug)]
enum LaunchDecision {
    /// 启动下一批 worker
    Launch {
        /// 要启动的 worker 数量
        workers_to_add: u64,
        /// 当前所有 worker 的速度
        speeds: Vec<Option<DownloadSpeed>>,
    },
    /// 等待条件满足
    Wait(WaitReason),
    /// 不启动（所有阶段已完成或无需启动）
    Skip,
}

/// 等待原因
#[derive(Debug)]
enum WaitReason {
    /// Worker 速度未达标
    InsufficientSpeed {
        speeds: Vec<Option<DownloadSpeed>>,
        threshold: Option<u64>,
    },
    /// 预期剩余时间过短
    TimeRemainsTooShort {
        estimated_secs: f64,
        threshold_secs: f64,
    },
}

/// 渐进式启动管理器（内部逻辑）
///
/// 封装了 Worker 渐进式启动的状态和决策逻辑
struct ProgressiveLauncherLogic {
    /// 渐进式启动阶段序列（预计算的目标worker数量序列，如 [3, 6, 9, 12]）
    worker_launch_stages: Vec<u64>,
    /// 下一个启动阶段的索引（0 表示已完成第一批，1 表示准备启动第二批）
    next_launch_stage: usize,
}

impl ProgressiveLauncherLogic {
    /// 格式化速度列表为可读的字符串
    ///
    /// 将 `Vec<Option<DownloadSpeed>>` 转换为格式化的速度字符串
    /// 例如: "1.2 MB/s, 1.5 MB/s, N/A"
    fn format_speeds(
        speeds: &[Option<DownloadSpeed>],
        size_standard: net_bytes::SizeStandard,
    ) -> String {
        speeds
            .iter()
            .map(|speed| match speed {
                Some(s) => s.to_formatted(size_standard).to_string(),
                None => "N/A".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// 创建新的渐进式启动逻辑管理器
    fn new(config: &DownloadConfig) -> Self {
        let total_worker_count = config.concurrency().worker_count();

        // 根据配置的比例序列计算渐进式启动阶段
        let worker_launch_stages: Vec<u64> = config
            .progressive()
            .worker_ratios()
            .iter()
            .map(|&ratio| {
                let stage_count =
                    ((total_worker_count as f64 * ratio).ceil() as u64).min(total_worker_count);
                // 确保至少启动1个worker
                stage_count.max(1)
            })
            .collect();

        Self {
            worker_launch_stages,
            next_launch_stage: 1, // 第一批已启动，下一个是第二批（索引1）
        }
    }

    /// 获取第一批 Worker 的数量
    fn initial_worker_count(&self) -> u64 {
        self.worker_launch_stages[0]
    }

    /// 检查是否还有下一批 Worker 待启动
    fn should_check_next_stage(&self) -> bool {
        self.next_launch_stage < self.worker_launch_stages.len()
    }

    /// 检查所有已启动 Worker 的速度是否达到阈值
    fn check_worker_speeds(
        &self,
        aggregated_stats: &AggregatedStats,
        config: &DownloadConfig,
    ) -> Result<Vec<Option<DownloadSpeed>>, WaitReason> {
        let mut speeds = Vec::with_capacity(aggregated_stats.len());
        let threshold = config.progressive().min_speed_threshold();

        for (_, executor_stats) in aggregated_stats.iter() {
            let instant_speed = executor_stats.get_instant_speed();

            // 检查速度是否达标
            if let Some(threshold) = threshold {
                if instant_speed.is_none() || instant_speed.unwrap().as_u64() < threshold.get() {
                    speeds.push(instant_speed);
                    return Err(WaitReason::InsufficientSpeed {
                        speeds,
                        threshold: Some(threshold.get()),
                    });
                }
            }

            speeds.push(instant_speed);
        }

        Ok(speeds)
    }

    /// 检查预期剩余下载时间是否足够
    fn check_remaining_time(
        &self,
        written_bytes: u64,
        total_bytes: u64,
        aggregated_stats: &AggregatedStats,
        config: &DownloadConfig,
    ) -> Result<(), WaitReason> {
        let window_avg_speed = aggregated_stats.get_total_window_avg_speed();

        if let Some(window_avg_speed) = window_avg_speed {
            let remaining_bytes = total_bytes.saturating_sub(written_bytes);
            let estimated_time_left = remaining_bytes as f64 / window_avg_speed.as_u64() as f64;
            let threshold = config.progressive().min_time_before_finish().as_secs_f64();

            debug!(
                "渐进式启动 - 剩余时间检测: 剩余 {} bytes, 窗口平均速度 {}, 预期剩余 {:.1}s, 阈值 {:.1}s",
                remaining_bytes,
                window_avg_speed.to_formatted(config.speed().size_standard()),
                estimated_time_left,
                threshold
            );

            if estimated_time_left < threshold {
                return Err(WaitReason::TimeRemainsTooShort {
                    estimated_secs: estimated_time_left,
                    threshold_secs: threshold,
                });
            }
        }

        Ok(())
    }

    /// 决策是否启动下一批 worker（纯决策逻辑，不执行）
    ///
    /// 按顺序检查：
    /// 1. 是否还有阶段待启动
    /// 2. 所有 worker 速度是否达标
    /// 3. 剩余时间是否足够
    /// 4. 是否有需要启动的 worker
    fn decide_next_launch(
        &self,
        aggregated_stats: &AggregatedStats,
        written_bytes: u64,
        total_bytes: u64,
        config: &DownloadConfig,
    ) -> LaunchDecision {
        // 检查是否所有阶段已完成
        if !self.should_check_next_stage() {
            return LaunchDecision::Skip;
        }

        let current_worker_count = aggregated_stats.len() as u64;

        // 检查 worker 速度
        match self.check_worker_speeds(aggregated_stats, config) {
            Err(reason) => return LaunchDecision::Wait(reason),
            Ok(speeds) => {
                // 检查剩余时间
                if let Err(reason) =
                    self.check_remaining_time(written_bytes, total_bytes, aggregated_stats, config)
                {
                    return LaunchDecision::Wait(reason);
                }

                // 计算需要启动的 worker 数量
                let next_target = self.worker_launch_stages[self.next_launch_stage];
                let workers_to_add = next_target.saturating_sub(current_worker_count);

                if workers_to_add > 0 {
                    LaunchDecision::Launch {
                        workers_to_add,
                        speeds,
                    }
                } else {
                    LaunchDecision::Skip
                }
            }
        }
    }

    /// 检测并调整启动阶段（应对 worker 数量变化）
    fn adjust_stage_for_worker_count(&mut self, current_worker_count: u64) {
        if self.next_launch_stage > 0 {
            let current_target = self.worker_launch_stages[self.next_launch_stage - 1];
            if current_worker_count < current_target {
                // Worker 数量少于预期，需要降级批次
                // 找到当前 worker 数量对应的合适阶段
                let mut new_stage = 0;
                for (idx, &stage_target) in self.worker_launch_stages.iter().enumerate() {
                    if current_worker_count <= stage_target {
                        new_stage = idx + 1;
                        break;
                    }
                }

                if new_stage != self.next_launch_stage {
                    info!(
                        "渐进式启动 - Worker 数量降级: 当前 {} workers < 预期 {} workers，从第 {} 批降级到第 {} 批",
                        current_worker_count,
                        current_target,
                        self.next_launch_stage + 1,
                        new_stage + 1
                    );
                    self.next_launch_stage = new_stage;
                }
            }
        }
    }
}

/// 渐进式启动 Actor 参数
pub(super) struct ProgressiveLauncherParams {
    /// 配置
    pub config: Arc<DownloadConfig>,
    /// 聚合统计数据读取器（从 DownloadStats 获取）
    pub aggregated_stats: LocalReader<AggregatedStats>,
    /// 文件总大小
    pub total_size: u64,
    /// 启动偏移时间
    pub start_offset: std::time::Duration,
}

/// Actor 通信通道
struct ActorChannels {
    /// 关闭接收器
    shutdown_rx: lite::Receiver<()>,
    /// Worker 启动请求发送器
    launch_request_tx: mpsc::Sender<WorkerLaunchRequest>,
    /// 检查定时器
    check_timer: tokio::time::Interval,
}

/// 渐进式启动 Actor
///
/// 独立运行的 actor，负责定期检测并自动启动新 worker
struct ProgressiveLauncherActor {
    /// 内部逻辑管理器
    logic: ProgressiveLauncherLogic,
    /// 下载状态参数
    params: ProgressiveLauncherParams,
    /// 通信通道
    channels: ActorChannels,
}

impl ProgressiveLauncherActor {
    /// 创建新的 actor
    fn new(
        params: ProgressiveLauncherParams,
        shutdown_rx: lite::Receiver<()>,
        launch_request_tx: mpsc::Sender<WorkerLaunchRequest>,
    ) -> Self {
        let logic = ProgressiveLauncherLogic::new(&params.config);
        let check_interval = params.config.speed().instant_speed_window();

        info!(
            "渐进式启动配置: 目标 {} workers, 阶段: {:?}, 启动偏移: {:?}",
            params.config.concurrency().worker_count(),
            logic.worker_launch_stages,
            params.start_offset
        );

        let start_time = tokio::time::Instant::now() + params.start_offset;
        let check_timer = tokio::time::interval_at(start_time, check_interval);

        let channels = ActorChannels {
            shutdown_rx,
            launch_request_tx,
            check_timer,
        };

        Self {
            logic,
            params,
            channels,
        }
    }

    /// 运行 actor 事件循环（使用 tokio::select!）
    async fn run(mut self) {
        debug!("ProgressiveLauncherActor started");

        loop {
            tokio::select! {
                // 内部定时器：自主触发检查
                _ = self.channels.check_timer.tick() => {
                    if !self.check_and_launch().await {
                        break;
                    }
                }
                // 外部消息
                msg = &mut self.channels.shutdown_rx => {
                    match msg {
                        Ok(_) => {
                            debug!("ProgressiveLauncherActor shutting down");
                            break;
                        }
                        Err(e) => {
                            // Channel 已关闭
                            debug!("ProgressiveLauncherActor message channel closed: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }

        debug!("ProgressiveLauncherActor stopped");
    }

    /// 检查并尝试启动下一批 worker
    /// 
    /// 返回 `true` 表示应继续运行，`false` 表示应终止
    async fn check_and_launch(&mut self) -> bool {
        // 检查是否还有下一批待启动
        if !self.logic.should_check_next_stage() {
            return true;
        }

        let decision = {
            let aggregated_stats = self.params.aggregated_stats.load();
            let bytes_summary = aggregated_stats.get_bytes_summary();

            // 检查是否下载已完成
            if bytes_summary.written_bytes >= self.params.total_size {
                debug!("下载已完成，ProgressiveLauncherActor 自动终止");
                return false;
            }

            // 检测并调整启动阶段（应对 worker 数量变化）
            let current_worker_count = aggregated_stats.len() as u64;
            self.logic
                .adjust_stage_for_worker_count(current_worker_count);

            // 从聚合统计获取信息并决策
            self.logic.decide_next_launch(
                &*aggregated_stats,
                bytes_summary.written_bytes,
                self.params.total_size,
                &self.params.config,
            )
        };

        // 根据决策结果执行相应操作
        match decision {
            LaunchDecision::Launch {
                workers_to_add,
                speeds,
            } => {
                let next_target = self.logic.worker_launch_stages[self.logic.next_launch_stage];
                let formatted_speeds = ProgressiveLauncherLogic::format_speeds(
                    &speeds,
                    self.params.config.speed().size_standard(),
                );
                info!(
                    "渐进式启动 - 第{}批: 所有worker速度达标 (当前速度: {})，准备启动 {} 个新 workers (总计 {} 个)",
                    self.logic.next_launch_stage + 1,
                    formatted_speeds,
                    workers_to_add,
                    next_target
                );

                let request = WorkerLaunchRequest {
                    count: workers_to_add,
                    stage: self.logic.next_launch_stage,
                };

                if let Err(e) = self.channels.launch_request_tx.send(request).await {
                    error!("发送 worker 启动请求失败: {:?}", e);
                } else {
                    // 成功发送后推进到下一阶段
                    self.logic.next_launch_stage += 1;
                }
            }
            LaunchDecision::Wait(reason) => {
                self.log_wait_reason(&reason);
            }
            LaunchDecision::Skip => {
                // 无需启动，继续等待
            }
        }

        true
    }

    /// 记录等待原因的日志
    fn log_wait_reason(&self, reason: &WaitReason) {
        match reason {
            WaitReason::InsufficientSpeed { speeds, threshold } => {
                let formatted_speeds = ProgressiveLauncherLogic::format_speeds(
                    speeds,
                    self.params.config.speed().size_standard(),
                );
                match threshold {
                    Some(threshold) => {
                        let threshold_formatted = DownloadSpeed::from_raw(*threshold)
                            .to_formatted(self.params.config.speed().size_standard());
                        debug!(
                            "渐进式启动 - 等待第{}批worker速度达标 (当前速度: {}, 阈值: {})",
                            self.logic.next_launch_stage + 1,
                            formatted_speeds,
                            threshold_formatted
                        );
                    }
                    None => debug!(
                        "渐进式启动 - 等待第{}批worker速度达标 (当前速度: {})",
                        self.logic.next_launch_stage + 1,
                        formatted_speeds
                    ),
                };
            }
            WaitReason::TimeRemainsTooShort {
                estimated_secs,
                threshold_secs,
            } => {
                info!(
                    "渐进式启动 - 预期剩余时间 {:.1}s < 阈值 {:.1}s，不启动新 workers",
                    estimated_secs, threshold_secs
                );
            }
        }
    }
}

/// 渐进式启动管理器 Handle
///
/// 提供与 ProgressiveLauncherActor 通信的接口
pub(super) struct ProgressiveLauncher {
    /// 初始 worker 数量
    initial_worker_count: u64,
    /// Actor 任务句柄
    actor_handle: Option<tokio::task::JoinHandle<()>>,
    /// 关闭发送器
    shutdown_tx: lite::Sender<()>,
}

impl ProgressiveLauncher {
    /// 创建新的渐进式启动管理器（启动 actor）
    ///
    /// 返回 `(Self, mpsc::Receiver<WorkerLaunchRequest>)`，调用者需持有接收器
    pub(super) fn new(
        params: ProgressiveLauncherParams,
    ) -> (Self, mpsc::Receiver<WorkerLaunchRequest>) {
        // 先创建临时逻辑对象获取初始 worker 数量
        let temp_logic = ProgressiveLauncherLogic::new(&params.config);
        let initial_worker_count = temp_logic.initial_worker_count();

        // 使用 oneshot channel
        let (shutdown_tx, shutdown_rx) = lite::channel();
        let (launch_request_tx, launch_request_rx) = mpsc::channel(10);

        // 启动 actor 任务
        let actor_handle = tokio::spawn(async move {
            ProgressiveLauncherActor::new(params, shutdown_rx, launch_request_tx)
                .run()
                .await;
        });

        let launcher = Self {
            shutdown_tx,
            initial_worker_count,
            actor_handle: Some(actor_handle),
        };

        (launcher, launch_request_rx)
    }

    /// 获取第一批 Worker 的数量
    pub(super) fn initial_worker_count(&self) -> u64 {
        self.initial_worker_count
    }

    /// 关闭 actor 并等待其完全停止
    ///
    /// 这个方法会消耗 self，确保 actor 完全停止并释放所有引用
    pub(super) async fn shutdown_and_wait(mut self) {
        // 发送关闭消息
        let _ = self.shutdown_tx.notify(());

        // 等待 actor 任务完成
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
            debug!("ProgressiveLauncher actor has fully stopped");
        }
    }
}
