//! 渐进式 Worker 启动模块（Actor 模式）
//!
//! 负责管理 Worker 的分阶段启动逻辑，根据当前 Worker 的速度表现决定何时启动下一批 Worker
//! 采用 Actor 模式，完全独立于主下载循环
//!
//! # 核心逻辑
//! 用户配置：最大下载线程数 (`max`) 和 预期最低单线程速度 (`min_speed`)
//! 启动条件：`current_threads < max` 且 `total_speed > ((current_threads + 1) * min_speed)`
//! 启动数量：`floor(total_speed / min_speed - current_threads)`

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
    /// 批次编号（用于日志追踪）
    pub batch_id: usize,
}

/// 启动决策结果
#[derive(Debug)]
enum LaunchDecision {
    /// 启动下一批 worker
    Launch {
        /// 要启动的 worker 数量
        workers_to_add: u64,
        /// 当前总速度
        total_speed: DownloadSpeed,
    },
    /// 等待条件满足
    Wait {
        /// 当前总速度
        current_speed: DownloadSpeed,
        /// 要求的速度阈值
        required_speed: u64,
    },
    /// 不启动（已达最大线程数或无需启动）
    Skip,
}

/// 渐进式启动管理器（内部逻辑）
struct ProgressiveLauncherLogic {
    /// 当前批次计数
    next_batch_id: usize,
}

impl ProgressiveLauncherLogic {
    /// 创建新的渐进式启动逻辑管理器
    fn new() -> Self {
        Self {
            next_batch_id: 1, // 0 是初始启动，后续从 1 开始
        }
    }

    /// 决策是否启动下一批 worker
    fn decide_next_launch(
        &self,
        aggregated_stats: &AggregatedStats,
        config: &DownloadConfig,
    ) -> LaunchDecision {
        let max_threads = config.progressive().max_concurrent_downloads();
        let min_speed = config.progressive().min_speed_per_thread().get();
        let current_threads = aggregated_stats.running_count() as u64;

        // 1. 检查是否达到或超过最大线程数
        if current_threads >= max_threads {
            return LaunchDecision::Skip;
        }

        // 2. 获取当前总速度（窗口平均）
        let total_speed = match aggregated_stats.get_total_window_avg_speed() {
            Some(s) => s,
            None => return LaunchDecision::Skip, // 暂无速度数据
        };
        let total_speed_val = total_speed.as_u64();

        // 3. 核心逻辑检测
        // 阈值 = (当前线程数 + 1) * 预期最低单线程速度
        // 当总速度 > 阈值 时，说明带宽还有余量，可以增加线程
        let threshold = (current_threads + 1).saturating_mul(min_speed);

        if total_speed_val > threshold {
            // 计算建议的总线程数 = 总速度 / 预期最低单线程速度
            // 启动数量 = 建议总线程数 - 当前线程数
            let suggested_total = total_speed_val / min_speed;
            let workers_to_add = suggested_total.saturating_sub(current_threads);

            if workers_to_add > 0 {
                // 限制不超过最大配置
                let space_left = max_threads.saturating_sub(current_threads);
                let final_add = workers_to_add.min(space_left);

                if final_add > 0 {
                    return LaunchDecision::Launch {
                        workers_to_add: final_add,
                        total_speed,
                    };
                }
            }
        }

        // 记录等待原因（仅当速度不足时）
        LaunchDecision::Wait {
            current_speed: total_speed,
            required_speed: threshold,
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
        let logic = ProgressiveLauncherLogic::new();
        let check_interval = params.config.speed().instant_speed_window();

        info!(
            "渐进式启动配置: 最大 {} workers, 预期最低单线程速度 {}, 启动偏移: {:?}",
            params.config.progressive().max_concurrent_downloads(),
            DownloadSpeed::from_raw(params.config.progressive().min_speed_per_thread().get())
                .to_formatted(params.config.speed().size_standard()),
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
        let decision = {
            let aggregated_stats = self.params.aggregated_stats.load();
            let bytes_summary = aggregated_stats.get_bytes_summary();

            // 检查是否下载已完成
            if bytes_summary.written_bytes >= self.params.total_size {
                debug!("下载已完成，ProgressiveLauncherActor 自动终止");
                return false;
            }

            // 从聚合统计获取信息并决策
            self.logic
                .decide_next_launch(&aggregated_stats, &self.params.config)
        };

        // 根据决策结果执行相应操作
        match decision {
            LaunchDecision::Launch {
                workers_to_add,
                total_speed,
            } => {
                let formatted_speed =
                    total_speed.to_formatted(self.params.config.speed().size_standard());

                info!(
                    "渐进式启动 - 批次 #{}: 速度充足 ({})，正在启动 {} 个新 workers",
                    self.logic.next_batch_id, formatted_speed, workers_to_add,
                );

                let request = WorkerLaunchRequest {
                    count: workers_to_add,
                    batch_id: self.logic.next_batch_id,
                };

                if let Err(e) = self.channels.launch_request_tx.send(request).await {
                    error!("发送 worker 启动请求失败: {:?}", e);
                } else {
                    // 成功发送后增加批次计数
                    self.logic.next_batch_id += 1;
                }
            }
            LaunchDecision::Wait {
                current_speed,
                required_speed,
            } => {
                let formatted_current =
                    current_speed.to_formatted(self.params.config.speed().size_standard());
                let formatted_required = DownloadSpeed::from_raw(required_speed)
                    .to_formatted(self.params.config.speed().size_standard());

                // 只有在开启 debug 时才频繁打印等待日志，避免刷屏
                debug!(
                    "渐进式启动 - 速度未达增加阈值: 当前 {}, 需要 > {}",
                    formatted_current, formatted_required
                );
            }
            LaunchDecision::Skip => {
                // 无需启动，继续等待
            }
        }

        true
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
        // 从配置获取初始 worker 数量
        let initial_worker_count = params.config.progressive().initial_worker_count();

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
        self.shutdown_tx.send_unchecked(());

        // 等待 actor 任务完成
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
            debug!("ProgressiveLauncher actor has fully stopped");
        }
    }
}
