//! 渐进式 Worker 启动模块（Actor 模式）
//! 
//! 负责管理 Worker 的分阶段启动逻辑，根据当前 Worker 的速度表现决定何时启动下一批 Worker
//! 采用 Actor 模式，完全独立于主下载循环

use crate::config::DownloadConfig;
use log::{debug, info, error};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use arc_swap::ArcSwap;
use crate::pool::download::DownloadWorkerHandle;

/// Actor 消息类型
#[derive(Debug)]
enum ActorMessage {
    /// 请求启动下一批 worker（如果条件满足）
    #[allow(dead_code)]
    TryLaunchNext,
    /// 关闭 actor
    Shutdown,
}

/// Worker 启动请求
#[derive(Debug, Clone)]
pub(super) struct WorkerLaunchRequest {
    /// 要启动的 worker 数量
    pub count: u64,
    /// 当前阶段索引
    pub stage: usize,
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
    /// 创建新的渐进式启动逻辑管理器
    fn new(config: &DownloadConfig) -> Self {
        let total_worker_count = config.concurrency().worker_count();
        
        // 根据配置的比例序列计算渐进式启动阶段
        let worker_launch_stages: Vec<u64> = config.progressive().worker_ratios()
            .iter()
            .map(|&ratio| {
                let stage_count = ((total_worker_count as f64 * ratio).ceil() as u64).min(total_worker_count);
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
    
    /// 决策是否启动下一批 worker（纯决策逻辑，不执行）
    /// 
    /// 检查所有已启动 Worker 的实时速度，并决定是否应该启动下一批
    fn decide_next_launch<C: crate::utils::io_traits::HttpClient>(
        &self,
        worker_handles: &im::HashMap<u64, DownloadWorkerHandle<C>>,
        written_bytes: u64,
        total_bytes: u64,
        global_stats: &crate::utils::stats::TaskStats,
        config: &DownloadConfig,
    ) -> Option<WorkerLaunchRequest> {
        let current_worker_count = worker_handles.len() as u64;
        
        // 检查是否所有阶段已完成
        if self.next_launch_stage >= self.worker_launch_stages.len() {
            return None;
        }
        
        // 检查所有已启动 Worker 的速度是否达到阈值
        let mut all_ready = true;
        let mut speeds = Vec::with_capacity(worker_handles.len());
        
        // 遍历所有实际的 worker
        for (_, handle) in worker_handles.iter() {
            let (instant_speed, valid) = handle.instant_speed();
            speeds.push(instant_speed);
            
            // 所有worker的速度都必须有效且达到阈值
            if !valid || instant_speed < config.progressive().min_speed_threshold() as f64 {
                all_ready = false;
            }
        }
        
        if !all_ready {
            debug!(
                "渐进式启动 - 等待第{}批worker速度达标 (当前速度: {:?} bytes/s, 阈值: {} bytes/s)",
                self.next_launch_stage + 1,
                speeds,
                config.progressive().min_speed_threshold()
            );
            return None;
        }
        
        // 检查预期剩余下载时间，避免在即将完成时启动新 worker
        let (window_avg_speed, speed_valid) = global_stats.get_window_avg_speed();
        if speed_valid && window_avg_speed > 0.0 {
            let remaining_bytes = total_bytes.saturating_sub(written_bytes);
            let estimated_time_left = remaining_bytes as f64 / window_avg_speed;
            let threshold = config.progressive().min_time_before_finish().as_secs_f64();
            
            debug!(
                "渐进式启动 - 剩余时间检测: 剩余 {} bytes, 窗口平均速度 {:.2} MB/s, 预期剩余 {:.1}s, 阈值 {:.1}s",
                remaining_bytes,
                window_avg_speed / 1024.0 / 1024.0,
                estimated_time_left,
                threshold
            );
            
            if estimated_time_left < threshold {
                info!(
                    "渐进式启动 - 预期剩余时间 {:.1}s < 阈值 {:.1}s，不启动新 workers",
                    estimated_time_left,
                    threshold
                );
                return None;
            }
        }
        
        // 决定启动下一批
        let next_target = self.worker_launch_stages[self.next_launch_stage];
        let workers_to_add = next_target.saturating_sub(current_worker_count);
        
        if workers_to_add > 0 {
            info!(
                "渐进式启动 - 第{}批: 所有已启动worker速度达标 ({:?} bytes/s >= {} bytes/s)，准备启动 {} 个新 workers (总计 {} 个)",
                self.next_launch_stage + 1,
                speeds,
                config.progressive().min_speed_threshold(),
                workers_to_add,
                next_target
            );
            
            Some(WorkerLaunchRequest {
                count: workers_to_add,
                stage: self.next_launch_stage,
            })
        } else {
            None
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

/// 渐进式启动 Actor
/// 
/// 独立运行的 actor，负责定期检测并自动启动新 worker
struct ProgressiveLauncherActor<C: crate::utils::io_traits::HttpClient> {
    /// 内部逻辑管理器
    logic: ProgressiveLauncherLogic,
    /// 配置
    config: Arc<DownloadConfig>,
    /// 消息接收器
    message_rx: mpsc::Receiver<ActorMessage>,
    /// 共享的 worker handles
    worker_handles: Arc<ArcSwap<im::HashMap<u64, DownloadWorkerHandle<C>>>>,
    /// 文件总大小
    total_size: u64,
    /// 已写入字节数（共享引用）
    written_bytes: Arc<AtomicU64>,
    /// 全局统计管理器
    global_stats: Arc<crate::utils::stats::TaskStats>,
    /// Worker 启动请求发送器
    launch_request_tx: mpsc::Sender<WorkerLaunchRequest>,
    /// 检查定时器（内部管理）
    check_timer: tokio::time::Interval,
}

impl<C: crate::utils::io_traits::HttpClient> ProgressiveLauncherActor<C> {
    /// 创建新的 actor
    async fn new(
        config: Arc<DownloadConfig>,
        message_rx: mpsc::Receiver<ActorMessage>,
        worker_handles: Arc<ArcSwap<im::HashMap<u64, DownloadWorkerHandle<C>>>>,
        total_size: u64,
        written_bytes: Arc<AtomicU64>,
        global_stats: Arc<crate::utils::stats::TaskStats>,
        launch_request_tx: mpsc::Sender<WorkerLaunchRequest>,
        check_interval: std::time::Duration,
    ) -> Self {
        let logic = ProgressiveLauncherLogic::new(&config);
        
        info!(
            "渐进式启动配置: 目标 {} workers, 阶段: {:?}",
            config.concurrency().worker_count(),
            logic.worker_launch_stages
        );
        
        let mut check_timer = tokio::time::interval(check_interval);
        check_timer.tick().await; // 跳过首次立即触发
        
        Self {
            logic,
            config,
            message_rx,
            worker_handles,
            total_size,
            written_bytes,
            global_stats,
            launch_request_tx,
            check_timer,
        }
    }
    
    /// 运行 actor 事件循环（使用 tokio::select!）
    async fn run(mut self) {
        debug!("ProgressiveLauncherActor started");
        
        loop {
            tokio::select! {
                // 内部定时器：自主触发检查
                _ = self.check_timer.tick() => {
                    self.check_and_launch().await;
                }
                // 外部消息
                msg = self.message_rx.recv() => {
                    match msg {
                        Some(ActorMessage::TryLaunchNext) => {
                            self.check_and_launch().await;
                        }
                        Some(ActorMessage::Shutdown) => {
                            debug!("ProgressiveLauncherActor shutting down");
                            break;
                        }
                        None => {
                            // Channel 已关闭
                            debug!("ProgressiveLauncherActor message channel closed");
                            break;
                        }
                    }
                }
            }
        }
        
        debug!("ProgressiveLauncherActor stopped");
    }
    
    /// 检查并尝试启动下一批 worker
    async fn check_and_launch(&mut self) {
        // 检查是否还有下一批待启动
        if !self.logic.should_check_next_stage() {
            return;
        }
        
        // 检测并调整启动阶段（应对 worker 数量变化）
        let current_worker_count = self.worker_handles.load().len() as u64;
        self.logic.adjust_stage_for_worker_count(current_worker_count);
        
        // 从共享数据获取信息并决策
        let handles = self.worker_handles.load();
        let written = self.written_bytes.load(Ordering::SeqCst);
        let decision = self.logic.decide_next_launch(
            &*handles,
            written,
            self.total_size,
            &*self.global_stats,
            &self.config,
        );
        
        // 如果决策是启动，发送启动请求并推进阶段
        if let Some(request) = decision {
            if let Err(e) = self.launch_request_tx.send(request).await {
                error!("发送 worker 启动请求失败: {:?}", e);
            } else {
                // 成功发送后推进到下一阶段
                self.logic.next_launch_stage += 1;
            }
        }
    }
}

/// 渐进式启动管理器 Handle
/// 
/// 提供与 ProgressiveLauncherActor 通信的接口
pub(super) struct ProgressiveLauncher<C: crate::utils::io_traits::HttpClient> {
    /// 消息发送器
    message_tx: mpsc::Sender<ActorMessage>,
    /// Worker 启动请求接收器（仅在创建时持有，之后转移）
    launch_request_rx: Option<mpsc::Receiver<WorkerLaunchRequest>>,
    /// 初始 worker 数量
    initial_worker_count: u64,
    /// Actor 任务句柄
    actor_handle: Option<tokio::task::JoinHandle<()>>,
    /// PhantomData 用于持有泛型参数
    _phantom: std::marker::PhantomData<C>,
}

impl<C: crate::utils::io_traits::HttpClient> ProgressiveLauncher<C> {
    /// 创建新的渐进式启动管理器（启动 actor）
    pub(super) fn new(
        config: Arc<DownloadConfig>,
        worker_handles: Arc<ArcSwap<im::HashMap<u64, DownloadWorkerHandle<C>>>>,
        total_size: u64,
        written_bytes: Arc<AtomicU64>,
        global_stats: Arc<crate::utils::stats::TaskStats>,
        check_interval: std::time::Duration,
    ) -> Self {
        // 先创建临时逻辑对象获取初始 worker 数量
        let temp_logic = ProgressiveLauncherLogic::new(&config);
        let initial_worker_count = temp_logic.initial_worker_count();
        
        // 使用有界 channel，容量 10
        let (message_tx, message_rx) = mpsc::channel(10);
        let (launch_request_tx, launch_request_rx) = mpsc::channel(10);
        
        // 启动 actor 任务
        let actor_handle = tokio::spawn(async move {
            ProgressiveLauncherActor::new(
                config,
                message_rx,
                worker_handles,
                total_size,
                written_bytes,
                global_stats,
                launch_request_tx,
                check_interval,
            ).await.run().await;
        });
        
        Self {
            message_tx,
            launch_request_rx: Some(launch_request_rx),
            initial_worker_count,
            actor_handle: Some(actor_handle),
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// 获取第一批 Worker 的数量
    pub(super) fn initial_worker_count(&self) -> u64 {
        self.initial_worker_count
    }
    
    /// 取出启动请求接收器（只能调用一次）
    pub(super) fn take_launch_request_rx(&mut self) -> Option<mpsc::Receiver<WorkerLaunchRequest>> {
        self.launch_request_rx.take()
    }
    
    /// 关闭 actor 并等待其完全停止
    /// 
    /// 这个方法会消耗 self，确保 actor 完全停止并释放所有引用
    pub(super) async fn shutdown_and_wait(mut self) {
        // 发送关闭消息
        let _ = self.message_tx.send(ActorMessage::Shutdown).await;
        
        // 等待 actor 任务完成
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
            debug!("ProgressiveLauncher actor has fully stopped");
        }
    }
}

