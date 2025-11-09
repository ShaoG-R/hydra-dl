//! 渐进式 Worker 启动模块
//! 
//! 负责管理 Worker 的分阶段启动逻辑，根据当前 Worker 的速度表现决定何时启动下一批 Worker

use crate::Result;
use crate::config::DownloadConfig;
use log::{debug, info};

/// 渐进式启动决策结果
/// 
/// 表示是否应该启动下一批 worker 的决策结果
#[derive(Debug, Clone)]
pub(super) enum LaunchDecision {
    /// 等待，不启动新 worker
    Wait { 
        /// 等待原因
        #[allow(dead_code)]
        reason: String 
    },
    /// 启动新 worker
    Launch { 
        /// 要启动的 worker 数量
        count: u64,
        /// 当前阶段索引
        stage: usize,
    },
    /// 渐进式启动已完成（所有阶段都已启动）
    Complete,
}

/// Worker 启动执行器 Trait
/// 
/// 定义渐进式启动器所需的外部能力接口，实现决策逻辑与执行逻辑的解耦
pub(super) trait WorkerLaunchExecutor {
    /// 获取当前 worker 数量
    fn current_worker_count(&self) -> u64;
    
    /// 获取所有活跃的 worker ID 列表
    /// 
    /// # Returns
    /// 
    /// 返回当前所有活跃 worker 的 ID 列表
    fn get_all_worker_ids(&self) -> Vec<u64>;
    
    /// 获取指定 worker 的瞬时速度
    /// 
    /// # Returns
    /// 
    /// 返回 `Some((速度, 是否有效))`，如果 worker 不存在则返回 None
    fn get_worker_instant_speed(&self, worker_id: u64) -> Option<(f64, bool)>;
    
    /// 获取总体窗口平均速度
    /// 
    /// # Returns
    /// 
    /// 返回 `(速度, 是否有效)`
    fn get_total_window_avg_speed(&self) -> (f64, bool);
    
    /// 获取下载进度
    /// 
    /// # Returns
    /// 
    /// 返回 `(已写入字节, 总字节)`
    fn get_download_progress(&self) -> (u64, u64);
    
    /// 执行 worker 启动
    /// 
    /// # Arguments
    /// 
    /// * `count` - 要启动的 worker 数量
    /// * `stage` - 当前阶段索引
    /// 
    /// # Returns
    /// 
    /// 成功返回新分配任务的取消通道列表 `Vec<(worker_id, cancel_tx)>`
    async fn execute_worker_launch(
        &mut self, 
        count: u64, 
        stage: usize,
    ) -> Result<Vec<(u64, tokio::sync::oneshot::Sender<()>)>>;
}

/// 渐进式启动管理器
/// 
/// 封装了 Worker 渐进式启动的状态和逻辑
pub(super) struct ProgressiveLauncher {
    /// 渐进式启动阶段序列（预计算的目标worker数量序列，如 [3, 6, 9, 12]）
    worker_launch_stages: Vec<u64>,
    /// 下一个启动阶段的索引（0 表示已完成第一批，1 表示准备启动第二批）
    next_launch_stage: usize,
}

impl ProgressiveLauncher {
    /// 创建新的渐进式启动管理器
    /// 
    /// # Arguments
    /// 
    /// * `config` - 下载配置，包含 worker 总数和启动比例序列
    /// 
    /// # Returns
    /// 
    /// 返回初始化好的 ProgressiveLauncher 实例
    pub(super) fn new(config: &DownloadConfig) -> Self {
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
        
        info!(
            "渐进式启动配置: 目标 {} workers, 阶段: {:?}",
            total_worker_count, worker_launch_stages
        );
        
        Self {
            worker_launch_stages,
            next_launch_stage: 1, // 第一批已启动，下一个是第二批（索引1）
        }
    }
    
    /// 获取第一批 Worker 的数量
    /// 
    /// # Returns
    /// 
    /// 返回初始启动的 Worker 数量
    pub(super) fn initial_worker_count(&self) -> u64 {
        self.worker_launch_stages[0]
    }
    
    /// 检查是否还有下一批 Worker 待启动
    /// 
    /// # Returns
    /// 
    /// 如果还有未启动的批次返回 true，否则返回 false
    pub(super) fn should_check_next_stage(&self) -> bool {
        self.next_launch_stage < self.worker_launch_stages.len()
    }
    
    /// 推进到下一个启动阶段
    /// 
    /// 此方法应在成功执行 worker 启动后调用
    pub(super) fn advance_stage(&mut self) {
        self.next_launch_stage += 1;
    }
    
    /// 决策是否启动下一批 worker（纯决策逻辑，不执行）
    /// 
    /// 检查所有已启动 Worker 的实时速度，并决定是否应该启动下一批
    /// 
    /// 此方法会：
    /// - 检测 worker 数量变化并建议降级批次
    /// - 检查速度是否达标
    /// - 检测预期剩余下载时间，避免在下载即将结束时启动新 worker
    /// 
    /// # Arguments
    /// 
    /// * `executor` - 实现了 WorkerLaunchExecutor trait 的执行器
    /// * `config` - 下载配置
    /// 
    /// # Returns
    /// 
    /// 返回 LaunchDecision 表示决策结果
    pub(super) fn decide_next_launch<E: WorkerLaunchExecutor>(
        &self,
        executor: &E,
        config: &DownloadConfig,
    ) -> LaunchDecision {
        let current_worker_count = executor.current_worker_count();
        
        // 检查是否所有阶段已完成
        if self.next_launch_stage >= self.worker_launch_stages.len() {
            return LaunchDecision::Complete;
        }
        
        // 检查所有已启动 Worker 的速度是否达到阈值
        let mut all_ready = true;
        let worker_ids = executor.get_all_worker_ids();
        let mut speeds = Vec::with_capacity(worker_ids.len());
        
        // 遍历所有实际的 worker_id
        for worker_id in worker_ids {
            let (instant_speed, valid) = executor.get_worker_instant_speed(worker_id)
                .unwrap_or((0.0, false));
            speeds.push(instant_speed);
            
            // 所有worker的速度都必须有效且达到阈值
            if !valid || instant_speed < config.progressive().min_speed_threshold() as f64 {
                all_ready = false;
            }
        }
        
        if !all_ready {
            let reason = format!(
                "等待第{}批worker速度达标 (当前速度: {:?} bytes/s, 阈值: {} bytes/s)",
                self.next_launch_stage + 1,
                speeds,
                config.progressive().min_speed_threshold()
            );
            debug!("渐进式启动 - {}", reason);
            return LaunchDecision::Wait { reason };
        }
        
        // 检查预期剩余下载时间，避免在即将完成时启动新 worker
        let (window_avg_speed, speed_valid) = executor.get_total_window_avg_speed();
        if speed_valid && window_avg_speed > 0.0 {
            let (written_bytes, total_bytes) = executor.get_download_progress();
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
                let reason = format!(
                    "预期剩余时间 {:.1}s < 阈值 {:.1}s",
                    estimated_time_left,
                    threshold
                );
                info!("渐进式启动 - {}，不启动新 workers", reason);
                return LaunchDecision::Wait { reason };
            }
        }
        
        // 决定启动下一批
        let next_target = self.worker_launch_stages[self.next_launch_stage];
        let workers_to_add = next_target - current_worker_count;
        
        if workers_to_add > 0 {
            info!(
                "渐进式启动 - 第{}批: 所有已启动worker速度达标 ({:?} bytes/s >= {} bytes/s)，准备启动 {} 个新 workers (总计 {} 个)",
                self.next_launch_stage + 1,
                speeds,
                config.progressive().min_speed_threshold(),
                workers_to_add,
                next_target
            );
            
            LaunchDecision::Launch {
                count: workers_to_add,
                stage: self.next_launch_stage,
            }
        } else {
            LaunchDecision::Wait {
                reason: "没有需要启动的新 worker".to_string(),
            }
        }
    }
    
    /// 检测并调整启动阶段（应对 worker 数量变化）
    /// 
    /// 当 worker 数量少于预期时（如部分 worker 被手动关闭），自动降级到合适的阶段
    pub(super) fn adjust_stage_for_worker_count(&mut self, current_worker_count: u64) {
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

