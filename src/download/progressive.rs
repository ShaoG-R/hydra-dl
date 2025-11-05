//! 渐进式 Worker 启动模块
//! 
//! 负责管理 Worker 的分阶段启动逻辑，根据当前 Worker 的速度表现决定何时启动下一批 Worker

use crate::Result;
use crate::config::DownloadConfig;
use crate::pool::download::DownloadWorkerPool;
use crate::utils::io_traits::{AsyncFile, HttpClient};
use super::task_allocator::TaskAllocator;
use log::{debug, error, info};

/// 渐进式启动管理器
/// 
/// 封装了 Worker 渐进式启动的状态和逻辑
pub(super) struct ProgressiveLauncher {
    /// 渐进式启动阶段序列（预计算的目标worker数量序列，如 [3, 6, 9, 12]）
    worker_launch_stages: Vec<usize>,
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
        let worker_launch_stages: Vec<usize> = config.progressive().worker_ratios()
            .iter()
            .map(|&ratio| {
                let stage_count = ((total_worker_count as f64 * ratio).ceil() as usize).min(total_worker_count);
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
    pub(super) fn initial_worker_count(&self) -> usize {
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
    
    /// 检查当前 Worker 速度并决定是否启动下一批
    /// 
    /// 此方法会检查所有已启动 Worker 的实时速度，如果都达到配置的阈值，
    /// 则启动下一批 Worker 并为其分配初始任务
    /// 
    /// # Arguments
    /// 
    /// * `pool` - Worker 协程池
    /// * `client` - HTTP 客户端（用于创建新 Worker）
    /// * `config` - 下载配置
    /// * `task_allocator` - 任务分配器（用于为新 Worker 分配任务）
    /// 
    /// # Returns
    /// 
    /// 成功返回 Ok(())，失败返回错误
    pub(super) async fn check_and_launch_next_stage<C, F>(
        &mut self,
        pool: &mut DownloadWorkerPool<F>,
        client: C,
        config: &DownloadConfig,
        task_allocator: &mut TaskAllocator,
    ) -> Result<()>
    where
        C: HttpClient + Clone + Send + 'static,
        F: AsyncFile + 'static,
    {
        let current_worker_count = pool.worker_count();
        
        // 检查所有已启动 Worker 的速度是否达到阈值
        let mut all_ready = true;
        let mut speeds = Vec::with_capacity(current_worker_count);
        
        for worker_id in 0..current_worker_count {
            let (instant_speed, valid) = pool.get_worker_instant_speed(worker_id)
                .unwrap_or((0.0, false));
            speeds.push(instant_speed);
            
            // 所有worker的速度都必须有效且达到阈值
            if !valid || instant_speed < config.progressive().min_speed_threshold() as f64 {
                all_ready = false;
            }
        }
        
        if all_ready {
            // 启动下一批worker
            let next_target = self.worker_launch_stages[self.next_launch_stage];
            let workers_to_add = next_target - current_worker_count;
            
            if workers_to_add > 0 {
                info!(
                    "渐进式启动 - 第{}批: 所有已启动worker速度达标 ({:?} bytes/s >= {} bytes/s)，启动 {} 个新 workers (总计 {} 个)",
                    self.next_launch_stage + 1,
                    speeds,
                    config.progressive().min_speed_threshold(),
                    workers_to_add,
                    next_target
                );
                
                // 动态添加新 worker
                if let Err(e) = pool.add_workers(client.clone(), workers_to_add).await {
                    error!("添加新 workers 失败: {:?}", e);
                    // 继续执行，不影响已启动的 worker
                } else {
                    // 为新启动的worker加入队列并分配任务
                    for worker_id in current_worker_count..next_target {
                        // 将新 worker 加入空闲队列
                        task_allocator.mark_worker_idle(worker_id);
                        
                        let chunk_size = pool.get_worker_chunk_size(worker_id);
                        
                        if let Some(allocated) = task_allocator.try_allocate_task_to_idle_worker(chunk_size) {
                            let (task, assigned_worker, _cancel_tx) = allocated.into_parts();
                            info!("为新启动的 Worker #{} 分配任务，分块大小 {} bytes", assigned_worker, chunk_size);
                            if let Err(e) = pool.send_task(task, assigned_worker).await {
                                error!("为新 worker 分配任务失败: {:?}", e);
                                // 失败了，将 worker 放回队列
                                task_allocator.mark_worker_idle(assigned_worker);
                            }
                        } else {
                            debug!("没有足够的数据为新 worker #{} 分配任务", worker_id);
                            break;
                        }
                    }
                    
                    self.next_launch_stage += 1;
                }
            }
        } else {
            debug!(
                "渐进式启动 - 等待第{}批worker速度达标 (当前速度: {:?} bytes/s, 阈值: {} bytes/s)",
                self.next_launch_stage + 1,
                speeds,
                config.progressive().min_speed_threshold()
            );
        }
        
        Ok(())
    }
}

