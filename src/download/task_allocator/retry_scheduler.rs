//! 重试调度模块
//!
//! 负责管理失败任务的定时重试

use super::task_queue::FailedTaskInfo;
use crate::{DownloadError, Result};
use kestrel_timer::{TaskId, TimerService, TimerTask};
use log::{debug, error, info};
use ranged_mmap::AllocatedRange;
use rustc_hash::FxHashMap;
use std::time::Duration;

/// 重试配置
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 最大重试次数
    max_retry_count: usize,
    /// 重试延迟序列
    retry_delays: Vec<Duration>,
}

impl RetryConfig {
    /// 从下载配置创建
    pub fn from_download_config(config: &crate::config::DownloadConfig) -> Self {
        Self {
            max_retry_count: config.retry().max_retry_count(),
            retry_delays: config.retry().retry_delays().to_vec(),
        }
    }

    /// 获取指定重试次数对应的延迟
    pub fn get_delay(&self, retry_count: usize) -> Duration {
        self.retry_delays
            .get(retry_count)
            .copied()
            .or_else(|| self.retry_delays.last().copied())
            .unwrap_or(Duration::from_secs(3))
    }

    /// 检查是否超过最大重试次数
    #[inline]
    pub fn is_exhausted(&self, retry_count: usize) -> bool {
        retry_count >= self.max_retry_count
    }

    /// 获取最大重试次数
    #[inline]
    pub fn max_retry_count(&self) -> usize {
        self.max_retry_count
    }
}

/// 重试调度器
///
/// 管理失败任务的定时器注册和回调处理
pub struct RetryScheduler {
    /// 定时器服务
    timer_service: TimerService,
    /// 待重试的失败任务映射（定时器 TaskId -> 失败任务信息）
    pending_retries: FxHashMap<TaskId, FailedTaskInfo>,
    /// 重试配置
    config: RetryConfig,
}

impl RetryScheduler {
    /// 创建新的重试调度器
    pub fn new(timer_service: TimerService, config: RetryConfig) -> Self {
        Self {
            timer_service,
            pending_retries: FxHashMap::default(),
            config,
        }
    }

    /// 调度重试任务
    ///
    /// 返回 `Ok(true)` 表示成功调度
    /// 返回 `Ok(false)` 表示已达到最大重试次数
    /// 返回 `Err` 表示定时器注册失败
    pub fn schedule(&mut self, range: AllocatedRange, retry_count: usize) -> Result<bool> {
        let (start, end) = range.as_range_tuple();

        // 检查是否超过最大重试次数
        if self.config.is_exhausted(retry_count) {
            error!(
                "任务 range {}..{} 已达到最大重试次数 {}，标记为永久失败",
                start,
                end,
                self.config.max_retry_count()
            );
            return Ok(false);
        }

        // 计算重试延迟
        let delay = self.config.get_delay(retry_count);

        info!(
            "任务 range {}..{} 将在 {:.1}s 后进行第 {} 次重试",
            start,
            end,
            delay.as_secs_f64(),
            retry_count + 1
        );

        // 注册定时器
        self.register_timer(range, retry_count + 1, delay)?;

        Ok(true)
    }

    /// 注册定时器
    fn register_timer(
        &mut self,
        range: AllocatedRange,
        retry_count: usize,
        delay: Duration,
    ) -> Result<()> {
        let (start, end) = range.as_range_tuple();
        debug!(
            "注册重试定时器 range {}..{}, 重试次数 {}, 延迟 {:.1}s",
            start,
            end,
            retry_count,
            delay.as_secs_f64()
        );

        let task_handle = self.timer_service.allocate_handle();
        let task_id = task_handle.task_id();

        // 创建定时器任务（无回调，仅通知）
        let timer_task = TimerTask::new_oneshot(delay, None);

        // 注册到 TimerService
        self.timer_service
            .register(task_handle, timer_task)
            .map_err(|e| DownloadError::Other(format!("注册定时器失败: {:?}", e)))?;

        // 存储映射关系
        self.pending_retries
            .insert(task_id, FailedTaskInfo { range, retry_count });

        Ok(())
    }

    /// 处理定时器触发
    ///
    /// 返回对应的失败任务信息，如果定时器 ID 无效则返回 None
    pub fn on_timer_fired(&mut self, task_id: TaskId) -> Option<FailedTaskInfo> {
        let info = self.pending_retries.remove(&task_id)?;
        let (start, end) = info.range.as_range_tuple();
        info!(
            "定时器触发，重试任务 range {}..{}, 重试次数 {}",
            start, end, info.retry_count
        );
        Some(info)
    }

    /// 获取等待中的重试任务数量
    #[inline]
    pub fn pending_count(&self) -> usize {
        self.pending_retries.len()
    }

    /// 获取重试配置
    #[inline]
    #[allow(dead_code)]
    pub fn config(&self) -> &RetryConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kestrel_timer::{TimerWheel, config::ServiceConfig};
    use ranged_mmap::MmapFile;
    use std::num::NonZeroU64;
    use tempfile::NamedTempFile;

    fn create_test_config() -> RetryConfig {
        RetryConfig {
            max_retry_count: 3,
            retry_delays: vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
            ],
        }
    }

    fn allocate_test_range(size: u64) -> AllocatedRange {
        let temp_file = NamedTempFile::new().unwrap();
        let (_file, mut allocator) =
            MmapFile::create(temp_file.path(), NonZeroU64::new(size).unwrap()).unwrap();
        allocator.allocate(NonZeroU64::new(size).unwrap()).unwrap()
    }

    #[test]
    fn test_retry_config_delay() {
        let config = create_test_config();

        assert_eq!(config.get_delay(0), Duration::from_millis(100));
        assert_eq!(config.get_delay(1), Duration::from_millis(200));
        assert_eq!(config.get_delay(2), Duration::from_millis(400));
        // 超出范围使用最后一个
        assert_eq!(config.get_delay(10), Duration::from_millis(400));
    }

    #[test]
    fn test_retry_config_exhausted() {
        let config = create_test_config();

        assert!(!config.is_exhausted(0));
        assert!(!config.is_exhausted(2));
        assert!(config.is_exhausted(3));
        assert!(config.is_exhausted(10));
    }

    #[tokio::test]
    async fn test_schedule_success() {
        let timer = TimerWheel::with_defaults();
        let timer_service = timer.create_service(ServiceConfig::default());
        let config = create_test_config();
        let mut scheduler = RetryScheduler::new(timer_service, config);

        let range = allocate_test_range(100);
        let result = scheduler.schedule(range, 0);

        assert!(result.is_ok());
        assert!(result.unwrap()); // 成功调度
        assert_eq!(scheduler.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_schedule_exhausted() {
        let timer = TimerWheel::with_defaults();
        let timer_service = timer.create_service(ServiceConfig::default());
        let config = create_test_config();
        let mut scheduler = RetryScheduler::new(timer_service, config);

        let range = allocate_test_range(100);
        let result = scheduler.schedule(range, 3); // 已达到最大次数

        assert!(result.is_ok());
        assert!(!result.unwrap()); // 不再调度
        assert_eq!(scheduler.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_timer_fired() {
        let timer = TimerWheel::with_defaults();
        let scheduler_service = timer.create_service(ServiceConfig::default());
        let config = create_test_config();
        let mut scheduler = RetryScheduler::new(scheduler_service, config);

        let range = allocate_test_range(100);
        scheduler.schedule(range, 1).unwrap();

        // 等待定时器触发
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 验证 pending count 增加了
        assert_eq!(scheduler.pending_count(), 1);
    }
}
