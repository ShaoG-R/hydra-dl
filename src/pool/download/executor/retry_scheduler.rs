//! 重试调度器
//!
//! 基于任务计数的重试调度策略，管理待重试任务队列。
//! 失败后等待 n 个任务完成再重试，实现指数退避效果。

use crate::config::RetryConfig;
use log::debug;
use std::collections::BTreeMap;
use std::time::Duration;

/// 待重试的任务信息
#[derive(Debug)]
pub(crate) struct PendingRetry {
    /// 待重试的 Range
    pub range: ranged_mmap::AllocatedRange,
    /// 当前重试次数
    pub retry_count: usize,
}

/// 重试调度器
///
/// 管理待重试任务队列，基于任务计数决定何时重试。
/// 使用 BTreeMap 按目标任务 ID 排序，确保按序处理。
pub(crate) struct RetryScheduler {
    /// 待重试任务映射：target_task_id -> Vec<PendingRetry>
    pending_retries: BTreeMap<u64, Vec<PendingRetry>>,
    /// 重试配置
    retry_config: RetryConfig,
}

impl RetryScheduler {
    /// 创建新的重试调度器
    pub fn new(retry_config: RetryConfig) -> Self {
        Self {
            pending_retries: BTreeMap::new(),
            retry_config,
        }
    }

    /// 获取到期的重试任务
    ///
    /// 返回 target_task_id <= current_task_id 的第一个待重试任务
    pub fn get_due_task(&mut self, current_task_id: u64) -> Option<PendingRetry> {
        // 查找 target_task_id <= current_task_id 的第一个任务
        let due_task_id = self.pending_retries.keys().next().copied()?;
        if due_task_id > current_task_id {
            return None;
        }

        let tasks = self.pending_retries.get_mut(&due_task_id)?;
        let task = tasks.pop();
        if tasks.is_empty() {
            self.pending_retries.remove(&due_task_id);
        }
        task
    }

    /// 调度重试任务
    ///
    /// 根据重试次数计算等待任务数，将任务加入待重试队列
    pub fn schedule(
        &mut self,
        current_task_id: u64,
        range: ranged_mmap::AllocatedRange,
        retry_count: usize,
    ) {
        let wait_count = self.retry_config.get_wait_task_count(retry_count);
        let target_task_id = current_task_id + wait_count;

        debug!(
            "调度重试: range {}..{}, retry_count={}, wait_count={}, target_task_id={}",
            range.start(),
            range.end(),
            retry_count,
            wait_count,
            target_task_id
        );

        self.pending_retries
            .entry(target_task_id)
            .or_default()
            .push(PendingRetry { range, retry_count });
    }

    /// 将所有待重试任务提前到指定 task_id
    ///
    /// 当没有新任务可分配时调用，使所有待重试任务立即可执行
    pub fn advance_all(&mut self, target_task_id: u64) {
        let all_tasks: Vec<PendingRetry> = self
            .pending_retries
            .values_mut()
            .flat_map(|v| v.drain(..))
            .collect();

        self.pending_retries.clear();

        if !all_tasks.is_empty() {
            self.pending_retries.insert(target_task_id, all_tasks);
        }
    }

    /// 检查是否没有待重试任务
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending_retries.is_empty()
    }

    /// 获取待重试任务总数
    #[allow(dead_code)]
    pub fn pending_count(&self) -> usize {
        self.pending_retries.values().map(|v| v.len()).sum()
    }

    /// 获取重试延迟时间
    #[inline]
    pub fn retry_delay(&self) -> Duration {
        self.retry_config.retry_delay()
    }

    /// 获取最大重试次数
    #[inline]
    pub fn max_retry_count(&self) -> usize {
        self.retry_config.max_retry_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ranged_mmap::allocator::RangeAllocator;
    use std::time::Duration;

    fn create_test_config() -> RetryConfig {
        RetryConfig {
            retry_task_counts: vec![1, 2, 4],
            retry_delay: Duration::from_millis(100),
        }
    }

    /// 创建一个从 0 开始的模拟范围，size 应为 4K 对齐
    fn create_mock_range(size: u64) -> ranged_mmap::AllocatedRange {
        use ranged_mmap::allocator::concurrent::Allocator;
        use std::num::NonZeroU64;
        
        let allocator = Allocator::new(NonZeroU64::new(size).unwrap());
        allocator.allocate(NonZeroU64::new(size).unwrap()).unwrap()
    }

    #[test]
    fn test_new_scheduler_is_empty() {
        let scheduler = RetryScheduler::new(create_test_config());
        assert!(scheduler.is_empty());
        assert_eq!(scheduler.pending_count(), 0);
    }

    #[test]
    fn test_schedule_and_get_due_task() {
        let mut scheduler = RetryScheduler::new(create_test_config());
        let range = create_mock_range(4096);

        // 调度重试，retry_count=0 时 wait_count=1
        scheduler.schedule(0, range, 0);
        assert!(!scheduler.is_empty());
        assert_eq!(scheduler.pending_count(), 1);

        // task_id=0 时任务还未到期
        assert!(scheduler.get_due_task(0).is_none());

        // task_id=1 时任务到期
        let task = scheduler.get_due_task(1);
        assert!(task.is_some());
        assert_eq!(task.unwrap().retry_count, 0);
        assert!(scheduler.is_empty());
    }

    #[test]
    fn test_advance_all() {
        let mut scheduler = RetryScheduler::new(create_test_config());
        
        let range1 = create_mock_range(4096);
        let range2 = create_mock_range(4096);
        
        scheduler.schedule(0, range1, 0); // target_task_id = 1
        scheduler.schedule(0, range2, 1); // target_task_id = 2
        
        assert_eq!(scheduler.pending_count(), 2);

        // 提前所有任务到 task_id=0
        scheduler.advance_all(0);
        
        // 现在所有任务都应该在 task_id=0 可获取
        assert!(scheduler.get_due_task(0).is_some());
        assert!(scheduler.get_due_task(0).is_some());
        assert!(scheduler.is_empty());
    }
}
