//! 任务分配器
//!
//! 统一管理新任务分配和重试任务调度。
//! 提供单一入口获取下一个待执行任务。

use super::retry_scheduler::RetryScheduler;
use super::state::TaskInternalState;
use crate::config::RetryConfig;
use log::debug;
use ranged_mmap::allocator::concurrent::Allocator as ConcurrentAllocator;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

/// 分配的任务类型
#[derive(Debug)]
pub(crate) enum AllocatedTask {
    /// 新分配的任务
    New {
        range: ranged_mmap::AllocatedRange,
        state: TaskInternalState,
    },
    /// 重试任务
    Retry {
        range: ranged_mmap::AllocatedRange,
        retry_count: usize,
        state: TaskInternalState,
    },
}

impl AllocatedTask {
    /// 获取任务的 Range
    #[inline]
    pub fn range(&self) -> &ranged_mmap::AllocatedRange {
        match self {
            AllocatedTask::New { range, .. } | AllocatedTask::Retry { range, .. } => range,
        }
    }

    /// 获取重试次数（新任务为 0）
    #[inline]
    pub fn retry_count(&self) -> usize {
        match self {
            AllocatedTask::New { .. } => 0,
            AllocatedTask::Retry { retry_count, .. } => *retry_count,
        }
    }

    /// 消费并返回 Range
    #[inline]
    #[allow(unused)]
    pub fn into_range(self) -> ranged_mmap::AllocatedRange {
        match self {
            AllocatedTask::New { range, .. } | AllocatedTask::Retry { range, .. } => range,
        }
    }
}

/// 任务分配结果
pub(crate) enum AllocationResult {
    /// 成功获取任务
    Task(AllocatedTask),
    /// 没有新任务，但有待重试任务
    WaitForRetry {
        /// 建议等待时间
        delay: Duration,
        /// 待重试任务数
        pending_count: usize,
    },
    /// 所有任务已完成
    Done,
}

/// 任务分配器
///
/// 统一管理新任务分配和重试任务调度，提供单一入口获取下一个待执行任务。
/// 优先处理到期的重试任务，其次分配新任务。
pub(crate) struct TaskAllocator {
    /// 并发范围分配器
    allocator: Arc<ConcurrentAllocator>,
    /// 重试调度器
    retry_scheduler: RetryScheduler,
    /// 当前任务 ID（每处理一个任务递增）
    task_id: u64,
}

impl TaskAllocator {
    /// 创建新的任务分配器
    pub fn new(allocator: Arc<ConcurrentAllocator>, retry_config: RetryConfig) -> Self {
        Self {
            allocator,
            retry_scheduler: RetryScheduler::new(retry_config),
            task_id: 0,
        }
    }

    /// 获取下一个待执行任务
    ///
    /// 返回值：
    /// - `Task`: 成功获取到任务（新任务或重试任务）
    /// - `WaitForRetry`: 没有新任务，但有待重试任务，需要等待
    /// - `Done`: 所有任务已完成
    pub fn next_task(&mut self, chunk_size: u64) -> AllocationResult {
        // 1. 优先检查到期的重试任务
        if let Some(retry) = self.retry_scheduler.get_due_task(self.task_id) {
            debug!(
                "TaskAllocator: 获取重试任务 (task_id={}, retry_count={})",
                self.task_id, retry.retry_count
            );
            return AllocationResult::Task(AllocatedTask::Retry {
                range: retry.range,
                retry_count: retry.retry_count,
                state: TaskInternalState::new(),
            });
        }

        // 2. 尝试分配新任务
        if let Some(range) = self
            .allocator
            .allocate(NonZeroU64::new(chunk_size).unwrap())
        {
            debug!(
                "TaskAllocator: 分配新任务 (task_id={}, range {}..{})",
                self.task_id,
                range.start(),
                range.end()
            );
            return AllocationResult::Task(AllocatedTask::New {
                range,
                state: TaskInternalState::new(),
            });
        }

        // 3. 没有新任务，检查是否有待重试任务
        if !self.retry_scheduler.is_empty() {
            AllocationResult::WaitForRetry {
                delay: self.retry_scheduler.retry_delay(),
                pending_count: self.retry_scheduler.pending_count(),
            }
        } else {
            AllocationResult::Done
        }
    }

    /// 调度重试任务
    pub fn schedule_retry(&mut self, range: ranged_mmap::AllocatedRange, retry_count: usize) {
        self.retry_scheduler
            .schedule(self.task_id, range, retry_count);
    }

    /// 将所有待重试任务提前到当前 task_id
    ///
    /// 当等待结束后调用，使所有待重试任务立即可执行
    pub fn advance_all_retries(&mut self) {
        self.retry_scheduler.advance_all(self.task_id);
    }

    /// 递增任务 ID
    ///
    /// 每完成（成功、失败或需重试）一个任务后调用
    #[inline]
    pub fn advance_task_id(&mut self) {
        self.task_id += 1;
    }

    /// 获取当前任务 ID
    #[inline]
    #[allow(dead_code)]
    pub fn current_task_id(&self) -> u64 {
        self.task_id
    }

    /// 获取最大重试次数
    #[inline]
    pub fn max_retry_count(&self) -> usize {
        self.retry_scheduler.max_retry_count()
    }

    /// 获取待重试任务数
    #[inline]
    #[allow(dead_code)]
    pub fn pending_retry_count(&self) -> usize {
        self.retry_scheduler.pending_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ranged_mmap::allocator::RangeAllocator;
    use std::time::Duration;

    fn create_test_allocator(size: u64) -> Arc<ConcurrentAllocator> {
        use std::num::NonZeroU64;
        Arc::new(ConcurrentAllocator::new(NonZeroU64::new(size).unwrap()))
    }

    fn create_test_config() -> RetryConfig {
        RetryConfig {
            retry_task_counts: vec![1, 2, 4],
            retry_delay: Duration::from_millis(100),
        }
    }

    // 4K 对齐常量
    const PAGE_SIZE: u64 = 4096;

    #[test]
    fn test_allocate_new_tasks() {
        // allocator 是 4K 对齐的，使用 2 * 4K = 8K
        let allocator = create_test_allocator(PAGE_SIZE * 2);
        let mut task_alloc = TaskAllocator::new(allocator, create_test_config());

        // 分配第一个任务 (4K)
        match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(task) => {
                assert_eq!(task.retry_count(), 0);
                assert!(matches!(task, AllocatedTask::New { .. }));
            }
            _ => panic!("Expected Task"),
        }
        task_alloc.advance_task_id();

        // 分配第二个任务 (4K)
        match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(task) => {
                assert_eq!(task.retry_count(), 0);
            }
            _ => panic!("Expected Task"),
        }
        task_alloc.advance_task_id();

        // 没有更多任务
        assert!(matches!(
            task_alloc.next_task(PAGE_SIZE),
            AllocationResult::Done
        ));
    }

    #[test]
    fn test_retry_priority() {
        // 使用 3 * 4K = 12K，足够分配 3 个 4K 块
        let allocator = create_test_allocator(PAGE_SIZE * 3);
        let mut task_alloc = TaskAllocator::new(allocator, create_test_config());

        // 分配并完成第一个任务
        let task1 = match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(t) => t,
            _ => panic!("Expected Task"),
        };
        task_alloc.advance_task_id();

        // 调度重试（retry_count=0 时 wait_count=1，目标 task_id=2）
        task_alloc.schedule_retry(task1.into_range(), 0);

        // 分配新任务（task_id=1）
        match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(task) => {
                assert_eq!(task.retry_count(), 0); // 新任务
            }
            _ => panic!("Expected Task"),
        }
        task_alloc.advance_task_id();

        // 现在 task_id=2，重试任务应该到期
        match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(task) => {
                assert_eq!(task.retry_count(), 0); // 重试任务
                assert!(matches!(task, AllocatedTask::Retry { .. }));
            }
            _ => panic!("Expected Retry Task"),
        }
    }

    #[test]
    fn test_wait_for_retry() {
        // 使用 1 * 4K
        let allocator = create_test_allocator(PAGE_SIZE);
        let mut task_alloc = TaskAllocator::new(allocator, create_test_config());

        // 分配并"失败"一个任务
        let task = match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(t) => t,
            _ => panic!("Expected Task"),
        };
        // 先 advance_task_id，然后调度重试
        // 这样 target_task_id = 1 + 1 = 2，而当前 task_id = 1
        task_alloc.advance_task_id();
        task_alloc.schedule_retry(task.into_range(), 0);

        // 现在没有新任务（allocator 已耗尽），有待重试但未到期
        match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::WaitForRetry { pending_count, .. } => {
                assert_eq!(pending_count, 1);
            }
            _ => panic!("Expected WaitForRetry"),
        }

        // 提前所有重试
        task_alloc.advance_all_retries();

        // 现在应该能获取重试任务
        match task_alloc.next_task(PAGE_SIZE) {
            AllocationResult::Task(task) => {
                assert!(matches!(task, AllocatedTask::Retry { .. }));
            }
            _ => panic!("Expected Retry Task"),
        }
    }
}
