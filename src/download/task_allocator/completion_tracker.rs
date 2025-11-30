//! 完成状态跟踪模块
//!
//! 负责跟踪任务完成状态和永久失败

use log::{error, info};
use ranged_mmap::AllocatedRange;
use tokio::sync::oneshot;

/// 失败的 Range 信息
pub type FailedRange = (AllocatedRange, String);

/// 任务完成结果
#[derive(Debug)]
pub enum CompletionResult {
    /// 所有任务成功完成
    Success,
    /// 有任务永久失败
    PermanentFailure { failures: Vec<FailedRange> },
}

/// 完成状态跟踪器
///
/// 跟踪永久失败的任务并检查完成条件
pub struct CompletionTracker {
    /// 永久失败的任务列表
    permanently_failed: Vec<FailedRange>,
    /// 完成通知发送器
    completion_tx: Option<oneshot::Sender<CompletionResult>>,
}

impl CompletionTracker {
    /// 创建新的完成跟踪器
    pub fn new(completion_tx: oneshot::Sender<CompletionResult>) -> Self {
        Self {
            permanently_failed: Vec::new(),
            completion_tx: Some(completion_tx),
        }
    }

    /// 记录永久失败的任务
    pub fn record_permanent_failure(&mut self, range: AllocatedRange, error: String) {
        let (start, end) = range.as_range_tuple();
        error!("任务永久失败 range {}..{}: {}", start, end, error);
        self.permanently_failed.push((range, error));
    }

    /// 检查是否有永久失败的任务
    #[inline]
    pub fn has_permanent_failures(&self) -> bool {
        !self.permanently_failed.is_empty()
    }

    /// 获取永久失败任务数量
    #[inline]
    #[allow(dead_code)]
    pub fn permanent_failure_count(&self) -> usize {
        self.permanently_failed.len()
    }

    /// 获取永久失败任务的详细信息
    #[inline]
    #[allow(dead_code)]
    pub fn get_permanent_failures(&self) -> &[FailedRange] {
        &self.permanently_failed
    }

    /// 检查完成条件并发送通知
    ///
    /// # 参数
    /// - `remaining`: 剩余待分配字节数
    /// - `pending_retries`: 等待重试的任务数（定时器等待 + 就绪队列）
    /// - `idle_count`: 空闲 worker 数量
    /// - `total_workers`: worker 总数
    ///
    /// # 返回
    /// 如果任务完成（成功或失败），返回 `true`
    pub fn check_and_notify(
        &mut self,
        remaining: u64,
        pending_retries: usize,
        idle_count: usize,
        total_workers: usize,
    ) -> bool {
        // 先检查永久失败
        if self.has_permanent_failures() {
            let failures = self.permanently_failed.clone();
            info!("遇到永久失败，终止下载：{} 个任务失败", failures.len());

            if let Some(tx) = self.completion_tx.take() {
                let _ = tx.send(CompletionResult::PermanentFailure { failures });
            }
            return true;
        }

        // 检查是否所有任务已成功完成
        // 条件：无剩余字节 + 无待重试任务 + 所有 worker 都空闲
        if remaining == 0 && pending_retries == 0 && idle_count == total_workers {
            info!("所有任务已成功完成");

            if let Some(tx) = self.completion_tx.take() {
                let _ = tx.send(CompletionResult::Success);
            }
            return true;
        }

        false
    }

    /// 强制发送完成通知（用于提前终止）
    #[allow(dead_code)]
    pub fn force_complete(&mut self, result: CompletionResult) {
        if let Some(tx) = self.completion_tx.take() {
            let _ = tx.send(result);
        }
    }

    /// 检查是否已发送完成通知
    #[allow(dead_code)]
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completion_tx.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ranged_mmap::MmapFile;
    use std::num::NonZeroU64;
    use tempfile::NamedTempFile;

    fn allocate_test_range(size: u64) -> AllocatedRange {
        let temp_file = NamedTempFile::new().unwrap();
        let (_file, mut allocator) =
            MmapFile::create(temp_file.path(), NonZeroU64::new(size).unwrap()).unwrap();
        allocator.allocate(NonZeroU64::new(size).unwrap()).unwrap()
    }

    #[test]
    fn test_initial_state() {
        let (tx, _rx) = oneshot::channel();
        let tracker = CompletionTracker::new(tx);

        assert!(!tracker.has_permanent_failures());
        assert_eq!(tracker.permanent_failure_count(), 0);
        assert!(!tracker.is_completed());
    }

    #[test]
    fn test_record_permanent_failure() {
        let (tx, _rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        let range = allocate_test_range(100);
        tracker.record_permanent_failure(range, "Network error".to_string());

        assert!(tracker.has_permanent_failures());
        assert_eq!(tracker.permanent_failure_count(), 1);

        let failures = tracker.get_permanent_failures();
        assert_eq!(failures[0].1, "Network error");
    }

    #[test]
    fn test_check_success_completion() {
        let (tx, rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        // 所有条件满足：无剩余、无重试、所有 worker 空闲
        let completed = tracker.check_and_notify(0, 0, 4, 4);

        assert!(completed);
        assert!(tracker.is_completed());

        // 验证收到成功通知
        let result = rx.blocking_recv().unwrap();
        assert!(matches!(result, CompletionResult::Success));
    }

    #[test]
    fn test_check_failure_completion() {
        let (tx, rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        // 记录永久失败
        let range = allocate_test_range(100);
        tracker.record_permanent_failure(range, "Error".to_string());

        // 检查完成
        let completed = tracker.check_and_notify(500, 0, 4, 4);

        assert!(completed);
        assert!(tracker.is_completed());

        // 验证收到失败通知
        let result = rx.blocking_recv().unwrap();
        assert!(matches!(
            result,
            CompletionResult::PermanentFailure { failures } if failures.len() == 1
        ));
    }

    #[test]
    fn test_not_completed_with_remaining() {
        let (tx, _rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        // 还有剩余字节
        let completed = tracker.check_and_notify(100, 0, 4, 4);

        assert!(!completed);
        assert!(!tracker.is_completed());
    }

    #[test]
    fn test_not_completed_with_pending_retries() {
        let (tx, _rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        // 还有待重试任务
        let completed = tracker.check_and_notify(0, 2, 4, 4);

        assert!(!completed);
        assert!(!tracker.is_completed());
    }

    #[test]
    fn test_not_completed_with_active_workers() {
        let (tx, _rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        // 还有 worker 在工作
        let completed = tracker.check_and_notify(0, 0, 2, 4);

        assert!(!completed);
        assert!(!tracker.is_completed());
    }

    #[test]
    fn test_force_complete() {
        let (tx, rx) = oneshot::channel();
        let mut tracker = CompletionTracker::new(tx);

        tracker.force_complete(CompletionResult::Success);

        assert!(tracker.is_completed());

        let result = rx.blocking_recv().unwrap();
        assert!(matches!(result, CompletionResult::Success));
    }
}
