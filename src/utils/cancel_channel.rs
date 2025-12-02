//! 任务取消通道
//!
//! 提供 task_id 感知的取消机制，解决以下问题：
//! - 健康检查器决定取消 task_id=X 时，executor 可能已经完成任务 X 并开始任务 X+1
//! - 需要验证取消请求的 task_id 与当前任务 task_id 是否匹配
//!
//! # 设计
//!
//! - `CancelSender`: 取消信号发送端（健康检查器持有）
//! - `CancelReceiver`: 取消信号接收端（executor 持有）
//! - `CancelHandle`: 发送端获取的句柄，包含获取时的 task_id
//! - `CancelChannelShared`: 共享状态，包含 task_id 和 oneshot channel
//!
//! # 工作流程
//!
//! 1. Executor 开始新任务时调用 `receiver.reset(task_id)` 创建新的 oneshot channel
//! 2. 健康检查器调用 `sender.get_handle()` 获取包含当前 task_id 的句柄
//! 3. 健康检查器决定取消时调用 `handle.cancel()`，内部验证 task_id 是否匹配
//! 4. 只有 task_id 匹配时才发送取消信号

use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// 共享内部状态
struct CancelChannelInner {
    /// 当前任务 ID
    task_id: u64,
    /// oneshot sender（需要 Option 以支持 take）
    tx: Option<oneshot::Sender<()>>,
}

/// 共享状态
///
/// 使用单个 Mutex 保护 task_id 和 tx，确保原子性
struct CancelChannelShared {
    inner: Mutex<CancelChannelInner>,
}

/// 取消信号发送端
///
/// 由健康检查器持有，用于发送取消信号
#[derive(Clone)]
pub struct CancelSender {
    shared: Arc<CancelChannelShared>,
}

/// 取消信号接收端
///
/// 由 executor 持有，用于接收取消信号并重置 channel
pub struct CancelReceiver {
    shared: Arc<CancelChannelShared>,
}

/// 取消句柄
///
/// 包含获取时的 task_id，发送取消信号时验证 task_id 是否匹配
pub struct CancelHandle {
    shared: Arc<CancelChannelShared>,
    /// 获取句柄时的 task_id
    task_id: u64,
}

/// 创建新的取消通道
///
/// 返回 (sender, receiver) 对
pub fn cancel_channel() -> (CancelSender, CancelReceiver) {
    let shared = Arc::new(CancelChannelShared {
        inner: Mutex::new(CancelChannelInner {
            task_id: 0,
            tx: None,
        }),
    });

    let sender = CancelSender {
        shared: Arc::clone(&shared),
    };
    let receiver = CancelReceiver { shared };

    (sender, receiver)
}

impl CancelSender {
    /// 获取当前任务的取消句柄
    ///
    /// 句柄包含获取时的 task_id，用于后续验证
    pub fn get_handle(&self) -> CancelHandle {
        let task_id = self.shared.inner.lock().unwrap().task_id;
        CancelHandle {
            shared: Arc::clone(&self.shared),
            task_id,
        }
    }

    /// 获取当前 task_id（仅用于调试/日志）
    #[inline]
    pub fn current_task_id(&self) -> u64 {
        self.shared.inner.lock().unwrap().task_id
    }
}

impl CancelHandle {
    /// 获取此句柄的 task_id
    #[inline]
    pub fn task_id(&self) -> u64 {
        self.task_id
    }

    /// 发送取消信号
    ///
    /// 只有当前 task_id 与句柄的 task_id 匹配时才发送
    ///
    /// # Returns
    ///
    /// - `true`: 取消信号已发送
    /// - `false`: task_id 不匹配或 channel 已关闭，取消信号未发送
    pub fn cancel(self) -> bool {
        let mut guard = self.shared.inner.lock().unwrap();

        // 验证 task_id 是否匹配
        if guard.task_id != self.task_id {
            return false;
        }

        // task_id 匹配，取出 tx 并发送
        if let Some(tx) = guard.tx.take() {
            drop(guard); // 释放锁后再发送
            let _ = tx.send(());
            true
        } else {
            false
        }
    }

    /// 检查 task_id 是否仍然匹配（不发送取消信号）
    pub fn is_valid(&self) -> bool {
        self.shared.inner.lock().unwrap().task_id == self.task_id
    }
}

impl CancelReceiver {
    /// 重置 channel，为新任务创建新的 oneshot channel
    ///
    /// 返回新的 receiver，用于 tokio::select!
    ///
    /// # Arguments
    ///
    /// * `task_id` - 新任务的 ID
    ///
    /// # Returns
    ///
    /// 新的 oneshot receiver
    pub fn reset(&mut self, task_id: u64) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();

        let mut guard = self.shared.inner.lock().unwrap();
        guard.task_id = task_id;
        guard.tx = Some(tx);

        rx
    }

    /// 获取当前 task_id
    #[inline]
    pub fn current_task_id(&self) -> u64 {
        self.shared.inner.lock().unwrap().task_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_channel_basic() {
        let (sender, mut receiver) = cancel_channel();

        // 初始状态
        assert_eq!(sender.current_task_id(), 0);

        // 重置为任务 1，获取 rx
        let _rx = receiver.reset(1);
        assert_eq!(sender.current_task_id(), 1);
    }

    #[test]
    fn test_cancel_handle_matching_task_id() {
        let (sender, mut receiver) = cancel_channel();

        // 任务 1
        let _rx = receiver.reset(1);
        let handle = sender.get_handle();
        assert_eq!(handle.task_id(), 1);

        // 发送取消（task_id 匹配）
        assert!(handle.cancel());
    }

    #[test]
    fn test_cancel_handle_mismatched_task_id() {
        let (sender, mut receiver) = cancel_channel();

        // 任务 1
        let _rx = receiver.reset(1);
        let handle = sender.get_handle();
        assert_eq!(handle.task_id(), 1);

        // 任务变为 2
        let _rx = receiver.reset(2);
        assert_eq!(sender.current_task_id(), 2);

        // 发送取消（task_id 不匹配）
        assert!(!handle.cancel());
    }

    #[test]
    fn test_cancel_handle_is_valid() {
        let (sender, mut receiver) = cancel_channel();

        let _rx = receiver.reset(1);
        let handle = sender.get_handle();

        assert!(handle.is_valid());

        let _rx = receiver.reset(2);

        assert!(!handle.is_valid());
    }

    #[tokio::test]
    async fn test_cancel_receiver_in_select() {
        let (sender, mut receiver) = cancel_channel();

        let mut rx = receiver.reset(1);
        let handle = sender.get_handle();

        // 在 select 中使用
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            handle.cancel();
        });

        let result = (&mut rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cancel_not_received_on_mismatch() {
        let (sender, mut receiver) = cancel_channel();

        let _rx = receiver.reset(1);
        let handle = sender.get_handle();

        // 任务变为 2
        let mut rx = receiver.reset(2);

        // 尝试取消旧任务（应该失败）
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let cancelled = handle.cancel();
            assert!(!cancelled);
        });

        // 新任务的 rx 不应该收到取消信号
        let result = tokio::time::timeout(
            tokio::time::Duration::from_millis(50),
            &mut rx,
        ).await;

        // 应该超时，因为取消信号没有发送到新 channel
        assert!(result.is_err());
    }
}
