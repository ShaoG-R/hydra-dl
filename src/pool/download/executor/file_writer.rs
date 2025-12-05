//! 文件写入辅助协程
//!
//! 负责处理所有文件写入操作，将写入从下载主循环中分离出来。
//!
//! # 设计说明
//!
//! - 使用有界 `mpsc` 通道接收写入请求，提供背压
//! - 使用单独的 `oneshot` 通道通知关闭
//! - 写入失败时通过专用 `oneshot` 通知 Executor 关闭
//! - Executor 在主 `select!` 中监听写入失败信号
//!
//! # 写入流程
//!
//! 1. Executor 下载完成后，调用 `FileWriter::write()` 发送写入请求
//! 2. Writer 协程从队列中取出请求并执行写入
//! 3. 写入成功：通过 `StatsUpdaterHandle` 通知统计更新
//! 4. 写入失败：通过 `failure_tx` 发送错误，触发 Executor 关闭

use bytes::Bytes;
use log::{debug, error, info};
use ranged_mmap::AllocatedRange;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::pool::common::WorkerId;
use crate::pool::download::stats_updater::StatsUpdaterHandle;
use crate::utils::writer::MmapWriter;

/// 写入失败错误信息
#[derive(Debug, Clone)]
pub(crate) struct WriteFailure {
    /// 失败的 range
    pub range: AllocatedRange,
    /// 错误信息
    pub error: String,
}

/// 写入请求
#[derive(Debug)]
pub(crate) struct WriteRequest {
    /// 要写入的范围
    pub(crate) range: AllocatedRange,
    /// 要写入的数据
    pub(crate) data: Bytes,
}

/// File Writer 配置
#[derive(Debug, Clone)]
pub(crate) struct FileWriterConfig {
    /// 写入请求通道容量（有界队列大小）
    pub channel_capacity: usize,
}

impl Default for FileWriterConfig {
    fn default() -> Self {
        Self {
            // 默认容量为 16，足够缓冲多个写入请求
            // 同时提供背压，避免内存无限增长
            channel_capacity: 16,
        }
    }
}

/// 写入失败接收器
///
/// Executor 持有此接收器，在主 select! 中监听写入失败
pub(crate) type WriteFailureReceiver = oneshot::Receiver<WriteFailure>;

/// 负责 Actor 生命周期的守卫
///
/// 当此结构体被 Drop 时，如果 handle 还在，说明没有正常关闭，
/// 它会负责 abort 掉后台任务。
struct ActorGuard {
    /// 任务句柄（仍然需要 Option，因为要在 Drop 中 take）
    handle: Option<JoinHandle<()>>,
    worker_id: WorkerId,
}

impl Drop for ActorGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            debug!("Worker {} FileWriter actor aborted on drop", self.worker_id);
        }
    }
}

/// File Writer Handle
pub(crate) struct FileWriter {
    /// 写入请求发送通道 (不再需要 Option)
    write_tx: mpsc::Sender<WriteRequest>,

    /// 关闭信号发送器 (不再需要 Option)
    shutdown_tx: oneshot::Sender<()>,

    /// 包含 JoinHandle 的守卫
    actor_guard: ActorGuard,

    /// Worker ID
    worker_id: WorkerId,
}

// 移除 impl Drop for FileWriter，逻辑移动到了 ActorGuard

impl FileWriter {
    pub(crate) fn new(
        worker_id: WorkerId,
        writer: MmapWriter,
        stats_handle: StatsUpdaterHandle,
        config: Option<FileWriterConfig>,
    ) -> (Self, WriteFailureReceiver) {
        let config = config.unwrap_or_default();
        let (write_tx, write_rx) = mpsc::channel(config.channel_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (failure_tx, failure_rx) = oneshot::channel();

        let actor = FileWriterActor {
            worker_id,
            writer,
            write_rx,
            shutdown_rx,
            failure_tx: Some(failure_tx),
            stats_handle,
        };

        let actor_handle = tokio::spawn(actor.run());

        let file_writer = Self {
            write_tx,    // 直接持有
            shutdown_tx, // 直接持有
            actor_guard: ActorGuard {
                handle: Some(actor_handle),
                worker_id,
            },
            worker_id,
        };

        (file_writer, failure_rx)
    }

    /// 发送写入请求
    ///
    /// 现在不需要 unwrap 或 if let，直接调用，性能更高且代码更简洁
    #[inline]
    pub(crate) async fn write(
        &self,
        range: AllocatedRange,
        data: Bytes,
    ) -> Result<(), mpsc::error::SendError<WriteRequest>> {
        self.write_tx.send(WriteRequest { range, data }).await
    }

    /// 关闭 Writer 并等待其完全停止
    ///
    /// 消费 self，直接解构字段
    pub(crate) async fn shutdown_and_wait(self) {
        // 解构 self
        let Self {
            shutdown_tx,
            mut actor_guard,
            worker_id,
            ..
        } = self;

        // 1. 发送关闭信号 (直接使用，无需 unwrap)
        let _ = shutdown_tx.send(());

        // 2. 等待 Actor 完成
        // 从 guard 中 take 走 handle，这样 guard drop 时就不会触发 abort
        if let Some(handle) = actor_guard.handle.take() {
            let _ = handle.await;
            debug!("Worker {} FileWriter actor has fully stopped", worker_id);
        }

        // 函数结束，shutdown_tx 和 write_tx 被 drop，actor_guard 被 drop (但 handle 为空)
    }

    /// 等待所有待处理写入完成后退出
    pub(crate) async fn drain_and_wait(self) {
        // 解构 self
        let Self {
            write_tx,
            shutdown_tx, // 保持持有，避免 drop 导致 Actor 收到 closed 信号
            mut actor_guard,
            worker_id,
        } = self;

        // 1. 显式 drop write_tx，关闭通道，让 Actor 处理完剩余数据后退出
        drop(write_tx);

        // 2. 等待 Actor 完成
        if let Some(handle) = actor_guard.handle.take() {
            let _ = handle.await;
            debug!("Worker {} FileWriter actor drained and stopped", worker_id);
        }

        // 3. shutdown_tx 在这里随作用域结束被 drop，但此时 Actor 已经退出了，无所谓
        let _ = shutdown_tx;
    }
}

/// File Writer Actor（内部使用）
///
/// 实际执行文件写入操作的协程
struct FileWriterActor {
    /// Worker ID（用于日志）
    worker_id: WorkerId,
    /// 文件写入器
    writer: MmapWriter,
    /// 写入请求接收通道
    write_rx: mpsc::Receiver<WriteRequest>,
    /// 关闭信号接收器
    shutdown_rx: oneshot::Receiver<()>,
    /// 写入失败发送器（只能发送一次）
    failure_tx: Option<oneshot::Sender<WriteFailure>>,
    /// Stats Updater 句柄（用于通知写入成功的字节数）
    stats_handle: StatsUpdaterHandle,
}

impl FileWriterActor {
    /// 运行 File Writer 主循环
    ///
    /// 持续接收写入请求并执行写入，直到：
    /// - 收到关闭信号
    /// - 写入失败（发送失败信号后退出）
    async fn run(mut self) {
        debug!("Worker {} File Writer 启动", self.worker_id);

        loop {
            tokio::select! {
                biased;
                // 监听关闭信号
                _ = &mut self.shutdown_rx => {
                    debug!("Worker {} File Writer 收到关闭信号", self.worker_id);
                    break;
                }
                // 处理写入请求
                request = self.write_rx.recv() => {
                    match request {
                        Some(WriteRequest { range, data }) => {
                            if !self.handle_write(range, data) {
                                return; // 写入失败，已发送错误信号
                            }
                        }
                        None => {
                            // 通道关闭
                            debug!("Worker {} File Writer 写入通道关闭", self.worker_id);
                            break;
                        }
                    }
                }
            }
        }

        debug!("Worker {} File Writer 退出", self.worker_id);
    }

    /// 处理单个写入请求
    ///
    /// 返回 true 表示成功，false 表示失败（需要退出）
    fn handle_write(&mut self, range: AllocatedRange, data: Bytes) -> bool {
        let range_start = range.start();
        let range_end = range.end();
        let written_bytes = range.len();

        match self.writer.write_range(range.clone(), data.as_ref()) {
            Ok(()) => {
                // 写入成功，通知统计更新
                self.stats_handle.send_bytes_written(written_bytes);
                debug!(
                    "Worker {} 写入成功: range {}..{} ({} bytes)",
                    self.worker_id, range_start, range_end, written_bytes
                );
                true
            }
            Err(e) => {
                // 写入失败，发送失败信号
                let error_msg = format!("写入失败: {:?}", e);
                error!("Worker {} {}", self.worker_id, error_msg);

                if let Some(failure_tx) = self.failure_tx.take() {
                    let _ = failure_tx.send(WriteFailure {
                        range,
                        error: error_msg,
                    });
                }

                info!("Worker {} File Writer 因写入失败退出", self.worker_id);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::common::WorkerId;
    use crate::pool::download::stats_updater::{StatsUpdater, WorkerBroadcaster};
    use crate::utils::cancel_channel::cancel_channel;
    use crate::utils::chunk_strategy::SpeedBasedChunkStrategy;
    use std::num::NonZeroU64;
    use tempfile::tempdir;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn test_file_writer_shutdown() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (writer, _allocator) = MmapWriter::new(&path, NonZeroU64::new(1024).unwrap()).unwrap();

        // 创建 stats updater
        let (broadcast_tx, _) = broadcast::channel(16);
        let worker_id = WorkerId::new(0, 0);
        let broadcaster = WorkerBroadcaster::new(worker_id, broadcast_tx);
        let chunk_strategy = Box::new(SpeedBasedChunkStrategy::new(
            1024 * 1024,      // min: 1 MB
            16 * 1024 * 1024, // max: 16 MB
            2.0,              // expected duration: 2s
            0.3,              // smoothing factor
            0.7,              // instant speed weight
            0.3,              // avg speed weight
        ));
        let initial_chunk_size = 4 * 1024 * 1024; // 4 MB
        let (cancel_tx, _cancel_rx) = cancel_channel();
        let (_stats_updater, stats_handle) = StatsUpdater::new(
            worker_id,
            broadcaster,
            chunk_strategy,
            initial_chunk_size,
            cancel_tx,
            None,
        );

        // 创建 file writer
        let (file_writer, _failure_rx) = FileWriter::new(worker_id, writer, stats_handle, None);

        // 使用 shutdown_and_wait 关闭 writer
        file_writer.shutdown_and_wait().await;
    }
}
