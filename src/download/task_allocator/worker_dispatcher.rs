//! Worker 任务派发模块
//!
//! 负责向 Worker 发送任务和管理取消信号

use crate::pool::download::DownloadWorkerHandle;
use crate::task::WorkerTask;
use crate::utils::io_traits::HttpClient;
use lite_sync::oneshot::lite;
use log::{debug, error, info};
use rustc_hash::FxHashMap;
use smr_swap::LocalReader;

/// Worker 派发器
///
/// 负责向 Worker 发送任务并管理取消信号
pub struct WorkerDispatcher<C: HttpClient> {
    /// Worker 句柄映射（只读访问）
    worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle<C>>>,
    /// 任务取消 sender 映射
    cancel_senders: FxHashMap<u64, lite::Sender<()>>,
    /// 默认 chunk 大小（当无法获取 worker 配置时使用）
    default_chunk_size: u64,
}

impl<C: HttpClient + Clone> WorkerDispatcher<C> {
    /// 创建新的 Worker 派发器
    pub fn new(
        worker_handles: LocalReader<FxHashMap<u64, DownloadWorkerHandle<C>>>,
        default_chunk_size: u64,
    ) -> Self {
        Self {
            worker_handles,
            cancel_senders: FxHashMap::default(),
            default_chunk_size,
        }
    }

    /// 派发任务给指定 worker
    ///
    /// 返回 `true` 表示成功发送，`false` 表示发送失败
    pub async fn dispatch(
        &mut self,
        worker_id: u64,
        task: WorkerTask,
        cancel_tx: lite::Sender<()>,
    ) -> bool {
        let handle = {
            let worker_handles = self.worker_handles.load();
            worker_handles.get(&worker_id).cloned()
        };

        if let Some(handle) = handle {
            if let Err(e) = handle.send_task(task).await {
                error!("Worker #{} 任务派发失败: {:?}", worker_id, e);
                return false;
            }
            self.cancel_senders.insert(worker_id, cancel_tx);
            debug!("成功派发任务到 Worker #{}", worker_id);
            true
        } else {
            error!("Worker #{} 不存在", worker_id);
            false
        }
    }

    /// 取消指定 worker 的当前任务
    pub fn cancel_worker(&mut self, worker_id: u64) -> bool {
        if let Some(cancel_tx) = self.cancel_senders.remove(&worker_id) {
            let _ = cancel_tx.notify(());
            info!("已取消 Worker #{} 的任务", worker_id);
            true
        } else {
            debug!("Worker #{} 没有正在执行的任务", worker_id);
            false
        }
    }

    /// 清理 worker 的取消信号（任务完成时调用）
    #[inline]
    pub fn cleanup_worker(&mut self, worker_id: u64) {
        self.cancel_senders.remove(&worker_id);
    }

    /// 获取指定 worker 的 chunk 大小
    pub fn get_chunk_size(&self, worker_id: u64) -> u64 {
        let handles = self.worker_handles.load();
        handles
            .get(&worker_id)
            .map(|h| h.chunk_size())
            .unwrap_or(self.default_chunk_size)
    }

    /// 获取当前 worker 总数
    pub fn worker_count(&self) -> usize {
        let handles = self.worker_handles.load();
        handles.len()
    }
}
