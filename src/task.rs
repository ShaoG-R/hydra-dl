use std::path::PathBuf;
use ranged_mmap::AllocatedRange;
use lite_sync::oneshot::lite;

/// 完整文件下载任务
#[derive(Debug, Clone)]
pub struct FileTask {
    pub url: String,
    pub save_path: PathBuf,
}

/// Worker 内部任务类型
/// 
/// 用于在 Worker 协程之间传递任务
#[derive(Debug)]
pub(crate) enum WorkerTask {
    /// Range 下载任务
    /// 
    /// 包含 URL 和已分配的 Range，确保不会重叠且有效
    Range { 
        url: String, 
        range: AllocatedRange,
        /// 当前任务的重试次数（首次为 0）
        retry_count: usize,
        /// 取消信号接收器
        /// 
        /// 当接收到信号时，worker 将中止当前下载任务
        cancel_rx: lite::Receiver<()>,
    },
}

/// Range 下载结果
/// 
/// Worker 完成下载并写入后发送的轻量级通知
/// 
/// 不包含数据本身，只包含完成状态：
/// - `Complete`: 成功下载并写入
/// - `DownloadFailed`: 下载失败（网络错误、服务器错误等）
/// - `WriteFailed`: 写入文件失败（磁盘错误、权限问题等）
#[derive(Debug)]
pub(crate) enum RangeResult {
    /// Range 成功完成
    Complete {
        /// 完成此任务的 worker ID
        worker_id: u64,
    },
    /// 下载失败
    DownloadFailed {
        /// 失败的 worker ID
        worker_id: u64,
        /// 失败的 range
        range: AllocatedRange,
        /// 错误信息
        error: String,
        /// 当前的重试次数
        retry_count: usize,
    },
    /// 写入失败
    WriteFailed {
        /// 失败的 worker ID
        worker_id: u64,
        /// 失败的 range
        range: AllocatedRange,
        /// 错误信息
        error: String,
    },
}
