use std::path::PathBuf;
use crate::tools::range_writer::AllocatedRange;

/// 完整文件下载任务
#[derive(Debug, Clone)]
pub struct FileTask {
    pub url: String,
    pub save_path: PathBuf,
}

/// Worker 内部任务类型
/// 
/// 用于在 Worker 协程之间传递任务
#[derive(Debug, Clone)]
pub(crate) enum WorkerTask {
    /// Range 下载任务
    /// 
    /// 包含 URL 和已分配的 Range，确保不会重叠且有效
    Range { url: String, range: AllocatedRange },
}

/// Range 下载结果
/// 
/// Worker 完成下载并写入后发送的轻量级通知
/// 
/// 不包含数据本身，只包含完成状态：
/// - `Complete`: 成功下载并写入
/// - `Failed`: 下载或写入失败，包含错误信息
#[derive(Debug)]
pub(crate) enum RangeResult {
    /// Range 成功完成
    Complete {
        /// 完成此任务的 worker ID
        worker_id: usize,
    },
    /// Range 失败
    Failed {
        /// 失败的 worker ID
        worker_id: usize,
        /// 失败的 range
        range: AllocatedRange,
        /// 错误信息
        error: String,
    },
}

/// Range 请求支持检测结果
#[derive(Debug)]
pub struct RangeSupport {
    pub supported: bool,
    pub content_length: Option<u64>,
}

