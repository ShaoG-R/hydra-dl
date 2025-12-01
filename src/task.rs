use std::path::PathBuf;

/// 完整文件下载任务
#[derive(Debug, Clone)]
pub struct FileTask {
    pub url: String,
    pub save_path: PathBuf,
}
