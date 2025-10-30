//! # rs-dn: Rust 高性能多线程下载器
//!
//! 基于 tokio 和 channel 的异步多线程下载库
//!
//! ## 特性
//!
//! - 多 URL 并发下载
//! - 单文件分块下载（支持 HTTP Range 请求）
//! - **动态分块机制**：根据实时下载速度自动调整分块大小
//! - Worker 池架构，基于 channel 任务调度
//! - 流式下载，内存占用低
//! - 实时进度监控和统计
//! - 使用 `log` crate 进行日志记录
//!
//! ## 示例
//!
//! ### 简单下载
//!
//! ```no_run
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), rs_dn::DownloadError> {
//!     // 简单下载单个文件（自动检测文件名）
//!     let save_path = rs_dn::download_file("https://example.com/file.zip", ".").await?;
//!     println!("文件已保存到: {:?}", save_path);
//!     Ok(())
//! }
//! ```
//!
//! ### Range 分段下载（带进度监听和动态分块）
//!
//! ```no_run
//! use std::path::PathBuf;
//! use rs_dn::{DownloadConfig, DownloadProgress};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), rs_dn::DownloadError> {
//!     // 使用默认配置：4个worker，动态分块（2-50 MB）
//!     let config = DownloadConfig::default();
//!     let (mut handle, save_path) = rs_dn::download_ranged(
//!         "https://example.com/large_file.zip",
//!         PathBuf::from("."),  // 保存到当前目录
//!         config
//!     ).await?;
//!
//!     println!("文件将保存到: {:?}", save_path);
//!
//!     // 监听下载进度
//!     while let Some(progress) = handle.progress_receiver().recv().await {
//!         match progress {
//!             DownloadProgress::Started { total_size, initial_chunk_size, .. } => {
//!                 println!("开始下载，总大小: {:.2} MB, 初始分块: {:.2} MB", 
//!                     total_size as f64 / 1024.0 / 1024.0,
//!                     initial_chunk_size as f64 / 1024.0 / 1024.0);
//!             }
//!             DownloadProgress::Progress { percentage, avg_speed, worker_stats, .. } => {
//!                 // 每个 worker 有各自的分块大小，可从 worker_stats 中获取
//!                 println!("进度: {:.1}%, 速度: {:.2} MB/s, {} 个 workers",
//!                     percentage,
//!                     avg_speed / 1024.0 / 1024.0,
//!                     worker_stats.len());
//!             }
//!             DownloadProgress::Completed { total_bytes, total_time, worker_stats, .. } => {
//!                 println!("下载完成！{:.2} MB in {:.2}s, {} 个 workers 完成",
//!                     total_bytes as f64 / 1024.0 / 1024.0, total_time, worker_stats.len());
//!             }
//!             DownloadProgress::Error { message } => {
//!                 eprintln!("下载出错: {}", message);
//!             }
//!             _ => {}
//!         }
//!     }
//!
//!     // 等待下载完成
//!     handle.wait().await?;
//!     Ok(())
//! }
//! ```
//!
//! ### 使用回调函数监听进度和自定义配置
//!
//! ```no_run
//! use std::path::PathBuf;
//! use rs_dn::{DownloadConfig, DownloadProgress};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), rs_dn::DownloadError> {
//!     // 自定义配置：更多 worker，更大的分块范围
//!     let config = DownloadConfig::builder()
//!         .worker_count(8)                         // 8 个并发 worker
//!         .initial_chunk_size(10 * 1024 * 1024)    // 初始 10 MB 分块
//!         .min_chunk_size(5 * 1024 * 1024)         // 最小 5 MB（慢速时）
//!         .max_chunk_size(100 * 1024 * 1024)       // 最大 100 MB（高速时）
//!         .build();
//!     
//!     let (handle, save_path) = rs_dn::download_ranged(
//!         "https://example.com/large_file.zip",
//!         PathBuf::from("."),
//!         config
//!     ).await?;
//!
//!     println!("文件将保存到: {:?}", save_path);
//!
//!     // 使用回调函数同时接收进度和等待完成
//!     handle.wait_with_progress(|progress| {
//!         match progress {
//!             DownloadProgress::Progress { percentage, .. } => {
//!                 println!("进度: {:.1}%", percentage);
//!             }
//!             _ => {}
//!         }
//!     }).await?;
//!
//!     Ok(())
//! }
//! ```

mod config;
mod download;
mod task;
mod worker;
pub mod cli;
pub mod tools {
    pub(crate) mod chunk_strategy;
    pub(crate) mod fetch;
    pub(crate) mod stats;
    pub mod io_traits;
    pub mod range_writer;
}

// 重新导出核心类型和函数
pub use config::{DownloadConfig, DownloadConfigBuilder};
pub use download::{download_ranged, DownloadHandle, DownloadProgress, WorkerStatSnapshot};
pub use task::FileTask;
pub use tools::fetch::{fetch_file_metadata, FileMetadata};

use std::path::Path;

/// 下载错误类型
#[derive(thiserror::Error, Debug)]
pub enum DownloadError {
    /// Fetch 错误
    #[error(transparent)]
    Fetch(#[from] tools::fetch::FetchError),
    
    /// Range Writer 错误
    #[error(transparent)]
    RangeWriter(#[from] tools::range_writer::RangeWriterError),
    
    /// 任务发送错误
    #[error("任务发送失败: {0}")]
    TaskSend(String),
    
    /// 下载任务 panic
    #[error("下载任务 panic: {0}")]
    TaskPanic(String),
    
    /// Worker 退出失败
    #[error("等待 Worker #{0} 退出失败")]
    WorkerExit(usize),
    
    /// Range Writer 所有权错误
    #[error("无法获取 RangeWriter 的所有权（仍有其他引用）")]
    WriterOwnership,
    
    /// 通用错误
    #[error("{0}")]
    Other(String),
}

/// 下载结果类型
pub type Result<T> = std::result::Result<T, DownloadError>;

/// 下载单个文件
///
/// 使用单个协程下载文件，自动从服务器或 URL 检测文件名
///
/// # Arguments
/// * `url` - 下载 URL
/// * `save_dir` - 保存目录路径
///
/// # Returns
///
/// 返回实际保存的文件路径（目录 + 自动检测的文件名）
///
/// # 文件名检测优先级
///
/// 1. Content-Disposition header（服务器建议的文件名）
/// 2. 重定向后 URL 中的文件名
/// 3. 原始 URL 中的文件名
/// 4. 时间戳文件名 `file_{unix_timestamp}`
///
/// # Example
///
/// ```no_run
/// # use rs_dn::download_file;
/// # use std::path::PathBuf;
/// # #[tokio::main]
/// # async fn main() -> Result<(), rs_dn::DownloadError> {
/// let save_path = download_file("https://example.com/file.txt", ".").await?;
/// println!("文件已保存到: {:?}", save_path);
/// # Ok(())
/// # }
/// ```
pub async fn download_file(url: &str, save_dir: impl AsRef<Path>) -> Result<std::path::PathBuf> {
    use reqwest::Client;
    use tools::fetch;
    use tools::io_traits::TokioFileSystem;

    let client = Client::new();
    let fs = TokioFileSystem::default();
    
    // 获取文件元数据以确定文件名
    let metadata = fetch::fetch_file_metadata(&client, url).await?;
    
    // 确定文件名
    let filename = metadata.suggested_filename
        .ok_or_else(|| DownloadError::Other("无法确定文件名".to_string()))?;
    
    // 组合完整路径
    let save_path = save_dir.as_ref().join(&filename);

    let task = FileTask {
        url: url.to_string(),
        save_path: save_path.clone(),
    };

    fetch::fetch_file(&client, task, &fs).await?;

    Ok(save_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_create_file_task() {
        let task = FileTask {
            url: "https://example.com/test.txt".to_string(),
            save_path: PathBuf::from("test.txt"),
        };

        assert_eq!(task.url, "https://example.com/test.txt");
        assert_eq!(task.save_path, PathBuf::from("test.txt"));
    }
}
