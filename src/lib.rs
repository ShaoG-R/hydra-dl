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
//! async fn main() -> anyhow::Result<()> {
//!     // 简单下载单个文件
//!     rs_dn::download_file("https://example.com/file.zip", "file.zip").await?;
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
//! async fn main() -> anyhow::Result<()> {
//!     // 使用默认配置：4个worker，动态分块（2-50 MB）
//!     let config = DownloadConfig::default();
//!     let mut handle = rs_dn::download_ranged(
//!         "https://example.com/large_file.zip",
//!         PathBuf::from("large_file.zip"),
//!         config
//!     ).await?;
//!
//!     // 监听下载进度
//!     while let Some(progress) = handle.progress_receiver().recv().await {
//!         match progress {
//!             DownloadProgress::Started { total_size, initial_chunk_size, .. } => {
//!                 println!("开始下载，总大小: {:.2} MB, 初始分块: {:.2} MB", 
//!                     total_size as f64 / 1024.0 / 1024.0,
//!                     initial_chunk_size as f64 / 1024.0 / 1024.0);
//!             }
//!             DownloadProgress::Progress { percentage, avg_speed, current_chunk_size, worker_stats, .. } => {
//!                 println!("进度: {:.1}%, 速度: {:.2} MB/s, 当前分块: {:.2} MB, {} 个 workers",
//!                     percentage,
//!                     avg_speed / 1024.0 / 1024.0,
//!                     current_chunk_size as f64 / 1024.0 / 1024.0,
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
//! async fn main() -> anyhow::Result<()> {
//!     // 自定义配置：更多 worker，更大的分块范围
//!     let config = DownloadConfig::builder()
//!         .worker_count(8)                         // 8 个并发 worker
//!         .initial_chunk_size(10 * 1024 * 1024)    // 初始 10 MB 分块
//!         .min_chunk_size(5 * 1024 * 1024)         // 最小 5 MB（慢速时）
//!         .max_chunk_size(100 * 1024 * 1024)       // 最大 100 MB（高速时）
//!         .build();
//!     
//!     let handle = rs_dn::download_ranged(
//!         "https://example.com/large_file.zip",
//!         PathBuf::from("large_file.zip"),
//!         config
//!     ).await?;
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

use anyhow::Result;
use std::path::Path;

/// 下载单个文件
///
/// 使用单个协程下载文件
///
/// # Arguments
/// * `url` - 下载 URL
/// * `path` - 保存路径
///
/// # Example
///
/// ```no_run
/// # use rs_dn::download_file;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// download_file("https://example.com/file.txt", "file.txt").await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_file(url: &str, path: impl AsRef<Path>) -> Result<()> {
    use reqwest::Client;
    use tools::fetch;
    use tools::io_traits::TokioFileSystem;

    let task = FileTask {
        url: url.to_string(),
        save_path: path.as_ref().to_path_buf(),
    };

    let client = Client::new();
    let fs = TokioFileSystem::default();
    fetch::fetch_file(&client, task, &fs).await?;

    Ok(())
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
