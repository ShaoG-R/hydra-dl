//! # hydra-dl: Rust 高性能多线程下载器
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
//! async fn main() -> Result<(), hydra_dl::DownloadError> {
//!     // 简单下载单个文件（自动检测文件名）
//!     let save_path = hydra_dl::download_file("https://example.com/file.zip", ".").await?;
//!     println!("文件已保存到: {:?}", save_path);
//!     Ok(())
//! }
//! ```
//!
//! ### Range 分段下载（带进度监听和动态分块）
//!
//! ```no_run
//! use std::path::PathBuf;
//! use hydra_dl::{DownloadConfig, DownloadProgress};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), hydra_dl::DownloadError> {
//!     // 使用默认配置：4个worker，动态分块（2-50 MB）
//!     let config = DownloadConfig::default();
//!     let (mut handle, save_path) = hydra_dl::download_ranged(
//!         "https://example.com/large_file.zip",
//!         PathBuf::from("."),  // 保存到当前目录
//!         config,
//!     ).await?;
//!
//!     println!("文件将保存到: {:?}", save_path);
//!
//!     // 监听下载进度
//!     while let Some(progress) = handle.progress_receiver().recv().await {
//!         match progress {
//!             DownloadProgress::Started { total_size, initial_chunk_size, .. } => {
//!                 println!("开始下载，总大小: {:.2} MB, 初始分块: {:.2} MB",
//!                     total_size.get() as f64 / 1024.0 / 1024.0,
//!                     initial_chunk_size as f64 / 1024.0 / 1024.0);
//!             }
//!             DownloadProgress::Progress { percentage, avg_speed, executor_stats, .. } => {
//!                 // 每个 executor 有各自的统计信息
//!                 let speed_mbps = avg_speed.map(|s| s.as_u64() as f64 / 1024.0 / 1024.0).unwrap_or(0.0);
//!                 println!("进度: {:.1}%, 速度: {:.2} MB/s, {} 个 executors",
//!                     percentage,
//!                     speed_mbps,
//!                     executor_stats.stats_map.len());
//!             }
//!             DownloadProgress::Completed { total_written_bytes, total_time, executor_stats, .. } => {
//!                 println!("下载完成！{:.2} MB in {:.2}s, {} 个 executors 完成",
//!                     total_written_bytes as f64 / 1024.0 / 1024.0, total_time, executor_stats.stats_map.len());
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
//! use hydra_dl::{DownloadConfig, DownloadProgress, constants::*};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), hydra_dl::DownloadError> {
//!     // 自定义配置：更多 worker，更大的分块范围
//!     let config = DownloadConfig::builder()
//!         .concurrency(|c| c.worker_count(8))         // 8 个并发 worker
//!         .chunk(|c| c
//!             .initial_size(10 * MB)                  // 初始 10 MB 分块
//!             .min_size(5 * MB)                       // 最小 5 MB（慢速时）
//!             .max_size(100 * MB))                    // 最大 100 MB（高速时）
//!         .build();
//!     
//!     let (handle, save_path) = hydra_dl::download_ranged(
//!         "https://example.com/large_file.zip",
//!         PathBuf::from("."),
//!         config,
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

pub mod cli;
pub mod config;
pub mod download;
mod task;

pub mod pool {
    pub mod common;
    pub mod download;
}
pub mod utils {
    pub(crate) mod chunk_strategy;
    pub mod fetch;
    pub mod io_traits;
    pub(crate) mod speed_calculator;
    pub(crate) mod stats;
    pub mod writer;
    pub mod cancel_channel;
}

/// 常用单位常量
///
/// 提供方便的字节单位常量，用于配置下载参数
///
/// # Example
///
/// ```
/// use hydra_dl::constants::*;
/// use hydra_dl::DownloadConfig;
/// use std::num::NonZeroU64;
///
/// let config = DownloadConfig::builder()
///     .chunk(|c| c.initial_size(10 * MB))  // 10 MB
///     .progressive(|p| p.min_speed_threshold(NonZeroU64::new(5 * MB)))  // 5 MB/s
///     .build();
/// ```
pub mod constants {
    /// 1 KB = 1024 bytes
    pub const KB: u64 = 1024;
    /// 1 MB = 1024 KB
    pub const MB: u64 = 1024 * KB;
    /// 1 GB = 1024 MB
    pub const GB: u64 = 1024 * MB;
}

// 重新导出核心类型和函数
pub use config::{
    ChunkConfig, ChunkConfigBuilder, ChunkDefaults, ConcurrencyConfig, ConcurrencyConfigBuilder,
    ConcurrencyDefaults, DownloadConfig, DownloadConfigBuilder, NetworkConfig,
    NetworkConfigBuilder, NetworkDefaults, ProgressiveConfig, ProgressiveConfigBuilder,
    ProgressiveDefaults, RetryConfig, RetryConfigBuilder, RetryDefaults, SpeedConfig,
    SpeedConfigBuilder, SpeedDefaults,
};
pub use download::{DownloadHandle, DownloadProgress, download_ranged};
pub use task::FileTask;
pub use utils::fetch::{FileMetadata, fetch_file_metadata};

use std::path::Path;

/// 下载错误类型
#[derive(thiserror::Error, Debug)]
pub enum DownloadError {
    /// Fetch 错误
    #[error(transparent)]
    Fetch(#[from] utils::fetch::FetchError),

    /// HTTP 客户端构建错误
    #[error("创建 HTTP 客户端失败: {0}")]
    BuildClient(#[from] reqwest::Error),

    /// Mmap 写入错误
    #[error(transparent)]
    MmapWrite(#[from] utils::writer::MmapWriterError),

    /// 任务发送错误
    #[error("任务发送失败: {0}")]
    TaskSend(String),

    /// 下载任务 panic
    #[error("下载任务 panic: {0}")]
    TaskPanic(String),

    /// Worker 退出失败
    #[error("等待 Worker #{0} 退出失败")]
    WorkerExit(usize),

    /// Worker 不存在或已被移除
    #[error("Worker #{0} 不存在或已被移除")]
    WorkerNotFound(usize),

    /// Worker 数量超过最大限制
    #[error("Worker 数量 {0} 超过最大限制 {1}")]
    WorkerCountExceeded(usize, usize),

    /// Worker 池已满
    #[error("Worker 池已满，无法添加更多 worker（最大 {0} 个）")]
    WorkerPoolFull(usize),

    /// Worker ID 无效（超出范围）
    #[error("Worker ID {0} 无效，必须在范围 [0, {1}) 内")]
    InvalidWorkerId(usize, usize),

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
/// # use hydra_dl::download_file;
/// # use std::path::PathBuf;
/// # #[tokio::main]
/// # async fn main() -> Result<(), hydra_dl::DownloadError> {
/// let save_path = download_file("https://example.com/file.txt", ".").await?;
/// println!("文件已保存到: {:?}", save_path);
/// # Ok(())
/// # }
/// ```
pub async fn download_file(url: &str, save_dir: impl AsRef<Path>) -> Result<std::path::PathBuf> {
    use reqwest::Client;
    use utils::fetch;

    // 创建带超时设置的 HTTP 客户端（使用默认超时配置）
    let default_config = DownloadConfig::default();
    let client = Client::builder()
        .timeout(default_config.network().timeout())
        .connect_timeout(default_config.network().connect_timeout())
        .build()?;

    // 获取文件元数据以确定文件名
    let metadata = fetch::fetch_file_metadata(&client, url).await?;

    // 确定文件名
    let filename = metadata
        .suggested_filename
        .ok_or_else(|| DownloadError::Other("无法确定文件名".to_string()))?;

    // 组合完整路径
    let save_path = save_dir.as_ref().join(&filename);

    let task = FileTask {
        url: url.to_string(),
        save_path: save_path.clone(),
    };

    fetch::fetch_file(&client, task).await?;

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
