use crate::utils::io_traits::IoError;
use thiserror::Error;

pub mod downloader;
pub mod metadata;
pub mod range;

pub use downloader::{ChunkRecorder, RangeFetcher, fetch_file};
pub use metadata::{
    FileMetadata, FileMetadataFetcher, extract_filename_from_content_disposition,
    extract_filename_from_url, fetch_file_metadata, generate_timestamp_filename, is_valid_filename,
};
pub use range::{FetchRange, FetchRangeResult};

/// Fetch 操作错误类型
#[derive(Error, Debug)]
pub enum FetchError {
    /// IO 错误
    #[error(transparent)]
    Io(#[from] IoError),

    /// HTTP 请求失败
    #[error("HTTP 请求失败，状态码: {0}")]
    HttpStatus(u16),

    /// Content-Range header 缺失
    #[error("缺少 Content-Range header")]
    MissingContentRange,

    /// Content-Range 格式错误
    #[error("Content-Range 格式错误，缺少 '/' 分隔符")]
    InvalidContentRangeFormat,

    /// 范围无效
    #[error("范围无效，start >= end")]
    InvalidRange,

    /// 无法解析文件总大小
    #[error("无法解析文件总大小为 u64: {0}")]
    InvalidContentRangeSize(String),

    /// 下载数据大小与预期不符
    #[error("下载数据大小不匹配: 预期 {expected} bytes，实际 {actual} bytes")]
    SizeMismatch { expected: u64, actual: u64 },
}

pub type Result<T> = std::result::Result<T, FetchError>;
