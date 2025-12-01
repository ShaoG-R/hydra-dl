use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use lite_sync::oneshot::lite;
use log::{debug, info};
use ranged_mmap::AllocatedRange;
use smr_swap::SmrSwap;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

use crate::constants::KB;
use crate::task::FileTask;
use crate::utils::io_traits::{HttpClient, HttpResponse, IoError};
use crate::utils::stats::WorkerStats;

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

// ============================================================================
// FetchRange：专为 Fetch 设计的 Range 类型
// ============================================================================

/// Fetch 专用的 Range 类型
///
/// 用于处理 HTTP Range 请求的闭区间 `[start, end]` 和文件操作的半开区间 `[start, end)` 之间的转换。
///
/// # 区间格式说明
///
/// - **文件操作（半开区间）**：`[start, end)` - start 包含，end 不包含
///   - 例如：`[0, 10)` 表示字节 0-9，共 10 字节
///   - 这是 `AllocatedRange` 使用的格式
///
/// - **HTTP Range 请求（闭区间）**：`[start, end]` - start 和 end 都包含
///   - 例如：`[0, 9]` 表示字节 0-9，共 10 字节
///   - HTTP Range header 格式：`Range: bytes=0-9`
///
/// # 转换关系
///
/// - 文件范围 `[start, end)` → HTTP Range `[start, end-1]`
/// - HTTP Range `[start, end]` → 文件范围 `[start, end+1)`
///
/// # Examples
///
/// ```rust
/// # use hydra_dl::utils::fetch::FetchRange;
/// # use ranged_mmap::{MmapFile, AllocatedRange};
/// # use std::num::NonZeroU64;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // 从 AllocatedRange 创建
/// let (file, mut allocator) = MmapFile::create_default("test.bin", NonZeroU64::new(1000).unwrap())?;
/// let allocated = allocator.allocate(NonZeroU64::new(10).unwrap()).unwrap();
/// let fetch_range = FetchRange::from_allocated_range(&allocated)?;
///
/// // 获取 HTTP Range（闭区间）
/// let (http_start, http_end) = fetch_range.as_http_range();
/// assert_eq!(http_start, 0);
/// assert_eq!(http_end, 9); // end-1 因为 HTTP Range 是闭区间
///
/// // 获取文件范围（半开区间）
/// let (file_start, file_end) = fetch_range.as_file_range();
/// assert_eq!(file_start, 0);
/// assert_eq!(file_end, 10); // 半开区间
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchRange {
    /// 范围起始位置（包含）
    start: u64,

    /// 范围结束位置（不包含）- 文件操作使用的半开区间格式
    end: u64,
}

impl FetchRange {
    /// 从 AllocatedRange 创建
    #[inline]
    pub fn from_allocated_range(range: &AllocatedRange) -> Result<Self> {
        if range.start() >= range.end() {
            return Err(FetchError::InvalidRange);
        }
        Ok(Self {
            start: range.start(),
            end: range.end(),
        })
    }

    /// 获取 HTTP Range 格式（闭区间 `[start, end]`）
    ///
    /// 返回用于 HTTP Range 请求的闭区间。
    ///
    /// # Returns
    /// `(start, end)` - 其中 start 和 end 都是包含的
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use hydra_dl::utils::fetch::FetchRange;
    /// # use ranged_mmap::{MmapFile, AllocatedRange};
    /// # use std::num::NonZeroU64;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let (file, mut allocator) = MmapFile::create_default("test.bin", NonZeroU64::new(1000).unwrap())?;
    /// let allocated = allocator.allocate(NonZeroU64::new(10).unwrap()).unwrap();
    /// let range = FetchRange::from_allocated_range(&allocated)?;
    /// let (http_start, http_end) = range.as_http_range();
    /// assert_eq!(http_start, 0);
    /// assert_eq!(http_end, 9); // 文件的 [0, 10) 对应 HTTP 的 [0, 9]
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn as_http_range(&self) -> (u64, u64) {
        // HTTP Range 是闭区间 [start, end]
        // 文件范围 [start, end) 转换为 HTTP Range [start, end-1]
        (self.start, self.end.saturating_sub(1))
    }

    /// 获取文件范围格式（半开区间 `[start, end)`）
    ///
    /// 返回用于文件操作的半开区间。
    ///
    /// # Returns
    /// `(start, end)` - 其中 start 包含，end 不包含
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use hydra_dl::utils::fetch::FetchRange;
    /// # use ranged_mmap::{MmapFile, AllocatedRange};
    /// # use std::num::NonZeroU64;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let (file, mut allocator) = MmapFile::create_default("test.bin", NonZeroU64::new(1000).unwrap())?;
    /// let allocated = allocator.allocate(NonZeroU64::new(10).unwrap()).unwrap();
    /// let range = FetchRange::from_allocated_range(&allocated)?;
    /// let (file_start, file_end) = range.as_file_range();
    /// assert_eq!(file_start, 0);
    /// assert_eq!(file_end, 10); // 半开区间
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn as_file_range(&self) -> (u64, u64) {
        (self.start, self.end)
    }

    /// 获取范围长度
    #[inline]
    pub fn len(&self) -> u64 {
        self.end - self.start
    }
}

/// fetch_range 的返回结果
#[derive(Debug, Clone)]
pub enum FetchRangeResult {
    /// 下载完成
    Complete(Bytes),
    /// 下载被取消（包含已下载的部分数据和已下载的字节数）
    Cancelled {
        /// 已下载的部分数据
        data: Bytes,
        /// 已下载的字节数
        ///
        /// 调用方可以使用此值配合 `AllocatedRange::split_at` 方法来拆分原始 range
        bytes_downloaded: u64,
    },
}

// ============================================================================
// 文件元数据结构体
// ============================================================================

/// 文件元数据
///
/// 包含文件的所有相关元数据信息，包括 Range 支持、大小和建议的文件名
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// 服务器是否支持 Range 请求
    pub range_supported: bool,
    /// 文件大小（bytes）
    pub content_length: Option<u64>,
    /// 服务器建议的文件名（从 Content-Disposition 或 URL 提取）
    pub suggested_filename: Option<String>,
    /// 重定向后的最终 URL
    pub final_url: Option<String>,
}

// ============================================================================
// 文件名提取和验证辅助函数
// ============================================================================

/// 从 Content-Disposition header 中提取文件名
///
/// 支持以下格式：
/// - `attachment; filename="example.zip"`
/// - `attachment; filename=example.zip`
/// - `attachment; filename*=UTF-8''example.zip`
///
/// # Arguments
///
/// * `header_value` - Content-Disposition header 的值
///
/// # Returns
///
/// 成功时返回解析出的文件名，失败时返回 None
pub fn extract_filename_from_content_disposition(header_value: &str) -> Option<String> {
    // 尝试提取 filename*= (RFC 5987 编码格式)
    if let Some(start_idx) = header_value.find("filename*=") {
        let value = &header_value[start_idx + 10..]; // "filename*=".len() == 10

        // 格式：UTF-8''filename 或 charset'lang'filename
        if let Some(quote_idx) = value.find("''") {
            let encoded = value[quote_idx + 2..].trim_matches('"').trim_matches('\'');
            // URL 解码
            if let Ok(decoded) = urlencoding::decode(encoded) {
                let filename = decoded.to_string();
                if is_valid_filename(&filename) {
                    return Some(filename);
                }
            }
        }
    }

    // 尝试提取 filename= (标准格式)
    if let Some(start_idx) = header_value.find("filename=") {
        let value = &header_value[start_idx + 9..]; // "filename=".len() == 9

        // 移除引号和分号
        let filename = value
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();

        if is_valid_filename(&filename) {
            return Some(filename);
        }
    }

    None
}

/// 从 URL 中提取文件名
///
/// 提取 URL 路径中最后一个 `/` 之后的部分，并进行 URL 解码
///
/// # Arguments
///
/// * `url` - URL 字符串
///
/// # Returns
///
/// 成功时返回文件名，失败时返回 None
pub fn extract_filename_from_url(url: &str) -> Option<String> {
    // 解析 URL
    let parsed_url = url::Url::parse(url).ok()?;

    // 获取路径段
    let mut path_segments = parsed_url.path_segments()?;

    // 获取最后一个段作为文件名
    let filename = path_segments.next_back()?;

    if filename.is_empty() {
        return None;
    }

    // URL 解码
    let decoded = urlencoding::decode(filename).ok()?.to_string();

    if is_valid_filename(&decoded) {
        Some(decoded)
    } else {
        None
    }
}

/// 验证文件名是否合法
///
/// 检查文件名是否包含文件系统非法字符或为空
///
/// Windows 非法字符：`< > : " / \ | ? *`
/// 同时检查是否为空字符串或只有空格
///
/// # Arguments
///
/// * `name` - 待验证的文件名
///
/// # Returns
///
/// 如果文件名合法返回 true，否则返回 false
pub fn is_valid_filename(name: &str) -> bool {
    if name.trim().is_empty() {
        return false;
    }

    // Windows 文件系统非法字符
    const INVALID_CHARS: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];

    !name.chars().any(|c| INVALID_CHARS.contains(&c))
}

/// 生成基于时间戳的文件名
///
/// 格式：`file_{unix_timestamp}`
///
/// # Returns
///
/// 返回生成的文件名
pub fn generate_timestamp_filename() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    format!("file_{}", timestamp)
}

/// 获取并保存完整文件
///
/// 用于不支持 Range 请求的文件下载，使用标准的流式写入
pub async fn fetch_file<C>(client: &C, task: FileTask) -> Result<()>
where
    C: HttpClient,
{
    use tokio::io::AsyncWriteExt;

    info!("开始下载: {} -> {:?}", task.url, task.save_path);

    let response = client.get(&task.url).await?;

    if !response.status().is_success() {
        return Err(FetchError::HttpStatus(response.status().as_u16()));
    }

    // 创建文件（使用标准的 tokio::fs::File）
    let mut file = tokio::fs::File::create(&task.save_path)
        .await
        .map_err(IoError::FileCreate)?;

    // 流式下载
    let mut stream = response.bytes_stream();
    let mut downloaded: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let chunk_len = chunk.len() as u64;

        // 写入数据
        file.write_all(&chunk).await.map_err(IoError::FileWrite)?;

        downloaded += chunk_len;
    }

    // 刷新到磁盘
    file.sync_data().await.map_err(IoError::FileFlush)?;

    info!(
        "下载完成: {} ({} bytes) -> {:?}",
        task.url, downloaded, task.save_path
    );

    Ok(())
}

/// Range 下载器
///
/// 封装 Range 下载的参数和逻辑，提供可取消和不可取消两种下载方式
pub struct RangeFetcher<'a, C: HttpClient> {
    client: &'a C,
    url: &'a str,
    range: FetchRange,
    stats: &'a mut SmrSwap<WorkerStats>,
}

impl<'a, C: HttpClient> RangeFetcher<'a, C> {
    /// 创建新的 Range 下载器
    ///
    /// # Arguments
    /// * `client` - HTTP 客户端
    /// * `url` - 下载 URL
    /// * `range` - 已分配的 Range，定义要下载的字节范围
    /// * `stats` - 共享的下载统计，每个 chunk 到达时实时更新
    pub(crate) fn new(
        client: &'a C,
        url: &'a str,
        range: FetchRange,
        stats: &'a mut SmrSwap<WorkerStats>,
    ) -> Self {
        Self {
            client,
            url,
            range,
            stats,
        }
    }

    /// 下载指定范围的数据（可取消）
    ///
    /// # Arguments
    /// * `cancel_rx` - 取消信号接收器，收到信号时中途停止下载
    ///
    /// # Returns
    ///
    /// 返回 `FetchRangeResult`：
    /// - `Complete(Bytes)`: 下载完整完成，包含所有请求的数据
    /// - `Cancelled { data, bytes_downloaded }`: 下载被取消，包含已下载的部分数据和已下载的字节数
    pub async fn fetch_with_cancel(
        mut self,
        cancel_rx: lite::Receiver<()>,
    ) -> Result<FetchRangeResult> {
        let (http_start, http_end) = self.range.as_http_range();
        debug!(
            "开始下载 Range (可取消): {} (bytes {}-{})",
            self.url, http_start, http_end
        );

        let response = self
            .client
            .get_with_range(self.url, http_start, http_end)
            .await?;

        if !response.status().is_success() && response.status().as_u16() != 206 {
            return Err(FetchError::HttpStatus(response.status().as_u16()));
        }

        let result = self
            .download_stream_with_cancel(response.bytes_stream(), cancel_rx)
            .await?;

        let (start, end) = self.range.as_file_range();
        match &result {
            FetchRangeResult::Complete(data) => {
                debug!("Range {}..{} 下载完成: {} bytes", start, end, data.len());
            }
            FetchRangeResult::Cancelled {
                bytes_downloaded, ..
            } => {
                debug!(
                    "Range {}..{} 下载被取消，已下载: {} bytes",
                    start, end, bytes_downloaded
                );
            }
        }

        Ok(result)
    }

    /// 下载数据流（可取消版本）
    async fn download_stream_with_cancel<S>(
        &mut self,
        mut stream: S,
        mut cancel_rx: lite::Receiver<()>,
    ) -> Result<FetchRangeResult>
    where
        S: futures::Stream<Item = std::result::Result<Bytes, IoError>> + Unpin,
    {
        let expected_size = self.range.len();

        // 性能优化 1: Vec 容量预分配
        // 典型 HTTP chunk 大小为 8KB-64KB，这里使用 16KB 作为保守估计
        // 预分配可以避免多次 realloc 和数据移动
        let estimated_chunks = ((expected_size + 16 * KB - 1) / (16 * KB)).max(4) as usize;
        let mut chunks = Vec::with_capacity(estimated_chunks);
        let mut downloaded_bytes = 0u64;

        loop {
            // 性能优化 4: biased select - 优先检查数据流（热路径）
            tokio::select! {
                biased;
                chunk_result = stream.next() => {
                    match chunk_result {
                        Some(chunk) => {
                            let chunk = chunk?;
                            let chunk_size = chunk.len() as u64;

                            // 实时记录 chunk
                            self.stats.update_and_fetch(|stats| {
                                let mut stats = stats.clone();
                                stats.record_chunk(chunk_size);
                                stats
                            });

                            chunks.push(chunk);
                            downloaded_bytes += chunk_size;
                        }
                        None => {
                            // 流结束，下载完成，验证大小并合并
                            if downloaded_bytes != expected_size {
                                return Err(FetchError::SizeMismatch {
                                    expected: expected_size,
                                    actual: downloaded_bytes,
                                });
                            }
                            let data = self.merge_chunks(chunks);
                            return Ok(FetchRangeResult::Complete(data));
                        }
                    }
                }
                _ = &mut cancel_rx => {
                    // 收到取消信号，返回已下载的字节数和数据
                    let data = self.merge_chunks(chunks);
                    return Ok(FetchRangeResult::Cancelled {
                        data,
                        bytes_downloaded: downloaded_bytes,
                    });
                }
            }
        }
    }

    /// 合并多个 Bytes chunk 为单个 Bytes
    ///
    /// 性能优化：
    /// - 零拷贝：单个 chunk 直接返回
    /// - 预分配：精确分配总大小，避免重新分配
    /// - 批量复制：一次性合并所有数据
    #[inline]
    fn merge_chunks(&self, chunks: Vec<Bytes>) -> Bytes {
        match chunks.len() {
            0 => Bytes::new(),
            1 => {
                // 性能优化 2: 单 chunk 零拷贝快速路径
                // SAFETY: 已确认 len == 1
                unsafe { chunks.into_iter().next().unwrap_unchecked() }
            }
            _ => {
                // 性能优化 2: 精确预分配并批量复制
                let total_size: usize = chunks.iter().map(|c| c.len()).sum();
                let mut buffer = BytesMut::with_capacity(total_size);

                // 编译器可能会向量化这个循环
                for chunk in chunks {
                    buffer.extend_from_slice(&chunk);
                }

                buffer.freeze()
            }
        }
    }
}

/// 文件元数据获取器
///
/// 封装获取文件元数据的逻辑，包括：
/// - Range 请求支持检测
/// - 文件大小获取
/// - 文件名提取（从 Content-Disposition 或 URL）
///
/// 提供多种检测策略：
/// 1. 首先尝试 HEAD 请求（高效，无需下载数据）
/// 2. HEAD 失败时回退到小的 Range GET 请求（bytes=0-0，处理不支持 HEAD 的服务器）
pub struct FileMetadataFetcher<'a, C: HttpClient> {
    client: &'a C,
    url: &'a str,
}

impl<'a, C: HttpClient> FileMetadataFetcher<'a, C> {
    /// 创建新的文件元数据获取器
    pub fn new(client: &'a C, url: &'a str) -> Self {
        Self { client, url }
    }

    /// 获取文件元数据
    ///
    /// 策略：
    /// 1. 首先尝试 HEAD 请求（高效）
    /// 2. HEAD 失败时（如 405 Method Not Allowed）回退到小的 Range GET 请求（bytes=0-0）
    ///
    /// 这种策略能够处理以下场景：
    /// - 服务器不支持 HEAD 方法
    /// - 原始 URL 通过 302 重定向到实际下载链接
    /// - reqwest 自动跟随重定向，Range GET 可到达最终 URL
    ///
    /// 文件名提取优先级：
    /// 1. Content-Disposition header
    /// 2. 重定向后 URL 的文件名
    /// 3. 原始 URL 的文件名
    /// 4. 时间戳文件名
    pub async fn fetch(&self) -> Result<FileMetadata> {
        // 尝试 HEAD
        match self.fetch_with_head().await {
            Ok(metadata) => {
                debug!(
                    "HEAD 请求成功获取元数据: range={}, size={:?}, filename={:?}",
                    metadata.range_supported, metadata.content_length, metadata.suggested_filename
                );
                Ok(metadata)
            }
            Err(e) => {
                debug!("HEAD 请求失败: {}, 尝试 Range GET 回退", e);
                self.fetch_with_range_get().await
            }
        }
    }

    /// 使用 HEAD 请求获取元数据
    async fn fetch_with_head(&self) -> Result<FileMetadata> {
        let response = self.client.head(self.url).await?;

        // 检查状态码，如果不是 2xx 成功状态，返回错误触发回退
        if !response.status().is_success() {
            return Err(FetchError::HttpStatus(response.status().as_u16()));
        }

        Self::parse_metadata_from_response(&response, self.url)
    }

    /// 使用小的 Range GET 请求获取元数据（bytes=0-0）
    ///
    /// 发送一个只请求第一个字节的 Range 请求：
    /// - 如果服务器支持 Range，返回 206 Partial Content + Content-Range header
    /// - 如果不支持，可能返回 200 OK + 完整内容（或部分内容）
    async fn fetch_with_range_get(&self) -> Result<FileMetadata> {
        debug!("发送 Range GET 请求 (bytes=0-0) 到: {}", self.url);
        let response = self.client.get_with_range(self.url, 0, 0).await?;
        Self::parse_metadata_from_range_response(&response, self.url)
    }

    /// 从响应中解析文件元数据（通用方法，适用于 HEAD 和 GET）
    ///
    /// 提取：
    /// - Accept-Ranges header（Range 支持）
    /// - Content-Length header（文件大小）
    /// - Content-Disposition header（文件名）
    /// - 最终 URL（重定向后）
    fn parse_metadata_from_response<R: HttpResponse>(
        response: &R,
        original_url: &str,
    ) -> Result<FileMetadata> {
        let range_supported = response
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .map(|v| v != "none")
            .unwrap_or(false);

        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        let final_url = response.url().to_string();

        // 提取文件名（按优先级）
        let suggested_filename = Self::determine_filename(response, &final_url, original_url);

        Ok(FileMetadata {
            range_supported,
            content_length,
            suggested_filename,
            final_url: Some(final_url),
        })
    }

    /// 从 Range GET 响应解析元数据
    ///
    /// 根据状态码判断：
    /// - 206 Partial Content：支持 Range，从 Content-Range 获取总大小
    /// - 200 OK：不支持 Range，从 Content-Length 获取大小
    /// - 其他：检测失败
    fn parse_metadata_from_range_response<R: HttpResponse>(
        response: &R,
        original_url: &str,
    ) -> Result<FileMetadata> {
        let status = response.status();
        let final_url = response.url().to_string();

        if status.as_u16() == 206 {
            // 支持 Range，从 Content-Range 获取总大小
            let content_length = Self::parse_content_range_total(response)?;
            debug!(
                "Range GET 返回 206，支持 Range，文件大小: {} bytes",
                content_length
            );

            let suggested_filename = Self::determine_filename(response, &final_url, original_url);

            Ok(FileMetadata {
                range_supported: true,
                content_length: Some(content_length),
                suggested_filename,
                final_url: Some(final_url),
            })
        } else if status.is_success() {
            // 不支持 Range，从 Content-Length 获取大小
            let content_length = response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());

            debug!(
                "Range GET 返回 {}, 不支持 Range，文件大小: {:?}",
                status, content_length
            );

            let suggested_filename = Self::determine_filename(response, &final_url, original_url);

            Ok(FileMetadata {
                range_supported: false,
                content_length,
                suggested_filename,
                final_url: Some(final_url),
            })
        } else {
            Err(FetchError::HttpStatus(status.as_u16()))
        }
    }

    /// 确定文件名（按优先级）
    ///
    /// 优先级：
    /// 1. Content-Disposition header
    /// 2. 重定向后 URL 的文件名
    /// 3. 原始 URL 的文件名
    /// 4. 时间戳文件名
    fn determine_filename<R: HttpResponse>(
        response: &R,
        final_url: &str,
        original_url: &str,
    ) -> Option<String> {
        // 1. 尝试从 Content-Disposition 提取
        if let Some(content_disp) = response.headers().get("content-disposition")
            && let Ok(value) = content_disp.to_str()
            && let Some(filename) = extract_filename_from_content_disposition(value)
        {
            debug!("从 Content-Disposition 提取文件名: {}", filename);
            return Some(filename);
        }

        // 2. 尝试从重定向后的 URL 提取
        if final_url != original_url
            && let Some(filename) = extract_filename_from_url(final_url)
        {
            debug!("从重定向后 URL 提取文件名: {}", filename);
            return Some(filename);
        }

        // 3. 尝试从原始 URL 提取
        if let Some(filename) = extract_filename_from_url(original_url) {
            debug!("从原始 URL 提取文件名: {}", filename);
            return Some(filename);
        }

        // 4. 使用时间戳文件名
        let timestamp_filename = generate_timestamp_filename();
        debug!("使用时间戳文件名: {}", timestamp_filename);
        Some(timestamp_filename)
    }

    /// 从 Content-Range header 解析总大小
    ///
    /// Content-Range 格式：`bytes <start>-<end>/<total>`
    /// 例如：`bytes 0-0/12345` 表示总大小为 12345 字节
    fn parse_content_range_total<R: HttpResponse>(response: &R) -> Result<u64> {
        let content_range = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .ok_or(FetchError::MissingContentRange)?;

        // 解析 "bytes 0-0/12345" 格式，提取 "/" 后的总大小
        let total = content_range
            .split('/')
            .nth(1)
            .ok_or(FetchError::InvalidContentRangeFormat)?
            .trim()
            .parse::<u64>()
            .map_err(|e| FetchError::InvalidContentRangeSize(e.to_string()))?;

        Ok(total)
    }
}

/// 获取文件元数据
///
/// 内部使用 `FileMetadataFetcher`，会自动处理 HEAD 失败的情况
///
/// 提取的信息包括：
/// - Range 请求支持
/// - 文件大小
/// - 服务器建议的文件名（从 Content-Disposition 或 URL）
/// - 重定向后的最终 URL
#[inline]
pub async fn fetch_file_metadata<C>(client: &C, url: &str) -> Result<FileMetadata>
where
    C: HttpClient,
{
    FileMetadataFetcher::new(client, url).fetch().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::io_traits::mock::{MockHttpClient, MockHttpResponse};
    use ranged_mmap::MmapFile;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_fetch_file_success() {
        // 准备测试数据
        let test_data = b"Hello, World! This is test data.";
        let test_url = "http://example.com/file.txt";
        let temp_dir = tempfile::tempdir().unwrap();
        let save_path = temp_dir.path().join("test_file.txt");

        // 创建 mock 客户端
        let client = MockHttpClient::new();

        // 设置 GET 请求的响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-length",
            test_data.len().to_string().parse().unwrap(),
        );
        client.set_response(
            test_url,
            reqwest::StatusCode::OK,
            headers,
            Bytes::from_static(test_data),
        );

        // 创建任务
        let task = FileTask {
            url: test_url.to_string(),
            save_path: save_path.clone(),
        };

        // 执行下载
        let result = fetch_file(&client, task).await;
        assert!(result.is_ok(), "下载应该成功");

        // 验证请求日志
        let log = client.get_request_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0], format!("GET {}", test_url));

        // 注意：由于使用了真实的 AsyncFile，文件会实际创建在磁盘上
        // 这里我们只验证下载逻辑是否正常
    }

    #[tokio::test]
    async fn test_fetch_file_http_error() {
        let test_url = "http://example.com/not_found.txt";
        let save_path = PathBuf::from("/tmp/test_file.txt");

        let client = MockHttpClient::new();

        // 设置 404 响应
        client.set_response(
            test_url,
            reqwest::StatusCode::NOT_FOUND,
            HeaderMap::new(),
            Bytes::new(),
        );

        let task = FileTask {
            url: test_url.to_string(),
            save_path,
        };

        // 执行下载
        let result = fetch_file(&client, task).await;
        assert!(result.is_err(), "应该返回错误");
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_complete_download() {
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试完整下载（不触发取消）
        let test_url = "http://example.com/file.bin";
        // 使用 4K 对齐的数据大小
        let full_data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();

        let client = MockHttpClient::new();
        let mut stats = SmrSwap::new(WorkerStats::default());

        // 创建临时文件和 allocator
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(full_data.len() as u64).unwrap()).unwrap();
        let range = allocator.allocate(NonZeroU64::new(4096).unwrap()).unwrap(); // 分配整个 4K

        let expected_data = &full_data[0..4096];

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-4095/{}", full_data.len()).parse().unwrap(),
        );
        client.set_range_response(
            test_url,
            0,
            4095,
            reqwest::StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::copy_from_slice(expected_data),
        );

        // 创建一个永远不会发送的取消信号
        let (_cancel_tx, cancel_rx) = lite::channel();

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut stats)
            .fetch_with_cancel(cancel_rx)
            .await;

        assert!(result.is_ok(), "下载应该成功: {:?}", result);

        // 验证返回完整数据
        match result.unwrap() {
            FetchRangeResult::Complete(data) => {
                assert_eq!(data.as_ref(), expected_data, "下载的数据应该匹配");
            }
            FetchRangeResult::Cancelled { .. } => {
                panic!("不应该被取消");
            }
        }

        // 验证统计信息
        let (total_bytes, _, _) = stats.load().get_summary();
        assert_eq!(
            total_bytes,
            expected_data.len() as u64,
            "统计的字节数应该匹配"
        );
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_http_error() {
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试 HTTP 错误
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();
        let mut stats = SmrSwap::new(WorkerStats::default());

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) = MmapFile::create_default(path, NonZeroU64::new(100).unwrap()).unwrap();
        let range = allocator.allocate(NonZeroU64::new(10).unwrap()).unwrap();

        // 设置错误响应
        client.set_range_response(
            test_url,
            0,
            9,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            HeaderMap::new(),
            Bytes::new(),
        );

        // 创建取消通道（虽然不会用到）
        let (_cancel_tx, cancel_rx) = lite::channel();

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut stats)
            .fetch_with_cancel(cancel_rx)
            .await;

        assert!(result.is_err(), "应该返回错误");
    }

    #[tokio::test]
    async fn test_check_range_support_supported() {
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置支持 Range 的响应
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("content-length", "1024".parse().unwrap());
        client.set_head_response(test_url, reqwest::StatusCode::OK, headers);

        // 检查 Range 支持
        let result = fetch_file_metadata(&client, test_url).await;
        assert!(result.is_ok(), "检查应该成功");

        let range_support = result.unwrap();
        assert!(range_support.range_supported, "应该支持 Range");
        assert_eq!(range_support.content_length, Some(1024), "文件大小应该匹配");

        // 验证使用了 HEAD 请求
        let log = client.get_request_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0], format!("HEAD {}", test_url));
    }

    #[tokio::test]
    async fn test_check_range_support_not_supported() {
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置不支持 Range 的响应
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", "none".parse().unwrap());
        headers.insert("content-length", "2048".parse().unwrap());
        client.set_head_response(test_url, reqwest::StatusCode::OK, headers);

        // 检查 Range 支持
        let result = fetch_file_metadata(&client, test_url).await;
        assert!(result.is_ok(), "检查应该成功");

        let range_support = result.unwrap();
        assert!(!range_support.range_supported, "应该不支持 Range");
        assert_eq!(range_support.content_length, Some(2048), "文件大小应该匹配");
    }

    #[tokio::test]
    async fn test_check_range_support_no_header() {
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置没有 accept-ranges 头的响应
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "512".parse().unwrap());
        client.set_head_response(test_url, reqwest::StatusCode::OK, headers);

        // 检查 Range 支持
        let result = fetch_file_metadata(&client, test_url).await;
        assert!(result.is_ok(), "检查应该成功");

        let range_support = result.unwrap();
        assert!(
            !range_support.range_supported,
            "没有 accept-ranges 头时应该认为不支持"
        );
        assert_eq!(range_support.content_length, Some(512), "文件大小应该匹配");
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_large_stream() {
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试流式传输大数据（不触发取消）
        let test_url = "http://example.com/large_file.bin";

        // 创建一个较大的数据（大于默认 chunk size 8192）
        let large_data: Vec<u8> = (0..20000).map(|i| (i % 256) as u8).collect();
        let start = 0;
        let end = large_data.len() as u64;

        let client = MockHttpClient::new();
        let mut stats = SmrSwap::new(WorkerStats::default());

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) = MmapFile::create_default(path, NonZeroU64::new(end).unwrap()).unwrap();
        let range = allocator.allocate(NonZeroU64::new(end).unwrap()).unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes {}-{}/{}", start, end - 1, end)
                .parse()
                .unwrap(),
        );
        client.set_range_response(
            test_url,
            start,
            end - 1,
            reqwest::StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from(large_data.clone()),
        );

        // 创建取消通道（不发送取消信号）
        let (_cancel_tx, cancel_rx) = lite::channel();

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut stats)
            .fetch_with_cancel(cancel_rx)
            .await;

        assert!(result.is_ok(), "大数据下载应该成功");

        // 验证返回完整数据
        match result.unwrap() {
            FetchRangeResult::Complete(data) => {
                assert_eq!(data.len(), large_data.len(), "下载的数据大小应该匹配");
                assert_eq!(data.as_ref(), &large_data[..], "下载的数据内容应该匹配");
            }
            FetchRangeResult::Cancelled { .. } => {
                panic!("不应该被取消");
            }
        }

        // 验证统计信息（应该记录了多个 chunk）
        let (total_bytes, _, _) = stats.load().get_summary();
        assert_eq!(total_bytes, large_data.len() as u64, "统计的字节数应该匹配");
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_cancelled_midway() {
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试中途取消下载
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 36 bytes

        let client = MockHttpClient::new();
        let mut stats = SmrSwap::new(WorkerStats::default());

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(test_data.len() as u64).unwrap()).unwrap();
        let range = allocator
            .allocate(NonZeroU64::new(test_data.len() as u64).unwrap())
            .unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-35/{}", test_data.len()).parse().unwrap(),
        );
        client.set_range_response(
            test_url,
            0,
            35,
            reqwest::StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from_static(test_data),
        );

        // 创建取消通道，在稍后发送取消信号
        let (cancel_tx, cancel_rx) = lite::channel();

        tokio::spawn(async move {
            // 给予一点时间让下载开始
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let _ = cancel_tx.notify(());
        });

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut stats)
            .fetch_with_cancel(cancel_rx)
            .await;

        assert!(result.is_ok(), "调用应该成功");

        // 验证结果（注意：Mock 实现可能一次性返回所有数据，在取消前就完成）
        match result.unwrap() {
            FetchRangeResult::Cancelled {
                data,
                bytes_downloaded,
            } => {
                // 下载被取消，验证部分数据
                assert!(
                    data.len() <= test_data.len(),
                    "取消时的数据不应该超过总大小"
                );
                assert_eq!(
                    bytes_downloaded,
                    data.len() as u64,
                    "已下载字节数应该匹配数据长度"
                );
            }
            FetchRangeResult::Complete(data) => {
                // Mock 在取消信号到达前就返回了所有数据
                assert_eq!(data.len(), test_data.len(), "完整数据应该匹配");
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_immediate_cancel() {
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试立即取消（在下载开始前就发送取消信号）
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        let client = MockHttpClient::new();
        let mut stats = SmrSwap::new(WorkerStats::default());

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(test_data.len() as u64).unwrap()).unwrap();
        let range = allocator
            .allocate(NonZeroU64::new(test_data.len() as u64).unwrap())
            .unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-35/{}", test_data.len()).parse().unwrap(),
        );
        client.set_range_response(
            test_url,
            0,
            35,
            reqwest::StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from_static(test_data),
        );

        // 立即发送取消信号
        let (cancel_tx, cancel_rx) = lite::channel();
        let _ = cancel_tx.notify(()); // 立即取消

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut stats)
            .fetch_with_cancel(cancel_rx)
            .await;

        // 应该成功（可能完成也可能取消，取决于执行时机）
        assert!(result.is_ok(), "调用应该成功");

        match result.unwrap() {
            FetchRangeResult::Cancelled {
                data,
                bytes_downloaded,
            } => {
                // 被立即取消，可能没有下载任何数据
                assert_eq!(bytes_downloaded, data.len() as u64);
            }
            FetchRangeResult::Complete(_) => {
                // 在取消信号处理前就完成了
            }
        }
    }

    #[tokio::test]
    async fn test_range_support_checker_head_success_with_range() {
        // 测试 HEAD 成功且支持 Range
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置支持 Range 的 HEAD 响应
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("content-length", "1024".parse().unwrap());
        client.set_head_response(test_url, StatusCode::OK, headers);

        // 执行检测
        let fetcher = FileMetadataFetcher::new(&client, test_url);
        let result = fetcher.fetch().await;

        assert!(result.is_ok(), "检测应该成功");
        let support = result.unwrap();
        assert!(support.range_supported, "应该支持 Range");
        assert_eq!(support.content_length, Some(1024), "文件大小应该匹配");

        // 验证只使用了 HEAD 请求
        let log = client.get_request_log();
        assert_eq!(log.len(), 1, "应该只有一个请求");
        assert_eq!(log[0], format!("HEAD {}", test_url));
    }

    #[tokio::test]
    async fn test_range_support_checker_head_success_without_range() {
        // 测试 HEAD 成功但不支持 Range
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置不支持 Range 的 HEAD 响应
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", "none".parse().unwrap());
        headers.insert("content-length", "2048".parse().unwrap());
        client.set_head_response(test_url, StatusCode::OK, headers);

        // 执行检测
        let fetcher = FileMetadataFetcher::new(&client, test_url);
        let result = fetcher.fetch().await;

        assert!(result.is_ok(), "检测应该成功");
        let support = result.unwrap();
        assert!(!support.range_supported, "应该不支持 Range");
        assert_eq!(support.content_length, Some(2048), "文件大小应该匹配");
    }

    #[tokio::test]
    async fn test_range_support_checker_head_fail_fallback_to_range_get_206() {
        // 测试 HEAD 失败（405）但 Range GET 返回 206（支持 Range）
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();
        let file_size = 12345u64;

        // 不设置 HEAD 响应，导致 HEAD 请求失败

        // 设置 Range GET 响应（返回 206）
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-range",
            format!("bytes 0-0/{}", file_size).parse().unwrap(),
        );
        headers.insert("content-length", "1".parse().unwrap());
        client.set_range_response(
            test_url,
            0,
            0,
            StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from_static(b"X"),
        );

        // 执行检测
        let fetcher = FileMetadataFetcher::new(&client, test_url);
        let result = fetcher.fetch().await;

        assert!(result.is_ok(), "检测应该成功: {:?}", result);
        let support = result.unwrap();
        assert!(
            support.range_supported,
            "应该支持 Range（通过 Range GET 检测到）"
        );
        assert_eq!(
            support.content_length,
            Some(file_size),
            "文件大小应该从 Content-Range 解析"
        );

        // 验证先尝试了 HEAD，失败后使用了 Range GET
        let log = client.get_request_log();
        assert_eq!(log.len(), 2, "应该有两个请求：HEAD + Range GET");
        assert_eq!(log[0], format!("HEAD {}", test_url));
        assert_eq!(log[1], format!("GET {} Range: 0-0", test_url));
    }

    #[tokio::test]
    async fn test_range_support_checker_head_fail_fallback_to_range_get_200() {
        // 测试 HEAD 失败且 Range GET 返回 200（不支持 Range）
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();
        let file_size = 9999u64;

        // 不设置 HEAD 响应

        // 设置 Range GET 响应（返回 200，表示不支持 Range）
        let mut headers = HeaderMap::new();
        headers.insert("content-length", file_size.to_string().parse().unwrap());
        client.set_range_response(
            test_url,
            0,
            0,
            StatusCode::OK,
            headers,
            Bytes::from_static(b"X"),
        );

        // 执行检测
        let fetcher = FileMetadataFetcher::new(&client, test_url);
        let result = fetcher.fetch().await;

        assert!(result.is_ok(), "检测应该成功");
        let support = result.unwrap();
        assert!(!support.range_supported, "应该不支持 Range");
        assert_eq!(
            support.content_length,
            Some(file_size),
            "文件大小应该从 Content-Length 解析"
        );

        // 验证使用了两个请求
        let log = client.get_request_log();
        assert_eq!(log.len(), 2, "应该有两个请求");
    }

    #[tokio::test]
    async fn test_range_support_checker_parse_content_range() {
        // 测试 Content-Range header 解析
        let test_cases = vec![
            ("bytes 0-0/12345", 12345u64),
            ("bytes 0-99/1000000", 1000000u64),
            ("bytes 100-199/500", 500u64),
        ];

        for (header_value, expected_total) in test_cases {
            let mut headers = HeaderMap::new();
            headers.insert("content-range", header_value.parse().unwrap());

            let response = MockHttpResponse {
                status: StatusCode::PARTIAL_CONTENT,
                headers,
                body: Bytes::new(),
                url: "http://example.com/file.bin".to_string(),
            };

            let result =
                FileMetadataFetcher::<MockHttpClient>::parse_content_range_total(&response);
            assert!(result.is_ok(), "解析 '{}' 应该成功", header_value);
            assert_eq!(
                result.unwrap(),
                expected_total,
                "解析 '{}' 的结果应该匹配",
                header_value
            );
        }
    }

    #[tokio::test]
    async fn test_range_support_checker_parse_content_range_invalid() {
        // 测试无效的 Content-Range header
        let invalid_cases = vec![
            "",              // 空字符串
            "bytes 0-0",     // 缺少总大小
            "invalid",       // 完全无效
            "bytes 0-0/abc", // 非数字总大小
        ];

        for header_value in invalid_cases {
            let mut headers = HeaderMap::new();
            if !header_value.is_empty() {
                headers.insert("content-range", header_value.parse().unwrap());
            }

            let response = MockHttpResponse {
                status: StatusCode::PARTIAL_CONTENT,
                headers,
                body: Bytes::new(),
                url: "http://example.com/file.bin".to_string(),
            };

            let result =
                FileMetadataFetcher::<MockHttpClient>::parse_content_range_total(&response);
            assert!(result.is_err(), "解析 '{}' 应该失败", header_value);
        }
    }

    #[tokio::test]
    async fn test_check_range_support_uses_checker() {
        // 测试公共 API 使用新的 RangeSupportChecker
        let test_url = "http://example.com/test.bin";
        let client = MockHttpClient::new();

        // 设置 HEAD 响应
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("content-length", "8888".parse().unwrap());
        client.set_head_response(test_url, StatusCode::OK, headers);

        // 使用公共 API
        let result = fetch_file_metadata(&client, test_url).await;

        assert!(result.is_ok(), "公共 API 应该成功");
        let support = result.unwrap();
        assert!(support.range_supported, "应该支持 Range");
        assert_eq!(support.content_length, Some(8888u64));
    }

    #[tokio::test]
    async fn test_extract_filename_from_content_disposition() {
        // 测试标准格式
        assert_eq!(
            extract_filename_from_content_disposition("attachment; filename=\"example.zip\""),
            Some("example.zip".to_string())
        );

        // 测试无引号格式
        assert_eq!(
            extract_filename_from_content_disposition("attachment; filename=example.zip"),
            Some("example.zip".to_string())
        );

        // 测试带分号的格式
        assert_eq!(
            extract_filename_from_content_disposition(
                "attachment; filename=\"example.zip\"; size=1024"
            ),
            Some("example.zip".to_string())
        );

        // 测试 RFC 5987 编码格式
        assert_eq!(
            extract_filename_from_content_disposition(
                "attachment; filename*=UTF-8''example%20file.zip"
            ),
            Some("example file.zip".to_string())
        );

        // 测试无效格式
        assert_eq!(
            extract_filename_from_content_disposition("attachment"),
            None
        );

        // 测试空字符串
        assert_eq!(extract_filename_from_content_disposition(""), None);
    }

    #[tokio::test]
    async fn test_extract_filename_from_url() {
        // 测试标准 URL
        assert_eq!(
            extract_filename_from_url("http://example.com/path/file.zip"),
            Some("file.zip".to_string())
        );

        // 测试带查询参数的 URL
        assert_eq!(
            extract_filename_from_url("http://example.com/file.zip?token=abc"),
            Some("file.zip".to_string())
        );

        // 测试 URL 编码的文件名
        assert_eq!(
            extract_filename_from_url("http://example.com/my%20file.zip"),
            Some("my file.zip".to_string())
        );

        // 测试无文件名的 URL
        assert_eq!(extract_filename_from_url("http://example.com/"), None);

        // 测试无效 URL
        assert_eq!(extract_filename_from_url("not a url"), None);
    }

    #[tokio::test]
    async fn test_is_valid_filename() {
        // 合法文件名
        assert!(is_valid_filename("example.zip"));
        assert!(is_valid_filename("my_file.txt"));
        assert!(is_valid_filename("file-123.bin"));

        // 非法文件名（包含非法字符）
        assert!(!is_valid_filename("file<test>.zip"));
        assert!(!is_valid_filename("file>test.zip"));
        assert!(!is_valid_filename("file:test.zip"));
        assert!(!is_valid_filename("file\"test.zip"));
        assert!(!is_valid_filename("file/test.zip"));
        assert!(!is_valid_filename("file\\test.zip"));
        assert!(!is_valid_filename("file|test.zip"));
        assert!(!is_valid_filename("file?test.zip"));
        assert!(!is_valid_filename("file*test.zip"));

        // 空字符串或只有空格
        assert!(!is_valid_filename(""));
        assert!(!is_valid_filename("   "));
    }

    #[tokio::test]
    async fn test_generate_timestamp_filename() {
        let filename = generate_timestamp_filename();

        // 验证格式
        assert!(filename.starts_with("file_"));

        // 验证时间戳是数字
        let timestamp_str = filename.strip_prefix("file_").unwrap();
        assert!(timestamp_str.parse::<u64>().is_ok());

        // 验证两次生成的文件名不同（时间戳不同）
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let filename2 = generate_timestamp_filename();
        assert_ne!(filename, filename2, "时间戳应该不同");
    }

    #[tokio::test]
    async fn test_file_metadata_fetcher_with_content_disposition() {
        // 测试完整的元数据获取流程，包括文件名提取
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置 HEAD 响应，包含 Content-Disposition
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("content-length", "1024".parse().unwrap());
        headers.insert(
            "content-disposition",
            "attachment; filename=\"downloaded.zip\"".parse().unwrap(),
        );
        client.set_head_response(test_url, StatusCode::OK, headers);

        // 获取元数据
        let result = fetch_file_metadata(&client, test_url).await;
        assert!(result.is_ok(), "应该成功获取元数据");

        let metadata = result.unwrap();
        assert!(metadata.range_supported, "应该支持 Range");
        assert_eq!(metadata.content_length, Some(1024));
        assert_eq!(
            metadata.suggested_filename,
            Some("downloaded.zip".to_string()),
            "应该从 Content-Disposition 提取文件名"
        );
        assert!(metadata.final_url.is_some());
    }

    #[tokio::test]
    async fn test_file_metadata_fetcher_filename_priority() {
        // 测试文件名提取的优先级顺序
        let original_url = "http://example.com/original_file.bin";
        let client = MockHttpClient::new();

        // 测试 1: 有 Content-Disposition 时优先使用
        {
            let mut headers = HeaderMap::new();
            headers.insert("accept-ranges", "bytes".parse().unwrap());
            headers.insert("content-length", "1024".parse().unwrap());
            headers.insert(
                "content-disposition",
                "attachment; filename=\"from_header.zip\"".parse().unwrap(),
            );
            client.set_head_response(original_url, StatusCode::OK, headers);

            let metadata = fetch_file_metadata(&client, original_url).await.unwrap();
            assert_eq!(
                metadata.suggested_filename,
                Some("from_header.zip".to_string())
            );
        }

        // 测试 2: 没有 Content-Disposition 时从 URL 提取
        {
            let mut headers = HeaderMap::new();
            headers.insert("accept-ranges", "bytes".parse().unwrap());
            headers.insert("content-length", "1024".parse().unwrap());
            client.set_head_response(original_url, StatusCode::OK, headers);

            let metadata = fetch_file_metadata(&client, original_url).await.unwrap();
            assert_eq!(
                metadata.suggested_filename,
                Some("original_file.bin".to_string())
            );
        }
    }

    #[tokio::test]
    async fn test_range_support_checker_head_405_fallback() {
        // 测试 HEAD 返回 405 状态码的场景（模拟用户的真实情况）
        // 这种情况下应该自动回退到 Range GET
        let test_url = "http://example.com/redirect_file.bin";
        let client = MockHttpClient::new();
        let file_size = 212768400u64;

        // 设置 HEAD 响应为 405 Method Not Allowed
        let mut head_headers = HeaderMap::new();
        head_headers.insert("content-length", "19".parse().unwrap());
        client.set_head_response(test_url, StatusCode::METHOD_NOT_ALLOWED, head_headers);

        // 设置 Range GET 响应（返回 206，模拟重定向后的服务器支持 Range）
        let mut range_headers = HeaderMap::new();
        range_headers.insert(
            "content-range",
            format!("bytes 0-0/{}", file_size).parse().unwrap(),
        );
        range_headers.insert("content-length", "1".parse().unwrap());
        range_headers.insert("accept-ranges", "bytes".parse().unwrap());
        client.set_range_response(
            test_url,
            0,
            0,
            StatusCode::PARTIAL_CONTENT,
            range_headers,
            Bytes::from_static(b"X"),
        );

        // 执行检测
        let result = fetch_file_metadata(&client, test_url).await;

        assert!(
            result.is_ok(),
            "检测应该成功（通过 Range GET 回退）: {:?}",
            result
        );
        let support = result.unwrap();
        assert!(support.range_supported, "应该检测到支持 Range");
        assert_eq!(
            support.content_length,
            Some(file_size),
            "应该正确解析文件大小"
        );

        // 验证使用了两个请求：HEAD (405) + Range GET (206)
        let log = client.get_request_log();
        assert_eq!(log.len(), 2, "应该有两个请求");
        assert_eq!(log[0], format!("HEAD {}", test_url), "第一个应该是 HEAD");
        assert_eq!(
            log[1],
            format!("GET {} Range: 0-0", test_url),
            "第二个应该是 Range GET"
        );
    }
}
