use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use log::{debug, info};
use std::sync::Arc;
use thiserror::Error;

use crate::tools::io_traits::{AsyncFile, FileSystem, HttpClient, HttpResponse, IoError};
use crate::tools::range_writer::AllocatedRange;
use crate::task::{FileTask, RangeSupport};
use crate::tools::stats::DownloadStats;

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
    
    /// 无法解析文件总大小
    #[error("无法解析文件总大小为 u64: {0}")]
    InvalidContentRangeSize(String),
}

pub type Result<T> = std::result::Result<T, FetchError>;

/// 获取并保存完整文件
pub async fn fetch_file<C, FS>(
    client: &C,
    task: FileTask,
    fs: &FS,
) -> Result<()>
where
    C: HttpClient,
    FS: FileSystem,
{
    info!("开始下载: {} -> {:?}", task.url, task.save_path);

    let response = client.get(&task.url).await?;

    if !response.status().is_success() {
        return Err(FetchError::HttpStatus(response.status().as_u16()));
    }

    // 创建文件
    let mut file = fs.create(&task.save_path).await?;

    // 流式下载
    let mut stream = response.bytes_stream();
    let mut downloaded: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
    }

    file.flush().await?;

    info!(
        "下载完成: {} ({} bytes) -> {:?}",
        task.url, downloaded, task.save_path
    );

    Ok(())
}

/// 获取文件的指定范围
/// 
/// # Arguments
/// * `client` - HTTP 客户端
/// * `url` - 下载 URL
/// * `range` - 已分配的 Range，定义要下载的字节范围
/// * `stats` - 共享的下载统计，每个 chunk 到达时实时更新
/// 
/// # Returns
/// 
/// 返回下载的数据
pub async fn fetch_range<C>(
    client: &C,
    url: &str,
    range: AllocatedRange,
    stats: Arc<DownloadStats>,
) -> Result<Bytes>
where
    C: HttpClient,
{
    debug!(
        "开始下载 Range: {} (bytes {}-{})",
        url, range.start(), range.end() - 1  // HTTP Range 是包含结束位置的
    );

    let response = client
        .get_with_range(url, range.start(), range.end() - 1)
        .await?;

    if !response.status().is_success() && response.status().as_u16() != 206 {
        return Err(FetchError::HttpStatus(response.status().as_u16()));
    }

    // 流式下载到内存，每个 chunk 到达时实时更新统计
    let mut stream = response.bytes_stream();
    let mut buffer = BytesMut::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let chunk_size = chunk.len() as u64;
        
        // 实时记录 chunk
        stats.record_chunk(chunk_size);
        
        buffer.extend_from_slice(&chunk);
    }

    debug!(
        "Range {}..{} 下载完成: {} bytes",
        range.start(), range.end(), buffer.len()
    );

    Ok(buffer.freeze())
}

/// Range 支持检测器
/// 
/// 封装检测服务器是否支持 Range 请求的逻辑，提供多种检测策略：
/// 1. 首先尝试 HEAD 请求（高效，无需下载数据）
/// 2. HEAD 失败时回退到小的 Range GET 请求（bytes=0-0，处理不支持 HEAD 的服务器）
pub struct RangeSupportChecker<'a, C: HttpClient> {
    client: &'a C,
    url: &'a str,
}

impl<'a, C: HttpClient> RangeSupportChecker<'a, C> {
    /// 创建新的 Range 支持检测器
    pub fn new(client: &'a C, url: &'a str) -> Self {
        Self { client, url }
    }
    
    /// 检测 Range 支持
    /// 
    /// 策略：
    /// 1. 首先尝试 HEAD 请求（高效）
    /// 2. HEAD 失败时（如 405 Method Not Allowed）回退到小的 Range GET 请求（bytes=0-0）
    /// 
    /// 这种策略能够处理以下场景：
    /// - 服务器不支持 HEAD 方法
    /// - 原始 URL 通过 302 重定向到实际下载链接
    /// - reqwest 自动跟随重定向，Range GET 可到达最终 URL
    pub async fn check(&self) -> Result<RangeSupport> {
        // 尝试 HEAD
        match self.check_with_head().await {
            Ok(support) => {
                debug!("HEAD 请求成功检测 Range 支持: supported={}, content_length={:?}", 
                    support.supported, support.content_length);
                Ok(support)
            }
            Err(e) => {
                debug!("HEAD 请求失败: {}, 尝试 Range GET 回退", e);
                self.check_with_range_get().await
            }
        }
    }
    
    /// 使用 HEAD 请求检测
    async fn check_with_head(&self) -> Result<RangeSupport> {
        let response = self.client.head(self.url).await?;
        
        // 检查状态码，如果不是 2xx 成功状态，返回错误触发回退
        if !response.status().is_success() {
            return Err(FetchError::HttpStatus(response.status().as_u16()));
        }
        
        Self::parse_from_head_response(&response)
    }
    
    /// 使用小的 Range GET 请求检测（bytes=0-0）
    /// 
    /// 发送一个只请求第一个字节的 Range 请求：
    /// - 如果服务器支持 Range，返回 206 Partial Content + Content-Range header
    /// - 如果不支持，可能返回 200 OK + 完整内容（或部分内容）
    async fn check_with_range_get(&self) -> Result<RangeSupport> {
        debug!("发送 Range GET 请求 (bytes=0-0) 到: {}", self.url);
        let response = self.client.get_with_range(self.url, 0, 0).await?;
        Self::parse_from_range_response(&response)
    }
    
    /// 从 HEAD 响应解析 Range 支持
    /// 
    /// 检查 Accept-Ranges header：
    /// - "bytes" 表示支持
    /// - "none" 表示不支持
    /// - 缺失时默认不支持
    fn parse_from_head_response<R: HttpResponse>(response: &R) -> Result<RangeSupport> {
        let supported = response
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

        Ok(RangeSupport {
            supported,
            content_length,
        })
    }
    
    /// 从 Range GET 响应解析支持情况
    /// 
    /// 根据状态码判断：
    /// - 206 Partial Content：支持 Range，从 Content-Range 获取总大小
    /// - 200 OK：不支持 Range，从 Content-Length 获取大小
    /// - 其他：检测失败
    fn parse_from_range_response<R: HttpResponse>(response: &R) -> Result<RangeSupport> {
        let status = response.status();
        
        if status.as_u16() == 206 {
            // 支持 Range，从 Content-Range 获取总大小
            let content_length = Self::parse_content_range_total(response)?;
            debug!("Range GET 返回 206，支持 Range，文件大小: {} bytes", content_length);
            Ok(RangeSupport {
                supported: true,
                content_length: Some(content_length),
            })
        } else if status.is_success() {
            // 不支持 Range，从 Content-Length 获取大小
            let content_length = response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            
            debug!("Range GET 返回 {}, 不支持 Range，文件大小: {:?}", status, content_length);
            Ok(RangeSupport {
                supported: false,
                content_length,
            })
        } else {
            Err(FetchError::HttpStatus(status.as_u16()))
        }
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

/// 检查服务器是否支持 Range 请求
/// 
/// 内部使用 `RangeSupportChecker`，会自动处理 HEAD 失败的情况
#[inline]
pub async fn check_range_support<C>(client: &C, url: &str) -> Result<RangeSupport>
where
    C: HttpClient,
{
    RangeSupportChecker::new(client, url).check().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::io_traits::mock::{MockHttpClient, MockFileSystem, MockHttpResponse};
    use crate::tools::range_writer::RangeAllocator;
    use reqwest::header::HeaderMap;
    use reqwest::StatusCode;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_fetch_file_success() {
        // 准备测试数据
        let test_data = b"Hello, World! This is test data.";
        let test_url = "http://example.com/file.txt";
        let save_path = PathBuf::from("/tmp/test_file.txt");

        // 创建 mock 客户端和文件系统
        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

        // 设置 GET 请求的响应
        let mut headers = HeaderMap::new();
        headers.insert("content-length", test_data.len().to_string().parse().unwrap());
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
        let result = fetch_file(&client, task, &fs).await;
        assert!(result.is_ok(), "下载应该成功");

        // 验证请求日志
        let log = client.get_request_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0], format!("GET {}", test_url));

        // 注意：MockFileSystem 的实现有问题，暂时无法验证文件内容
        // 需要修复 MockFileSystem 以支持获取文件内容
    }

    #[tokio::test]
    async fn test_fetch_file_http_error() {
        let test_url = "http://example.com/not_found.txt";
        let save_path = PathBuf::from("/tmp/test_file.txt");

        let client = MockHttpClient::new();
        let fs = MockFileSystem::new();

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
        let result = fetch_file(&client, task, &fs).await;
        assert!(result.is_err(), "应该返回错误");
    }

    #[tokio::test]
    async fn test_fetch_range_success() {
        let test_url = "http://example.com/file.bin";
        let full_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        
        let client = MockHttpClient::new();
        let stats = Arc::new(DownloadStats::default());

        // 创建一个 RangeAllocator 来生成 AllocatedRange
        // RangeAllocator 总是从 0 开始分配
        let mut allocator = RangeAllocator::new(full_data.len() as u64);
        let range = allocator.allocate(10).unwrap(); // 分配 10 bytes，范围是 0-9
        
        // range.start() = 0, range.end() = 10
        let expected_data = &full_data[0..10]; // "0123456789"

        // 设置 Range 响应（注意：HTTP Range 的 end 是包含的，所以是 0-9）
        let mut headers = HeaderMap::new();
        headers.insert("content-range", format!("bytes 0-9/{}", full_data.len()).parse().unwrap());
        client.set_range_response(
            test_url,
            0,
            9, // HTTP Range 的 end 是包含的
            reqwest::StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::copy_from_slice(expected_data),
        );

        // 执行下载
        let result = fetch_range(&client, test_url, range, stats.clone()).await;
        assert!(result.is_ok(), "Range 下载应该成功: {:?}", result);

        let data = result.unwrap();
        assert_eq!(data.as_ref(), expected_data, "下载的数据应该匹配");

        // 验证统计信息（只验证字节数，不验证 range 计数，因为 fetch_range 不负责调用 record_range_complete）
        let (total_bytes, _, _) = stats.get_summary();
        assert_eq!(total_bytes, expected_data.len() as u64, "统计的字节数应该匹配");
    }

    #[tokio::test]
    async fn test_fetch_range_http_error() {
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();
        let stats = Arc::new(DownloadStats::default());

        let mut allocator = RangeAllocator::new(100);
        let range = allocator.allocate(10).unwrap();

        // 设置错误响应
        client.set_range_response(
            test_url,
            0,
            9,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            HeaderMap::new(),
            Bytes::new(),
        );

        // 执行下载
        let result = fetch_range(&client, test_url, range, stats).await;
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
        client.set_head_response(
            test_url,
            reqwest::StatusCode::OK,
            headers,
        );

        // 检查 Range 支持
        let result = check_range_support(&client, test_url).await;
        assert!(result.is_ok(), "检查应该成功");

        let range_support = result.unwrap();
        assert!(range_support.supported, "应该支持 Range");
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
        client.set_head_response(
            test_url,
            reqwest::StatusCode::OK,
            headers,
        );

        // 检查 Range 支持
        let result = check_range_support(&client, test_url).await;
        assert!(result.is_ok(), "检查应该成功");

        let range_support = result.unwrap();
        assert!(!range_support.supported, "应该不支持 Range");
        assert_eq!(range_support.content_length, Some(2048), "文件大小应该匹配");
    }

    #[tokio::test]
    async fn test_check_range_support_no_header() {
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();

        // 设置没有 accept-ranges 头的响应
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "512".parse().unwrap());
        client.set_head_response(
            test_url,
            reqwest::StatusCode::OK,
            headers,
        );

        // 检查 Range 支持
        let result = check_range_support(&client, test_url).await;
        assert!(result.is_ok(), "检查应该成功");

        let range_support = result.unwrap();
        assert!(!range_support.supported, "没有 accept-ranges 头时应该认为不支持");
        assert_eq!(range_support.content_length, Some(512), "文件大小应该匹配");
    }

    #[tokio::test]
    async fn test_fetch_range_chunked_stream() {
        // 测试流式传输，确保大数据被正确分块处理
        let test_url = "http://example.com/large_file.bin";
        
        // 创建一个较大的数据（大于默认 chunk size 8192）
        let large_data: Vec<u8> = (0..20000).map(|i| (i % 256) as u8).collect();
        let start = 0;
        let end = large_data.len() as u64;

        let client = MockHttpClient::new();
        let stats = Arc::new(DownloadStats::default());

        let mut allocator = RangeAllocator::new(end);
        let range = allocator.allocate(end).unwrap();

        // 设置 Range 响应
        let mut headers = HeaderMap::new();
        headers.insert("content-range", format!("bytes {}-{}/{}", start, end - 1, end).parse().unwrap());
        client.set_range_response(
            test_url,
            start,
            end - 1,
            reqwest::StatusCode::PARTIAL_CONTENT,
            headers,
            Bytes::from(large_data.clone()),
        );

        // 执行下载
        let result = fetch_range(&client, test_url, range, stats.clone()).await;
        assert!(result.is_ok(), "大数据 Range 下载应该成功");

        let data = result.unwrap();
        assert_eq!(data.len(), large_data.len(), "下载的数据大小应该匹配");
        assert_eq!(data.as_ref(), &large_data[..], "下载的数据内容应该匹配");

        // 验证统计信息（应该记录了多个 chunk）
        let (total_bytes, _, _) = stats.get_summary();
        assert_eq!(total_bytes, large_data.len() as u64, "统计的字节数应该匹配");
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
        client.set_head_response(
            test_url,
            StatusCode::OK,
            headers,
        );

        // 执行检测
        let checker = RangeSupportChecker::new(&client, test_url);
        let result = checker.check().await;
        
        assert!(result.is_ok(), "检测应该成功");
        let support = result.unwrap();
        assert!(support.supported, "应该支持 Range");
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
        client.set_head_response(
            test_url,
            StatusCode::OK,
            headers,
        );

        // 执行检测
        let checker = RangeSupportChecker::new(&client, test_url);
        let result = checker.check().await;
        
        assert!(result.is_ok(), "检测应该成功");
        let support = result.unwrap();
        assert!(!support.supported, "应该不支持 Range");
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
        headers.insert("content-range", format!("bytes 0-0/{}", file_size).parse().unwrap());
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
        let checker = RangeSupportChecker::new(&client, test_url);
        let result = checker.check().await;
        
        assert!(result.is_ok(), "检测应该成功: {:?}", result);
        let support = result.unwrap();
        assert!(support.supported, "应该支持 Range（通过 Range GET 检测到）");
        assert_eq!(support.content_length, Some(file_size), "文件大小应该从 Content-Range 解析");

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
        let checker = RangeSupportChecker::new(&client, test_url);
        let result = checker.check().await;
        
        assert!(result.is_ok(), "检测应该成功");
        let support = result.unwrap();
        assert!(!support.supported, "应该不支持 Range");
        assert_eq!(support.content_length, Some(file_size), "文件大小应该从 Content-Length 解析");

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
            };

            let result = RangeSupportChecker::<MockHttpClient>::parse_content_range_total(&response);
            assert!(result.is_ok(), "解析 '{}' 应该成功", header_value);
            assert_eq!(result.unwrap(), expected_total, "解析 '{}' 的结果应该匹配", header_value);
        }
    }

    #[tokio::test]
    async fn test_range_support_checker_parse_content_range_invalid() {
        // 测试无效的 Content-Range header
        let invalid_cases = vec![
            "",                     // 空字符串
            "bytes 0-0",            // 缺少总大小
            "invalid",              // 完全无效
            "bytes 0-0/abc",        // 非数字总大小
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
            };

            let result = RangeSupportChecker::<MockHttpClient>::parse_content_range_total(&response);
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
        client.set_head_response(
            test_url,
            StatusCode::OK,
            headers,
        );

        // 使用公共 API
        let result = check_range_support(&client, test_url).await;
        
        assert!(result.is_ok(), "公共 API 应该成功");
        let support = result.unwrap();
        assert!(support.supported, "应该支持 Range");
        assert_eq!(support.content_length, Some(8888));
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
        client.set_head_response(
            test_url,
            StatusCode::METHOD_NOT_ALLOWED,
            head_headers,
        );

        // 设置 Range GET 响应（返回 206，模拟重定向后的服务器支持 Range）
        let mut range_headers = HeaderMap::new();
        range_headers.insert("content-range", format!("bytes 0-0/{}", file_size).parse().unwrap());
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
        let result = check_range_support(&client, test_url).await;
        
        assert!(result.is_ok(), "检测应该成功（通过 Range GET 回退）: {:?}", result);
        let support = result.unwrap();
        assert!(support.supported, "应该检测到支持 Range");
        assert_eq!(support.content_length, Some(file_size), "应该正确解析文件大小");

        // 验证使用了两个请求：HEAD (405) + Range GET (206)
        let log = client.get_request_log();
        assert_eq!(log.len(), 2, "应该有两个请求");
        assert_eq!(log[0], format!("HEAD {}", test_url), "第一个应该是 HEAD");
        assert_eq!(log[1], format!("GET {} Range: 0-0", test_url), "第二个应该是 Range GET");
    }
}

