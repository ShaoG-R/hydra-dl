use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use log::{debug, info};
use std::sync::Arc;

use crate::tools::io_traits::{AsyncFile, FileSystem, HttpClient, HttpResponse};
use crate::tools::range_writer::AllocatedRange;
use crate::task::{FileTask, RangeSupport};
use crate::tools::stats::DownloadStats;

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

    let response = client
        .get(&task.url)
        .await
        .context("发送 HTTP 请求失败")?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP 请求失败，状态码: {}", response.status());
    }

    // 创建文件
    let mut file = fs.create(&task.save_path)
        .await
        .context("创建文件失败")?;

    // 流式下载
    let mut stream = response.bytes_stream();
    let mut downloaded: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("读取数据块失败")?;
        file.write_all(&chunk).await.context("写入文件失败")?;
        downloaded += chunk.len() as u64;
    }

    file.flush().await.context("刷新文件缓冲区失败")?;

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
        .await
        .context("发送 HTTP Range 请求失败")?;

    if !response.status().is_success() && response.status().as_u16() != 206 {
        anyhow::bail!("HTTP 请求失败，状态码: {}", response.status());
    }

    // 流式下载到内存，每个 chunk 到达时实时更新统计
    let mut stream = response.bytes_stream();
    let mut buffer = BytesMut::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("读取数据块失败")?;
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

/// 检查服务器是否支持 Range 请求
pub async fn check_range_support<C>(client: &C, url: &str) -> Result<RangeSupport>
where
    C: HttpClient,
{
    let response = client.head(url).await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::io_traits::mock::{MockHttpClient, MockFileSystem};
    use crate::tools::range_writer::RangeAllocator;
    use reqwest::header::HeaderMap;
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
}

