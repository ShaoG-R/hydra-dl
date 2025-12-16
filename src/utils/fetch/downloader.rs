use super::range::FetchRange;
use super::{FetchError, Result};
use crate::constants::KB;
use crate::task::FileTask;
use crate::utils::io_traits::{HttpClient, HttpResponse, IoError};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use log::{debug, info};
use std::future::Future;

// ============================================================================
// Chunk 记录器和 RangeFetcher
// ============================================================================

/// RangeFetcher 结果处理器
///
/// 定义下载结果的构造逻辑。
/// 这个 Trait 允许调用者完全控制返回值类型，实现最大程度的解耦。
///
/// # Type Parameters
/// * `CancelOutput`: 取消信号 Future 的输出类型
pub trait FetchHandler<CancelOutput> {
    /// 下载操作的返回结果类型
    type Result;

    /// 当下载完整完成时调用
    fn on_complete(self, data: Bytes) -> Self::Result;

    /// 当下载被取消（Future ready）时调用
    fn on_cancelled(self, output: CancelOutput, data: Bytes, bytes_downloaded: u64)
    -> Self::Result;
}

/// Chunk 记录器 trait
///
/// 用于解耦 RangeFetcher 和具体的统计实现
pub trait ChunkRecorder {
    /// 记录下载的 chunk
    ///
    /// # Arguments
    /// * `bytes` - 本次下载的字节数
    fn record_chunk(&mut self, bytes: u64);
}

/// Range 下载器
///
/// 封装 Range 下载的参数和逻辑
pub struct RangeFetcher<'a, C: HttpClient, R: ChunkRecorder> {
    client: &'a C,
    url: &'a str,
    range: FetchRange,
    recorder: &'a mut R,
}

impl<'a, C: HttpClient, R: ChunkRecorder> RangeFetcher<'a, C, R> {
    /// 创建新的 Range 下载器
    ///
    /// # Arguments
    /// * `client` - HTTP 客户端
    /// * `url` - 下载 URL
    /// * `range` - 已分配的 Range，定义要下载的字节范围
    /// * `recorder` - Chunk 记录器，用于记录下载进度
    pub(crate) fn new(client: &'a C, url: &'a str, range: FetchRange, recorder: &'a mut R) -> Self {
        Self {
            client,
            url,
            range,
            recorder,
        }
    }

    /// 执行下载任务
    ///
    /// # Arguments
    /// * `cancel` - 取消信号 Future。当此 Future Ready 时，下载将被中断。
    /// * `handler` - 结果处理器，定义如何处理完成或取消时的结果。
    ///
    /// # Returns
    ///
    /// 返回 `Result<H::Result>`，其中 `H::Result` 由 `handler` 构造
    pub async fn fetch<F, H>(mut self, cancel: F, handler: H) -> Result<H::Result>
    where
        F: Future + Unpin + Send,
        H: FetchHandler<F::Output> + Send,
    {
        let (http_start, http_end) = self.range.as_http_range();
        debug!(
            "开始下载 Range: {} (bytes {}-{})",
            self.url, http_start, http_end
        );

        let response = self
            .client
            .get_with_range(self.url, http_start, http_end)
            .await?;

        if !response.status().is_success() && response.status().as_u16() != 206 {
            return Err(FetchError::HttpStatus(response.status().as_u16()));
        }

        self.download_stream(response.bytes_stream(), cancel, handler)
            .await
    }

    /// 下载数据流
    async fn download_stream<S, F, H>(
        &mut self,
        mut stream: S,
        mut cancel: F,
        handler: H,
    ) -> Result<H::Result>
    where
        S: futures::Stream<Item = std::result::Result<Bytes, IoError>> + Unpin,
        F: Future + Unpin + Send,
        H: FetchHandler<F::Output> + Send,
    {
        let expected_size = self.range.len();

        // 性能优化 1: Vec 容量预分配
        // 典型 HTTP chunk 大小为 8KB-64KB，这里使用 16KB 作为保守估计
        // 预分配可以避免多次 realloc 和数据移动
        let estimated_chunks = expected_size.div_ceil(16 * KB).max(4) as usize;
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
                            self.recorder.record_chunk(chunk_size);

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
                            return Ok(handler.on_complete(data));
                        }
                    }
                }
                output = &mut cancel => {
                    // 收到取消信号
                    let data = self.merge_chunks(chunks);
                    return Ok(handler.on_cancelled(output, data, downloaded_bytes));
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

// ============================================================================
// 文件下载 helper
// ============================================================================

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::io_traits::mock::MockHttpClient;
    use reqwest::header::HeaderMap;
    use std::path::PathBuf;
    use tokio::sync::oneshot;

    #[derive(Debug)]
    enum TestResult {
        Complete(Bytes),
        Cancelled { data: Bytes, bytes_downloaded: u64 },
    }

    struct TestFetchHandler;

    impl FetchHandler<std::result::Result<(), oneshot::error::RecvError>> for TestFetchHandler {
        type Result = TestResult;
        fn on_complete(self, data: Bytes) -> Self::Result {
            TestResult::Complete(data)
        }
        fn on_cancelled(
            self,
            _output: std::result::Result<(), oneshot::error::RecvError>,
            data: Bytes,
            bytes_downloaded: u64,
        ) -> Self::Result {
            TestResult::Cancelled {
                data,
                bytes_downloaded,
            }
        }
    }

    /// 测试用 Mock ChunkRecorder
    #[derive(Default)]
    struct MockChunkRecorder {
        total_bytes: u64,
        chunk_count: usize,
    }

    impl MockChunkRecorder {
        fn total_bytes(&self) -> u64 {
            self.total_bytes
        }
    }

    impl ChunkRecorder for MockChunkRecorder {
        fn record_chunk(&mut self, bytes: u64) {
            self.total_bytes += bytes;
            self.chunk_count += 1;
        }
    }

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
        use ranged_mmap::MmapFile;
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试完整下载（不触发取消）
        let test_url = "http://example.com/file.bin";
        // 使用 4K 对齐的数据大小
        let full_data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();

        let client = MockHttpClient::new();
        let mut recorder = MockChunkRecorder::default();

        // 创建临时文件和 allocator
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(full_data.len() as u64).unwrap())
                .unwrap();
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
        let (_cancel_tx, cancel_rx) = oneshot::channel();

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let handler = TestFetchHandler;

        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut recorder)
            .fetch(cancel_rx, handler)
            .await;

        assert!(result.is_ok(), "下载应该成功: {:?}", result);

        // 验证返回完整数据
        match result.unwrap() {
            TestResult::Complete(data) => {
                assert_eq!(data.as_ref(), expected_data, "下载的数据应该匹配");
            }
            TestResult::Cancelled { .. } => {
                panic!("不应该被取消");
            }
        }

        // 验证统计信息
        assert_eq!(
            recorder.total_bytes(),
            expected_data.len() as u64,
            "统计的字节数应该匹配"
        );
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_http_error() {
        use ranged_mmap::MmapFile;
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试 HTTP 错误
        let test_url = "http://example.com/file.bin";
        let client = MockHttpClient::new();
        let mut recorder = MockChunkRecorder::default();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(100).unwrap()).unwrap();
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
        let (_cancel_tx, cancel_rx) = oneshot::channel();

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let handler = TestFetchHandler;

        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut recorder)
            .fetch(cancel_rx, handler)
            .await;

        assert!(result.is_err(), "应该返回错误");
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_large_stream() {
        use ranged_mmap::MmapFile;
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试流式传输大数据（不触发取消）
        let test_url = "http://example.com/large_file.bin";

        // 创建一个较大的数据（大于默认 chunk size 8192）
        let large_data: Vec<u8> = (0..20000).map(|i| (i % 256) as u8).collect();
        let start = 0;
        let end = large_data.len() as u64;

        let client = MockHttpClient::new();
        let mut recorder = MockChunkRecorder::default();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(end).unwrap()).unwrap();
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
        let (_cancel_tx, cancel_rx) = oneshot::channel();

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let handler = TestFetchHandler;

        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut recorder)
            .fetch(cancel_rx, handler)
            .await;

        assert!(result.is_ok(), "大数据下载应该成功");

        // 验证返回完整数据
        match result.unwrap() {
            TestResult::Complete(data) => {
                assert_eq!(data.len(), large_data.len(), "下载的数据大小应该匹配");
                assert_eq!(data.as_ref(), &large_data[..], "下载的数据内容应该匹配");
            }
            TestResult::Cancelled { .. } => {
                panic!("不应该被取消");
            }
        }

        // 验证统计信息（应该记录了多个 chunk）
        assert_eq!(
            recorder.total_bytes(),
            large_data.len() as u64,
            "统计的字节数应该匹配"
        );
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_cancelled_midway() {
        use ranged_mmap::MmapFile;
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试中途取消下载
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 36 bytes

        let client = MockHttpClient::new();
        let mut recorder = MockChunkRecorder::default();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(test_data.len() as u64).unwrap())
                .unwrap();
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
        let (cancel_tx, cancel_rx) = oneshot::channel();

        tokio::spawn(async move {
            // 给予一点时间让下载开始
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let _ = cancel_tx.send(());
        });

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let handler = TestFetchHandler;

        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut recorder)
            .fetch(cancel_rx, handler)
            .await;

        assert!(result.is_ok(), "调用应该成功");

        // 验证结果
        match result.unwrap() {
            TestResult::Cancelled {
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
            TestResult::Complete(data) => {
                // Mock 在取消信号到达前就返回了所有数据
                assert_eq!(data.len(), test_data.len(), "完整数据应该匹配");
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_with_cancel_immediate_cancel() {
        use ranged_mmap::MmapFile;
        use std::num::NonZeroU64;
        use tempfile::tempdir;

        // 测试立即取消（在下载开始前就发送取消信号）
        let test_url = "http://example.com/file.bin";
        let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        let client = MockHttpClient::new();
        let mut recorder = MockChunkRecorder::default();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let (_file, mut allocator) =
            MmapFile::create_default(path, NonZeroU64::new(test_data.len() as u64).unwrap())
                .unwrap();
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
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let _ = cancel_tx.send(()); // 立即取消

        // 执行下载
        let fetch_range = FetchRange::from_allocated_range(&range).unwrap();
        let handler = TestFetchHandler;

        let result = RangeFetcher::new(&client, test_url, fetch_range, &mut recorder)
            .fetch(cancel_rx, handler)
            .await;

        // 应该成功（可能完成也可能取消，取决于执行时机）
        assert!(result.is_ok(), "调用应该成功");

        match result.unwrap() {
            TestResult::Cancelled {
                data,
                bytes_downloaded,
            } => {
                // 被立即取消，可能没有下载任何数据
                assert_eq!(bytes_downloaded, data.len() as u64);
            }
            TestResult::Complete(_) => {
                // 在取消信号处理前就完成了
            }
        }
    }
}
