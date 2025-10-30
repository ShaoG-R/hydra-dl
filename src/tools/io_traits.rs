//! IO抽象层：HTTP客户端和文件系统trait
//!
//! 为HTTP客户端和文件系统操作提供trait抽象，便于测试

use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use reqwest::{header::HeaderMap, StatusCode};
use std::io::SeekFrom;
use async_trait::async_trait;
use std::path::Path;
use std::pin::Pin;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

/// HTTP客户端trait
///
/// 抽象HTTP客户端的核心操作，支持GET和HEAD请求
#[async_trait]
pub trait HttpClient: Send + Sync {
    /// HTTP响应类型
    type Response: HttpResponse;

    /// 发送GET请求
    async fn get(&self, url: &str) -> Result<Self::Response>;

    /// 发送HEAD请求
    async fn head(&self, url: &str) -> Result<Self::Response>;

    /// 发送带Range头的GET请求
    async fn get_with_range(&self, url: &str, start: u64, end: u64) -> Result<Self::Response>;
}

/// HTTP响应trait
///
/// 抽象HTTP响应的核心操作
pub trait HttpResponse: Send {
    /// 字节流类型
    type BytesStream: Stream<Item = Result<Bytes, anyhow::Error>> + Send + Unpin;

    /// 获取HTTP状态码
    fn status(&self) -> StatusCode;

    /// 获取响应头
    fn headers(&self) -> &HeaderMap;

    /// 获取字节流
    fn bytes_stream(self) -> Self::BytesStream;
}

/// 文件系统trait
///
/// 抽象文件系统操作，支持文件创建
#[async_trait]
pub trait FileSystem: Send + Sync {
    /// 异步文件类型
    type File: AsyncFile;

    /// 创建文件
    async fn create(&self, path: &Path) -> Result<Self::File>;
}

/// 异步文件trait
///
/// 抽象异步文件操作
#[async_trait]
pub trait AsyncFile: Send + 'static {
    /// 写入所有数据
    async fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> Result<()>;

    /// 刷新缓冲区
    async fn flush(&mut self) -> Result<()>;

    /// 设置文件大小（预分配）
    async fn set_len(&self, size: u64) -> Result<()>;

    /// 文件定位
    async fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
}

// ============================================================================
// 为真实类型实现trait
// ============================================================================

/// reqwest::Response的字节流包装器
pub struct ReqwestBytesStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
}

impl Stream for ReqwestBytesStream {
    type Item = Result<Bytes, anyhow::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(|e| anyhow::anyhow!(e))))
    }
}

/// 为reqwest::Client实现HttpClient
#[async_trait]
impl HttpClient for reqwest::Client {
    type Response = reqwest::Response;

    async fn get(&self, url: &str) -> Result<Self::Response> {
        let url = url.to_string();
        self.get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP GET请求失败: {}", e))
    }

    async fn head(&self, url: &str) -> Result<Self::Response> {
        let url = url.to_string();
        self.head(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP HEAD请求失败: {}", e))
    }

    async fn get_with_range(&self, url: &str, start: u64, end: u64) -> Result<Self::Response> {
        let url = url.to_string();
        self.get(&url)
            .header("Range", format!("bytes={}-{}", start, end))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP Range请求失败: {}", e))
    }
}

/// 为reqwest::Response实现HttpResponse
impl HttpResponse for reqwest::Response {
    type BytesStream = ReqwestBytesStream;

    fn status(&self) -> StatusCode {
        self.status()
    }

    fn headers(&self) -> &HeaderMap {
        self.headers()
    }

    fn bytes_stream(self) -> Self::BytesStream {
        let stream = self.bytes_stream();
        ReqwestBytesStream {
            inner: Box::pin(stream),
        }
    }
}

/// Tokio文件系统实现
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioFileSystem;

#[async_trait]
impl FileSystem for TokioFileSystem {
    type File = tokio::fs::File;

    async fn create(&self, path: &Path) -> Result<Self::File> {
        let path = path.to_path_buf();
        tokio::fs::File::create(&path)
            .await
            .map_err(|e| anyhow::anyhow!("创建文件失败: {}", e))
    }
}

/// 为tokio::fs::File实现AsyncFile
#[async_trait]
impl AsyncFile for tokio::fs::File {
    async fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> Result<()> {
        AsyncWriteExt::write_all(self, buf)
            .await
            .map_err(|e| anyhow::anyhow!("写入文件失败: {}", e))
    }

    async fn flush(&mut self) -> Result<()> {
        AsyncWriteExt::flush(self)
            .await
            .map_err(|e| anyhow::anyhow!("刷新文件缓冲区失败: {}", e))
    }

    async fn set_len(&self, size: u64) -> Result<()> {
        self.set_len(size)
            .await
            .map_err(|e| anyhow::anyhow!("设置文件大小失败: {}", e))
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        AsyncSeekExt::seek(self, pos)
            .await
            .map_err(|e| anyhow::anyhow!("文件定位失败: {}", e))
    }
}

// ============================================================================
// 测试用的 Mock 实现
// ============================================================================

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::path::PathBuf;
    use std::sync::Arc;
    use futures::stream;
    
    /// Mock HTTP 响应
    pub struct MockHttpResponse {
        pub status: StatusCode,
        pub headers: HeaderMap,
        pub body: Bytes,
    }
    
    impl HttpResponse for MockHttpResponse {
        type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + Unpin>>;
        
        fn status(&self) -> StatusCode {
            self.status
        }
        
        fn headers(&self) -> &HeaderMap {
            &self.headers
        }
        
        fn bytes_stream(self) -> Self::BytesStream {
            // 将 body 按块分割，模拟真实的流式传输
            let chunk_size = 8192;
            let mut chunks = Vec::new();
            let bytes = self.body;
            
            for i in (0..bytes.len()).step_by(chunk_size) {
                let end = (i + chunk_size).min(bytes.len());
                chunks.push(Ok(bytes.slice(i..end)));
            }
            
            Box::pin(stream::iter(chunks))
        }
    }
    
    /// Mock HTTP 客户端
    /// 
    /// 支持预设响应和请求记录
    #[derive(Clone)]
    pub struct MockHttpClient { 
        /// GET 请求的响应：URL -> Response
        get_responses: Arc<Mutex<HashMap<String, MockHttpResponse>>>,
        /// HEAD 请求的响应：URL -> Response
        head_responses: Arc<Mutex<HashMap<String, MockHttpResponse>>>,
        /// Range 请求的响应：(URL, start, end) -> Response
        range_responses: Arc<Mutex<HashMap<(String, u64, u64), MockHttpResponse>>>,
        /// 记录所有请求
        request_log: Arc<Mutex<Vec<String>>>,
    }
    
    impl MockHttpClient {
        pub fn new() -> Self {
            Self {
                get_responses: Arc::new(Mutex::new(HashMap::new())),
                head_responses: Arc::new(Mutex::new(HashMap::new())),
                range_responses: Arc::new(Mutex::new(HashMap::new())),
                request_log: Arc::new(Mutex::new(Vec::new())),
            }
        }
        
        /// 设置 GET 请求的响应
        pub fn set_response(&self, url: impl Into<String>, status: StatusCode, headers: HeaderMap, body: Bytes) {
            let response = MockHttpResponse {
                status,
                headers,
                body,
            };
            self.get_responses.lock().unwrap().insert(url.into(), response);
        }
        
        /// 设置 HEAD 请求的响应
        pub fn set_head_response(&self, url: impl Into<String>, status: StatusCode, headers: HeaderMap) {
            let response = MockHttpResponse {
                status,
                headers,
                body: Bytes::new(),
            };
            self.head_responses.lock().unwrap().insert(url.into(), response);
        }
        
        /// 设置 Range 请求的响应
        pub fn set_range_response(&self, url: impl Into<String>, start: u64, end: u64, status: StatusCode, headers: HeaderMap, body: Bytes) {
            let response = MockHttpResponse {
                status,
                headers,
                body,
            };
            self.range_responses.lock().unwrap().insert((url.into(), start, end), response);
        }
        
        /// 获取请求日志
        pub fn get_request_log(&self) -> Vec<String> {
            self.request_log.lock().unwrap().clone()
        }
        
        /// 清空请求日志
        pub fn clear_request_log(&self) {
            self.request_log.lock().unwrap().clear();
        }
    }
    
    impl Default for MockHttpClient {
        fn default() -> Self {
            Self::new()
        }
    }
    
    #[async_trait]
    impl HttpClient for MockHttpClient {
        type Response = MockHttpResponse;
        
        async fn get(&self, url: &str) -> Result<Self::Response> {
            self.request_log.lock().unwrap().push(format!("GET {}", url));
            
            self.get_responses.lock().unwrap()
                .remove(url)
                .ok_or_else(|| anyhow::anyhow!("未找到预设 GET 响应: {}", url))
        }
        
        async fn head(&self, url: &str) -> Result<Self::Response> {
            self.request_log.lock().unwrap().push(format!("HEAD {}", url));
            
            self.head_responses.lock().unwrap()
                .remove(url)
                .ok_or_else(|| anyhow::anyhow!("未找到预设 HEAD 响应: {}", url))
        }
        
        async fn get_with_range(&self, url: &str, start: u64, end: u64) -> Result<Self::Response> {
            self.request_log.lock().unwrap().push(format!("GET {} Range: {}-{}", url, start, end));
            
            self.range_responses.lock().unwrap()
                .remove(&(url.to_string(), start, end))
                .ok_or_else(|| anyhow::anyhow!("未找到预设 Range 响应: {} ({}-{})", url, start, end))
        }
    }
    
    /// Mock 文件
    pub struct MockFile {
        /// 文件内容（内存缓冲区）
        buffer: Mutex<Vec<u8>>,
        /// 当前位置
        position: Mutex<u64>,
        /// 文件大小
        size: Mutex<u64>,
    }
    
    impl MockFile {
        pub fn new() -> Self {
            Self {
                buffer: Mutex::new(Vec::new()),
                position: Mutex::new(0),
                size: Mutex::new(0),
            }
        }
        
        /// 获取文件内容
        pub fn get_contents(&self) -> Vec<u8> {
            self.buffer.lock().unwrap().clone()
        }
        
        /// 获取文件大小
        pub fn len(&self) -> u64 {
            *self.size.lock().unwrap()
        }
    }
    
    impl Default for MockFile {
        fn default() -> Self {
            Self::new()
        }
    }
    
    #[async_trait]
    impl AsyncFile for MockFile {
        async fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> Result<()> {
            let mut buffer = self.buffer.lock().unwrap();
            let mut position = self.position.lock().unwrap();
            
            let pos = *position as usize;
            let end_pos = pos + buf.len();
            
            // 扩展缓冲区（如果需要）
            if end_pos > buffer.len() {
                buffer.resize(end_pos, 0);
            }
            
            // 写入数据
            buffer[pos..end_pos].copy_from_slice(buf);
            *position = end_pos as u64;
            
            Ok(())
        }
        
        async fn flush(&mut self) -> Result<()> {
            // Mock 实现无需实际刷新
            Ok(())
        }
        
        async fn set_len(&self, size: u64) -> Result<()> {
            let mut buffer = self.buffer.lock().unwrap();
            let mut file_size = self.size.lock().unwrap();
            
            buffer.resize(size as usize, 0);
            *file_size = size;
            
            Ok(())
        }
        
        async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
            let mut position = self.position.lock().unwrap();
            let size = *self.size.lock().unwrap();
            
            let new_pos = match pos {
                SeekFrom::Start(offset) => offset as i64,
                SeekFrom::End(offset) => size as i64 + offset,
                SeekFrom::Current(offset) => *position as i64 + offset,
            };
            
            if new_pos < 0 {
                anyhow::bail!("Seek 位置无效: {}", new_pos);
            }
            
            *position = new_pos as u64;
            Ok(*position)
        }
    }
    
    /// Mock 文件系统
    #[derive(Clone)]
    pub struct MockFileSystem {
        /// 已创建的文件：路径 -> 文件
        files: Arc<Mutex<HashMap<PathBuf, Arc<MockFile>>>>,
    }
    
    impl MockFileSystem {
        pub fn new() -> Self {
            Self {
                files: Arc::new(Mutex::new(HashMap::new())),
            }
        }
        
        /// 获取已创建的文件
        pub fn get_file(&self, path: &Path) -> Option<Arc<MockFile>> {
            self.files.lock().unwrap().get(path).cloned()
        }
        
        /// 获取所有文件路径
        pub fn get_file_paths(&self) -> Vec<PathBuf> {
            self.files.lock().unwrap().keys().cloned().collect()
        }
    }
    
    impl Default for MockFileSystem {
        fn default() -> Self {
            Self::new()
        }
    }
    
    #[async_trait]
    impl FileSystem for MockFileSystem {
        type File = MockFile;
        
        async fn create(&self, path: &Path) -> Result<Self::File> {
            let file = MockFile::new();
            let arc_file = Arc::new(file);
            
            // 克隆一份用于返回（不能直接返回 Arc）
            let return_file = MockFile::new();
            
            self.files.lock().unwrap().insert(path.to_path_buf(), arc_file);
            
            Ok(return_file)
        }
    }
}

