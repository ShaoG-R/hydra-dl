//! Range 数据流式写入器
//! 
//! 提供高效的文件分段写入功能，支持：
//! - Range 单向分配，防止重叠
//! - 乱序数据写入
//! - 原子定位写入（利用 OS 原生 API）
//! - 进度追踪
//! - 内存优化（不缓存所有数据）
//! - 真正的并发写入（无锁设计）
//! 
//! # 核心组件
//! 
//! - [`AllocatedRange`] - 已分配的 Range，确保类型安全和不可变性
//! - [`RangeAllocator`] - Range 分配器，从文件开头向结尾单向分配
//! - [`RangeWriter`] - Range 写入器，管理文件的分段写入
//! 
//! # 性能优化
//! 
//! 相比传统的 seek + write 方式，使用了以下优化：
//! - Windows: 使用 `seek_write` 原子操作，一次系统调用完成定位+写入
//! - Unix/Linux: 使用 `write_at` (pwrite)，无需改变文件位置指针
//! - 无锁并发：多个 worker 可以同时写入不同位置，无需互斥锁
//! - 零拷贝：直接在 blocking 线程池中处理，避免额外的数据拷贝
//! 
//! # 安全性保证
//! 
//! [`AllocatedRange`] 只能通过 [`RangeAllocator::allocate`] 方法创建，
//! 保证了以下安全性：
//! - 所有 Range 都是有效的（start <= end）
//! - 所有 Range 都不重叠
//! - Range 创建后不可修改
//! 
//! # 使用示例
//! 
//! ```no_run
//! # use hydra_dl::utils::range_writer::RangeWriter;
//! # use bytes::Bytes;
//! # use std::path::PathBuf;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // 创建写入器和分配器
//! let (writer, mut allocator) = RangeWriter::new(
//!     PathBuf::from("download.bin"),
//!     1024 * 1024  // 1MB
//! ).await?;
//!
//! // 分配 Range（保证不重叠）
//! let range1 = allocator.allocate(100).unwrap();
//! let range2 = allocator.allocate(200).unwrap();
//!
//! // 可以乱序并发写入（无需锁）
//! let w1 = writer.clone();
//! let w2 = writer.clone();
//! 
//! let t1 = tokio::spawn(async move {
//!     w1.write_range(range1, Bytes::from(vec![1; 100])).await
//! });
//! let t2 = tokio::spawn(async move {
//!     w2.write_range(range2, Bytes::from(vec![2; 200])).await
//! });
//!
//! t1.await??;
//! t2.await??;
//!
//! // 检查完成并刷新
//! if writer.is_complete() {
//!     writer.finalize().await?;
//! }
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use log::info;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

use crate::utils::async_file::AsyncFile;

/// Range Writer 错误类型
#[derive(Error, Debug)]
pub enum RangeWriterError {
    /// IO 错误
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
    
    /// 数据大小与 Range 大小不匹配
    #[error("数据大小 ({data_len} bytes) 与 Range 大小 ({range_size} bytes) 不匹配")]
    SizeMismatch {
        data_len: u64,
        range_size: u64,
    },
}

pub type Result<T> = std::result::Result<T, RangeWriterError>;

/// 已分配的 Range
/// 
/// 表示一个已经通过 [`RangeAllocator`] 分配的文件范围
/// 
/// 这个类型只能通过 [`RangeAllocator::allocate`] 方法创建，
/// 保证了所有 Range 都是有效的且不重叠的
/// 
/// # Range 格式说明
/// 
/// 内部使用 **左闭右开** 区间 `[start, end)`：
/// - `start`: 包含的起始位置
/// - `end`: **不包含**的结束位置
/// 
/// 例如：`AllocatedRange { start: 0, end: 10 }` 表示字节 0-9（共 10 字节）
/// 
/// ## 文件操作 vs HTTP Range 请求
/// 
/// - **文件操作**：使用 `start()` 和 `end()`，直接对应 `[start, end)` 格式
/// - **HTTP Range 请求**：需要转换为 `[start, end_inclusive]` 格式，使用 `as_http_range()`
/// 
/// # 安全性保证
/// 
/// - start 总是小于等于 end
/// - 只能通过分配器创建，防止重叠
/// - 提供不可变访问，防止修改
/// 
/// # Examples
/// 
/// ```
/// # use hydra_dl::utils::range_writer::RangeAllocator;
/// let mut allocator = RangeAllocator::new(100);
/// let range = allocator.allocate(10).unwrap();
/// 
/// // 文件操作格式 [start, end)
/// assert_eq!(range.start(), 0);      // 起始位置：0
/// let (start, end) = range.as_file_range();
/// assert_eq!(start, 0);              // 起始位置：0
/// assert_eq!(end, 10);               // 结束位置：10（不包含）
/// assert_eq!(range.len(), 10);       // 长度：10 字节
/// 
/// // HTTP Range 格式 [start, end_inclusive]
/// let (http_start, http_end) = range.as_http_range();
/// assert_eq!(http_start, 0);         // HTTP Range start: 0
/// assert_eq!(http_end, 9);           // HTTP Range end: 9（包含）
/// // 对应 HTTP header: "Range: bytes=0-9"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatedRange {
    start: u64,
    end: u64,
}

impl AllocatedRange {
    /// 从文件操作范围创建新的 AllocatedRange（内部使用）
    /// 
    /// 使用左闭右开区间 `[start, end)` 创建 Range
    /// 
    /// 这个方法是 crate 内部可见的，外部用户无法直接创建
    /// 
    /// # Arguments
    /// 
    /// * `start` - 起始位置（包含）
    /// * `end` - 结束位置（不包含）
    /// 
    /// # Panics
    /// 
    /// 在 debug 模式下，如果 `start > end` 会 panic
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// // 内部使用示例
    /// let range = AllocatedRange::from_file_range(0, 10);
    /// // 表示字节 0-9（共 10 字节）
    /// ```
    #[inline]
    pub(crate) fn from_file_range(start: u64, end: u64) -> Self {
        debug_assert!(start <= end, "start 必须小于等于 end");
        Self { start, end }
    }

    /// 从 HTTP Range 格式创建新的 AllocatedRange（内部使用）
    /// 
    /// 将 HTTP Range 的左闭右闭区间 `[start, end]` 转换为内部的左闭右开格式
    /// 
    /// 这个方法是 crate 内部可见的，外部用户无法直接创建
    /// 
    /// # Arguments
    /// 
    /// * `start` - HTTP Range 起始位置（包含）
    /// * `end` - HTTP Range 结束位置（包含）
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// // 从 HTTP Range "bytes=0-9" 创建
    /// let range = AllocatedRange::from_http_range(0, 9);
    /// // 内部存储为 [0, 10)，表示 10 字节
    /// assert_eq!(range.len(), 10);
    /// ```
    #[allow(unused)]
    #[inline]
    pub(crate) fn from_http_range(start: u64, end: u64) -> Self {
        Self { start, end: end + 1 }
    }
    
    /// 获取文件操作格式的 Range
    /// 
    /// 返回左闭右开区间 `(start, end)`，用于：
    /// - 文件 seek 操作
    /// - 切片索引
    /// - Range 计算
    /// 
    /// # Returns
    /// 
    /// `(start, end)` - 起始位置（包含）和结束位置（不包含）
    /// 
    /// # Example
    /// 
    /// ```
    /// # use hydra_dl::utils::range_writer::RangeAllocator;
    /// let mut allocator = RangeAllocator::new(100);
    /// let range = allocator.allocate(10).unwrap();
    /// 
    /// let (start, end) = range.as_file_range();
    /// assert_eq!(start, 0);   // 起始：0
    /// assert_eq!(end, 10);    // 结束：10（不包含）
    /// // 表示字节 0-9（共 10 字节）
    /// ```
    #[inline]
    pub fn as_file_range(&self) -> (u64, u64) {
        (self.start, self.end)
    }

    /// 获取起始位置
    /// 
    /// 这是一个便捷方法，等价于 `as_file_range().0`
    /// 
    /// # Returns
    /// 
    /// 返回 Range 的起始位置（包含）
    /// 
    /// # Example
    /// 
    /// ```
    /// # use hydra_dl::utils::range_writer::RangeAllocator;
    /// let mut allocator = RangeAllocator::new(100);
    /// let range = allocator.allocate(10).unwrap();
    /// 
    /// assert_eq!(range.start(), 0);
    /// ```
    #[inline]
    pub fn start(&self) -> u64 {
        self.start
    }
    
    /// 转换为 HTTP Range 格式
    /// 
    /// 返回 `(start, end_inclusive)` 元组，用于 HTTP Range 请求
    /// 
    /// HTTP Range header 使用左闭右闭区间 `[start, end]`，
    /// 例如 `bytes=0-9` 表示字节 0-9（共 10 字节，包含第 9 字节）
    /// 
    /// # Returns
    /// 
    /// `(start, end_inclusive)` - 可直接用于 HTTP Range 请求
    /// 
    /// # Panics
    /// 
    /// 如果 Range 为空（`start == end`），调用此方法会导致下溢（end - 1）
    /// 
    /// # Example
    /// 
    /// ```
    /// # use hydra_dl::utils::range_writer::RangeAllocator;
    /// let mut allocator = RangeAllocator::new(100);
    /// let range = allocator.allocate(10).unwrap();
    /// 
    /// let (start, end_inclusive) = range.as_http_range();
    /// // 用于 HTTP 请求: Range: bytes=0-9
    /// assert_eq!(start, 0);
    /// assert_eq!(end_inclusive, 9);
    /// ```
    #[inline]
    pub fn as_http_range(&self) -> (u64, u64) {
        debug_assert!(!self.is_empty(), "不能对空 Range 调用 as_http_range()");
        (self.start, self.end - 1)
    }
    
    /// 获取 Range 的长度（字节数）
    #[inline]
    pub fn len(&self) -> u64 {
        self.end - self.start
    }
    
    /// 检查 Range 是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
    
    /// 转换为标准 Range<u64>（文件操作格式）
    /// 
    /// 返回左闭右开区间 `start..end`
    #[inline]
    pub fn as_range(&self) -> Range<u64> {
        self.start..self.end
    }
}

impl From<AllocatedRange> for Range<u64> {
    #[inline]
    fn from(range: AllocatedRange) -> Self {
        range.as_range()
    }
}

/// Range 分配器
/// 
/// 从文件开头向结尾单向分配 Range，防止重叠
/// 
/// 返回 [`AllocatedRange`] 类型，保证所有分配的 Range 都是有效且不重叠的
/// 
/// # Example
/// 
/// ```
/// # use hydra_dl::utils::range_writer::RangeAllocator;
/// let mut allocator = RangeAllocator::new(1000);
///
/// // 分配 100 字节
/// let range1 = allocator.allocate(100).unwrap();
/// assert_eq!(range1.start(), 0);
/// let (start, end) = range1.as_file_range();
/// assert_eq!(start, 0);
/// assert_eq!(end, 100);
///
/// let range2 = allocator.allocate(150).unwrap();
/// assert_eq!(range2.start(), 100);
/// let (start2, end2) = range2.as_file_range();
/// assert_eq!(start2, 100);
/// assert_eq!(end2, 250);
///
/// assert_eq!(allocator.remaining(), 750);
/// ```
pub struct RangeAllocator {
    next_pos: u64,
    total_size: u64,
}

impl RangeAllocator {
    /// 创建新的 Range 分配器
    /// 
    /// # Arguments
    /// * `total_size` - 文件总大小（字节）
    #[inline]
    pub fn new(total_size: u64) -> Self {
        Self {
            next_pos: 0,
            total_size,
        }
    }
    
    /// 分配指定大小的 Range
    /// 
    /// 从当前未分配位置开始分配，如果空间不足则返回 None
    /// 
    /// # Arguments
    /// * `size` - 要分配的字节数
    /// 
    /// # Returns
    /// 
    /// 成功返回 `Some(AllocatedRange)`，空间不足返回 `None`
    #[inline]
    pub fn allocate(&mut self, size: u64) -> Option<AllocatedRange> {
        if self.next_pos + size > self.total_size {
            return None;
        }
        
        let start = self.next_pos;
        let end = start + size;
        self.next_pos = end;
        
        Some(AllocatedRange::from_file_range(start, end))
    }
    
    /// 获取剩余可分配字节数
    /// 
    /// # Returns
    /// 
    /// 返回还未分配的字节数
    #[inline]
    pub fn remaining(&self) -> u64 {
        self.total_size.saturating_sub(self.next_pos)
    }
    
    /// 获取总大小
    #[inline]
    pub fn total_size(&self) -> u64 {
        self.total_size
    }
    
    /// 获取下一个分配位置
    #[inline]
    pub fn next_pos(&self) -> u64 {
        self.next_pos
    }
}

/// Range 数据流式写入器
/// 
/// 管理 Range 数据的分配和定位写入，使用高性能异步文件实现
/// 
/// # 性能特性
/// 
/// - **真正的并发写入**：使用 `AsyncFile::write_all_at`，无需互斥锁
/// - **原子操作**：Windows 使用 `seek_write`，Unix 使用 `pwrite`
/// - **零拷贝**：数据直接在 blocking 线程池中写入
/// - **无锁进度追踪**：使用 `AtomicU64` 记录已写入字节数
/// 
/// # 并发安全
/// 
/// `RangeWriter` 可以安全地在多个 task 间共享（`Clone`），
/// 多个 worker 可以同时写入不同的 Range，无需额外同步。
/// 
/// # Example
/// 
/// ```no_run
/// # use hydra_dl::utils::range_writer::RangeWriter;
/// # use std::path::PathBuf;
/// # use bytes::Bytes;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (writer, mut allocator) = RangeWriter::new(
///     PathBuf::from("output.dat"),
///     1000  // 总大小 1000 bytes
/// ).await?;
///
/// // 使用 allocator 分配 Range（保证不重叠且有效）
/// let range1 = allocator.allocate(100).unwrap();
/// let range2 = allocator.allocate(100).unwrap();
///
/// // 并发写入（无需锁）
/// let w1 = writer.clone();
/// let w2 = writer.clone();
/// 
/// tokio::join!(
///     w1.write_range(range1, Bytes::from(vec![1; 100])),
///     w2.write_range(range2, Bytes::from(vec![2; 100]))
/// );
///
/// // 当所有字节都写入后，自动判断完成
/// if writer.is_complete() {
///     writer.finalize().await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RangeWriter {
    /// 异步文件句柄
    /// 
    /// `AsyncFile` 内部使用 `Arc<StdFile>`，支持安全的并发定位写入
    file: AsyncFile,
    
    /// 文件总大小
    total_bytes: u64,
    
    /// 已写入的字节数（原子计数器）
    written_bytes: Arc<AtomicU64>,
}

impl RangeWriter {
    /// 创建新的 writer，预分配文件大小
    /// 
    /// 返回 `(RangeWriter, RangeAllocator)` 元组，分离写入和分配职责：
    /// - `RangeWriter` 可以被多个 worker 共享（Clone），用于并发写入
    /// - `RangeAllocator` 在主线程中使用，用于预分配所有 Range
    /// 
    /// # Arguments
    /// * `path` - 文件路径
    /// * `total_size` - 文件总大小（字节）
    /// 
    /// # Returns
    /// 
    /// 返回 `(RangeWriter, RangeAllocator)` 元组
    /// 
    /// # Errors
    /// 
    /// 如果文件创建或预分配失败，返回错误
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// # use hydra_dl::utils::range_writer::RangeWriter;
    /// # use std::path::PathBuf;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let (writer, allocator) = RangeWriter::new(
    ///     PathBuf::from("download.bin"),
    ///     10 * 1024 * 1024  // 10 MB
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(path: PathBuf, total_size: u64) -> Result<(Self, RangeAllocator)> {
        info!("创建 RangeWriter: {:?} ({} bytes)", path, total_size);
        
        // 创建文件并预分配空间
        let file = AsyncFile::create(&path).await?;
        file.set_len(total_size).await?;
        
        let writer = Self {
            file,
            total_bytes: total_size,
            written_bytes: Arc::new(AtomicU64::new(0)),
        };
        
        let allocator = RangeAllocator::new(total_size);
        
        Ok((writer, allocator))
    }
    
    /// 写入单个 Range 数据到指定位置
    /// 
    /// 使用原子定位写入（`write_all_at`），支持真正的并发乱序写入
    /// 
    /// 接受 [`AllocatedRange`] 参数，确保所有写入的 Range 都是有效且不重叠的
    /// 
    /// # 性能特性
    /// 
    /// - **无锁并发**：多个 task 可以同时调用此方法写入不同位置
    /// - **原子操作**：一次系统调用完成定位+写入
    /// - **零拷贝**：数据直接在 blocking 线程池中处理
    /// 
    /// # Arguments
    /// * `range` - 已分配的文件范围
    /// * `data` - 要写入的数据
    /// 
    /// # Errors
    /// 
    /// 如果文件写入失败，返回错误
    /// 如果数据大小与 Range 大小不匹配，返回错误
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// # use hydra_dl::utils::range_writer::RangeWriter;
    /// # use bytes::Bytes;
    /// # use std::path::PathBuf;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (writer, mut allocator) = RangeWriter::new(PathBuf::from("test.bin"), 1000).await?;
    /// let range = allocator.allocate(100).unwrap();
    /// writer.write_range(range, Bytes::from(vec![0u8; 100])).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_range(&self, range: AllocatedRange, data: Bytes) -> Result<()> {
        let range_size = range.len();
        let data_len = data.len() as u64;
        
        if data_len != range_size {
            return Err(RangeWriterError::SizeMismatch { data_len, range_size });
        }

        let (start, end) = range.as_file_range();
        
        // 原子定位写入（无需锁）
        self.file.write_all_at(start, &data).await?;
        
        // 无锁更新已写入字节数
        let written = self.written_bytes.fetch_add(data_len, Ordering::SeqCst) + data_len;
        
        let progress = (written as f64 / self.total_bytes as f64) * 100.0;
        info!(
            "Range {}..{} 已写入 ({} bytes), 总进度: {:.1}% ({}/{} bytes)",
            start, end, data_len, progress, 
            written, self.total_bytes
        );
        
        Ok(())
    }
    
    /// 检查是否所有数据都已完成写入
    /// 
    /// 通过比较已写入字节数与文件总大小来判断
    /// 
    /// 使用 atomic load 无锁读取进度
    /// 
    /// # Returns
    /// 
    /// 如果已写入字节数等于文件总大小，返回 true
    pub fn is_complete(&self) -> bool {
        self.written_bytes.load(Ordering::SeqCst) == self.total_bytes
    }
    
    /// 获取进度信息
    /// 
    /// 使用 atomic load 无锁读取进度
    /// 
    /// # Returns
    /// 
    /// 返回元组 (已写入字节数, 总字节数)
    pub fn progress(&self) -> (u64, u64) {
        (self.written_bytes.load(Ordering::SeqCst), self.total_bytes)
    }
    
    /// 刷新并关闭文件
    /// 
    /// 将所有缓冲数据同步到磁盘
    /// 
    /// # Errors
    /// 
    /// 如果刷新失败，返回错误
    pub async fn finalize(self) -> Result<()> {
        self.file.sync_all().await?;
        
        let written = self.written_bytes.load(Ordering::SeqCst);
        info!(
            "RangeWriter 完成: {}/{} bytes 已写入",
            written, self.total_bytes
        );
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_new_creates_file_with_correct_size() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.dat");
        
        let (writer, _allocator) = RangeWriter::new(file_path.clone(), 1000).await.unwrap();
        
        // 检查文件是否创建
        assert!(file_path.exists());
        
        // 检查文件大小
        let metadata = fs::metadata(&file_path).await.unwrap();
        assert_eq!(metadata.len(), 1000);
        
        // 清理
        drop(writer);
    }

    #[tokio::test]
    async fn test_write_range_sequential() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_seq.dat");
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), 300).await.unwrap();
        
        // 顺序分配并写入 3 个 Range
        let range1 = allocator.allocate(100).unwrap();
        writer.write_range(range1, Bytes::from(vec![1u8; 100])).await.unwrap();
        
        let range2 = allocator.allocate(100).unwrap();
        writer.write_range(range2, Bytes::from(vec![2u8; 100])).await.unwrap();
        
        let range3 = allocator.allocate(100).unwrap();
        writer.write_range(range3, Bytes::from(vec![3u8; 100])).await.unwrap();
        
        assert!(writer.is_complete());
        let (written, _) = writer.progress();
        assert_eq!(written, 300);
        
        writer.finalize().await.unwrap();
        
        // 验证文件内容
        let mut file = fs::File::open(&file_path).await.unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).await.unwrap();
        
        assert_eq!(content.len(), 300);
        assert_eq!(&content[0..100], &vec![1u8; 100][..]);
        assert_eq!(&content[100..200], &vec![2u8; 100][..]);
        assert_eq!(&content[200..300], &vec![3u8; 100][..]);
    }

    #[tokio::test]
    async fn test_write_range_out_of_order() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_ooo.dat");
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), 300).await.unwrap();
        
        // 分配 3 个 Range
        let range1 = allocator.allocate(100).unwrap();
        let range2 = allocator.allocate(100).unwrap();
        let range3 = allocator.allocate(100).unwrap();
        
        // 乱序写入
        writer.write_range(range3, Bytes::from(vec![3u8; 100])).await.unwrap();
        writer.write_range(range1, Bytes::from(vec![1u8; 100])).await.unwrap();
        writer.write_range(range2, Bytes::from(vec![2u8; 100])).await.unwrap();
        
        assert!(writer.is_complete());
        
        writer.finalize().await.unwrap();
        
        // 验证文件内容
        let mut file = fs::File::open(&file_path).await.unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).await.unwrap();
        
        assert_eq!(content.len(), 300);
        assert_eq!(&content[0..100], &vec![1u8; 100][..]);
        assert_eq!(&content[100..200], &vec![2u8; 100][..]);
        assert_eq!(&content[200..300], &vec![3u8; 100][..]);
    }

    #[tokio::test]
    async fn test_is_complete() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_complete.dat");
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), 200).await.unwrap();
        
        assert!(!writer.is_complete());
        
        let range1 = allocator.allocate(100).unwrap();
        writer.write_range(range1, Bytes::from(vec![1u8; 100])).await.unwrap();
        assert!(!writer.is_complete());
        
        let range2 = allocator.allocate(100).unwrap();
        writer.write_range(range2, Bytes::from(vec![2u8; 100])).await.unwrap();
        assert!(writer.is_complete());
        
        writer.finalize().await.unwrap();
    }

    #[tokio::test]
    async fn test_progress() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_progress.dat");
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), 500).await.unwrap();
        
        let (written, total_bytes) = writer.progress();
        assert_eq!(written, 0);
        assert_eq!(total_bytes, 500);
        
        let range1 = allocator.allocate(100).unwrap();
        let range2 = allocator.allocate(100).unwrap();
        
        writer.write_range(range1, Bytes::from(vec![1u8; 100])).await.unwrap();
        writer.write_range(range2, Bytes::from(vec![3u8; 100])).await.unwrap();
        
        let (written, total_bytes) = writer.progress();
        assert_eq!(written, 200);
        assert_eq!(total_bytes, 500);
        
        writer.finalize().await.unwrap();
    }

    #[tokio::test]
    async fn test_large_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_large.dat");
        
        // 模拟 10MB 文件，分成 100 个 Range
        let total_size = 10 * 1024 * 1024u64;
        let range_count = 100usize;
        let range_size = total_size / range_count as u64;
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), total_size).await.unwrap();
        
        // 分配并写入所有 Range
        for i in 0..range_count {
            let size = if i == range_count - 1 {
                allocator.remaining()
            } else {
                range_size
            };
            let range = allocator.allocate(size).unwrap();
            writer.write_range(range, Bytes::from(vec![(i % 256) as u8; size as usize])).await.unwrap();
        }
        
        assert!(writer.is_complete());
        let (written, _) = writer.progress();
        assert_eq!(written, total_size);
        
        writer.finalize().await.unwrap();
        
        // 验证文件大小
        let metadata = fs::metadata(&file_path).await.unwrap();
        assert_eq!(metadata.len(), total_size);
    }

    #[tokio::test]
    async fn test_duplicate_range_write() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_dup.dat");
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), 200).await.unwrap();
        
        // 分配一个 Range，写入两次（模拟重试）
        let range = allocator.allocate(100).unwrap();
        writer.write_range(range, Bytes::from(vec![1u8; 100])).await.unwrap();
        writer.write_range(range, Bytes::from(vec![9u8; 100])).await.unwrap();
        
        // 由于没有去重机制，会累加计数
        let (written, _) = writer.progress();
        assert_eq!(written, 200);
        
        writer.finalize().await.unwrap();
        
        // 验证最后写入的数据
        let mut file = fs::File::open(&file_path).await.unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).await.unwrap();
        
        // 第二次写入应该覆盖第一次
        assert_eq!(&content[0..100], &vec![9u8; 100][..]);
    }
    
    #[tokio::test]
    async fn test_allocate_range() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_allocate.dat");
        
        let (writer, mut allocator) = RangeWriter::new(file_path.clone(), 300).await.unwrap();
        
        // 分配多个 Range
        let range1 = allocator.allocate(100).unwrap();
        assert_eq!(range1.start(), 0);
        let (start1, end1) = range1.as_file_range();
        assert_eq!(start1, 0);
        assert_eq!(end1, 100);
        assert_eq!(range1.len(), 100);
        
        let range2 = allocator.allocate(150).unwrap();
        assert_eq!(range2.start(), 100);
        let (start2, end2) = range2.as_file_range();
        assert_eq!(start2, 100);
        assert_eq!(end2, 250);
        assert_eq!(range2.len(), 150);
        
        let range3 = allocator.allocate(50).unwrap();
        assert_eq!(range3.start(), 250);
        let (start3, end3) = range3.as_file_range();
        assert_eq!(start3, 250);
        assert_eq!(end3, 300);
        assert_eq!(range3.len(), 50);
        
        // 空间不足，应该返回 None
        let range4 = allocator.allocate(1);
        assert!(range4.is_none());
        
        // 写入所有分配的 Range
        writer.write_range(range1, Bytes::from(vec![1u8; 100])).await.unwrap();
        writer.write_range(range2, Bytes::from(vec![2u8; 150])).await.unwrap();
        writer.write_range(range3, Bytes::from(vec![3u8; 50])).await.unwrap();
        
        assert!(writer.is_complete());
        writer.finalize().await.unwrap();
    }
}

