use log::info;
use ranged_mmap::{AllocatedRange, MmapFile, allocator::concurrent::Allocator as RangeAllocator};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 错误类型
#[derive(thiserror::Error, Debug)]
pub enum MmapWriterError {
    /// 写入者所有权错误
    #[error("写入者所有权错误")]
    WriterOwnership,

    /// Mmap 文件错误
    #[error(transparent)]
    MmapError(#[from] ranged_mmap::Error),
}

/// Result 类型别名
pub type Result<T> = std::result::Result<T, MmapWriterError>;

/// 基于 MmapFile 的范围写入器
///
/// 提供类似 RangeWriter 的接口，但使用内存映射文件实现，
/// 支持真正的零拷贝并发写入
///
/// # 特性
///
/// - **零拷贝写入**：数据直接写入映射内存，无需系统调用
/// - **无锁并发**：多个线程/task 可以同时写入不同范围，无需加锁
/// - **编译期安全**：通过 `AllocatedRange` 类型系统保证范围不重叠
/// - **运行时无关**：同步 API，可用于任何运行时环境
///
/// # Example
///
/// ```no_run
/// # use hydra_dl::utils::writer::MmapWriter;
/// # use std::path::PathBuf;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::num::NonZeroU64;
///
/// // 创建 writer 和 allocator
/// let (writer, mut allocator) = MmapWriter::new(
///     PathBuf::from("output.bin"),
///     NonZeroU64::new(10 * 1024 * 1024).unwrap()  // 10 MB
/// )?;
///
/// // 在主线程中预分配范围
/// let range1 = allocator.allocate(NonZeroU64::new(1024).unwrap()).unwrap();
/// let range2 = allocator.allocate(NonZeroU64::new(2048).unwrap()).unwrap();
///
/// // 并发写入（编译期安全！）
/// std::thread::scope(|s| {
///     let w1 = writer.clone();
///     let w2 = writer.clone();
///     s.spawn(move || w1.write_range(range1, &[1u8; 1024]));
///     s.spawn(move || w2.write_range(range2, &[2u8; 2048]));
/// });
///
/// // 完成写入
/// unsafe { writer.finalize()?; }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MmapWriter {
    /// 内存映射文件
    ///
    /// `MmapFile` 内部使用引用计数，支持安全的克隆和并发访问
    file: MmapFile,

    /// 已写入的字节数（原子计数器）
    written_bytes: Arc<AtomicU64>,
}

impl MmapWriter {
    /// 创建新的 writer，预分配文件大小
    ///
    /// 返回 `(MmapWriter, RangeAllocator)` 元组，分离写入和分配职责：
    /// - `MmapWriter` 可以被多个 worker 共享（Clone），用于并发写入
    /// - `RangeAllocator` 在主线程中使用，用于预分配所有 Range
    ///
    /// # Arguments
    /// * `path` - 文件路径
    /// * `total_size` - 文件总大小（字节）
    ///
    /// # Returns
    ///
    /// 返回 `(MmapWriter, RangeAllocator)` 元组
    ///
    /// # Errors
    ///
    /// 如果文件创建或预分配失败，返回错误
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use hydra_dl::utils::writer::MmapWriter;
    /// # use std::path::PathBuf;
    /// # use std::num::NonZeroU64;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let (writer, allocator) = MmapWriter::new(
    ///     PathBuf::from("download.bin"),
    ///     NonZeroU64::new(10 * 1024 * 1024).unwrap()  // 10 MB
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(path: impl AsRef<Path>, total_size: NonZeroU64) -> Result<(Self, RangeAllocator)> {
        info!(
            "创建 MmapWriter: {:?} ({} bytes)",
            path.as_ref(),
            total_size.get()
        );

        // 创建内存映射文件
        let (file, allocator) = MmapFile::create(path, total_size)?;

        let writer = Self {
            file,
            written_bytes: Arc::new(AtomicU64::new(0)),
        };

        Ok((writer, allocator))
    }

    /// 打开已存在的文件
    ///
    /// 返回 `(MmapWriter, RangeAllocator)` 元组
    ///
    /// # Arguments
    /// * `path` - 文件路径
    ///
    /// # Returns
    ///
    /// 返回 `(MmapWriter, RangeAllocator)` 元组
    ///
    /// # Errors
    ///
    /// 如果文件不存在或打开失败，返回错误
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, RangeAllocator)> {
        info!("打开 MmapWriter: {:?}", path.as_ref());

        let (file, allocator) = MmapFile::open(path)?;

        let writer = Self {
            file,
            written_bytes: Arc::new(AtomicU64::new(0)),
        };

        Ok((writer, allocator))
    }

    /// 写入单个 Range 数据到指定位置
    ///
    /// 直接写入内存映射区域，支持真正的零拷贝并发写入
    ///
    /// 接受 [`AllocatedRange`] 参数，确保所有写入的 Range 都是有效且不重叠的
    ///
    /// # 性能特性
    ///
    /// - **零拷贝**：数据直接写入映射内存，无需系统调用
    /// - **无锁并发**：多个线程可以同时调用此方法写入不同位置
    /// - **类型安全**：编译期保证范围不重叠，无运行时检查开销
    ///
    /// # Arguments
    /// * `range` - 已分配的文件范围
    /// * `data` - 要写入的数据
    ///
    /// # Returns
    ///
    /// 返回 [`WriteReceipt`] 凭据，可用于后续的 flush 操作
    ///
    /// # Errors
    ///
    /// 如果数据大小与 Range 大小不匹配，返回错误
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use hydra_dl::utils::writer::MmapWriter;
    /// # use std::path::PathBuf;
    /// # use std::num::NonZeroU64;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (writer, mut allocator) = MmapWriter::new(PathBuf::from("test.bin"), NonZeroU64::new(1000).unwrap())?;
    /// let range = allocator.allocate(NonZeroU64::new(100).unwrap()).unwrap();
    /// let receipt = writer.write_range(range, &[0u8; 100])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_range(&self, range: AllocatedRange, data: &[u8]) -> Result<()> {
        let range_size = range.len();
        let data_len = data.len() as u64;

        let start = range.start();
        let end = start + range_size;

        // 零拷贝写入到内存映射区域
        let receipt = self.file.write_range(range, data);

        // 无锁更新已写入字节数
        let written = self.written_bytes.fetch_add(data_len, Ordering::SeqCst) + data_len;

        let progress = (written as f64 / self.file.size().get() as f64) * 100.0;
        info!(
            "Range {}..{} 已写入 ({} bytes), 总进度: {:.1}% ({}/{} bytes)",
            start,
            end,
            data_len,
            progress,
            written,
            self.file.size().get()
        );

        self.file.flush_range(receipt)?;

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
        self.written_bytes.load(Ordering::SeqCst) == self.file.size().get()
    }

    /// 获取进度信息
    ///
    /// 使用 atomic load 无锁读取进度
    ///
    /// # Returns
    ///
    /// 返回元组 (已写入字节数, 总字节数)
    pub fn progress(&self) -> (u64, u64) {
        (
            self.written_bytes.load(Ordering::SeqCst),
            self.file.size().get(),
        )
    }

    /// 获取written_bytes的Arc引用（用于共享）
    pub fn written_bytes_ref(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.written_bytes)
    }

    /// 获取文件总大小
    pub fn total_size(&self) -> u64 {
        self.file.size().get()
    }

    /// 获取文件大小
    ///
    /// # Returns
    ///
    /// 返回文件总大小（字节）
    pub fn size(&self) -> NonZeroU64 {
        self.file.size()
    }

    /// 同步刷新并完成写入
    ///
    /// 将所有缓冲数据同步刷新到磁盘，阻塞直到完成
    ///
    /// # Safety
    ///
    /// 这个方法是安全的，因为它要求所有写入操作都已完成。
    /// 调用者必须确保没有其他线程正在进行写入操作，否则会导致数据不一致。
    ///
    /// # Errors
    ///
    /// 如果刷新失败，返回错误
    pub fn finalize(self) -> Result<()> {
        // 确保写入者所有权正确释放
        let written_bytes = Arc::try_unwrap(self.written_bytes)
            .map_err(|_| MmapWriterError::WriterOwnership)?
            .load(Ordering::SeqCst);

        unsafe {
            self.file.sync_all()?;
        }

        info!(
            "MmapWriter 完成: {}/{} bytes 已写入",
            written_bytes,
            self.file.size().get()
        );

        Ok(())
    }
}

impl std::fmt::Debug for MmapWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapWriter")
            .field("file", &self.file)
            .field("written_bytes", &self.written_bytes.load(Ordering::SeqCst))
            .finish()
    }
}

// Safety: MmapFile 实现了 Send 和 Sync
// AllocatedRange 保证不同线程写入不重叠区域
unsafe impl Send for MmapWriter {}
unsafe impl Sync for MmapWriter {}
