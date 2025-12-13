use super::{FetchError, Result};
use bytes::Bytes;
use ranged_mmap::AllocatedRange;

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

    /// 获取范围长度
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
