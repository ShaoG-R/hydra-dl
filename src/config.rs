//! 下载配置模块
//!
//! 提供下载任务的配置选项，包括分块策略和并发控制

/// 默认最小分块大小：2 MB
pub const DEFAULT_MIN_CHUNK_SIZE: u64 = 2 * 1024 * 1024;

/// 默认 Range 分块数量
pub const DEFAULT_RANGE_COUNT: usize = 8;

/// 默认 Worker 并发数
pub const DEFAULT_WORKER_COUNT: usize = 4;

/// 下载配置
///
/// 控制下载任务的分块策略和并发行为
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// 期望的 Range 分块数量
    ///
    /// 实际分块数可能会根据 `min_chunk_size` 动态调整
    pub(crate) range_count: usize,
    
    /// Worker 并发数量
    pub(crate) worker_count: usize,
    
    /// 最小分块大小（bytes）
    ///
    /// 如果按 `range_count` 计算的分块大小小于此值，
    /// 将自动减少分块数以满足最小分块要求
    pub(crate) min_chunk_size: u64,
}

impl DownloadConfig {
    /// 创建配置构建器
    ///
    /// # Example
    ///
    /// ```
    /// # use rs_dn::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .range_count(10)
    ///     .worker_count(4)
    ///     .build();
    /// ```
    pub fn builder() -> DownloadConfigBuilder {
        DownloadConfigBuilder::new()
    }
    
    /// 创建默认配置
    ///
    /// - 分块数: 8
    /// - Worker 数: 4
    /// - 最小分块: 2 MB
    pub fn default() -> Self {
        Self {
            range_count: DEFAULT_RANGE_COUNT,
            worker_count: DEFAULT_WORKER_COUNT,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
        }
    }
    
    /// 获取期望的分块数量
    pub fn range_count(&self) -> usize {
        self.range_count
    }
    
    /// 获取 Worker 数量
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }
    
    /// 获取最小分块大小
    pub fn min_chunk_size(&self) -> u64 {
        self.min_chunk_size
    }
}

/// 下载配置构建器
///
/// 使用 Builder 模式创建 `DownloadConfig`
///
/// # Example
///
/// ```
/// # use rs_dn::DownloadConfig;
/// let config = DownloadConfig::builder()
///     .range_count(16)
///     .worker_count(8)
///     .min_chunk_size(5 * 1024 * 1024)  // 5 MB
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct DownloadConfigBuilder {
    range_count: usize,
    worker_count: usize,
    min_chunk_size: u64,
}

impl DownloadConfigBuilder {
    /// 创建新的配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            range_count: DEFAULT_RANGE_COUNT,
            worker_count: DEFAULT_WORKER_COUNT,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
        }
    }
    
    /// 设置期望的 Range 分块数量
    ///
    /// 实际分块数可能会根据文件大小和最小分块大小动态调整
    ///
    /// # Arguments
    ///
    /// * `count` - 期望的分块数量（必须 > 0）
    pub fn range_count(mut self, count: usize) -> Self {
        self.range_count = count.max(1);
        self
    }
    
    /// 设置 Worker 并发数量
    ///
    /// # Arguments
    ///
    /// * `count` - Worker 数量（必须 > 0）
    pub fn worker_count(mut self, count: usize) -> Self {
        self.worker_count = count.max(1);
        self
    }
    
    /// 设置最小分块大小
    ///
    /// 如果按期望的分块数计算出的分块大小小于此值，
    /// 将自动减少分块数以满足最小分块要求
    ///
    /// # Arguments
    ///
    /// * `size` - 最小分块大小（bytes，必须 > 0）
    pub fn min_chunk_size(mut self, size: u64) -> Self {
        self.min_chunk_size = size.max(1);
        self
    }
    
    /// 构建配置对象
    pub fn build(self) -> DownloadConfig {
        DownloadConfig {
            range_count: self.range_count,
            worker_count: self.worker_count,
            min_chunk_size: self.min_chunk_size,
        }
    }
}

impl Default for DownloadConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DownloadConfig::default();
        assert_eq!(config.range_count(), DEFAULT_RANGE_COUNT);
        assert_eq!(config.worker_count(), DEFAULT_WORKER_COUNT);
        assert_eq!(config.min_chunk_size(), DEFAULT_MIN_CHUNK_SIZE);
    }

    #[test]
    fn test_builder_default() {
        let config = DownloadConfig::builder().build();
        assert_eq!(config.range_count(), DEFAULT_RANGE_COUNT);
        assert_eq!(config.worker_count(), DEFAULT_WORKER_COUNT);
        assert_eq!(config.min_chunk_size(), DEFAULT_MIN_CHUNK_SIZE);
    }

    #[test]
    fn test_builder_custom() {
        let config = DownloadConfig::builder()
            .range_count(16)
            .worker_count(8)
            .min_chunk_size(5 * 1024 * 1024)
            .build();
        
        assert_eq!(config.range_count(), 16);
        assert_eq!(config.worker_count(), 8);
        assert_eq!(config.min_chunk_size(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_builder_min_values() {
        let config = DownloadConfig::builder()
            .range_count(0)  // 应该被限制为 1
            .worker_count(0)  // 应该被限制为 1
            .min_chunk_size(0)  // 应该被限制为 1
            .build();
        
        assert_eq!(config.range_count(), 1);
        assert_eq!(config.worker_count(), 1);
        assert_eq!(config.min_chunk_size(), 1);
    }
}

