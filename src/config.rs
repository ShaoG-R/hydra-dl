//! 下载配置模块
//!
//! 提供下载任务的配置选项，包括分块策略和并发控制

use std::time::Duration;

/// 默认最小分块大小：2 MB
pub const DEFAULT_MIN_CHUNK_SIZE: u64 = 2 * 1024 * 1024;

/// 默认初始分块大小：5 MB
pub const DEFAULT_INITIAL_CHUNK_SIZE: u64 = 5 * 1024 * 1024;

/// 默认最大分块大小：50 MB
pub const DEFAULT_MAX_CHUNK_SIZE: u64 = 50 * 1024 * 1024;

/// 默认 Worker 并发数
pub const DEFAULT_WORKER_COUNT: usize = 4;

/// 默认实时速度窗口：1 秒
pub const DEFAULT_INSTANT_SPEED_WINDOW_SECS: u64 = 1;

/// 默认预期分块下载时长：5 秒
pub const DEFAULT_EXPECTED_CHUNK_DURATION_SECS: u64 = 5;

/// 默认分块大小平滑系数：0.7
pub const DEFAULT_SMOOTHING_FACTOR: f64 = 0.7;

/// 下载配置
///
/// 控制下载任务的动态分块策略和并发行为
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// Worker 并发数量
    pub(crate) worker_count: usize,
    
    /// 最小分块大小（bytes）
    ///
    /// 动态分块的下限
    pub(crate) min_chunk_size: u64,
    
    /// 初始分块大小（bytes）
    ///
    /// 下载开始时使用的分块大小
    pub(crate) initial_chunk_size: u64,
    
    /// 最大分块大小（bytes）
    ///
    /// 动态分块的上限
    pub(crate) max_chunk_size: u64,
    
    /// 实时速度窗口
    ///
    /// 用于计算瞬时速度的时间窗口，同时也是进度更新的时间间隔
    pub(crate) instant_speed_window: Duration,
    
    /// 预期单个分块下载时长
    ///
    /// 用于计算理想分块大小：理想大小 = worker速度 * 预期时长
    pub(crate) expected_chunk_duration: Duration,
    
    /// 分块大小平滑系数 (0.0 ~ 1.0)
    ///
    /// 用于平滑调整：新大小 = 旧大小 + (理想大小 - 旧大小) * 系数
    /// 越接近 1.0 响应越快，越接近 0.0 越平滑
    pub(crate) smoothing_factor: f64,
}

impl DownloadConfig {
    /// 创建配置构建器
    ///
    /// # Example
    ///
    /// ```
    /// # use rs_dn::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .worker_count(4)
    ///     .initial_chunk_size(5 * 1024 * 1024)
    ///     .build();
    /// ```
    pub fn builder() -> DownloadConfigBuilder {
        DownloadConfigBuilder::new()
    }
    
    /// 创建默认配置
    ///
    /// - Worker 数: 4
    /// - 最小分块: 2 MB
    /// - 初始分块: 5 MB
    /// - 最大分块: 50 MB
    /// - 实时速度窗口: 1 秒
    /// - 预期分块时长: 5 秒
    /// - 平滑系数: 0.7
    pub fn default() -> Self {
        Self {
            worker_count: DEFAULT_WORKER_COUNT,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            initial_chunk_size: DEFAULT_INITIAL_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            instant_speed_window: Duration::from_secs(DEFAULT_INSTANT_SPEED_WINDOW_SECS),
            expected_chunk_duration: Duration::from_secs(DEFAULT_EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: DEFAULT_SMOOTHING_FACTOR,
        }
    }
    
    /// 获取 Worker 数量
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }
    
    /// 获取最小分块大小
    pub fn min_chunk_size(&self) -> u64 {
        self.min_chunk_size
    }
    
    /// 获取初始分块大小
    pub fn initial_chunk_size(&self) -> u64 {
        self.initial_chunk_size
    }
    
    /// 获取最大分块大小
    pub fn max_chunk_size(&self) -> u64 {
        self.max_chunk_size
    }
    
    /// 获取实时速度窗口
    pub fn instant_speed_window(&self) -> Duration {
        self.instant_speed_window
    }
    
    /// 获取预期分块下载时长
    pub fn expected_chunk_duration(&self) -> Duration {
        self.expected_chunk_duration
    }
    
    /// 获取平滑系数
    pub fn smoothing_factor(&self) -> f64 {
        self.smoothing_factor
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
///     .worker_count(8)
///     .initial_chunk_size(10 * 1024 * 1024)  // 10 MB
///     .max_chunk_size(100 * 1024 * 1024)     // 100 MB
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct DownloadConfigBuilder {
    worker_count: usize,
    min_chunk_size: u64,
    initial_chunk_size: u64,
    max_chunk_size: u64,
    instant_speed_window: Duration,
    expected_chunk_duration: Duration,
    smoothing_factor: f64,
}

impl DownloadConfigBuilder {
    /// 创建新的配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            worker_count: DEFAULT_WORKER_COUNT,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            initial_chunk_size: DEFAULT_INITIAL_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            instant_speed_window: Duration::from_secs(DEFAULT_INSTANT_SPEED_WINDOW_SECS),
            expected_chunk_duration: Duration::from_secs(DEFAULT_EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: DEFAULT_SMOOTHING_FACTOR,
        }
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
    /// 动态分块机制的下限
    ///
    /// # Arguments
    ///
    /// * `size` - 最小分块大小（bytes，必须 > 0）
    pub fn min_chunk_size(mut self, size: u64) -> Self {
        self.min_chunk_size = size.max(1);
        self
    }
    
    /// 设置初始分块大小
    ///
    /// 下载开始时使用的分块大小，之后会根据速度动态调整
    ///
    /// # Arguments
    ///
    /// * `size` - 初始分块大小（bytes，必须 > 0）
    pub fn initial_chunk_size(mut self, size: u64) -> Self {
        self.initial_chunk_size = size.max(1);
        self
    }
    
    /// 设置最大分块大小
    ///
    /// 动态分块机制的上限
    ///
    /// # Arguments
    ///
    /// * `size` - 最大分块大小（bytes，必须 > 0）
    pub fn max_chunk_size(mut self, size: u64) -> Self {
        self.max_chunk_size = size.max(1);
        self
    }
    
    /// 设置实时速度窗口
    ///
    /// 用于计算瞬时速度的时间窗口，同时也是进度更新的时间间隔
    ///
    /// # Arguments
    ///
    /// * `window` - 实时速度窗口（Duration）
    pub fn instant_speed_window(mut self, window: Duration) -> Self {
        self.instant_speed_window = window;
        self
    }
    
    /// 设置预期分块下载时长
    ///
    /// 用于计算理想分块大小：理想大小 = worker速度 * 预期时长
    ///
    /// # Arguments
    ///
    /// * `duration` - 预期分块下载时长（Duration）
    pub fn expected_chunk_duration(mut self, duration: Duration) -> Self {
        self.expected_chunk_duration = duration;
        self
    }
    
    /// 设置分块大小平滑系数
    ///
    /// 用于平滑调整分块大小变化
    ///
    /// # Arguments
    ///
    /// * `factor` - 平滑系数（会被限制在 0.0 ~ 1.0 范围内）
    pub fn smoothing_factor(mut self, factor: f64) -> Self {
        self.smoothing_factor = factor.clamp(0.0, 1.0);
        self
    }
    
    /// 构建配置对象
    pub fn build(self) -> DownloadConfig {
        // 确保配置的合理性：min <= initial <= max
        let min_chunk_size = self.min_chunk_size;
        let initial_chunk_size = self.initial_chunk_size.max(min_chunk_size);
        let max_chunk_size = self.max_chunk_size.max(initial_chunk_size);
        
        DownloadConfig {
            worker_count: self.worker_count,
            min_chunk_size,
            initial_chunk_size,
            max_chunk_size,
            instant_speed_window: self.instant_speed_window,
            expected_chunk_duration: self.expected_chunk_duration,
            smoothing_factor: self.smoothing_factor,
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
        assert_eq!(config.worker_count(), DEFAULT_WORKER_COUNT);
        assert_eq!(config.min_chunk_size(), DEFAULT_MIN_CHUNK_SIZE);
        assert_eq!(config.initial_chunk_size(), DEFAULT_INITIAL_CHUNK_SIZE);
        assert_eq!(config.max_chunk_size(), DEFAULT_MAX_CHUNK_SIZE);
    }

    #[test]
    fn test_builder_default() {
        let config = DownloadConfig::builder().build();
        assert_eq!(config.worker_count(), DEFAULT_WORKER_COUNT);
        assert_eq!(config.min_chunk_size(), DEFAULT_MIN_CHUNK_SIZE);
        assert_eq!(config.initial_chunk_size(), DEFAULT_INITIAL_CHUNK_SIZE);
        assert_eq!(config.max_chunk_size(), DEFAULT_MAX_CHUNK_SIZE);
    }

    #[test]
    fn test_builder_custom() {
        let config = DownloadConfig::builder()
            .worker_count(8)
            .min_chunk_size(1 * 1024 * 1024)
            .initial_chunk_size(10 * 1024 * 1024)
            .max_chunk_size(100 * 1024 * 1024)
            .build();
        
        assert_eq!(config.worker_count(), 8);
        assert_eq!(config.min_chunk_size(), 1 * 1024 * 1024);
        assert_eq!(config.initial_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(config.max_chunk_size(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_builder_min_values() {
        let config = DownloadConfig::builder()
            .worker_count(0)  // 应该被限制为 1
            .min_chunk_size(0)  // 应该被限制为 1
            .initial_chunk_size(0)  // 应该被限制为 1
            .max_chunk_size(0)  // 应该被限制为 1
            .build();
        
        assert_eq!(config.worker_count(), 1);
        assert_eq!(config.min_chunk_size(), 1);
        assert_eq!(config.initial_chunk_size(), 1);
        assert_eq!(config.max_chunk_size(), 1);
    }

    #[test]
    fn test_builder_ensures_order() {
        // 测试 build() 确保 min <= initial <= max
        let config = DownloadConfig::builder()
            .min_chunk_size(10 * 1024 * 1024)
            .initial_chunk_size(5 * 1024 * 1024)  // 小于 min
            .max_chunk_size(3 * 1024 * 1024)      // 小于 initial
            .build();
        
        assert_eq!(config.min_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(config.initial_chunk_size(), 10 * 1024 * 1024);  // 调整为 min
        assert_eq!(config.max_chunk_size(), 10 * 1024 * 1024);      // 调整为 initial
    }
}

