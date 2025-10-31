//! 下载配置模块
//!
//! 提供下载任务的配置选项，包括分块策略和并发控制

use std::time::Duration;
use crate::constants::MB;

/// 最大 Worker 并发数量（硬编码限制）
pub const MAX_WORKER_COUNT: usize = 32;

/// 默认最小分块大小：2 MB
pub const DEFAULT_MIN_CHUNK_SIZE: u64 = 2 * MB;

/// 默认初始分块大小：5 MB
pub const DEFAULT_INITIAL_CHUNK_SIZE: u64 = 5 * MB;

/// 默认最大分块大小：50 MB
pub const DEFAULT_MAX_CHUNK_SIZE: u64 = 50 * MB;

/// 默认 Worker 并发数
pub const DEFAULT_WORKER_COUNT: usize = 4;

/// 默认实时速度窗口：1 秒
pub const DEFAULT_INSTANT_SPEED_WINDOW_SECS: u64 = 1;

/// 默认预期分块下载时长：5 秒
pub const DEFAULT_EXPECTED_CHUNK_DURATION_SECS: u64 = 5;

/// 默认分块大小平滑系数：0.7
pub const DEFAULT_SMOOTHING_FACTOR: f64 = 0.7;

/// 默认瞬时速度权重：0.7
///
/// 在计算理想分块大小时，瞬时速度的权重
pub const DEFAULT_INSTANT_SPEED_WEIGHT: f64 = 0.7;

/// 默认平均速度权重：0.3
///
/// 在计算理想分块大小时，平均速度的权重
pub const DEFAULT_AVG_SPEED_WEIGHT: f64 = 0.3;

/// 默认请求超时时间：30 秒
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// 默认连接超时时间：10 秒
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// 默认渐进式启动比例序列：[0.25, 0.5, 0.75, 1.0]
/// 
/// 表示按 25%, 50%, 75%, 100% 的比例逐步启动worker
pub const DEFAULT_PROGRESSIVE_WORKER_RATIOS: &[f64] = &[0.25, 0.5, 0.75, 1.0];

/// 默认最小速度阈值：1 MB/s
/// 
/// 当所有已启动的worker速度都达到此阈值时，才启动下一批worker
pub const DEFAULT_MIN_SPEED_THRESHOLD: u64 = 1 * MB;

/// 构建配置错误
#[derive(thiserror::Error, Debug)]
pub enum BuildError {
    #[error("Worker 数量 {0} 超过最大限制 {1}")]
    WorkerCountExceeded(usize, usize),
}

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
    
    /// 瞬时速度权重 (0.0 ~ 1.0)
    ///
    /// 在计算理想分块大小时，瞬时速度的权重
    /// 理想分块大小 = (瞬时速度 * 瞬时速度权重 + 平均速度 * 平均速度权重) * 预期时间
    pub(crate) instant_speed_weight: f64,
    
    /// 平均速度权重 (0.0 ~ 1.0)
    ///
    /// 在计算理想分块大小时，平均速度的权重
    /// 理想分块大小 = (瞬时速度 * 瞬时速度权重 + 平均速度 * 平均速度权重) * 预期时间
    pub(crate) avg_speed_weight: f64,
    
    /// HTTP 请求总体超时时间
    ///
    /// 包含连接建立、请求发送和响应接收的总时长
    pub(crate) timeout: Duration,
    
    /// HTTP 连接超时时间
    ///
    /// 只计算 TCP 连接建立的时长
    pub(crate) connect_timeout: Duration,
    
    /// 渐进式启动比例序列
    ///
    /// 定义分阶段启动worker的比例，如 [0.25, 0.5, 0.75, 1.0] 表示
    /// 按 25%, 50%, 75%, 100% 的比例逐步启动worker
    pub(crate) progressive_worker_ratios: Vec<f64>,
    
    /// 最小速度阈值（bytes/s）
    ///
    /// 当所有已启动的worker的瞬时速度都达到此阈值时，才启动下一批worker
    pub(crate) min_speed_threshold: u64,
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
    /// - 瞬时速度权重: 0.7
    /// - 平均速度权重: 0.3
    /// - 请求超时: 30 秒
    /// - 连接超时: 10 秒
    /// - 渐进启动比例: [0.25, 0.5, 0.75, 1.0]
    /// - 最小速度阈值: 1 MB/s
    pub fn default() -> Self {
        Self {
            worker_count: DEFAULT_WORKER_COUNT,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            initial_chunk_size: DEFAULT_INITIAL_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            instant_speed_window: Duration::from_secs(DEFAULT_INSTANT_SPEED_WINDOW_SECS),
            expected_chunk_duration: Duration::from_secs(DEFAULT_EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: DEFAULT_SMOOTHING_FACTOR,
            instant_speed_weight: DEFAULT_INSTANT_SPEED_WEIGHT,
            avg_speed_weight: DEFAULT_AVG_SPEED_WEIGHT,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS),
            progressive_worker_ratios: DEFAULT_PROGRESSIVE_WORKER_RATIOS.to_vec(),
            min_speed_threshold: DEFAULT_MIN_SPEED_THRESHOLD,
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
    
    /// 获取瞬时速度权重
    pub fn instant_speed_weight(&self) -> f64 {
        self.instant_speed_weight
    }
    
    /// 获取平均速度权重
    pub fn avg_speed_weight(&self) -> f64 {
        self.avg_speed_weight
    }
    
    /// 获取请求超时时间
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    
    /// 获取连接超时时间
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }
    
    /// 获取渐进式启动比例序列
    pub fn progressive_worker_ratios(&self) -> &[f64] {
        &self.progressive_worker_ratios
    }
    
    /// 获取最小速度阈值
    pub fn min_speed_threshold(&self) -> u64 {
        self.min_speed_threshold
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
    instant_speed_weight: f64,
    avg_speed_weight: f64,
    timeout: Duration,
    connect_timeout: Duration,
    progressive_worker_ratios: Vec<f64>,
    min_speed_threshold: u64,
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
            instant_speed_weight: DEFAULT_INSTANT_SPEED_WEIGHT,
            avg_speed_weight: DEFAULT_AVG_SPEED_WEIGHT,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS),
            progressive_worker_ratios: DEFAULT_PROGRESSIVE_WORKER_RATIOS.to_vec(),
            min_speed_threshold: DEFAULT_MIN_SPEED_THRESHOLD,
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
    
    /// 设置瞬时速度权重
    ///
    /// 在计算理想分块大小时，瞬时速度的权重
    ///
    /// # Arguments
    ///
    /// * `weight` - 瞬时速度权重（会被限制在 0.0 ~ 1.0 范围内）
    pub fn instant_speed_weight(mut self, weight: f64) -> Self {
        self.instant_speed_weight = weight.clamp(0.0, 1.0);
        self
    }
    
    /// 设置平均速度权重
    ///
    /// 在计算理想分块大小时，平均速度的权重
    ///
    /// # Arguments
    ///
    /// * `weight` - 平均速度权重（会被限制在 0.0 ~ 1.0 范围内）
    pub fn avg_speed_weight(mut self, weight: f64) -> Self {
        self.avg_speed_weight = weight.clamp(0.0, 1.0);
        self
    }
    
    /// 设置 HTTP 请求总体超时时间
    ///
    /// # Arguments
    ///
    /// * `timeout` - 请求超时时间
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    /// 设置 HTTP 连接超时时间
    ///
    /// # Arguments
    ///
    /// * `timeout` - 连接超时时间
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
    
    /// 设置渐进式启动比例序列
    ///
    /// 定义分阶段启动worker的比例。比例会自动排序并限制在 (0.0, 1.0] 范围内。
    ///
    /// # Arguments
    ///
    /// * `ratios` - 比例序列，如 `vec![0.25, 0.5, 0.75, 1.0]` 表示按 25%, 50%, 75%, 100% 逐步启动
    ///
    /// # Example
    ///
    /// ```
    /// # use rs_dn::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .worker_count(12)
    ///     .progressive_worker_ratios(vec![0.25, 0.5, 0.75, 1.0])  // 3, 6, 9, 12 个 worker
    ///     .build();
    /// ```
    pub fn progressive_worker_ratios(mut self, ratios: Vec<f64>) -> Self {
        // 过滤掉无效的比例值（必须在 0.0 < ratio <= 1.0 范围内）
        let mut valid_ratios: Vec<f64> = ratios
            .into_iter()
            .filter(|&r| r > 0.0 && r <= 1.0)
            .collect();
        
        // 排序并去重
        valid_ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
        valid_ratios.dedup();
        
        // 如果没有有效比例，使用默认值
        if valid_ratios.is_empty() {
            self.progressive_worker_ratios = DEFAULT_PROGRESSIVE_WORKER_RATIOS.to_vec();
        } else {
            self.progressive_worker_ratios = valid_ratios;
        }
        
        self
    }
    
    /// 设置最小速度阈值（bytes/s）
    ///
    /// 当所有已启动的worker的瞬时速度都达到此阈值时，才启动下一批worker
    ///
    /// # Arguments
    ///
    /// * `threshold` - 速度阈值（bytes/s）
    ///
    /// # Example
    ///
    /// ```
    /// # use rs_dn::DownloadConfig;
    /// # use rs_dn::constants::*;
    /// let config = DownloadConfig::builder()
    ///     .min_speed_threshold(5 * MB)  // 5 MB/s
    ///     .build();
    /// ```
    pub fn min_speed_threshold(mut self, threshold: u64) -> Self {
        self.min_speed_threshold = threshold;
        self
    }
    
    /// 构建配置对象
    pub fn build(self) -> Result<DownloadConfig, BuildError> {
        // 验证 worker_count 不超过最大限制
        if self.worker_count > MAX_WORKER_COUNT {
            return Err(BuildError::WorkerCountExceeded(self.worker_count, MAX_WORKER_COUNT));
        }
        
        // 确保配置的合理性：min <= initial <= max
        let min_chunk_size = self.min_chunk_size;
        let initial_chunk_size = self.initial_chunk_size.max(min_chunk_size);
        let max_chunk_size = self.max_chunk_size.max(initial_chunk_size);
        
        Ok(DownloadConfig {
            worker_count: self.worker_count,
            min_chunk_size,
            initial_chunk_size,
            max_chunk_size,
            instant_speed_window: self.instant_speed_window,
            expected_chunk_duration: self.expected_chunk_duration,
            smoothing_factor: self.smoothing_factor,
            instant_speed_weight: self.instant_speed_weight,
            avg_speed_weight: self.avg_speed_weight,
            timeout: self.timeout,
            connect_timeout: self.connect_timeout,
            progressive_worker_ratios: self.progressive_worker_ratios,
            min_speed_threshold: self.min_speed_threshold,
        })
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
        let config = DownloadConfig::builder().build().unwrap();
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
            .build().unwrap();
        
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
            .build().unwrap();
        
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
            .build().unwrap();
        
        assert_eq!(config.min_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(config.initial_chunk_size(), 10 * 1024 * 1024);  // 调整为 min
        assert_eq!(config.max_chunk_size(), 10 * 1024 * 1024);      // 调整为 initial
    }

    #[test]
    fn test_progressive_worker_ratios_default() {
        let config = DownloadConfig::default();
        assert_eq!(config.progressive_worker_ratios(), DEFAULT_PROGRESSIVE_WORKER_RATIOS);
        assert_eq!(config.min_speed_threshold(), DEFAULT_MIN_SPEED_THRESHOLD);
    }

    #[test]
    fn test_progressive_worker_ratios_custom() {
        let config = DownloadConfig::builder()
            .progressive_worker_ratios(vec![0.5, 1.0])
            .build().unwrap();
        
        assert_eq!(config.progressive_worker_ratios(), &[0.5, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_sorting() {
        // 测试自动排序
        let config = DownloadConfig::builder()
            .progressive_worker_ratios(vec![1.0, 0.25, 0.75, 0.5])
            .build().unwrap();
        
        assert_eq!(config.progressive_worker_ratios(), &[0.25, 0.5, 0.75, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_filtering() {
        // 测试过滤无效值（<= 0 或 > 1.0）
        let config = DownloadConfig::builder()
            .progressive_worker_ratios(vec![0.0, 0.5, 1.0, 1.5, -0.1])
            .build().unwrap();
        
        // 0.0, 1.5, -0.1 应该被过滤掉
        assert_eq!(config.progressive_worker_ratios(), &[0.5, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_dedup() {
        // 测试去重
        let config = DownloadConfig::builder()
            .progressive_worker_ratios(vec![0.5, 0.5, 1.0, 1.0, 0.25])
            .build().unwrap();
        
        assert_eq!(config.progressive_worker_ratios(), &[0.25, 0.5, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_empty_uses_default() {
        // 测试空序列或全部无效值时使用默认值
        let config = DownloadConfig::builder()
            .progressive_worker_ratios(vec![])
            .build().unwrap();
        
        assert_eq!(config.progressive_worker_ratios(), DEFAULT_PROGRESSIVE_WORKER_RATIOS);
        
        let config2 = DownloadConfig::builder()
            .progressive_worker_ratios(vec![0.0, -1.0, 2.0])
            .build().unwrap();
        
        assert_eq!(config2.progressive_worker_ratios(), DEFAULT_PROGRESSIVE_WORKER_RATIOS);
    }

    #[test]
    fn test_min_speed_threshold() {
        let config = DownloadConfig::builder()
            .min_speed_threshold(5 * 1024 * 1024)  // 5 MB/s
            .build().unwrap();
        
        assert_eq!(config.min_speed_threshold(), 5 * 1024 * 1024);
    }
}

