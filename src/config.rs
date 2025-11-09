//! 下载配置模块
//!
//! 提供下载任务的配置选项，按功能域分类组织

use std::{num::NonZeroU32, time::Duration};
use crate::constants::{KB, MB};

// ==================== 常量结构体 ====================

/// 分块配置常量
pub struct ChunkDefaults;

impl ChunkDefaults {
    /// 最小分块大小：2 MB
    pub const MIN_SIZE: u64 = 2 * MB;
    /// 初始分块大小：5 MB
    pub const INITIAL_SIZE: u64 = 5 * MB;
    /// 最大分块大小：50 MB
    pub const MAX_SIZE: u64 = 50 * MB;
}

/// 并发配置常量
pub struct ConcurrencyDefaults;

impl ConcurrencyDefaults {
    /// 默认 Worker 数量：4
    pub const WORKER_COUNT: u64 = 4;
}

/// 网络配置常量
pub struct NetworkDefaults;

impl NetworkDefaults {
    /// 请求超时时间：30 秒
    pub const TIMEOUT_SECS: u64 = 30;
    /// 连接超时时间：10 秒
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;
}

/// 速度计算配置常量
pub struct SpeedDefaults;

impl SpeedDefaults {
    /// 基础时间间隔：1000 毫秒（1 秒）
    pub const BASE_INTERVAL_MILLIS: u64 = 1000;
    /// 实时速度窗口倍数：1
    pub const INSTANT_WINDOW_MULTIPLIER: u32 = 1;
    /// 窗口平均速度倍数：5（计算过去 5 个时间窗口的平均速度）
    pub const WINDOW_AVG_MULTIPLIER: u32 = 5;
    /// 预期分块下载时长：5 秒
    pub const EXPECTED_CHUNK_DURATION_SECS: u64 = 5;
    /// 分块大小平滑系数：0.7
    pub const SMOOTHING_FACTOR: f64 = 0.7;
    /// 瞬时速度权重：0.7
    pub const INSTANT_WEIGHT: f64 = 0.7;
    /// 平均速度权重：0.3
    pub const AVG_WEIGHT: f64 = 0.3;
    /// 采样间隔：100 毫秒
    pub const SAMPLE_INTERVAL_MILLIS: u64 = 100;
    /// 环形缓冲区大小安全余量：1.2（在理论值基础上增加 20%）
    pub const BUFFER_SIZE_MARGIN: f64 = 1.2;
}

/// 渐进式启动配置常量
pub struct ProgressiveDefaults;

impl ProgressiveDefaults {
    /// 渐进式启动比例序列：[0.25, 0.5, 0.75, 1.0]
    pub const WORKER_RATIOS: &'static [f64] = &[0.25, 0.5, 0.75, 1.0];
    /// 最小速度阈值：1 MB/s
    pub const MIN_SPEED_THRESHOLD: u64 = MB;
    /// 预期下载结束前最小时间（秒）：20 秒
    pub const MIN_TIME_BEFORE_FINISH_SECS: u64 = 20;
}

/// 重试配置常量
pub struct RetryDefaults;

impl RetryDefaults {
    /// 最大重试次数：3 次
    pub const MAX_RETRY_COUNT: usize = 3;
    /// 重试延迟序列：[1s, 2s, 3s]
    pub const RETRY_DELAYS_SECS: &'static [u64] = &[1, 2, 3];
}

/// 健康检查配置常量
pub struct HealthCheckDefaults;

impl HealthCheckDefaults {
    /// 绝对速度阈值：100 KB/s
    pub const ABSOLUTE_SPEED_THRESHOLD: u64 = 100 * KB;
    /// 是否启用健康检查
    pub const ENABLED: bool = true;
    /// 最小worker数量阈值（低于此数量不执行健康检查）
    pub const MIN_WORKERS_FOR_CHECK: u64 = 3;
}

// ==================== 子配置结构体 ====================

/// 分块配置
///
/// 控制下载分块的大小范围
#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// 最小分块大小（bytes）
    pub min_size: u64,
    /// 初始分块大小（bytes）
    pub initial_size: u64,
    /// 最大分块大小（bytes）
    pub max_size: u64,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            min_size: ChunkDefaults::MIN_SIZE,
            initial_size: ChunkDefaults::INITIAL_SIZE,
            max_size: ChunkDefaults::MAX_SIZE,
        }
    }
}

impl ChunkConfig {
    #[inline]
    pub fn min_size(&self) -> u64 {
        self.min_size
    }

    #[inline]
    pub fn initial_size(&self) -> u64 {
        self.initial_size
    }

    #[inline]
    pub fn max_size(&self) -> u64 {
        self.max_size
    }
}

/// 并发配置
///
/// 控制 Worker 并发数量
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Worker 并发数量
    pub worker_count: u64,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            worker_count: ConcurrencyDefaults::WORKER_COUNT,
        }
    }
}

impl ConcurrencyConfig {
    #[inline]
    pub fn worker_count(&self) -> u64 {
        self.worker_count
    }
}

/// 网络配置
///
/// 控制 HTTP 请求的超时设置
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// HTTP 请求总体超时时间
    pub timeout: Duration,
    /// HTTP 连接超时时间
    pub connect_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(NetworkDefaults::TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(NetworkDefaults::CONNECT_TIMEOUT_SECS),
        }
    }
}

impl NetworkConfig {
    #[inline]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }
}

/// 速度计算配置
///
/// 控制速度计算和分块大小调整策略
#[derive(Debug, Clone)]
pub struct SpeedConfig {
    /// 基础时间间隔（统计数据的基础时间间隔）
    pub base_interval: Duration,
    /// 实时速度窗口倍数（实际窗口 = base_interval × multiplier）
    pub instant_speed_window_multiplier: NonZeroU32,
    /// 窗口平均速度倍数（实际窗口 = base_interval × multiplier）
    pub window_avg_multiplier: NonZeroU32,
    /// 预期单个分块下载时长
    pub expected_chunk_duration: Duration,
    /// 分块大小平滑系数 (0.0 ~ 1.0)
    pub smoothing_factor: f64,
    /// 瞬时速度权重 (0.0 ~ 1.0)
    pub instant_speed_weight: f64,
    /// 平均速度权重 (0.0 ~ 1.0)
    pub avg_speed_weight: f64,
    /// 采样间隔（用于速度计算的采样频率）
    pub sample_interval: Duration,
    /// 环形缓冲区大小安全余量（在理论值基础上增加的比例）
    pub buffer_size_margin: f64,
}

impl Default for SpeedConfig {
    fn default() -> Self {
        Self {
            base_interval: Duration::from_millis(SpeedDefaults::BASE_INTERVAL_MILLIS),
            instant_speed_window_multiplier: NonZeroU32::new(SpeedDefaults::INSTANT_WINDOW_MULTIPLIER).unwrap(),
            window_avg_multiplier: NonZeroU32::new(SpeedDefaults::WINDOW_AVG_MULTIPLIER).unwrap(),
            expected_chunk_duration: Duration::from_secs(SpeedDefaults::EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: SpeedDefaults::SMOOTHING_FACTOR,
            instant_speed_weight: SpeedDefaults::INSTANT_WEIGHT,
            avg_speed_weight: SpeedDefaults::AVG_WEIGHT,
            sample_interval: Duration::from_millis(SpeedDefaults::SAMPLE_INTERVAL_MILLIS),
            buffer_size_margin: SpeedDefaults::BUFFER_SIZE_MARGIN,
        }
    }
}

impl SpeedConfig {
    #[inline]
    pub fn base_interval(&self) -> Duration {
        self.base_interval
    }

    #[inline]
    pub fn instant_speed_window_multiplier(&self) -> u32 {
        self.instant_speed_window_multiplier.get()
    }

    #[inline]
    pub fn instant_speed_window(&self) -> Duration {
        self.base_interval * self.instant_speed_window_multiplier.get()
    }

    #[inline]
    pub fn window_avg_multiplier(&self) -> u32 {
        self.window_avg_multiplier.get()
    }

    #[inline]
    pub fn window_avg_duration(&self) -> Duration {
        self.base_interval * self.window_avg_multiplier.get()
    }

    #[inline]
    pub fn expected_chunk_duration(&self) -> Duration {
        self.expected_chunk_duration
    }

    #[inline]
    pub fn smoothing_factor(&self) -> f64 {
        self.smoothing_factor
    }

    #[inline]
    pub fn instant_speed_weight(&self) -> f64 {
        self.instant_speed_weight
    }

    #[inline]
    pub fn avg_speed_weight(&self) -> f64 {
        self.avg_speed_weight
    }

    #[inline]
    pub fn sample_interval(&self) -> Duration {
        self.sample_interval
    }

    #[inline]
    pub fn buffer_size_margin(&self) -> f64 {
        self.buffer_size_margin
    }
}

/// 渐进式启动配置
///
/// 控制 Worker 的渐进式启动策略
#[derive(Debug, Clone)]
pub struct ProgressiveConfig {
    /// 渐进式启动比例序列
    pub worker_ratios: Vec<f64>,
    /// 最小速度阈值（bytes/s）
    pub min_speed_threshold: u64,
    /// 预期下载结束前最小时间（在此时间内不启动新 worker）
    pub min_time_before_finish: Duration,
}

impl Default for ProgressiveConfig {
    fn default() -> Self {
        Self {
            worker_ratios: ProgressiveDefaults::WORKER_RATIOS.to_vec(),
            min_speed_threshold: ProgressiveDefaults::MIN_SPEED_THRESHOLD,
            min_time_before_finish: Duration::from_secs(ProgressiveDefaults::MIN_TIME_BEFORE_FINISH_SECS),
        }
    }
}

impl ProgressiveConfig {
    #[inline]
    pub fn worker_ratios(&self) -> &[f64] {
        &self.worker_ratios
    }

    #[inline]
    pub fn min_speed_threshold(&self) -> u64 {
        self.min_speed_threshold
    }

    #[inline]
    pub fn min_time_before_finish(&self) -> Duration {
        self.min_time_before_finish
    }
}

/// 重试配置
///
/// 控制失败任务的重试策略
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 最大重试次数
    pub max_retry_count: usize,
    /// 重试延迟序列
    pub retry_delays: Vec<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retry_count: RetryDefaults::MAX_RETRY_COUNT,
            retry_delays: RetryDefaults::RETRY_DELAYS_SECS
                .iter()
                .map(|&secs| Duration::from_secs(secs))
                .collect(),
        }
    }
}

impl RetryConfig {
    #[inline]
    pub fn max_retry_count(&self) -> usize {
        self.max_retry_count
    }

    #[inline]
    pub fn retry_delays(&self) -> &[Duration] {
        &self.retry_delays
    }
}

/// 健康检查配置
///
/// 控制异常下载线程的检测和处理策略
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// 是否启用健康检查
    pub enabled: bool,
    /// 绝对速度阈值（bytes/s）
    pub absolute_speed_threshold: u64,
    /// 最小worker数量阈值（低于此数量不执行健康检查）
    pub min_workers_for_check: u64,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: HealthCheckDefaults::ENABLED,
            absolute_speed_threshold: HealthCheckDefaults::ABSOLUTE_SPEED_THRESHOLD,
            min_workers_for_check: HealthCheckDefaults::MIN_WORKERS_FOR_CHECK,
        }
    }
}

impl HealthCheckConfig {
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    #[inline]
    pub fn absolute_speed_threshold(&self) -> u64 {
        self.absolute_speed_threshold
    }

    #[inline]
    pub fn min_workers_for_check(&self) -> u64 {
        self.min_workers_for_check
    }
}

// ==================== 主配置结构体 ====================

/// 下载配置
///
/// 控制下载任务的所有行为，按功能域分类组织
#[derive(Debug, Clone, Default)]
pub struct DownloadConfig {
    /// 分块配置
    chunk: ChunkConfig,
    /// 并发配置
    concurrency: ConcurrencyConfig,
    /// 网络配置
    network: NetworkConfig,
    /// 速度计算配置
    speed: SpeedConfig,
    /// 渐进式启动配置
    progressive: ProgressiveConfig,
    /// 重试配置
    retry: RetryConfig,
    /// 健康检查配置
    health_check: HealthCheckConfig,
}

impl DownloadConfig {
    /// 创建配置构建器
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .concurrency(|c| c.worker_count(4))
    ///     .chunk(|c| c.initial_size(5 * 1024 * 1024))
    ///     .build();
    /// ```
    pub fn builder() -> DownloadConfigBuilder {
        DownloadConfigBuilder::new()
    }
    
    #[inline]
    pub fn chunk(&self) -> &ChunkConfig {
        &self.chunk
    }

    #[inline]
    pub fn concurrency(&self) -> &ConcurrencyConfig {
        &self.concurrency
    }

    #[inline]
    pub fn network(&self) -> &NetworkConfig {
        &self.network
    }
    
    #[inline]
    pub fn speed(&self) -> &SpeedConfig {
        &self.speed
    }

    #[inline]
    pub fn progressive(&self) -> &ProgressiveConfig {
        &self.progressive
    }

    #[inline]
    pub fn retry(&self) -> &RetryConfig {
        &self.retry
    }

    #[inline]
    pub fn health_check(&self) -> &HealthCheckConfig {
        &self.health_check
    }
}

// ==================== 子配置构建器 ====================

/// 分块配置构建器
#[derive(Debug, Clone)]
pub struct ChunkConfigBuilder {
    min_size: u64,
    initial_size: u64,
    max_size: u64,
}

impl ChunkConfigBuilder {
    /// 创建新的分块配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            min_size: ChunkDefaults::MIN_SIZE,
            initial_size: ChunkDefaults::INITIAL_SIZE,
            max_size: ChunkDefaults::MAX_SIZE,
        }
    }

    /// 设置最小分块大小
    pub fn min_size(mut self, size: u64) -> Self {
        self.min_size = size.max(1);
        self
    }

    /// 设置初始分块大小
    pub fn initial_size(mut self, size: u64) -> Self {
        self.initial_size = size.max(1);
        self
    }

    /// 设置最大分块大小
    pub fn max_size(mut self, size: u64) -> Self {
        self.max_size = size.max(1);
        self
    }

    /// 构建分块配置
    pub fn build(self) -> ChunkConfig {
        ChunkConfig {
            min_size: self.min_size,
            initial_size: self.initial_size,
            max_size: self.max_size,
        }
    }
}

impl Default for ChunkConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 并发配置构建器
#[derive(Debug, Clone)]
pub struct ConcurrencyConfigBuilder {
    worker_count: u64,
}

impl ConcurrencyConfigBuilder {
    /// 创建新的并发配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            worker_count: ConcurrencyDefaults::WORKER_COUNT,
        }
    }

    /// 设置 Worker 并发数量
    pub fn worker_count(mut self, count: u64) -> Self {
        self.worker_count = count.max(1);
        self
    }

    /// 构建并发配置
    pub fn build(self) -> ConcurrencyConfig {
        ConcurrencyConfig {
            worker_count: self.worker_count,
        }
    }
}

impl Default for ConcurrencyConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 网络配置构建器
#[derive(Debug, Clone)]
pub struct NetworkConfigBuilder {
    timeout: Duration,
    connect_timeout: Duration,
}

impl NetworkConfigBuilder {
    /// 创建新的网络配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(NetworkDefaults::TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(NetworkDefaults::CONNECT_TIMEOUT_SECS),
        }
    }

    /// 设置 HTTP 请求总体超时时间
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 设置 HTTP 连接超时时间
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// 构建网络配置
    pub fn build(self) -> NetworkConfig {
        NetworkConfig {
            timeout: self.timeout,
            connect_timeout: self.connect_timeout,
        }
    }
}

impl Default for NetworkConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 速度配置构建器
#[derive(Debug, Clone)]
pub struct SpeedConfigBuilder {
    base_interval: Duration,
    instant_speed_window_multiplier: NonZeroU32,
    window_avg_multiplier: NonZeroU32,
    expected_chunk_duration: Duration,
    smoothing_factor: f64,
    instant_speed_weight: f64,
    avg_speed_weight: f64,
    sample_interval: Duration,
    buffer_size_margin: f64,
}

impl SpeedConfigBuilder {
    /// 创建新的速度配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            base_interval: Duration::from_millis(SpeedDefaults::BASE_INTERVAL_MILLIS),
            instant_speed_window_multiplier: NonZeroU32::new(SpeedDefaults::INSTANT_WINDOW_MULTIPLIER).unwrap(),
            window_avg_multiplier: NonZeroU32::new(SpeedDefaults::WINDOW_AVG_MULTIPLIER).unwrap(),
            expected_chunk_duration: Duration::from_secs(SpeedDefaults::EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: SpeedDefaults::SMOOTHING_FACTOR,
            instant_speed_weight: SpeedDefaults::INSTANT_WEIGHT,
            avg_speed_weight: SpeedDefaults::AVG_WEIGHT,
            sample_interval: Duration::from_millis(SpeedDefaults::SAMPLE_INTERVAL_MILLIS),
            buffer_size_margin: SpeedDefaults::BUFFER_SIZE_MARGIN,
        }
    }

    /// 设置基础时间间隔
    pub fn base_interval(mut self, interval: Duration) -> Self {
        self.base_interval = interval;
        self
    }

    /// 设置实时速度窗口倍数
    pub fn instant_window_multiplier(mut self, multiplier: NonZeroU32) -> Self {
        self.instant_speed_window_multiplier = multiplier;
        self
    }

    /// 设置窗口平均速度倍数
    pub fn window_avg_multiplier(mut self, multiplier: NonZeroU32) -> Self {
        self.window_avg_multiplier = multiplier;
        self
    }

    /// 设置预期分块下载时长
    pub fn expected_chunk_duration(mut self, duration: Duration) -> Self {
        self.expected_chunk_duration = duration;
        self
    }

    /// 设置分块大小平滑系数
    pub fn smoothing_factor(mut self, factor: f64) -> Self {
        self.smoothing_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// 设置瞬时速度权重
    pub fn instant_weight(mut self, weight: f64) -> Self {
        self.instant_speed_weight = weight.clamp(0.0, 1.0);
        self
    }

    /// 设置平均速度权重
    pub fn avg_weight(mut self, weight: f64) -> Self {
        self.avg_speed_weight = weight.clamp(0.0, 1.0);
        self
    }

    /// 设置采样间隔
    pub fn sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }

    /// 设置缓冲区大小安全余量
    pub fn buffer_size_margin(mut self, margin: f64) -> Self {
        self.buffer_size_margin = margin.max(1.0); // 至少为 1.0，不能缩小缓冲区
        self
    }

    /// 构建速度配置
    pub fn build(self) -> SpeedConfig {
        SpeedConfig {
            base_interval: self.base_interval,
            instant_speed_window_multiplier: self.instant_speed_window_multiplier,
            window_avg_multiplier: self.window_avg_multiplier,
            expected_chunk_duration: self.expected_chunk_duration,
            smoothing_factor: self.smoothing_factor,
            instant_speed_weight: self.instant_speed_weight,
            avg_speed_weight: self.avg_speed_weight,
            sample_interval: self.sample_interval,
            buffer_size_margin: self.buffer_size_margin,
        }
    }
}

impl Default for SpeedConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 渐进式配置构建器
#[derive(Debug, Clone)]
pub struct ProgressiveConfigBuilder {
    worker_ratios: Vec<f64>,
    min_speed_threshold: u64,
    min_time_before_finish: Duration,
}

impl ProgressiveConfigBuilder {
    /// 创建新的渐进式配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            worker_ratios: ProgressiveDefaults::WORKER_RATIOS.to_vec(),
            min_speed_threshold: ProgressiveDefaults::MIN_SPEED_THRESHOLD,
            min_time_before_finish: Duration::from_secs(ProgressiveDefaults::MIN_TIME_BEFORE_FINISH_SECS),
        }
    }

    /// 设置渐进式启动比例序列
    pub fn worker_ratios(mut self, ratios: Vec<f64>) -> Self {
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
            self.worker_ratios = ProgressiveDefaults::WORKER_RATIOS.to_vec();
        } else {
            self.worker_ratios = valid_ratios;
        }
        
        self
    }

    /// 设置最小速度阈值（bytes/s）
    pub fn min_speed_threshold(mut self, threshold: u64) -> Self {
        self.min_speed_threshold = threshold;
        self
    }

    /// 设置预期下载结束前最小时间
    pub fn min_time_before_finish(mut self, duration: Duration) -> Self {
        self.min_time_before_finish = duration;
        self
    }

    /// 构建渐进式配置
    pub fn build(self) -> ProgressiveConfig {
        ProgressiveConfig {
            worker_ratios: self.worker_ratios,
            min_speed_threshold: self.min_speed_threshold,
            min_time_before_finish: self.min_time_before_finish,
        }
    }
}

impl Default for ProgressiveConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 重试配置构建器
#[derive(Debug, Clone)]
pub struct RetryConfigBuilder {
    max_retry_count: usize,
    retry_delays: Vec<Duration>,
}

impl RetryConfigBuilder {
    /// 创建新的重试配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            max_retry_count: RetryDefaults::MAX_RETRY_COUNT,
            retry_delays: RetryDefaults::RETRY_DELAYS_SECS
                .iter()
                .map(|&secs| Duration::from_secs(secs))
                .collect(),
        }
    }

    /// 设置最大重试次数
    pub fn max_retry_count(mut self, count: usize) -> Self {
        self.max_retry_count = count;
        self
    }

    /// 设置重试延迟序列
    pub fn retry_delays(mut self, delays: Vec<Duration>) -> Self {
        if delays.is_empty() {
            // 使用默认延迟序列
            self.retry_delays = RetryDefaults::RETRY_DELAYS_SECS
                .iter()
                .map(|&secs| Duration::from_secs(secs))
                .collect();
        } else {
            self.retry_delays = delays;
        }
        self
    }

    /// 构建重试配置
    pub fn build(self) -> RetryConfig {
        RetryConfig {
            max_retry_count: self.max_retry_count,
            retry_delays: self.retry_delays,
        }
    }
}

impl Default for RetryConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 健康检查配置构建器
#[derive(Debug, Clone)]
pub struct HealthCheckConfigBuilder {
    enabled: bool,
    absolute_speed_threshold: u64,
    min_workers_for_check: u64,
}

impl HealthCheckConfigBuilder {
    /// 创建新的健康检查配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            enabled: HealthCheckDefaults::ENABLED,
            absolute_speed_threshold: HealthCheckDefaults::ABSOLUTE_SPEED_THRESHOLD,
            min_workers_for_check: HealthCheckDefaults::MIN_WORKERS_FOR_CHECK,
        }
    }

    /// 设置是否启用健康检查
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// 设置绝对速度阈值（bytes/s）
    pub fn absolute_speed_threshold(mut self, threshold: u64) -> Self {
        self.absolute_speed_threshold = threshold;
        self
    }

    /// 设置最小worker数量阈值
    pub fn min_workers_for_check(mut self, count: u64) -> Self {
        self.min_workers_for_check = count.max(2); // 至少需要2个worker才能比较
        self
    }

    /// 构建健康检查配置
    pub fn build(self) -> HealthCheckConfig {
        HealthCheckConfig {
            enabled: self.enabled,
            absolute_speed_threshold: self.absolute_speed_threshold,
            min_workers_for_check: self.min_workers_for_check,
        }
    }
}

impl Default for HealthCheckConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 主配置构建器 ====================

/// 下载配置构建器
///
/// 使用 Builder 模式和闭包风格创建 `DownloadConfig`
///
/// # Example
///
/// ```
/// # use hydra_dl::DownloadConfig;
/// let config = DownloadConfig::builder()
///     .chunk(|c| c.min_size(2 * 1024 * 1024).initial_size(10 * 1024 * 1024))
///     .concurrency(|c| c.worker_count(8))
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct DownloadConfigBuilder {
    chunk: ChunkConfig,
    concurrency: ConcurrencyConfig,
    network: NetworkConfig,
    speed: SpeedConfig,
    progressive: ProgressiveConfig,
    retry: RetryConfig,
    health_check: HealthCheckConfig,
}

impl DownloadConfigBuilder {
    /// 创建新的配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            chunk: ChunkConfig::default(),
            concurrency: ConcurrencyConfig::default(),
            network: NetworkConfig::default(),
            speed: SpeedConfig::default(),
            progressive: ProgressiveConfig::default(),
            retry: RetryConfig::default(),
            health_check: HealthCheckConfig::default(),
        }
    }
    
    // ==================== 闭包风格配置方法 ====================
    
    /// 配置分块设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .chunk(|c| c.min_size(2 * 1024 * 1024).max_size(50 * 1024 * 1024))
    ///     .build();
    /// ```
    pub fn chunk<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ChunkConfigBuilder) -> ChunkConfigBuilder,
    {
        let builder = ChunkConfigBuilder {
            min_size: self.chunk.min_size,
            initial_size: self.chunk.initial_size,
            max_size: self.chunk.max_size,
        };
        self.chunk = f(builder).build();
        self
    }
    
    /// 配置并发设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .concurrency(|c| c.worker_count(8))
    ///     .build();
    /// ```
    pub fn concurrency<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ConcurrencyConfigBuilder) -> ConcurrencyConfigBuilder,
    {
        let builder = ConcurrencyConfigBuilder {
            worker_count: self.concurrency.worker_count,
        };
        self.concurrency = f(builder).build();
        self
    }
    
    /// 配置网络设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// # use std::time::Duration;
    /// let config = DownloadConfig::builder()
    ///     .network(|n| n.timeout(Duration::from_secs(60)))
    ///     .build();
    /// ```
    pub fn network<F>(mut self, f: F) -> Self
    where
        F: FnOnce(NetworkConfigBuilder) -> NetworkConfigBuilder,
    {
        let builder = NetworkConfigBuilder {
            timeout: self.network.timeout,
            connect_timeout: self.network.connect_timeout,
        };
        self.network = f(builder).build();
        self
    }
    
    /// 配置速度计算设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .speed(|s| s.smoothing_factor(0.8).instant_weight(0.6))
    ///     .build();
    /// ```
    pub fn speed<F>(mut self, f: F) -> Self
    where
        F: FnOnce(SpeedConfigBuilder) -> SpeedConfigBuilder,
    {
        let builder = SpeedConfigBuilder {
            base_interval: self.speed.base_interval,
            instant_speed_window_multiplier: self.speed.instant_speed_window_multiplier,
            window_avg_multiplier: self.speed.window_avg_multiplier,
            expected_chunk_duration: self.speed.expected_chunk_duration,
            smoothing_factor: self.speed.smoothing_factor,
            instant_speed_weight: self.speed.instant_speed_weight,
            avg_speed_weight: self.speed.avg_speed_weight,
            sample_interval: self.speed.sample_interval,
            buffer_size_margin: self.speed.buffer_size_margin,
        };
        self.speed = f(builder).build();
        self
    }
    
    /// 配置渐进式启动设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .progressive(|p| p.worker_ratios(vec![0.5, 1.0]))
    ///     .build();
    /// ```
    pub fn progressive<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ProgressiveConfigBuilder) -> ProgressiveConfigBuilder,
    {
        let builder = ProgressiveConfigBuilder {
            worker_ratios: self.progressive.worker_ratios.clone(),
            min_speed_threshold: self.progressive.min_speed_threshold,
            min_time_before_finish: self.progressive.min_time_before_finish,
        };
        self.progressive = f(builder).build();
        self
    }
    
    /// 配置重试设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .retry(|r| r.max_retry_count(5))
    ///     .build();
    /// ```
    pub fn retry<F>(mut self, f: F) -> Self
    where
        F: FnOnce(RetryConfigBuilder) -> RetryConfigBuilder,
    {
        let builder = RetryConfigBuilder {
            max_retry_count: self.retry.max_retry_count,
            retry_delays: self.retry.retry_delays.clone(),
        };
        self.retry = f(builder).build();
        self
    }
    
    /// 配置健康检查设置
    ///
    /// # Example
    ///
    /// ```
    /// # use hydra_dl::DownloadConfig;
    /// let config = DownloadConfig::builder()
    ///     .health_check(|h| h.enabled(true).absolute_speed_threshold(100 * 1024))
    ///     .build();
    /// ```
    pub fn health_check<F>(mut self, f: F) -> Self
    where
        F: FnOnce(HealthCheckConfigBuilder) -> HealthCheckConfigBuilder,
    {
        let builder = HealthCheckConfigBuilder {
            enabled: self.health_check.enabled,
            absolute_speed_threshold: self.health_check.absolute_speed_threshold,
            min_workers_for_check: self.health_check.min_workers_for_check,
        };
        self.health_check = f(builder).build();
        self
    }
    
    // ==================== 构建方法 ====================
    
    /// 构建配置对象
    pub fn build(self) -> DownloadConfig {
        // 确保配置的合理性：min <= initial <= max
        let mut chunk = self.chunk;
        chunk.initial_size = chunk.initial_size.max(chunk.min_size);
        chunk.max_size = chunk.max_size.max(chunk.initial_size);
        
        DownloadConfig {
            chunk,
            concurrency: self.concurrency,
            network: self.network,
            speed: self.speed,
            progressive: self.progressive,
            retry: self.retry,
            health_check: self.health_check,
        }
    }
}

impl Default for DownloadConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 测试 ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DownloadConfig::default();
        assert_eq!(config.concurrency().worker_count(), ConcurrencyDefaults::WORKER_COUNT);
        assert_eq!(config.chunk().min_size(), ChunkDefaults::MIN_SIZE);
        assert_eq!(config.chunk().initial_size(), ChunkDefaults::INITIAL_SIZE);
        assert_eq!(config.chunk().max_size(), ChunkDefaults::MAX_SIZE);
    }

    #[test]
    fn test_builder_default() {
        let config = DownloadConfig::builder().build();
        assert_eq!(config.concurrency().worker_count(), ConcurrencyDefaults::WORKER_COUNT);
        assert_eq!(config.chunk().min_size(), ChunkDefaults::MIN_SIZE);
        assert_eq!(config.chunk().initial_size(), ChunkDefaults::INITIAL_SIZE);
        assert_eq!(config.chunk().max_size(), ChunkDefaults::MAX_SIZE);
    }

    #[test]
    fn test_builder_custom() {
        let config = DownloadConfig::builder()
            .concurrency(|c| c.worker_count(8))
            .chunk(|c| c
                .min_size(1 * 1024 * 1024)
                .initial_size(10 * 1024 * 1024)
                .max_size(100 * 1024 * 1024))
            .build();
        
        assert_eq!(config.concurrency().worker_count(), 8);
        assert_eq!(config.chunk().min_size(), 1 * 1024 * 1024);
        assert_eq!(config.chunk().initial_size(), 10 * 1024 * 1024);
        assert_eq!(config.chunk().max_size(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_builder_min_values() {
        let config = DownloadConfig::builder()
            .concurrency(|c| c.worker_count(0))  // 应该被限制为 1
            .chunk(|c| c
                .min_size(0)  // 应该被限制为 1
                .initial_size(0)  // 应该被限制为 1
                .max_size(0))  // 应该被限制为 1
            .build();
        
        assert_eq!(config.concurrency().worker_count(), 1);
        assert_eq!(config.chunk().min_size(), 1);
        assert_eq!(config.chunk().initial_size(), 1);
        assert_eq!(config.chunk().max_size(), 1);
    }

    #[test]
    fn test_builder_ensures_order() {
        // 测试 build() 确保 min <= initial <= max
        let config = DownloadConfig::builder()
            .chunk(|c| c
                .min_size(10 * 1024 * 1024)
                .initial_size(5 * 1024 * 1024)  // 小于 min
                .max_size(3 * 1024 * 1024))      // 小于 initial
            .build();
        
        assert_eq!(config.chunk().min_size(), 10 * 1024 * 1024);
        assert_eq!(config.chunk().initial_size(), 10 * 1024 * 1024);  // 调整为 min
        assert_eq!(config.chunk().max_size(), 10 * 1024 * 1024);      // 调整为 initial
    }

    #[test]
    fn test_progressive_worker_ratios_default() {
        let config = DownloadConfig::default();
        assert_eq!(config.progressive().worker_ratios(), ProgressiveDefaults::WORKER_RATIOS);
        assert_eq!(config.progressive().min_speed_threshold(), ProgressiveDefaults::MIN_SPEED_THRESHOLD);
    }

    #[test]
    fn test_progressive_worker_ratios_custom() {
        let config = DownloadConfig::builder()
            .progressive(|p| p.worker_ratios(vec![0.5, 1.0]))
            .build();
        
        assert_eq!(config.progressive().worker_ratios(), &[0.5, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_sorting() {
        // 测试自动排序
        let config = DownloadConfig::builder()
            .progressive(|p| p.worker_ratios(vec![1.0, 0.25, 0.75, 0.5]))
            .build();
        
        assert_eq!(config.progressive().worker_ratios(), &[0.25, 0.5, 0.75, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_filtering() {
        // 测试过滤无效值（<= 0 或 > 1.0）
        let config = DownloadConfig::builder()
            .progressive(|p| p.worker_ratios(vec![0.0, 0.5, 1.0, 1.5, -0.1]))
            .build();
        
        // 0.0, 1.5, -0.1 应该被过滤掉
        assert_eq!(config.progressive().worker_ratios(), &[0.5, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_dedup() {
        // 测试去重
        let config = DownloadConfig::builder()
            .progressive(|p| p.worker_ratios(vec![0.5, 0.5, 1.0, 1.0, 0.25]))
            .build();
        
        assert_eq!(config.progressive().worker_ratios(), &[0.25, 0.5, 1.0]);
    }

    #[test]
    fn test_progressive_worker_ratios_empty_uses_default() {
        // 测试空序列或全部无效值时使用默认值
        let config = DownloadConfig::builder()
            .progressive(|p| p.worker_ratios(vec![]))
            .build();
        
        assert_eq!(config.progressive().worker_ratios(), ProgressiveDefaults::WORKER_RATIOS);
        
        let config2 = DownloadConfig::builder()
            .progressive(|p| p.worker_ratios(vec![0.0, -1.0, 2.0]))
            .build();
        
        assert_eq!(config2.progressive().worker_ratios(), ProgressiveDefaults::WORKER_RATIOS);
    }

    #[test]
    fn test_min_speed_threshold() {
        let config = DownloadConfig::builder()
            .progressive(|p| p.min_speed_threshold(5 * 1024 * 1024))  // 5 MB/s
            .build();
        
        assert_eq!(config.progressive().min_speed_threshold(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_retry_config() {
        let config = DownloadConfig::builder()
            .retry(|r| r
                .max_retry_count(5)
                .retry_delays(vec![
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                    Duration::from_secs(5),
                ]))
            .build();

        assert_eq!(config.retry().max_retry_count(), 5);
        assert_eq!(config.retry().retry_delays().len(), 3);
        assert_eq!(config.retry().retry_delays()[0], Duration::from_secs(1));
        assert_eq!(config.retry().retry_delays()[1], Duration::from_secs(2));
        assert_eq!(config.retry().retry_delays()[2], Duration::from_secs(5));
    }

    #[test]
    fn test_retry_config_default() {
        let config = DownloadConfig::default();
        
        assert_eq!(config.retry().max_retry_count(), RetryDefaults::MAX_RETRY_COUNT);
        assert_eq!(config.retry().retry_delays().len(), 3);
        assert_eq!(config.retry().retry_delays()[0], Duration::from_secs(1));
        assert_eq!(config.retry().retry_delays()[1], Duration::from_secs(2));
        assert_eq!(config.retry().retry_delays()[2], Duration::from_secs(3));
    }

    #[test]
    fn test_retry_delays_empty_uses_default() {
        let config = DownloadConfig::builder()
            .retry(|r| r.retry_delays(vec![]))
            .build();

        assert_eq!(config.retry().retry_delays().len(), 3);
        assert_eq!(config.retry().retry_delays()[0], Duration::from_secs(1));
    }

    #[test]
    fn test_sub_configs() {
        // 测试多个配置组合
        let config = DownloadConfig::builder()
            .chunk(|c| c
                .min_size(1 * MB)
                .initial_size(5 * MB)
                .max_size(20 * MB))
            .concurrency(|c| c.worker_count(8))
            .build();
        
        assert_eq!(config.chunk.min_size, 1 * MB);
        assert_eq!(config.chunk.initial_size, 5 * MB);
        assert_eq!(config.chunk.max_size, 20 * MB);
        assert_eq!(config.concurrency.worker_count, 8);
    }
}
