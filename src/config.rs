//! 下载配置模块
//!
//! 提供下载任务的配置选项，按功能域分类组织


// ==================== 子模块 ====================

pub mod chunk;
pub mod concurrency;
pub mod health_check;
pub mod network;
pub mod progressive;
pub mod retry;
pub mod speed;

// ==================== 重导出 ====================

pub use chunk::{ChunkConfig, ChunkConfigBuilder, Defaults as ChunkDefaults};
pub use concurrency::{ConcurrencyConfig, ConcurrencyConfigBuilder, Defaults as ConcurrencyDefaults};
pub use health_check::{Defaults as HealthCheckDefaults, HealthCheckConfig, HealthCheckConfigBuilder};
pub use network::{Defaults as NetworkDefaults, NetworkConfig, NetworkConfigBuilder};
pub use progressive::{Defaults as ProgressiveDefaults, ProgressiveConfig, ProgressiveConfigBuilder};
pub use retry::{Defaults as RetryDefaults, RetryConfig, RetryConfigBuilder};
pub use speed::{Defaults as SpeedDefaults, SpeedConfig, SpeedConfigBuilder};

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
            size_standard: self.speed.size_standard,
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
    /// # use std::time::Duration;
    /// let config = DownloadConfig::builder()
    ///     .retry(|r| r
    ///         .retry_task_counts(vec![1, 2, 4, 8])  // 第n次重试等待的任务数
    ///         .retry_delay(Duration::from_millis(500)))  // 无新任务时的等待延迟
    ///     .build();
    /// ```
    pub fn retry<F>(mut self, f: F) -> Self
    where
        F: FnOnce(RetryConfigBuilder) -> RetryConfigBuilder,
    {
        let builder = RetryConfigBuilder {
            retry_task_counts: self.retry.retry_task_counts.clone(),
            retry_delay: self.retry.retry_delay,
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
    /// # use std::num::NonZeroU64;
    /// let config = DownloadConfig::builder()
    ///     .health_check(|h| h.enabled(true).absolute_speed_threshold(NonZeroU64::new(100 * 1024)))
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
    use crate::constants::MB;

    #[test]
    fn test_download_config_default() {
        let config = DownloadConfig::default();
        assert_eq!(
            config.concurrency().worker_count(),
            ConcurrencyDefaults::WORKER_COUNT
        );
        assert_eq!(config.chunk().min_size(), ChunkDefaults::MIN_SIZE);
        assert_eq!(config.chunk().initial_size(), ChunkDefaults::INITIAL_SIZE);
        assert_eq!(config.chunk().max_size(), ChunkDefaults::MAX_SIZE);
    }

    #[test]
    fn test_download_config_builder() {
        let config = DownloadConfig::builder()
            .concurrency(|c| c.worker_count(8))
            .chunk(|c| {
                c.min_size(1 * MB)
                    .initial_size(10 * MB)
                    .max_size(100 * MB)
            })
            .build();

        assert_eq!(config.concurrency().worker_count(), 8);
        assert_eq!(config.chunk().min_size(), 1 * MB);
        assert_eq!(config.chunk().initial_size(), 10 * MB);
        assert_eq!(config.chunk().max_size(), 100 * MB);
    }

    #[test]
    fn test_builder_ensures_chunk_order() {
        // 测试 build() 确保 min <= initial <= max
        let config = DownloadConfig::builder()
            .chunk(|c| {
                c.min_size(10 * MB)
                    .initial_size(5 * MB) // 小于 min
                    .max_size(3 * MB) // 小于 initial
            })
            .build();

        assert_eq!(config.chunk().min_size(), 10 * MB);
        assert_eq!(config.chunk().initial_size(), 10 * MB); // 调整为 min
        assert_eq!(config.chunk().max_size(), 10 * MB); // 调整为 initial
    }

    #[test]
    fn test_combined_configs() {
        // 测试多个配置组合
        let config = DownloadConfig::builder()
            .chunk(|c| c.min_size(1 * MB).initial_size(5 * MB).max_size(20 * MB))
            .concurrency(|c| c.worker_count(8))
            .progressive(|p| p.worker_ratios(vec![0.5, 1.0]))
            .build();

        assert_eq!(config.chunk.min_size, 1 * MB);
        assert_eq!(config.chunk.initial_size, 5 * MB);
        assert_eq!(config.chunk.max_size, 20 * MB);
        assert_eq!(config.concurrency.worker_count, 8);
        assert_eq!(config.progressive().worker_ratios(), &[0.5, 1.0]);
    }
}
