//! 渐进式启动配置模块

use crate::constants::MB;
use std::num::NonZeroU64;

// ==================== 常量 ====================

/// 渐进式启动配置常量
pub struct Defaults;

impl Defaults {
    /// 默认最大并发下载线程数：4
    pub const MAX_CONCURRENT_DOWNLOADS: u64 = 4;
    /// 默认初始下载线程数：1
    pub const INITIAL_WORKER_COUNT: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(1) };
    /// 默认预期最低单下载线程速度：1 MB/s
    pub const MIN_SPEED_PER_THREAD: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(MB) };
}

// ==================== 配置结构体 ====================

/// 渐进式启动配置
///
/// 控制 Worker 的渐进式启动策略
#[derive(Debug, Clone)]
pub struct ProgressiveConfig {
    /// 最大并发下载线程数
    pub max_concurrent_downloads: u64,
    /// 初始下载线程数
    pub initial_worker_count: NonZeroU64,
    /// 预期最低单下载线程速度
    pub min_speed_per_thread: NonZeroU64,
}

impl Default for ProgressiveConfig {
    fn default() -> Self {
        Self {
            max_concurrent_downloads: Defaults::MAX_CONCURRENT_DOWNLOADS,
            initial_worker_count: Defaults::INITIAL_WORKER_COUNT,
            min_speed_per_thread: Defaults::MIN_SPEED_PER_THREAD,
        }
    }
}

impl ProgressiveConfig {
    #[inline]
    pub fn max_concurrent_downloads(&self) -> u64 {
        self.max_concurrent_downloads
    }

    #[inline]
    pub fn initial_worker_count(&self) -> NonZeroU64 {
        self.initial_worker_count
    }

    #[inline]
    pub fn min_speed_per_thread(&self) -> NonZeroU64 {
        self.min_speed_per_thread
    }
}

// ==================== 构建器 ====================

/// 渐进式配置构建器
#[derive(Debug, Clone)]
pub struct ProgressiveConfigBuilder {
    pub(crate) max_concurrent_downloads: u64,
    pub(crate) initial_worker_count: NonZeroU64,
    pub(crate) min_speed_per_thread: NonZeroU64,
}

impl ProgressiveConfigBuilder {
    /// 创建新的渐进式配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            max_concurrent_downloads: Defaults::MAX_CONCURRENT_DOWNLOADS,
            initial_worker_count: Defaults::INITIAL_WORKER_COUNT,
            min_speed_per_thread: Defaults::MIN_SPEED_PER_THREAD,
        }
    }

    /// 设置最大并发下载线程数
    pub fn max_concurrent_downloads(mut self, count: u64) -> Self {
        self.max_concurrent_downloads = count;
        self
    }

    /// 设置初始下载线程数
    pub fn initial_worker_count(mut self, count: NonZeroU64) -> Self {
        self.initial_worker_count = count;
        self
    }

    /// 设置预期最低单下载线程速度
    pub fn min_speed_per_thread(mut self, speed: NonZeroU64) -> Self {
        self.min_speed_per_thread = speed;
        self
    }

    /// 构建渐进式配置
    pub fn build(self) -> ProgressiveConfig {
        ProgressiveConfig {
            max_concurrent_downloads: self.max_concurrent_downloads,
            initial_worker_count: self.initial_worker_count,
            min_speed_per_thread: self.min_speed_per_thread,
        }
    }
}

impl Default for ProgressiveConfigBuilder {
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
        let config = ProgressiveConfig::default();
        assert_eq!(
            config.max_concurrent_downloads(),
            Defaults::MAX_CONCURRENT_DOWNLOADS
        );
        assert_eq!(
            config.initial_worker_count(),
            Defaults::INITIAL_WORKER_COUNT
        );
        assert_eq!(
            config.min_speed_per_thread(),
            Defaults::MIN_SPEED_PER_THREAD
        );
    }

    #[test]
    fn test_builder_custom() {
        let speed = NonZeroU64::new(512 * 1024).unwrap();
        let config = ProgressiveConfigBuilder::new()
            .max_concurrent_downloads(8)
            .initial_worker_count(unsafe { NonZeroU64::new_unchecked(2) })
            .min_speed_per_thread(speed)
            .build();
        assert_eq!(config.max_concurrent_downloads(), 8);
        assert_eq!(config.initial_worker_count(), unsafe {
            NonZeroU64::new_unchecked(2)
        });
        assert_eq!(config.min_speed_per_thread(), speed);
    }
}
