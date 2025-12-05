//! 渐进式启动配置模块

use crate::constants::MB;
use std::{num::NonZeroU64, time::Duration};

// ==================== 常量 ====================

/// 渐进式启动配置常量
pub struct Defaults;

impl Defaults {
    /// 默认 worker 数量：4
    pub const WORKER_COUNT: u64 = 4;
    /// 渐进式启动比例序列：[0.25, 0.5, 0.75, 1.0]
    pub const WORKER_RATIOS: &'static [f64] = &[0.25, 0.5, 0.75, 1.0];
    /// 最小速度阈值：1 MB/s
    pub const MIN_SPEED_THRESHOLD: Option<NonZeroU64> = Some(NonZeroU64::new(MB).unwrap());
    /// 预期下载结束前最小时间（秒）：20 秒
    pub const MIN_TIME_BEFORE_FINISH_SECS: u64 = 20;
}

// ==================== 配置结构体 ====================

/// 渐进式启动配置
///
/// 控制 Worker 的渐进式启动策略
#[derive(Debug, Clone)]
pub struct ProgressiveConfig {
    pub worker_count: u64,
    /// 渐进式启动比例序列
    pub worker_ratios: Vec<f64>,
    /// 最小速度阈值（bytes/s）
    pub min_speed_threshold: Option<NonZeroU64>,
    /// 预期下载结束前最小时间（在此时间内不启动新 worker）
    pub min_time_before_finish: Duration,
}

impl Default for ProgressiveConfig {
    fn default() -> Self {
        Self {
            worker_count: Defaults::WORKER_COUNT,
            worker_ratios: Defaults::WORKER_RATIOS.to_vec(),
            min_speed_threshold: Defaults::MIN_SPEED_THRESHOLD,
            min_time_before_finish: Duration::from_secs(Defaults::MIN_TIME_BEFORE_FINISH_SECS),
        }
    }
}

impl ProgressiveConfig {
    #[inline]
    pub fn worker_count(&self) -> u64 {
        self.worker_count
    }

    #[inline]
    pub fn worker_ratios(&self) -> &[f64] {
        &self.worker_ratios
    }

    #[inline]
    pub fn min_speed_threshold(&self) -> Option<NonZeroU64> {
        self.min_speed_threshold
    }

    #[inline]
    pub fn min_time_before_finish(&self) -> Duration {
        self.min_time_before_finish
    }
}

// ==================== 构建器 ====================

/// 渐进式配置构建器
#[derive(Debug, Clone)]
pub struct ProgressiveConfigBuilder {
    pub(crate) worker_count: u64,
    pub(crate) worker_ratios: Vec<f64>,
    pub(crate) min_speed_threshold: Option<NonZeroU64>,
    pub(crate) min_time_before_finish: Duration,
}

impl ProgressiveConfigBuilder {
    /// 创建新的渐进式配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            worker_count: Defaults::WORKER_COUNT,
            worker_ratios: Defaults::WORKER_RATIOS.to_vec(),
            min_speed_threshold: Defaults::MIN_SPEED_THRESHOLD,
            min_time_before_finish: Duration::from_secs(Defaults::MIN_TIME_BEFORE_FINISH_SECS),
        }
    }

    /// 设置渐进式启动比例序列
    pub fn worker_count(mut self, count: u64) -> Self {
        self.worker_count = count;
        self
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
            self.worker_ratios = Defaults::WORKER_RATIOS.to_vec();
        } else {
            self.worker_ratios = valid_ratios;
        }

        self
    }

    /// 设置最小速度阈值（bytes/s）
    pub fn min_speed_threshold(mut self, threshold: Option<NonZeroU64>) -> Self {
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
            worker_count: self.worker_count,
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

// ==================== 测试 ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ProgressiveConfig::default();
        assert_eq!(config.worker_count(), Defaults::WORKER_COUNT);
        assert_eq!(config.worker_ratios(), Defaults::WORKER_RATIOS);
        assert_eq!(config.min_speed_threshold(), Defaults::MIN_SPEED_THRESHOLD);
    }

    #[test]
    fn test_builder_default() {
        let config = ProgressiveConfigBuilder::new().build();
        assert_eq!(config.worker_count(), Defaults::WORKER_COUNT);
        assert_eq!(config.worker_ratios(), Defaults::WORKER_RATIOS);
        assert_eq!(config.min_speed_threshold(), Defaults::MIN_SPEED_THRESHOLD);
    }

    #[test]
    fn test_worker_ratios_custom() {
        let config = ProgressiveConfigBuilder::new()
            .worker_count(2)
            .worker_ratios(vec![0.5, 1.0])
            .build();
        assert_eq!(config.worker_ratios(), &[0.5, 1.0]);
    }

    #[test]
    fn test_worker_ratios_sorting() {
        // 测试自动排序
        let config = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .worker_ratios(vec![1.0, 0.25, 0.75, 0.5])
            .build();
        assert_eq!(config.worker_ratios(), &[0.25, 0.5, 0.75, 1.0]);
    }

    #[test]
    fn test_worker_ratios_filtering() {
        // 测试过滤无效值（<= 0 或 > 1.0）
        let config = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .worker_ratios(vec![0.0, 0.5, 1.0, 1.5, -0.1])
            .build();
        // 0.0, 1.5, -0.1 应该被过滤掉
        assert_eq!(config.worker_ratios(), &[0.5, 1.0]);
    }

    #[test]
    fn test_worker_ratios_dedup() {
        // 测试去重
        let config = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .worker_ratios(vec![0.5, 0.5, 1.0, 1.0, 0.25])
            .build();
        assert_eq!(config.worker_ratios(), &[0.25, 0.5, 1.0]);
    }

    #[test]
    fn test_worker_ratios_empty_uses_default() {
        // 测试空序列使用默认值
        let config = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .worker_ratios(vec![])
            .build();
        assert_eq!(config.worker_ratios(), Defaults::WORKER_RATIOS);

        // 测试全部无效值时使用默认值
        let config2 = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .worker_ratios(vec![0.0, -1.0, 2.0])
            .build();
        assert_eq!(config2.worker_ratios(), Defaults::WORKER_RATIOS);
    }

    #[test]
    fn test_min_speed_threshold() {
        let config = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .min_speed_threshold(Some(NonZeroU64::new(5 * 1024 * 1024).unwrap()))
            .build();
        assert_eq!(
            config.min_speed_threshold(),
            Some(NonZeroU64::new(5 * 1024 * 1024).unwrap())
        );
    }

    #[test]
    fn test_min_time_before_finish() {
        let config = ProgressiveConfigBuilder::new()
            .worker_count(4)
            .min_time_before_finish(Duration::from_secs(30))
            .build();
        assert_eq!(config.min_time_before_finish(), Duration::from_secs(30));
    }
}
