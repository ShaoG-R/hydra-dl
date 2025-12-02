//! 健康检查配置模块

use crate::constants::KB;
use std::num::NonZeroU64;

// ==================== 常量 ====================

/// 健康检查配置常量
pub struct Defaults;

impl Defaults {
    /// 绝对速度阈值：100 KB/s
    pub const ABSOLUTE_SPEED_THRESHOLD: NonZeroU64 =
        unsafe { NonZeroU64::new_unchecked(100 * KB) };
    /// 是否启用健康检查
    pub const ENABLED: bool = true;
    /// 最小worker数量阈值（低于此数量不执行健康检查）
    pub const MIN_WORKERS_FOR_CHECK: u64 = 3;
}

// ==================== 配置结构体 ====================

/// 健康检查配置
///
/// 控制异常下载线程的检测和处理策略
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// 是否启用健康检查
    pub enabled: bool,
    /// 绝对速度阈值（bytes/s）
    pub absolute_speed_threshold: Option<NonZeroU64>,
    /// 最小worker数量阈值（低于此数量不执行健康检查）
    pub min_workers_for_check: u64,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: Defaults::ENABLED,
            absolute_speed_threshold: Some(Defaults::ABSOLUTE_SPEED_THRESHOLD),
            min_workers_for_check: Defaults::MIN_WORKERS_FOR_CHECK,
        }
    }
}

impl HealthCheckConfig {
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    #[inline]
    pub fn absolute_speed_threshold(&self) -> Option<NonZeroU64> {
        self.absolute_speed_threshold
    }

    #[inline]
    pub fn min_workers_for_check(&self) -> u64 {
        self.min_workers_for_check
    }
}

// ==================== 构建器 ====================

/// 健康检查配置构建器
#[derive(Debug, Clone)]
pub struct HealthCheckConfigBuilder {
    pub(crate) enabled: bool,
    pub(crate) absolute_speed_threshold: Option<NonZeroU64>,
    pub(crate) min_workers_for_check: u64,
}

impl HealthCheckConfigBuilder {
    /// 创建新的健康检查配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            enabled: Defaults::ENABLED,
            absolute_speed_threshold: Some(Defaults::ABSOLUTE_SPEED_THRESHOLD),
            min_workers_for_check: Defaults::MIN_WORKERS_FOR_CHECK,
        }
    }

    /// 设置是否启用健康检查
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// 设置绝对速度阈值（bytes/s）
    pub fn absolute_speed_threshold(mut self, threshold: Option<NonZeroU64>) -> Self {
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
