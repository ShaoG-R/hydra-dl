//! 健康检查配置模块

use crate::constants::KB;
use std::num::NonZeroU64;

// ==================== 常量 ====================

/// 健康检查配置常量
pub struct Defaults;

impl Defaults {
    /// 绝对速度阈值：100 KB/s
    pub const ABSOLUTE_SPEED_THRESHOLD: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(100 * KB) };
    /// 异常历史窗口大小
    pub const HISTORY_SIZE: usize = 10;
    /// 异常阈值比例（历史窗口中异常比例达到此值则触发）
    pub const ANOMALY_THRESHOLD_RATIO: f64 = 0.7;
    /// 超时时间倍数（基于 instant_speed_window 的倍数）
    pub const STALE_TIMEOUT_MULTIPLIER: u32 = 10;
}

// ==================== 配置结构体 ====================

/// 健康检查配置
///
/// 控制异常下载线程的检测和处理策略
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// 绝对速度阈值（bytes/s）
    pub absolute_speed_threshold: Option<NonZeroU64>,
    /// 异常历史窗口大小
    pub history_size: usize,
    /// 异常阈值（历史窗口中异常次数达到此值则触发取消）
    pub anomaly_threshold: usize,
    /// 超时时间倍数（基于 instant_speed_window 的倍数）
    pub stale_timeout_multiplier: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        let history_size = Defaults::HISTORY_SIZE;
        let anomaly_threshold =
            (history_size as f64 * Defaults::ANOMALY_THRESHOLD_RATIO).ceil() as usize;
        Self {
            absolute_speed_threshold: Some(Defaults::ABSOLUTE_SPEED_THRESHOLD),
            history_size,
            anomaly_threshold,
            stale_timeout_multiplier: Defaults::STALE_TIMEOUT_MULTIPLIER,
        }
    }
}

impl HealthCheckConfig {
    #[inline]
    pub fn absolute_speed_threshold(&self) -> Option<NonZeroU64> {
        self.absolute_speed_threshold
    }

    #[inline]
    pub fn history_size(&self) -> usize {
        self.history_size
    }

    #[inline]
    pub fn anomaly_threshold(&self) -> usize {
        self.anomaly_threshold
    }

    #[inline]
    pub fn stale_timeout_multiplier(&self) -> u32 {
        self.stale_timeout_multiplier
    }
}

// ==================== 构建器 ====================

/// 健康检查配置构建器
#[derive(Debug, Clone)]
pub struct HealthCheckConfigBuilder {
    pub(crate) absolute_speed_threshold: Option<NonZeroU64>,
    pub(crate) history_size: usize,
    pub(crate) anomaly_threshold: usize,
    pub(crate) stale_timeout_multiplier: u32,
}

impl HealthCheckConfigBuilder {
    /// 创建新的健康检查配置构建器（使用默认值）
    pub fn new() -> Self {
        let history_size = Defaults::HISTORY_SIZE;
        let anomaly_threshold =
            (history_size as f64 * Defaults::ANOMALY_THRESHOLD_RATIO).ceil() as usize;
        Self {
            absolute_speed_threshold: Some(Defaults::ABSOLUTE_SPEED_THRESHOLD),
            history_size,
            anomaly_threshold,
            stale_timeout_multiplier: Defaults::STALE_TIMEOUT_MULTIPLIER,
        }
    }

    /// 设置绝对速度阈值（bytes/s）
    pub fn absolute_speed_threshold(mut self, threshold: Option<NonZeroU64>) -> Self {
        self.absolute_speed_threshold = threshold;
        self
    }

    /// 设置异常历史窗口大小
    pub fn history_size(mut self, size: usize) -> Self {
        self.history_size = size.max(1);
        self
    }

    /// 设置异常阈值
    pub fn anomaly_threshold(mut self, threshold: usize) -> Self {
        self.anomaly_threshold = threshold;
        self
    }

    /// 设置超时时间倍数
    pub fn stale_timeout_multiplier(mut self, multiplier: u32) -> Self {
        self.stale_timeout_multiplier = multiplier.max(1);
        self
    }

    /// 构建健康检查配置
    pub fn build(self) -> HealthCheckConfig {
        HealthCheckConfig {
            absolute_speed_threshold: self.absolute_speed_threshold,
            history_size: self.history_size,
            anomaly_threshold: self.anomaly_threshold,
            stale_timeout_multiplier: self.stale_timeout_multiplier,
        }
    }
}

impl Default for HealthCheckConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
