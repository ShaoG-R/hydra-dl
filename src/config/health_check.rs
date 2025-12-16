//! 健康检查配置模块

use crate::constants::KB;
use std::num::NonZeroU64;

// ==================== 常量 ====================

/// 健康检查配置常量
pub struct Defaults;

impl Defaults {
    /// 绝对速度阈值：10 KB/s
    pub const ABSOLUTE_SPEED_THRESHOLD: NonZeroU64 = NonZeroU64::new(10 * KB).unwrap();
    /// 异常历史窗口大小
    pub const HISTORY_SIZE: usize = 10;
    /// 异常阈值比例（历史窗口中异常比例达到此值则触发）
    pub const ANOMALY_THRESHOLD_RATIO: f64 = 0.8;
    /// 超时时间倍数（基于 instant_speed_window 的倍数）
    pub const STALE_TIMEOUT_MULTIPLIER: u32 = 10;
}

// ==================== 配置结构体 ====================

/// 窗口检测配置
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowConfig {
    /// 异常历史窗口大小
    pub history_size: usize,
    /// 异常阈值（历史窗口中异常次数达到此值则触发取消）
    pub anomaly_threshold: usize,
}

impl Default for WindowConfig {
    fn default() -> Self {
        let history_size = Defaults::HISTORY_SIZE;
        let anomaly_threshold =
            (history_size as f64 * Defaults::ANOMALY_THRESHOLD_RATIO).ceil() as usize;
        Self {
            history_size,
            anomaly_threshold,
        }
    }
}

/// 绝对速度检测配置
#[derive(Debug, Clone)]
pub struct AbsoluteSpeedConfig {
    /// 绝对速度阈值（bytes/s）
    pub threshold: Option<NonZeroU64>,
    /// 窗口检测配置
    pub window: WindowConfig,
}

impl Default for AbsoluteSpeedConfig {
    fn default() -> Self {
        Self {
            threshold: Some(Defaults::ABSOLUTE_SPEED_THRESHOLD),
            window: WindowConfig::default(),
        }
    }
}

/// 相对速度检测配置
///
/// 用于检测当前 Worker 速度相对于其他 Workers 是否异常
#[derive(Debug, Clone)]
pub struct RelativeSpeedConfig {
    /// 窗口检测配置
    pub window: WindowConfig,
}

impl Default for RelativeSpeedConfig {
    fn default() -> Self {
        Self {
            window: WindowConfig::default(),
        }
    }
}

/// 健康检查配置
///
/// 控制异常下载线程的检测和处理策略
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// 绝对速度检测配置
    pub absolute: AbsoluteSpeedConfig,
    /// 相对速度检测配置
    pub relative: RelativeSpeedConfig,
    /// 超时时间倍数（基于 instant_speed_window 的倍数）
    pub stale_timeout_multiplier: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            absolute: AbsoluteSpeedConfig::default(),
            relative: RelativeSpeedConfig::default(),
            stale_timeout_multiplier: Defaults::STALE_TIMEOUT_MULTIPLIER,
        }
    }
}

impl HealthCheckConfig {
    #[inline]
    pub fn absolute(&self) -> &AbsoluteSpeedConfig {
        &self.absolute
    }

    #[inline]
    pub fn relative(&self) -> &RelativeSpeedConfig {
        &self.relative
    }

    #[inline]
    pub fn stale_timeout_multiplier(&self) -> u32 {
        self.stale_timeout_multiplier
    }
}

// ==================== 构建器 ====================

/// 健康检查配置构建器
#[derive(Debug, Clone, Default)]
pub struct HealthCheckConfigBuilder {
    absolute: Option<AbsoluteSpeedConfig>,
    relative: Option<RelativeSpeedConfig>,
    stale_timeout_multiplier: Option<u32>,
}

impl HealthCheckConfigBuilder {
    /// 创建新的健康检查配置构建器
    pub fn new() -> Self {
        Self::default()
    }

    /// 配置绝对速度检测
    pub fn absolute<F>(mut self, f: F) -> Self
    where
        F: FnOnce(AbsoluteSpeedConfigBuilder) -> AbsoluteSpeedConfigBuilder,
    {
        let builder = AbsoluteSpeedConfigBuilder::from(self.absolute.unwrap_or_default());
        self.absolute = Some(f(builder).build());
        self
    }

    /// 直接设置绝对速度检测配置
    pub fn absolute_config(mut self, config: AbsoluteSpeedConfig) -> Self {
        self.absolute = Some(config);
        self
    }

    /// 配置相对速度检测
    pub fn relative<F>(mut self, f: F) -> Self
    where
        F: FnOnce(RelativeSpeedConfigBuilder) -> RelativeSpeedConfigBuilder,
    {
        let builder = RelativeSpeedConfigBuilder::from(self.relative.unwrap_or_default());
        self.relative = Some(f(builder).build());
        self
    }

    /// 直接设置相对速度检测配置
    pub fn relative_config(mut self, config: RelativeSpeedConfig) -> Self {
        self.relative = Some(config);
        self
    }

    /// 设置超时时间倍数
    pub fn stale_timeout_multiplier(mut self, multiplier: u32) -> Self {
        self.stale_timeout_multiplier = Some(multiplier.max(1));
        self
    }

    /// 构建健康检查配置
    pub fn build(self) -> HealthCheckConfig {
        HealthCheckConfig {
            absolute: self.absolute.unwrap_or_default(),
            relative: self.relative.unwrap_or_default(),
            stale_timeout_multiplier: self
                .stale_timeout_multiplier
                .unwrap_or(Defaults::STALE_TIMEOUT_MULTIPLIER),
        }
    }
}

/// 绝对速度配置构建器
pub struct AbsoluteSpeedConfigBuilder {
    config: AbsoluteSpeedConfig,
}

impl From<AbsoluteSpeedConfig> for AbsoluteSpeedConfigBuilder {
    fn from(config: AbsoluteSpeedConfig) -> Self {
        Self { config }
    }
}

impl AbsoluteSpeedConfigBuilder {
    pub fn threshold(mut self, threshold: Option<NonZeroU64>) -> Self {
        self.config.threshold = threshold;
        self
    }

    pub fn window(mut self, size: usize) -> Self {
        self.config.window.history_size = size;
        self.config.window.anomaly_threshold =
            (size as f64 * Defaults::ANOMALY_THRESHOLD_RATIO).ceil() as usize;
        self
    }

    pub fn anomaly_threshold(mut self, threshold: usize) -> Self {
        self.config.window.anomaly_threshold = threshold;
        self
    }

    pub fn build(self) -> AbsoluteSpeedConfig {
        self.config
    }
}

/// 相对速度配置构建器
pub struct RelativeSpeedConfigBuilder {
    config: RelativeSpeedConfig,
}

impl From<RelativeSpeedConfig> for RelativeSpeedConfigBuilder {
    fn from(config: RelativeSpeedConfig) -> Self {
        Self { config }
    }
}

impl RelativeSpeedConfigBuilder {
    pub fn window(mut self, size: usize) -> Self {
        self.config.window.history_size = size;
        self.config.window.anomaly_threshold =
            (size as f64 * Defaults::ANOMALY_THRESHOLD_RATIO).ceil() as usize;
        self
    }

    pub fn anomaly_threshold(mut self, threshold: usize) -> Self {
        self.config.window.anomaly_threshold = threshold;
        self
    }

    pub fn build(self) -> RelativeSpeedConfig {
        self.config
    }
}
