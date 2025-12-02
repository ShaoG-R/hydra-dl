//! 速度计算配置模块

use net_bytes::SizeStandard;
use std::{num::NonZeroU32, time::Duration};

// ==================== 常量 ====================

/// 速度计算配置常量
pub struct Defaults;

impl Defaults {
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

// ==================== 配置结构体 ====================

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
    /// 文件大小单位标准
    pub size_standard: SizeStandard,
}

impl Default for SpeedConfig {
    fn default() -> Self {
        Self {
            base_interval: Duration::from_millis(Defaults::BASE_INTERVAL_MILLIS),
            instant_speed_window_multiplier: NonZeroU32::new(Defaults::INSTANT_WINDOW_MULTIPLIER)
                .unwrap(),
            window_avg_multiplier: NonZeroU32::new(Defaults::WINDOW_AVG_MULTIPLIER).unwrap(),
            expected_chunk_duration: Duration::from_secs(Defaults::EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: Defaults::SMOOTHING_FACTOR,
            instant_speed_weight: Defaults::INSTANT_WEIGHT,
            avg_speed_weight: Defaults::AVG_WEIGHT,
            sample_interval: Duration::from_millis(Defaults::SAMPLE_INTERVAL_MILLIS),
            buffer_size_margin: Defaults::BUFFER_SIZE_MARGIN,
            size_standard: SizeStandard::IEC,
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

    #[inline]
    pub fn size_standard(&self) -> SizeStandard {
        self.size_standard
    }
}

// ==================== 构建器 ====================

/// 速度配置构建器
#[derive(Debug, Clone)]
pub struct SpeedConfigBuilder {
    pub(crate) base_interval: Duration,
    pub(crate) instant_speed_window_multiplier: NonZeroU32,
    pub(crate) window_avg_multiplier: NonZeroU32,
    pub(crate) expected_chunk_duration: Duration,
    pub(crate) smoothing_factor: f64,
    pub(crate) instant_speed_weight: f64,
    pub(crate) avg_speed_weight: f64,
    pub(crate) sample_interval: Duration,
    pub(crate) buffer_size_margin: f64,
    pub(crate) size_standard: SizeStandard,
}

impl SpeedConfigBuilder {
    /// 创建新的速度配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            base_interval: Duration::from_millis(Defaults::BASE_INTERVAL_MILLIS),
            instant_speed_window_multiplier: NonZeroU32::new(Defaults::INSTANT_WINDOW_MULTIPLIER)
                .unwrap(),
            window_avg_multiplier: NonZeroU32::new(Defaults::WINDOW_AVG_MULTIPLIER).unwrap(),
            expected_chunk_duration: Duration::from_secs(Defaults::EXPECTED_CHUNK_DURATION_SECS),
            smoothing_factor: Defaults::SMOOTHING_FACTOR,
            instant_speed_weight: Defaults::INSTANT_WEIGHT,
            avg_speed_weight: Defaults::AVG_WEIGHT,
            sample_interval: Duration::from_millis(Defaults::SAMPLE_INTERVAL_MILLIS),
            buffer_size_margin: Defaults::BUFFER_SIZE_MARGIN,
            size_standard: SizeStandard::SI,
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

    /// 缓冲区大小安全余量
    pub fn buffer_size_margin(mut self, margin: f64) -> Self {
        self.buffer_size_margin = margin.max(1.0); // 至少为 1.0，不能缩小缓冲区
        self
    }

    /// 设置文件大小单位标准
    pub fn size_standard(mut self, standard: SizeStandard) -> Self {
        self.size_standard = standard;
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
            size_standard: self.size_standard,
        }
    }
}

impl Default for SpeedConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
