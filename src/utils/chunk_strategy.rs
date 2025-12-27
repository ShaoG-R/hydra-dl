//! 动态分块策略模块
//!
//! 提供根据下载速度动态调整分块大小的策略接口和实现

use crate::config::DownloadConfig;
use net_bytes::DownloadSpeed;

/// 分块策略 trait
///
/// 定义了动态调整下载分块大小的接口
pub(crate) trait ChunkStrategy: Send + Sync + 'static {
    /// 根据实时下载速度和平均速度更新分块大小（带平滑）
    ///
    /// # Arguments
    ///
    /// * `instant_speed` - 实时下载速度 (bytes/s)
    /// * `avg_speed` - 平均下载速度 (bytes/s)
    fn update_chunk_size(&mut self, instant_speed: DownloadSpeed, avg_speed: DownloadSpeed);

    /// 获取当前分块大小
    fn current_size(&self) -> u64;
}

/// 基于速度的自适应分块策略
///
/// 根据实时下载速度和平均速度动态调整分块大小，使用平滑算法：
/// 1. 理想分块大小 = (瞬时速度 * 瞬时速度权重 + 平均速度 * 平均速度权重) * 预期时间
/// 2. 实际分配分块大小 = 上次分块大小 + (理想分块大小 - 上次分块大小) * 平滑系数
/// 3. 实际分配分块大小限制在 min_chunk_size ~ max_chunk_size 区间内
#[derive(Debug)]
pub(crate) struct SpeedBasedChunkStrategy {
    /// 最小分块大小 (bytes)
    min_chunk_size: u64,
    /// 最大分块大小 (bytes)
    max_chunk_size: u64,
    /// 预期分块下载时长 (秒)
    expected_duration: f64,
    /// 平滑系数 (0.0 ~ 1.0)
    smoothing_factor: f64,
    /// 瞬时速度权重 (0.0 ~ 1.0)
    instant_speed_weight: f64,
    /// 平均速度权重 (0.0 ~ 1.0)
    avg_speed_weight: f64,
    /// 当前分块大小 (bytes)
    current_chunk_size: u64,
}

impl SpeedBasedChunkStrategy {
    /// 创建新的速度自适应分块策略
    ///
    /// # Arguments
    ///
    /// * `min_chunk_size` - 最小分块大小 (bytes)
    /// * `max_chunk_size` - 最大分块大小 (bytes)
    /// * `expected_duration_secs` - 预期分块下载时长 (秒)
    /// * `smoothing_factor` - 平滑系数 (0.0 ~ 1.0)
    /// * `instant_speed_weight` - 瞬时速度权重 (0.0 ~ 1.0)
    /// * `avg_speed_weight` - 平均速度权重 (0.0 ~ 1.0)
    /// * `initial_chunk_size` - 初始分块大小 (bytes)
    pub(crate) fn new(
        min_chunk_size: u64,
        max_chunk_size: u64,
        expected_duration_secs: f64,
        smoothing_factor: f64,
        instant_speed_weight: f64,
        avg_speed_weight: f64,
        initial_chunk_size: u64,
    ) -> Self {
        Self {
            min_chunk_size,
            max_chunk_size,
            expected_duration: expected_duration_secs,
            smoothing_factor,
            instant_speed_weight,
            avg_speed_weight,
            current_chunk_size: initial_chunk_size,
        }
    }

    /// 从配置创建速度自适应分块策略
    ///
    /// # Arguments
    ///
    /// * `config` - 下载配置
    pub(crate) fn from_config(config: &DownloadConfig) -> Self {
        Self::new(
            config.chunk().min_size(),
            config.chunk().max_size(),
            config.speed().expected_chunk_duration().as_secs_f64(),
            config.speed().smoothing_factor(),
            config.speed().instant_speed_weight(),
            config.speed().avg_speed_weight(),
            config.chunk().initial_size(),
        )
    }
}

impl ChunkStrategy for SpeedBasedChunkStrategy {
    fn update_chunk_size(&mut self, instant_speed: DownloadSpeed, avg_speed: DownloadSpeed) {
        // 获取速度的字节/秒值
        let instant_speed_bps = instant_speed.as_u64() as f64;
        let avg_speed_bps = avg_speed.as_u64() as f64;

        // 计算加权速度
        let weighted_speed = (instant_speed_bps * self.instant_speed_weight)
            + (avg_speed_bps * self.avg_speed_weight);

        // 计算理想分块大小 (bytes/s * s = bytes)
        let ideal_chunk_size = weighted_speed * self.expected_duration;

        // 应用平滑算法: 新大小 = 当前大小 + (理想大小 - 当前大小) * 平滑系数
        let current_as_f64 = self.current_chunk_size as f64;
        let smoothed_size =
            current_as_f64 + (ideal_chunk_size - current_as_f64) * self.smoothing_factor;
        let new_chunk_size = smoothed_size.max(0.0) as u64;

        // 确保分块大小在允许的范围内并更新状态
        self.current_chunk_size = new_chunk_size.clamp(self.min_chunk_size, self.max_chunk_size);
    }

    fn current_size(&self) -> u64 {
        self.current_chunk_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use net_bytes::DownloadSpeed;
    // Helper function to create a DownloadSpeed from bytes per second
    fn speed(bps: u64) -> DownloadSpeed {
        DownloadSpeed::from_raw(bps)
    }

    #[test]
    fn test_speed_based_chunk_strategy_creation() {
        let strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds expected duration
            0.3,              // 0.3 smoothing factor
            0.7,              // 0.7 instant speed weight
            0.3,              // 0.3 avg speed weight
            10 * 1024 * 1024, // 10 MB initial
        );

        assert_eq!(strategy.min_chunk_size, 2 * 1024 * 1024);
        assert_eq!(strategy.max_chunk_size, 50 * 1024 * 1024);
        assert_eq!(strategy.current_chunk_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_speed_based_chunk_strategy_from_config() {
        use std::time::Duration;

        let config = DownloadConfig::builder()
            .chunk(|c| {
                c.initial_size(10 * 1024 * 1024)
                    .min_size(5 * 1024 * 1024)
                    .max_size(100 * 1024 * 1024)
            })
            .speed(|s| {
                s.expected_chunk_duration(Duration::from_secs(3))
                    .smoothing_factor(0.5)
                    .instant_weight(0.6)
                    .avg_weight(0.4)
            })
            .build();

        let strategy = SpeedBasedChunkStrategy::from_config(&config);

        assert_eq!(strategy.min_chunk_size, 5 * 1024 * 1024);
        assert_eq!(strategy.max_chunk_size, 100 * 1024 * 1024);
        assert_eq!(strategy.expected_duration, 3.0);
        assert_eq!(strategy.smoothing_factor, 0.5);
        assert_eq!(strategy.instant_speed_weight, 0.6);
        assert_eq!(strategy.avg_speed_weight, 0.4);
        assert_eq!(strategy.current_chunk_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_smoothing() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds expected duration
            0.5,              // 0.5 smoothing factor
            1.0,              // 1.0 instant speed weight (只用瞬时速度)
            0.0,              // 0.0 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 瞬时速度: 10 MB/s, 平均速度: 8 MB/s (但权重为0)
        // 加权速度: 10 * 1.0 + 8 * 0.0 = 10 MB/s
        // 理想分块: 10 * 2 = 20 MB
        // 当前: 5 MB, 新大小: 5 + (20 - 5) * 0.5 = 12.5 MB
        strategy.update_chunk_size(speed(10 * 1024 * 1024), speed(8 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 12 * 1024 * 1024 + 512 * 1024); // 12.5 MB

        // 再次计算，应该继续平滑调整
        // 当前: 12.5 MB, 新大小: 12.5 + (20 - 12.5) * 0.5 = 16.25 MB
        strategy.update_chunk_size(speed(10 * 1024 * 1024), speed(8 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 16 * 1024 * 1024 + 256 * 1024); // 16.25 MB
    }

    #[test]
    fn test_calculate_chunk_size_respects_min() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 非常慢的速度: 100 KB/s, 理想分块: 100 * 1024 * 2 = 200 KB
        // 应该立即调整到理想值并被限制在最小值 2 MB
        strategy.update_chunk_size(speed(100 * 1024), speed(100 * 1024));
        assert_eq!(strategy.current_size(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_respects_max() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 非常快的速度: 100 MB/s, 理想分块: 100 * 2 = 200 MB
        // 应该被限制在最大值 50 MB
        strategy.update_chunk_size(speed(100 * 1024 * 1024), speed(100 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 50 * 1024 * 1024);
    }

    #[test]
    fn test_smoothing_factor_zero() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            0.0,              // 0.0 smoothing (完全不响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 即使速度很快，分块大小也不应该变化
        strategy.update_chunk_size(speed(100 * 1024 * 1024), speed(100 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 5 * 1024 * 1024); // 保持当前值
    }

    #[test]
    fn test_smoothing_factor_one() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 速度: 10 MB/s, 理想分块: 10 * 2 = 20 MB
        // 应该立即调整到理想值
        strategy.update_chunk_size(speed(10 * 1024 * 1024), speed(10 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 20 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_size_smoothing_progression() {
        // 测试平滑算法的渐进性：策略是stateful的
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            0.5,              // 0.5 smoothing
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 第一次计算：从 5 MB 开始
        strategy.update_chunk_size(speed(10 * 1024 * 1024), speed(8 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 12 * 1024 * 1024 + 512 * 1024); // 12.5 MB

        // 第二次计算：使用上次的结果作为当前大小
        strategy.update_chunk_size(speed(10 * 1024 * 1024), speed(8 * 1024 * 1024));
        assert_eq!(strategy.current_size(), 16 * 1024 * 1024 + 256 * 1024); // 16.25 MB
    }

    #[test]
    fn test_weighted_speed_calculation() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            0.7,              // 0.7 instant speed weight
            0.3,              // 0.3 avg speed weight
            5 * 1024 * 1024,  // 5 MB initial
        );

        // 瞬时速度: 10 MB/s, 平均速度: 6 MB/s
        // 加权速度: 10 * 0.7 + 6 * 0.3 = 7 + 1.8 = 8.8 MB/s
        // 理想分块: 8.8 * 2 = 17.6 MB

        // 由于 smoothing_factor = 1.0，应该立即调整到理想值
        strategy.update_chunk_size(speed(10 * 1024 * 1024), speed(6 * 1024 * 1024));
        let expected = (8.8 * 2.0 * 1024.0 * 1024.0) as u64;
        assert_eq!(strategy.current_size(), expected);
    }
}
