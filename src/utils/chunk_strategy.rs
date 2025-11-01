//! 动态分块策略模块
//!
//! 提供根据下载速度动态调整分块大小的策略接口和实现

use crate::config::DownloadConfig;
use std::sync::atomic::{AtomicU64, Ordering};

/// 分块策略 trait
///
/// 定义了动态调整下载分块大小的接口
pub(crate) trait ChunkStrategy: Send + Sync + 'static {
    /// 根据实时下载速度和平均速度计算合适的分块大小（带平滑）
    ///
    /// # Arguments
    ///
    /// * `instant_speed` - 实时下载速度 (bytes/s)
    /// * `avg_speed` - 平均下载速度 (bytes/s)
    ///
    /// # Returns
    ///
    /// 计算得到的分块大小 (bytes)
    fn calculate_chunk_size(&self, instant_speed: f64, avg_speed: f64) -> u64;

    /// 获取当前分块大小
    ///
    /// # Returns
    ///
    /// 当前使用的分块大小 (bytes)
    fn current_chunk_size(&self) -> u64;
}

/// 基于速度的自适应分块策略
///
/// 根据实时下载速度和平均速度动态调整分块大小，使用平滑算法：
/// 1. 理想分块大小 = (瞬时速度 * 瞬时速度权重 + 平均速度 * 平均速度权重) * 预期时间
/// 2. 实际分配分块大小 = 上次分块大小 + (理想分块大小 - 上次分块大小) * 平滑系数
/// 3. 实际分配分块大小限制在 min_chunk_size ~ max_chunk_size 区间内
#[derive(Debug)]
pub(crate) struct SpeedBasedChunkStrategy {
    /// 当前分块大小 (bytes)，使用原子操作支持无锁并发访问
    current_chunk_size: AtomicU64,
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
}

impl SpeedBasedChunkStrategy {
    /// 创建新的速度自适应分块策略
    ///
    /// # Arguments
    ///
    /// * `initial_chunk_size` - 初始分块大小 (bytes)
    /// * `min_chunk_size` - 最小分块大小 (bytes)
    /// * `max_chunk_size` - 最大分块大小 (bytes)
    /// * `expected_duration_secs` - 预期分块下载时长 (秒)
    /// * `smoothing_factor` - 平滑系数 (0.0 ~ 1.0)
    /// * `instant_speed_weight` - 瞬时速度权重 (0.0 ~ 1.0)
    /// * `avg_speed_weight` - 平均速度权重 (0.0 ~ 1.0)
    pub(crate) fn new(
        initial_chunk_size: u64,
        min_chunk_size: u64,
        max_chunk_size: u64,
        expected_duration_secs: f64,
        smoothing_factor: f64,
        instant_speed_weight: f64,
        avg_speed_weight: f64,
    ) -> Self {
        Self {
            current_chunk_size: AtomicU64::new(initial_chunk_size),
            min_chunk_size,
            max_chunk_size,
            expected_duration: expected_duration_secs,
            smoothing_factor,
            instant_speed_weight,
            avg_speed_weight,
        }
    }

    /// 从配置创建速度自适应分块策略
    ///
    /// # Arguments
    ///
    /// * `config` - 下载配置
    pub(crate) fn from_config(config: &DownloadConfig) -> Self {
        Self::new(
            config.chunk().initial_size(),
            config.chunk().min_size(),
            config.chunk().max_size(),
            config.speed().expected_chunk_duration().as_secs_f64(),
            config.speed().smoothing_factor(),
            config.speed().instant_speed_weight(),
            config.speed().avg_speed_weight(),
        )
    }
}

impl ChunkStrategy for SpeedBasedChunkStrategy {
    fn calculate_chunk_size(&self, instant_speed: f64, avg_speed: f64) -> u64 {
        // 1. 读取当前分块大小（使用 Relaxed ordering，因为这只是性能估算）
        let current_size = self.current_chunk_size.load(Ordering::Relaxed);
        
        // 2. 计算加权速度 = (瞬时速度 * 瞬时速度权重 + 平均速度 * 平均速度权重)
        let weighted_speed = instant_speed * self.instant_speed_weight + avg_speed * self.avg_speed_weight;
        
        // 3. 计算理想分块大小 = 加权速度 * 预期时间
        let ideal_chunk_size = (weighted_speed * self.expected_duration) as u64;
        
        // 4. 应用平滑算法：新大小 = 旧大小 + (理想大小 - 旧大小) * 平滑系数
        let smoothed_size = current_size as f64 
            + (ideal_chunk_size as f64 - current_size as f64) * self.smoothing_factor;
        
        // 5. 限制在配置范围内
        let new_size = (smoothed_size as u64).clamp(self.min_chunk_size, self.max_chunk_size);
        
        // 6. 更新当前分块大小（使用 Relaxed ordering）
        self.current_chunk_size.store(new_size, Ordering::Relaxed);
        
        new_size
    }

    fn current_chunk_size(&self) -> u64 {
        self.current_chunk_size.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_speed_based_chunk_strategy_creation() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds expected duration
            0.3,              // 0.3 smoothing factor
            0.7,              // 0.7 instant speed weight
            0.3,              // 0.3 avg speed weight
        );

        assert_eq!(strategy.current_chunk_size(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_speed_based_chunk_strategy_from_config() {
        use std::time::Duration;
        
        let config = DownloadConfig::builder()
            .chunk(|c| c
                .initial_size(10 * 1024 * 1024)
                .min_size(5 * 1024 * 1024)
                .max_size(100 * 1024 * 1024))
            .speed(|s| s
                .expected_chunk_duration(Duration::from_secs(3))
                .smoothing_factor(0.5)
                .instant_weight(0.6)
                .avg_weight(0.4))
            .build().unwrap();

        let strategy = SpeedBasedChunkStrategy::from_config(&config);

        assert_eq!(strategy.current_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(strategy.min_chunk_size, 5 * 1024 * 1024);
        assert_eq!(strategy.max_chunk_size, 100 * 1024 * 1024);
        assert_eq!(strategy.expected_duration, 3.0);
        assert_eq!(strategy.smoothing_factor, 0.5);
        assert_eq!(strategy.instant_speed_weight, 0.6);
        assert_eq!(strategy.avg_speed_weight, 0.4);
    }

    #[test]
    fn test_calculate_chunk_size_smoothing() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds expected duration
            0.5,              // 0.5 smoothing factor
            1.0,              // 1.0 instant speed weight (只用瞬时速度)
            0.0,              // 0.0 avg speed weight
        );

        // 瞬时速度: 10 MB/s, 平均速度: 8 MB/s (但权重为0)
        // 加权速度: 10 * 1.0 + 8 * 0.0 = 10 MB/s
        // 理想分块: 10 * 2 = 20 MB
        // 当前: 5 MB, 新大小: 5 + (20 - 5) * 0.5 = 12.5 MB
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0, 8.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 12 * 1024 * 1024 + 512 * 1024); // 12.5 MB

        // 再次计算，应该继续平滑调整
        // 当前: 12.5 MB, 新大小: 12.5 + (20 - 12.5) * 0.5 = 16.25 MB
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0, 8.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 16 * 1024 * 1024 + 256 * 1024); // 16.25 MB
    }

    #[test]
    fn test_calculate_chunk_size_respects_min() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
        );

        // 非常慢的速度: 100 KB/s, 理想分块: 100 * 1024 * 2 = 200 KB
        // 应该立即调整到理想值并被限制在最小值 2 MB
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0, 100.0 * 1024.0);
        assert_eq!(chunk_size, 2 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_respects_max() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
        );

        // 非常快的速度: 100 MB/s, 理想分块: 100 * 2 = 200 MB
        // 应该被限制在最大值 50 MB
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0 * 1024.0, 100.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_smoothing_factor_zero() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            0.0,              // 0.0 smoothing (完全不响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
        );

        // 即使速度很快，分块大小也不应该变化
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0 * 1024.0, 100.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 5 * 1024 * 1024); // 保持初始值
    }

    #[test]
    fn test_smoothing_factor_one() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            1.0,              // 1.0 instant speed weight
            0.0,              // 0.0 avg speed weight
        );

        // 速度: 10 MB/s, 理想分块: 10 * 2 = 20 MB
        // 应该立即调整到理想值
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0, 10.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 20 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_size_updates_state() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
            2.0,
            0.5,
            1.0,  // 1.0 instant speed weight
            0.0,  // 0.0 avg speed weight
        );

        assert_eq!(strategy.current_chunk_size(), 5 * 1024 * 1024);

        // 计算后应该更新内部状态
        strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0, 8.0 * 1024.0 * 1024.0);
        assert_eq!(strategy.current_chunk_size(), 12 * 1024 * 1024 + 512 * 1024);
    }
    
    #[test]
    fn test_weighted_speed_calculation() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
            0.7,              // 0.7 instant speed weight
            0.3,              // 0.3 avg speed weight
        );

        // 瞬时速度: 10 MB/s, 平均速度: 6 MB/s
        // 加权速度: 10 * 0.7 + 6 * 0.3 = 7 + 1.8 = 8.8 MB/s
        // 理想分块: 8.8 * 2 = 17.6 MB
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0, 6.0 * 1024.0 * 1024.0);
        
        // 由于 smoothing_factor = 1.0，应该立即调整到理想值
        let expected = (8.8 * 2.0 * 1024.0 * 1024.0) as u64;
        assert_eq!(chunk_size, expected);
    }
}

