//! 动态分块策略模块
//!
//! 提供根据下载速度动态调整分块大小的策略接口和实现

use crate::config::DownloadConfig;

/// 分块策略 trait
///
/// 定义了动态调整下载分块大小的接口
pub(crate) trait ChunkStrategy {
    /// 根据实时下载速度计算合适的分块大小（带平滑）
    ///
    /// # Arguments
    ///
    /// * `instant_speed` - 实时下载速度 (bytes/s)
    ///
    /// # Returns
    ///
    /// 计算得到的分块大小 (bytes)
    fn calculate_chunk_size(&mut self, instant_speed: f64) -> u64;

    /// 获取当前分块大小
    ///
    /// # Returns
    ///
    /// 当前使用的分块大小 (bytes)
    fn current_chunk_size(&self) -> u64;
}

/// 基于速度的自适应分块策略
///
/// 根据实时下载速度动态调整分块大小，使用平滑算法：
/// 1. 理想分块大小 = 单个协程下载速度 * 预期时间
/// 2. 实际分配分块大小 = 上次分块大小 + (理想分块大小 - 上次分块大小) * 平滑系数
/// 3. 实际分配分块大小限制在 min_chunk_size ~ max_chunk_size 区间内
#[derive(Debug, Clone)]
pub(crate) struct SpeedBasedChunkStrategy {
    /// 当前分块大小 (bytes)
    current_chunk_size: u64,
    /// 最小分块大小 (bytes)
    min_chunk_size: u64,
    /// 最大分块大小 (bytes)
    max_chunk_size: u64,
    /// 预期分块下载时长 (秒)
    expected_duration: f64,
    /// 平滑系数 (0.0 ~ 1.0)
    smoothing_factor: f64,
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
    pub(crate) fn new(
        initial_chunk_size: u64,
        min_chunk_size: u64,
        max_chunk_size: u64,
        expected_duration_secs: f64,
        smoothing_factor: f64,
    ) -> Self {
        Self {
            current_chunk_size: initial_chunk_size,
            min_chunk_size,
            max_chunk_size,
            expected_duration: expected_duration_secs,
            smoothing_factor,
        }
    }

    /// 从配置创建速度自适应分块策略
    ///
    /// # Arguments
    ///
    /// * `config` - 下载配置
    pub(crate) fn from_config(config: &DownloadConfig) -> Self {
        Self::new(
            config.initial_chunk_size(),
            config.min_chunk_size(),
            config.max_chunk_size(),
            config.expected_chunk_duration().as_secs_f64(),
            config.smoothing_factor(),
        )
    }
}

impl ChunkStrategy for SpeedBasedChunkStrategy {
    fn calculate_chunk_size(&mut self, instant_speed: f64) -> u64 {
        // 1. 计算理想分块大小 = 速度 * 预期时间
        let ideal_chunk_size = (instant_speed * self.expected_duration) as u64;
        
        // 2. 应用平滑算法：新大小 = 旧大小 + (理想大小 - 旧大小) * 平滑系数
        let smoothed_size = self.current_chunk_size as f64 
            + (ideal_chunk_size as f64 - self.current_chunk_size as f64) * self.smoothing_factor;
        
        // 3. 限制在配置范围内
        let new_size = (smoothed_size as u64).clamp(self.min_chunk_size, self.max_chunk_size);
        
        // 4. 更新当前分块大小
        self.current_chunk_size = new_size;
        
        new_size
    }

    fn current_chunk_size(&self) -> u64 {
        self.current_chunk_size
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
        );

        assert_eq!(strategy.current_chunk_size(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_speed_based_chunk_strategy_from_config() {
        use std::time::Duration;
        
        let config = DownloadConfig::builder()
            .initial_chunk_size(10 * 1024 * 1024)
            .min_chunk_size(5 * 1024 * 1024)
            .max_chunk_size(100 * 1024 * 1024)
            .expected_chunk_duration(Duration::from_secs(3))
            .smoothing_factor(0.5)
            .build();

        let strategy = SpeedBasedChunkStrategy::from_config(&config);

        assert_eq!(strategy.current_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(strategy.min_chunk_size, 5 * 1024 * 1024);
        assert_eq!(strategy.max_chunk_size, 100 * 1024 * 1024);
        assert_eq!(strategy.expected_duration, 3.0);
        assert_eq!(strategy.smoothing_factor, 0.5);
    }

    #[test]
    fn test_calculate_chunk_size_smoothing() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds expected duration
            0.5,              // 0.5 smoothing factor
        );

        // 速度: 10 MB/s, 理想分块: 10 * 2 = 20 MB
        // 当前: 5 MB, 新大小: 5 + (20 - 5) * 0.5 = 12.5 MB
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 12 * 1024 * 1024 + 512 * 1024); // 12.5 MB

        // 再次计算，应该继续平滑调整
        // 当前: 12.5 MB, 新大小: 12.5 + (20 - 12.5) * 0.5 = 16.25 MB
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 16 * 1024 * 1024 + 256 * 1024); // 16.25 MB
    }

    #[test]
    fn test_calculate_chunk_size_respects_min() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
        );

        // 非常慢的速度: 100 KB/s, 理想分块: 100 * 1024 * 2 = 200 KB
        // 应该立即调整到理想值并被限制在最小值 2 MB
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0);
        assert_eq!(chunk_size, 2 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_respects_max() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
        );

        // 非常快的速度: 100 MB/s, 理想分块: 100 * 2 = 200 MB
        // 应该被限制在最大值 50 MB
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_smoothing_factor_zero() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            0.0,              // 0.0 smoothing (完全不响应)
        );

        // 即使速度很快，分块大小也不应该变化
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 5 * 1024 * 1024); // 保持初始值
    }

    #[test]
    fn test_smoothing_factor_one() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,  // 5 MB initial
            2 * 1024 * 1024,  // 2 MB min
            50 * 1024 * 1024, // 50 MB max
            2.0,              // 2 seconds
            1.0,              // 1.0 smoothing (立即响应)
        );

        // 速度: 10 MB/s, 理想分块: 10 * 2 = 20 MB
        // 应该立即调整到理想值
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 20 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_size_updates_state() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
            2.0,
            0.5,
        );

        assert_eq!(strategy.current_chunk_size(), 5 * 1024 * 1024);

        // 计算后应该更新内部状态
        strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0);
        assert_eq!(strategy.current_chunk_size(), 12 * 1024 * 1024 + 512 * 1024);
    }
}

