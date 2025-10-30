//! 动态分块策略模块
//!
//! 提供根据下载速度动态调整分块大小的策略接口和实现

use crate::config::DownloadConfig;

/// 单位常量：KB (bytes)
const KB: f64 = 1024.0;

/// 单位常量：MB (bytes)
const MB: f64 = 1024.0 * 1024.0;

/// 分块策略 trait
///
/// 定义了动态调整下载分块大小的接口
pub(crate) trait ChunkStrategy {
    /// 根据实时下载速度计算合适的分块大小
    ///
    /// # Arguments
    ///
    /// * `instant_speed` - 实时下载速度 (bytes/s)
    ///
    /// # Returns
    ///
    /// 计算得到的分块大小 (bytes)
    fn calculate_chunk_size(&self, instant_speed: f64) -> u64;

    /// 获取当前分块大小
    ///
    /// # Returns
    ///
    /// 当前使用的分块大小 (bytes)
    fn current_chunk_size(&self) -> u64;

    /// 更新当前分块大小
    ///
    /// # Arguments
    ///
    /// * `new_size` - 新的分块大小 (bytes)
    fn update_chunk_size(&mut self, new_size: u64);
}

/// 基于速度的自适应分块策略
///
/// 根据实时下载速度动态调整分块大小：
/// - 速度越快，分块越大（减少请求开销）
/// - 速度越慢，分块越小（提高并发，更灵活的任务分配）
///
/// 分级策略：
/// - < 500 KB/s: 使用最小分块
/// - 500 KB/s ~ 2 MB/s: 2 倍最小分块
/// - 2 MB/s ~ 10 MB/s: 5 倍最小分块
/// - > 10 MB/s: 使用最大分块
#[derive(Debug, Clone)]
pub(crate) struct SpeedBasedChunkStrategy {
    /// 当前分块大小 (bytes)
    current_chunk_size: u64,
    /// 最小分块大小 (bytes)
    min_chunk_size: u64,
    /// 最大分块大小 (bytes)
    max_chunk_size: u64,
}

impl SpeedBasedChunkStrategy {
    /// 创建新的速度自适应分块策略
    ///
    /// # Arguments
    ///
    /// * `initial_chunk_size` - 初始分块大小 (bytes)
    /// * `min_chunk_size` - 最小分块大小 (bytes)
    /// * `max_chunk_size` - 最大分块大小 (bytes)
    pub(crate) fn new(
        initial_chunk_size: u64,
        min_chunk_size: u64,
        max_chunk_size: u64,
    ) -> Self {
        Self {
            current_chunk_size: initial_chunk_size,
            min_chunk_size,
            max_chunk_size,
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
        )
    }
}

impl ChunkStrategy for SpeedBasedChunkStrategy {
    fn calculate_chunk_size(&self, instant_speed: f64) -> u64 {
        let chunk_size = if instant_speed < 500.0 * KB {
            // < 500 KB/s: 使用最小分块
            self.min_chunk_size
        } else if instant_speed < 2.0 * MB {
            // 500 KB/s ~ 2 MB/s: 2 倍最小分块
            (self.min_chunk_size * 2).min(self.max_chunk_size)
        } else if instant_speed < 10.0 * MB {
            // 2 MB/s ~ 10 MB/s: 5 倍最小分块
            (self.min_chunk_size * 5).min(self.max_chunk_size)
        } else {
            // > 10 MB/s: 使用最大分块
            self.max_chunk_size
        };

        // 确保在合理范围内
        chunk_size.clamp(self.min_chunk_size, self.max_chunk_size)
    }

    fn current_chunk_size(&self) -> u64 {
        self.current_chunk_size
    }

    fn update_chunk_size(&mut self, new_size: u64) {
        self.current_chunk_size = new_size;
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
        );

        assert_eq!(strategy.current_chunk_size(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_speed_based_chunk_strategy_from_config() {
        let config = DownloadConfig::builder()
            .initial_chunk_size(10 * 1024 * 1024)
            .min_chunk_size(5 * 1024 * 1024)
            .max_chunk_size(100 * 1024 * 1024)
            .build();

        let strategy = SpeedBasedChunkStrategy::from_config(&config);

        assert_eq!(strategy.current_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(strategy.min_chunk_size, 5 * 1024 * 1024);
        assert_eq!(strategy.max_chunk_size, 100 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_very_slow() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // < 500 KB/s: 应该返回最小分块
        let chunk_size = strategy.calculate_chunk_size(100.0 * 1024.0); // 100 KB/s
        assert_eq!(chunk_size, 2 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_slow() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // 500 KB/s ~ 2 MB/s: 应该返回 2 倍最小分块
        let chunk_size = strategy.calculate_chunk_size(1.0 * 1024.0 * 1024.0); // 1 MB/s
        assert_eq!(chunk_size, 4 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_medium() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // 2 MB/s ~ 10 MB/s: 应该返回 5 倍最小分块
        let chunk_size = strategy.calculate_chunk_size(5.0 * 1024.0 * 1024.0); // 5 MB/s
        assert_eq!(chunk_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_fast() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // > 10 MB/s: 应该返回最大分块
        let chunk_size = strategy.calculate_chunk_size(20.0 * 1024.0 * 1024.0); // 20 MB/s
        assert_eq!(chunk_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_chunk_size_respects_max() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            8 * 1024 * 1024,  // 较小的最大值
        );

        // 即使是中等速度，也不应该超过最大值
        let chunk_size = strategy.calculate_chunk_size(5.0 * 1024.0 * 1024.0); // 5 MB/s
        assert_eq!(chunk_size, 8 * 1024 * 1024); // 不超过 max
    }

    #[test]
    fn test_update_chunk_size() {
        let mut strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        assert_eq!(strategy.current_chunk_size(), 5 * 1024 * 1024);

        strategy.update_chunk_size(10 * 1024 * 1024);
        assert_eq!(strategy.current_chunk_size(), 10 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_size_boundary_500kb() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // 边界值：刚好 500 KB/s
        let chunk_size = strategy.calculate_chunk_size(500.0 * 1024.0);
        assert_eq!(chunk_size, 4 * 1024 * 1024); // 应该是 2 倍最小分块
    }

    #[test]
    fn test_chunk_size_boundary_2mb() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // 边界值：刚好 2 MB/s
        let chunk_size = strategy.calculate_chunk_size(2.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 10 * 1024 * 1024); // 应该是 5 倍最小分块
    }

    #[test]
    fn test_chunk_size_boundary_10mb() {
        let strategy = SpeedBasedChunkStrategy::new(
            5 * 1024 * 1024,
            2 * 1024 * 1024,
            50 * 1024 * 1024,
        );

        // 边界值：刚好 10 MB/s
        let chunk_size = strategy.calculate_chunk_size(10.0 * 1024.0 * 1024.0);
        assert_eq!(chunk_size, 50 * 1024 * 1024); // 应该是最大分块
    }
}

