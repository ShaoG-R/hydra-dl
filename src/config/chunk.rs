//! 分块配置模块

use crate::constants::MB;

// ==================== 常量 ====================

/// 分块配置常量
pub struct Defaults;

impl Defaults {
    /// 最小分块大小：2 MB
    pub const MIN_SIZE: u64 = 2 * MB;
    /// 初始分块大小：5 MB
    pub const INITIAL_SIZE: u64 = 5 * MB;
    /// 最大分块大小：50 MB
    pub const MAX_SIZE: u64 = 50 * MB;
}

// ==================== 配置结构体 ====================

/// 分块配置
///
/// 控制下载分块的大小范围
#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// 最小分块大小（bytes）
    pub min_size: u64,
    /// 初始分块大小（bytes）
    pub initial_size: u64,
    /// 最大分块大小（bytes）
    pub max_size: u64,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            min_size: Defaults::MIN_SIZE,
            initial_size: Defaults::INITIAL_SIZE,
            max_size: Defaults::MAX_SIZE,
        }
    }
}

impl ChunkConfig {
    #[inline]
    pub fn min_size(&self) -> u64 {
        self.min_size
    }

    #[inline]
    pub fn initial_size(&self) -> u64 {
        self.initial_size
    }

    #[inline]
    pub fn max_size(&self) -> u64 {
        self.max_size
    }
}

// ==================== 构建器 ====================

/// 分块配置构建器
#[derive(Debug, Clone)]
pub struct ChunkConfigBuilder {
    pub(crate) min_size: u64,
    pub(crate) initial_size: u64,
    pub(crate) max_size: u64,
}

impl ChunkConfigBuilder {
    /// 创建新的分块配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            min_size: Defaults::MIN_SIZE,
            initial_size: Defaults::INITIAL_SIZE,
            max_size: Defaults::MAX_SIZE,
        }
    }

    /// 设置最小分块大小
    pub fn min_size(mut self, size: u64) -> Self {
        self.min_size = size.max(1);
        self
    }

    /// 设置初始分块大小
    pub fn initial_size(mut self, size: u64) -> Self {
        self.initial_size = size.max(1);
        self
    }

    /// 设置最大分块大小
    pub fn max_size(mut self, size: u64) -> Self {
        self.max_size = size.max(1);
        self
    }

    /// 构建分块配置
    pub fn build(self) -> ChunkConfig {
        ChunkConfig {
            min_size: self.min_size,
            initial_size: self.initial_size,
            max_size: self.max_size,
        }
    }
}

impl Default for ChunkConfigBuilder {
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
        let config = ChunkConfig::default();
        assert_eq!(config.min_size(), Defaults::MIN_SIZE);
        assert_eq!(config.initial_size(), Defaults::INITIAL_SIZE);
        assert_eq!(config.max_size(), Defaults::MAX_SIZE);
    }

    #[test]
    fn test_builder_default() {
        let config = ChunkConfigBuilder::new().build();
        assert_eq!(config.min_size(), Defaults::MIN_SIZE);
        assert_eq!(config.initial_size(), Defaults::INITIAL_SIZE);
        assert_eq!(config.max_size(), Defaults::MAX_SIZE);
    }

    #[test]
    fn test_builder_custom() {
        let config = ChunkConfigBuilder::new()
            .min_size(1 * 1024 * 1024)
            .initial_size(10 * 1024 * 1024)
            .max_size(100 * 1024 * 1024)
            .build();

        assert_eq!(config.min_size(), 1 * 1024 * 1024);
        assert_eq!(config.initial_size(), 10 * 1024 * 1024);
        assert_eq!(config.max_size(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_builder_min_values() {
        let config = ChunkConfigBuilder::new()
            .min_size(0)
            .initial_size(0)
            .max_size(0)
            .build();

        // 应该被限制为 1
        assert_eq!(config.min_size(), 1);
        assert_eq!(config.initial_size(), 1);
        assert_eq!(config.max_size(), 1);
    }
}
