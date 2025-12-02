//! 并发配置模块

// ==================== 常量 ====================

/// 并发配置常量
pub struct Defaults;

impl Defaults {
    /// 默认 Worker 数量：4
    pub const WORKER_COUNT: u64 = 4;
}

// ==================== 配置结构体 ====================

/// 并发配置
///
/// 控制 Worker 并发数量
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Worker 并发数量
    pub worker_count: u64,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            worker_count: Defaults::WORKER_COUNT,
        }
    }
}

impl ConcurrencyConfig {
    #[inline]
    pub fn worker_count(&self) -> u64 {
        self.worker_count
    }
}

// ==================== 构建器 ====================

/// 并发配置构建器
#[derive(Debug, Clone)]
pub struct ConcurrencyConfigBuilder {
    pub(crate) worker_count: u64,
}

impl ConcurrencyConfigBuilder {
    /// 创建新的并发配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            worker_count: Defaults::WORKER_COUNT,
        }
    }

    /// 设置 Worker 并发数量
    pub fn worker_count(mut self, count: u64) -> Self {
        self.worker_count = count.max(1);
        self
    }

    /// 构建并发配置
    pub fn build(self) -> ConcurrencyConfig {
        ConcurrencyConfig {
            worker_count: self.worker_count,
        }
    }
}

impl Default for ConcurrencyConfigBuilder {
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
        let config = ConcurrencyConfig::default();
        assert_eq!(config.worker_count(), Defaults::WORKER_COUNT);
    }

    #[test]
    fn test_builder_default() {
        let config = ConcurrencyConfigBuilder::new().build();
        assert_eq!(config.worker_count(), Defaults::WORKER_COUNT);
    }

    #[test]
    fn test_builder_custom() {
        let config = ConcurrencyConfigBuilder::new().worker_count(8).build();
        assert_eq!(config.worker_count(), 8);
    }

    #[test]
    fn test_builder_min_value() {
        let config = ConcurrencyConfigBuilder::new().worker_count(0).build();
        // 应该被限制为 1
        assert_eq!(config.worker_count(), 1);
    }
}
