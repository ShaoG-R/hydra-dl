//! 重试配置模块

use std::time::Duration;

// ==================== 常量 ====================

/// 重试配置常量
pub struct Defaults;

impl Defaults {
    /// 重试延迟序列：等待 n 个任务完成后重试
    /// [1, 2, 4] 表示第一次重试等待1个任务完成，第二次等待2个，第三次等待4个
    pub const RETRY_TASK_COUNTS: &'static [u64] = &[1, 2, 4];
    /// 无新任务时的等待重试延迟（毫秒）
    pub const RETRY_DELAY_MILLIS: u64 = 1000;
}

// ==================== 配置结构体 ====================

/// 重试配置
///
/// 控制失败任务的重试策略
///
/// # 重试机制
///
/// 当任务失败时，根据 `retry_task_counts` 计算下一次重试的时机：
/// - 等待指定数量的其他任务完成后自动重试
/// - 如果没有新任务可执行，则等待 `retry_delay` 后重试
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 重试任务数序列：等待 n 个任务完成后重试
    /// 例如 [1, 2, 4] 表示：
    /// - 第1次失败后，等待1个任务完成后重试
    /// - 第2次失败后，等待2个任务完成后重试
    /// - 第3次失败后，等待4个任务完成后重试
    pub retry_task_counts: Vec<u64>,
    /// 无新任务时的等待重试延迟
    pub retry_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retry_task_counts: Defaults::RETRY_TASK_COUNTS.to_vec(),
            retry_delay: Duration::from_millis(Defaults::RETRY_DELAY_MILLIS),
        }
    }
}

impl RetryConfig {
    /// 获取最大重试次数
    #[inline]
    pub fn max_retry_count(&self) -> usize {
        self.retry_task_counts.len()
    }

    /// 获取重试任务数序列
    #[inline]
    pub fn retry_task_counts(&self) -> &[u64] {
        &self.retry_task_counts
    }

    /// 获取指定重试次数对应的等待任务数
    ///
    /// # Arguments
    ///
    /// - `retry_count`: 当前重试次数（从0开始）
    ///
    /// # Returns
    ///
    /// 返回需要等待的任务数，如果超出序列长度则返回最后一个值
    #[inline]
    pub fn get_wait_task_count(&self, retry_count: usize) -> u64 {
        self.retry_task_counts
            .get(retry_count)
            .or_else(|| self.retry_task_counts.last())
            .copied()
            .unwrap_or(1)
    }

    /// 获取无新任务时的等待重试延迟
    #[inline]
    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

// ==================== 构建器 ====================

/// 重试配置构建器
#[derive(Debug, Clone)]
pub struct RetryConfigBuilder {
    pub(crate) retry_task_counts: Vec<u64>,
    pub(crate) retry_delay: Duration,
}

impl RetryConfigBuilder {
    /// 创建新的重试配置构建器（使用默认值）
    pub fn new() -> Self {
        Self {
            retry_task_counts: Defaults::RETRY_TASK_COUNTS.to_vec(),
            retry_delay: Duration::from_millis(Defaults::RETRY_DELAY_MILLIS),
        }
    }

    /// 设置重试任务数序列
    ///
    /// 序列中的每个元素表示第 n 次重试需要等待的任务完成数
    pub fn retry_task_counts(mut self, counts: Vec<u64>) -> Self {
        if counts.is_empty() {
            self.retry_task_counts = Defaults::RETRY_TASK_COUNTS.to_vec();
        } else {
            self.retry_task_counts = counts;
        }
        self
    }

    /// 设置无新任务时的等待重试延迟
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// 构建重试配置
    pub fn build(self) -> RetryConfig {
        RetryConfig {
            retry_task_counts: self.retry_task_counts,
            retry_delay: self.retry_delay,
        }
    }
}

impl Default for RetryConfigBuilder {
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
        let config = RetryConfig::default();
        assert_eq!(config.max_retry_count(), 3);
        assert_eq!(config.retry_task_counts().len(), 3);
        assert_eq!(config.retry_task_counts()[0], 1);
        assert_eq!(config.retry_task_counts()[1], 2);
        assert_eq!(config.retry_task_counts()[2], 4);
        assert_eq!(config.retry_delay(), Duration::from_millis(1000));
    }

    #[test]
    fn test_builder_default() {
        let config = RetryConfigBuilder::new().build();
        assert_eq!(config.max_retry_count(), 3);
        assert_eq!(config.retry_task_counts(), Defaults::RETRY_TASK_COUNTS);
        assert_eq!(
            config.retry_delay(),
            Duration::from_millis(Defaults::RETRY_DELAY_MILLIS)
        );
    }

    #[test]
    fn test_builder_custom() {
        let config = RetryConfigBuilder::new()
            .retry_task_counts(vec![1, 3, 5])
            .retry_delay(Duration::from_secs(2))
            .build();

        assert_eq!(config.max_retry_count(), 3);
        assert_eq!(config.retry_task_counts().len(), 3);
        assert_eq!(config.retry_task_counts()[0], 1);
        assert_eq!(config.retry_task_counts()[1], 3);
        assert_eq!(config.retry_task_counts()[2], 5);
        assert_eq!(config.retry_delay(), Duration::from_secs(2));
    }

    #[test]
    fn test_retry_task_counts_empty_uses_default() {
        let config = RetryConfigBuilder::new().retry_task_counts(vec![]).build();
        assert_eq!(config.retry_task_counts().len(), 3);
        assert_eq!(config.retry_task_counts()[0], 1);
    }

    #[test]
    fn test_get_wait_task_count() {
        let config = RetryConfigBuilder::new()
            .retry_task_counts(vec![1, 2, 4])
            .build();

        assert_eq!(config.get_wait_task_count(0), 1);
        assert_eq!(config.get_wait_task_count(1), 2);
        assert_eq!(config.get_wait_task_count(2), 4);
        // 超出范围时返回最后一个值
        assert_eq!(config.get_wait_task_count(3), 4);
        assert_eq!(config.get_wait_task_count(100), 4);
    }
}
