//! 异常检测器
//!
//! 使用滑动窗口记录异常状态，O(1) 增量更新
#[derive(Debug, Clone)]
pub struct AnomalyChecker {
    /// 异常历史（环形缓冲区）
    history: Vec<bool>,
    /// 当前写入位置
    position: usize,
    /// 当前异常计数
    anomaly_count: usize,
    /// 窗口大小
    size: usize,
    /// 异常阈值
    threshold: usize,
}

impl AnomalyChecker {
    /// 创建新的异常检测器
    pub fn new(size: usize, threshold: usize) -> Self {
        Self {
            history: vec![false; size],
            position: 0,
            anomaly_count: 0,
            size,
            threshold,
        }
    }

    /// 记录一次检查结果（O(1)）
    pub fn record(&mut self, is_anomaly: bool) {
        if self.size == 0 {
            return;
        }

        // 移除旧记录的计数
        if self.history[self.position] {
            self.anomaly_count -= 1;
        }
        // 添加新记录的计数
        if is_anomaly {
            self.anomaly_count += 1;
        }
        // 更新历史
        self.history[self.position] = is_anomaly;
        self.position = (self.position + 1) % self.size;
    }

    /// 检查是否超过异常阈值
    #[inline]
    pub fn exceeds_threshold(&self) -> bool {
        self.size > 0 && self.anomaly_count >= self.threshold
    }

    /// 重置检测器
    pub fn reset(&mut self) {
        self.history.fill(false);
        self.position = 0;
        self.anomaly_count = 0;
    }

    /// 获取当前异常计数
    #[inline]
    pub fn anomaly_count(&self) -> usize {
        self.anomaly_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anomaly_checker_basic() {
        let mut checker = AnomalyChecker::new(5, 3);

        // 初始状态
        assert_eq!(checker.anomaly_count(), 0);
        assert!(!checker.exceeds_threshold());

        // 记录 3 次异常
        checker.record(true);
        checker.record(true);
        checker.record(true);

        assert_eq!(checker.anomaly_count(), 3);
        assert!(checker.exceeds_threshold());
    }

    #[test]
    fn test_anomaly_checker_sliding_window() {
        let mut checker = AnomalyChecker::new(3, 2);

        // 记录: [true, true, false]
        checker.record(true);
        checker.record(true);
        checker.record(false);

        assert_eq!(checker.anomaly_count(), 2);
        assert!(checker.exceeds_threshold());

        // 滑动: [true, false, false] -> [false, false]
        checker.record(false);
        assert_eq!(checker.anomaly_count(), 1);
        assert!(!checker.exceeds_threshold());
    }

    #[test]
    fn test_anomaly_checker_reset() {
        let mut checker = AnomalyChecker::new(5, 3);

        checker.record(true);
        checker.record(true);
        checker.record(true);

        assert_eq!(checker.anomaly_count(), 3);

        checker.reset();

        assert_eq!(checker.anomaly_count(), 0);
        assert!(!checker.exceeds_threshold());
    }

    #[test]
    fn test_zero_size() {
        let mut checker = AnomalyChecker::new(0, 0);
        checker.record(true);
        assert_eq!(checker.anomaly_count(), 0);
        assert!(!checker.exceeds_threshold());
    }
}
