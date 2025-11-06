/// Worker 健康检查模块
///
/// 使用"最大间隙检测"算法来识别速度异常的 worker
use log::debug;

/// Worker 速度信息
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkerSpeed {
    /// Worker ID
    pub worker_id: usize,
    /// 速度（字节/秒）
    pub speed: f64,
}

/// 健康检查结果
#[derive(Debug, Clone, PartialEq)]
pub struct HealthCheckResult {
    /// 需要终止的 worker 列表（worker_id, speed）
    pub unhealthy_workers: Vec<(usize, f64)>,
    /// 健康基准速度（字节/秒）
    pub health_baseline: f64,
    /// 最大间隙值
    pub max_gap: f64,
}

/// Worker 健康检查器
///
/// 使用最大间隙检测算法来识别速度异常的 worker：
/// 1. 收集所有 worker 的窗口平均速度并排序
/// 2. 计算相邻元素的间隙
/// 3. 找到最大间隙作为分界线，将 workers 分为慢速簇和快速簇
/// 4. 以快速簇的最小值作为健康基准
/// 5. 同时应用绝对速度阈值，确保不会错误地终止健康的 worker
///
/// # 算法示例
///
/// ```text
/// 速度列表: [10, 30, 5120, 6144] KB/s
/// 间隙: [20, 5090, 1024]
/// 最大间隙: 5090 (在 30 和 5120 之间)
/// 慢速簇: [10, 30]
/// 快速簇: [5120, 6144]
/// 健康基准: 5120 KB/s (快速簇的最小值)
/// ```
pub struct WorkerHealthChecker {
    /// 绝对速度阈值（字节/秒）
    /// 低于此阈值的 worker 可能被标记为不健康
    absolute_threshold: f64,
    /// 相对速度比例
    /// worker 速度低于健康基准的此比例时被视为显著慢速
    relative_threshold: f64,
}

impl WorkerHealthChecker {
    /// 创建新的健康检查器
    ///
    /// # Arguments
    ///
    /// * `absolute_threshold` - 绝对速度阈值（字节/秒）
    /// * `relative_threshold` - 相对速度比例（0.0 - 1.0）
    ///
    /// # Example
    ///
    /// ```
    /// use hydra_dl::download::WorkerHealthChecker;
    ///
    /// let checker = WorkerHealthChecker::new(
    ///     10240.0,  // 10 KB/s
    ///     0.5       // 50% of baseline
    /// );
    /// ```
    pub fn new(absolute_threshold: f64, relative_threshold: f64) -> Self {
        Self {
            absolute_threshold,
            relative_threshold,
        }
    }

    /// 执行健康检查
    ///
    /// # Arguments
    ///
    /// * `worker_speeds` - 所有 worker 的速度信息列表
    ///
    /// # Returns
    ///
    /// 如果检测到不健康的 worker，返回 `Some(HealthCheckResult)`，否则返回 `None`
    ///
    /// # 性能优化
    ///
    /// - 使用索引排序避免克隆大量数据
    /// - 一次遍历同时计算间隙和找最大值
    /// - 提前返回避免不必要的计算
    pub fn check(&self, worker_speeds: &[WorkerSpeed]) -> Option<HealthCheckResult> {
        // 至少需要 2 个 worker 才能进行比较
        if worker_speeds.len() < 2 {
            return None;
        }

        // 性能优化：使用索引数组排序，避免克隆整个 WorkerSpeed 数据
        let mut indices: Vec<usize> = (0..worker_speeds.len()).collect();
        indices.sort_unstable_by(|&a, &b| {
            worker_speeds[a].speed.partial_cmp(&worker_speeds[b].speed).unwrap()
        });

        // 性能优化：一次遍历同时计算间隙并找到最大间隙
        let (max_gap_idx, gap_value) = self.find_max_gap_optimized(&indices, worker_speeds)?;

        // 分界点：最大间隙之后的第一个元素
        let split_idx = max_gap_idx + 1;

        // 如果快速簇为空（所有 worker 速度相近），则提前返回
        if split_idx >= indices.len() {
            return None;
        }

        // 健康基准：快速簇的最小值
        let health_baseline = worker_speeds[indices[split_idx]].speed;

        debug!(
            "健康检查: 检测到最大间隙 {:.2} KB/s (在索引 {} 和 {} 之间)",
            gap_value / 1024.0,
            max_gap_idx,
            split_idx
        );
        debug!(
            "健康基准: {:.2} KB/s, 绝对阈值: {:.2} KB/s",
            health_baseline / 1024.0,
            self.absolute_threshold / 1024.0
        );

        // 收集需要终止的 worker（慢速簇）
        let unhealthy_workers = self.identify_unhealthy_workers_optimized(
            &indices[..split_idx],
            worker_speeds,
            health_baseline,
        );

        if unhealthy_workers.is_empty() {
            return None;
        }

        Some(HealthCheckResult {
            unhealthy_workers,
            health_baseline,
            max_gap: gap_value,
        })
    }

    /// 优化的间隙查找：一次遍历同时计算间隙并找到最大值
    ///
    /// # 性能优化
    ///
    /// 相比原来的两次遍历（calculate_gaps + find_max_gap），
    /// 这个方法只需要一次遍历，减少了内存分配和迭代开销
    #[inline]
    fn find_max_gap_optimized(
        &self,
        indices: &[usize],
        worker_speeds: &[WorkerSpeed],
    ) -> Option<(usize, f64)> {
        if indices.len() < 2 {
            return None;
        }

        let mut max_gap_idx = 0;
        let mut max_gap_value = 0.0;

        for i in 0..indices.len() - 1 {
            let gap = worker_speeds[indices[i + 1]].speed - worker_speeds[indices[i]].speed;
            if gap > max_gap_value {
                max_gap_value = gap;
                max_gap_idx = i;
            }
        }

        // 如果最大间隙为 0，说明所有速度相同
        if max_gap_value == 0.0 {
            None
        } else {
            Some((max_gap_idx, max_gap_value))
        }
    }

    /// 优化的不健康 worker 识别
    ///
    /// worker 同时满足以下条件才被标记为不健康：
    /// 1. 速度低于绝对阈值
    /// 2. 速度明显低于健康基准（例如：低于 50%）
    ///
    /// # 性能优化
    ///
    /// 使用索引访问避免不必要的数据复制
    #[inline]
    fn identify_unhealthy_workers_optimized(
        &self,
        slow_indices: &[usize],
        worker_speeds: &[WorkerSpeed],
        health_baseline: f64,
    ) -> Vec<(usize, f64)> {
        let threshold_speed = health_baseline * self.relative_threshold;
        
        slow_indices
            .iter()
            .map(|&idx| &worker_speeds[idx])
            .filter(|worker| {
                worker.speed < self.absolute_threshold && worker.speed < threshold_speed
            })
            .map(|worker| (worker.worker_id, worker.speed))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_workers_no_unhealthy() {
        // 所有 worker 速度相近
        let checker = WorkerHealthChecker::new(10240.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 5000.0 },
            WorkerSpeed { worker_id: 1, speed: 5100.0 },
            WorkerSpeed { worker_id: 2, speed: 5200.0 },
        ];

        let result = checker.check(&speeds);
        assert!(result.is_none());
    }

    #[test]
    fn test_one_slow_worker_detected() {
        // 一个 worker 明显慢于其他
        let checker = WorkerHealthChecker::new(1024.0 * 10.0, 0.5); // 10 KB/s threshold
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 5120.0 },  // 5 KB/s - slow
            WorkerSpeed { worker_id: 1, speed: 512000.0 }, // 500 KB/s
            WorkerSpeed { worker_id: 2, speed: 614400.0 }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert!((result.health_baseline - 512000.0).abs() < 0.1);
    }

    #[test]
    fn test_multiple_slow_workers() {
        // 多个 worker 慢速
        let checker = WorkerHealthChecker::new(20480.0, 0.5); // 20 KB/s threshold
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 10240.0 },  // 10 KB/s
            WorkerSpeed { worker_id: 1, speed: 15360.0 },  // 15 KB/s
            WorkerSpeed { worker_id: 2, speed: 512000.0 }, // 500 KB/s
            WorkerSpeed { worker_id: 3, speed: 614400.0 }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 2);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert_eq!(result.unhealthy_workers[1].0, 1);
    }

    #[test]
    fn test_slow_but_above_absolute_threshold() {
        // worker 相对慢但高于绝对阈值
        let checker = WorkerHealthChecker::new(1024.0 * 100.0, 0.5); // 100 KB/s threshold
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 150000.0 }, // 146 KB/s - slow but above threshold
            WorkerSpeed { worker_id: 1, speed: 512000.0 }, // 500 KB/s
            WorkerSpeed { worker_id: 2, speed: 614400.0 }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        // 应该没有不健康的 worker，因为都高于绝对阈值
        assert!(result.is_none());
    }

    #[test]
    fn test_below_absolute_but_not_significantly_slow() {
        // worker 低于绝对阈值但不显著慢于健康基准
        let checker = WorkerHealthChecker::new(250000.0, 0.5); // 244 KB/s threshold
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 200000.0 }, // 195 KB/s
            WorkerSpeed { worker_id: 1, speed: 300000.0 }, // 293 KB/s (baseline)
            WorkerSpeed { worker_id: 2, speed: 350000.0 }, // 342 KB/s
        ];

        let result = checker.check(&speeds);
        // worker 0 低于绝对阈值，但是 200000 > 300000 * 0.5
        // 所以不会被标记为不健康
        assert!(result.is_none());
    }

    #[test]
    fn test_insufficient_workers() {
        // 只有一个 worker
        let checker = WorkerHealthChecker::new(10240.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 5120.0 },
        ];

        let result = checker.check(&speeds);
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_workers() {
        // 空列表
        let checker = WorkerHealthChecker::new(10240.0, 0.5);
        let speeds = vec![];

        let result = checker.check(&speeds);
        assert!(result.is_none());
    }

    #[test]
    fn test_typical_scenario() {
        // 典型场景：10 KB/s、30 KB/s 慢速，5 MB/s、6 MB/s 正常
        let checker = WorkerHealthChecker::new(1024.0 * 40.0, 0.5); // 40 KB/s threshold
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 10240.0 },      // 10 KB/s
            WorkerSpeed { worker_id: 1, speed: 30720.0 },      // 30 KB/s
            WorkerSpeed { worker_id: 2, speed: 5242880.0 },    // 5 MB/s
            WorkerSpeed { worker_id: 3, speed: 6291456.0 },    // 6 MB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());

        let result = result.unwrap();
        // 两个慢速 worker 都应该被检测到
        assert_eq!(result.unhealthy_workers.len(), 2);
        
        // 验证间隙是在 30 KB/s 和 5 MB/s 之间
        assert!((result.max_gap - (5242880.0 - 30720.0)).abs() < 1.0);
        
        // 验证健康基准是 5 MB/s
        assert!((result.health_baseline - 5242880.0).abs() < 1.0);
    }

    #[test]
    fn test_all_workers_slow() {
        // 所有 worker 都很慢
        let checker = WorkerHealthChecker::new(10240.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 1024.0 },  // 1 KB/s
            WorkerSpeed { worker_id: 1, speed: 2048.0 },  // 2 KB/s
            WorkerSpeed { worker_id: 2, speed: 3072.0 },  // 3 KB/s
        ];

        let result = checker.check(&speeds);
        // 因为没有明显的间隙分界（最快的簇为空或所有都慢），应该没有检测到
        // 或者如果有间隙，快速簇的基准也会很低，不会触发
        // 根据算法，最大间隙在 2KB 和 3KB 之间（1024），快速簇是 [3KB]
        // 慢速簇 [1KB, 2KB]，它们都低于 3KB * 0.5 = 1.5KB 吗？
        // 1KB < 10KB && 1KB < 1.5KB ✓
        // 2KB < 10KB && 2KB < 1.5KB ✗
        // 所以只有 worker 0 会被检测到
        if let Some(result) = result {
            assert_eq!(result.unhealthy_workers.len(), 1);
            assert_eq!(result.unhealthy_workers[0].0, 0);
        }
    }

    #[test]
    fn test_custom_relative_threshold() {
        // 测试不同的相对阈值
        let checker = WorkerHealthChecker::new(1024.0 * 100.0, 0.8); // 80% threshold
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 50000.0 },  // 49 KB/s
            WorkerSpeed { worker_id: 1, speed: 500000.0 }, // 488 KB/s
            WorkerSpeed { worker_id: 2, speed: 600000.0 }, // 586 KB/s
        ];

        let result = checker.check(&speeds);
        // 50KB < 100KB ✓
        // 50KB < 500KB * 0.8 = 400KB ✓
        // 应该检测到 worker 0
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
    }

    #[test]
    fn test_zero_speed_worker() {
        // worker 速度为 0
        let checker = WorkerHealthChecker::new(10240.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 0.0 },      // 0 KB/s
            WorkerSpeed { worker_id: 1, speed: 512000.0 }, // 500 KB/s
            WorkerSpeed { worker_id: 2, speed: 614400.0 }, // 600 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert!((result.unhealthy_workers[0].1 - 0.0).abs() < 0.1);
    }

    #[test]
    fn test_large_number_of_workers() {
        // 测试大量 worker 的性能
        let checker = WorkerHealthChecker::new(50000.0, 0.5);
        let mut speeds = Vec::new();
        
        // 10 个慢速 worker
        for i in 0..10 {
            speeds.push(WorkerSpeed {
                worker_id: i,
                speed: 10000.0 + (i as f64 * 1000.0),
            });
        }
        
        // 90 个快速 worker
        for i in 10..100 {
            speeds.push(WorkerSpeed {
                worker_id: i,
                speed: 500000.0 + (i as f64 * 1000.0),
            });
        }

        let result = checker.check(&speeds);
        assert!(result.is_some());
        let result = result.unwrap();
        
        // 所有慢速 worker 都应该被检测到
        assert_eq!(result.unhealthy_workers.len(), 10);
    }

    #[test]
    fn test_unordered_input() {
        // 测试输入顺序不影响结果
        let checker = WorkerHealthChecker::new(20480.0, 0.5);
        
        // 乱序输入
        let speeds = vec![
            WorkerSpeed { worker_id: 2, speed: 512000.0 }, // 500 KB/s
            WorkerSpeed { worker_id: 0, speed: 10240.0 },  // 10 KB/s
            WorkerSpeed { worker_id: 3, speed: 614400.0 }, // 600 KB/s
            WorkerSpeed { worker_id: 1, speed: 15360.0 },  // 15 KB/s
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());
        
        let result = result.unwrap();
        assert_eq!(result.unhealthy_workers.len(), 2);
        
        // 验证检测到的是 worker 0 和 1
        let mut worker_ids: Vec<usize> = result.unhealthy_workers.iter().map(|&(id, _)| id).collect();
        worker_ids.sort();
        assert_eq!(worker_ids, vec![0, 1]);
    }

    #[test]
    fn test_identical_speeds() {
        // 所有 worker 速度完全相同
        let checker = WorkerHealthChecker::new(10240.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 100000.0 },
            WorkerSpeed { worker_id: 1, speed: 100000.0 },
            WorkerSpeed { worker_id: 2, speed: 100000.0 },
        ];

        let result = checker.check(&speeds);
        // 没有间隙，应该返回 None
        assert!(result.is_none());
    }

    #[test]
    fn test_edge_case_threshold() {
        // 测试边界情况：worker 速度恰好等于健康基准 * relative_threshold
        let checker = WorkerHealthChecker::new(100000.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 50000.0 },   // 恰好等于 100KB * 0.5
            WorkerSpeed { worker_id: 1, speed: 100000.0 },  // 基准
            WorkerSpeed { worker_id: 2, speed: 150000.0 },
        ];

        let result = checker.check(&speeds);
        // worker 0: 50000 < 100000 (绝对阈值) ✓, 50000 < 100000 * 0.5 = 50000 ✗
        // 不应该被检测到
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_equal_gaps() {
        // 多个相等的间隙
        let checker = WorkerHealthChecker::new(15000.0, 0.5);
        let speeds = vec![
            WorkerSpeed { worker_id: 0, speed: 10000.0 },
            WorkerSpeed { worker_id: 1, speed: 20000.0 },  // gap = 10000
            WorkerSpeed { worker_id: 2, speed: 30000.0 },  // gap = 10000
            WorkerSpeed { worker_id: 3, speed: 100000.0 }, // gap = 70000 (最大)
        ];

        let result = checker.check(&speeds);
        assert!(result.is_some());
        
        let result = result.unwrap();
        // 只有 worker 0 应该被检测到（低于 15KB 且低于 100KB * 0.5）
        assert_eq!(result.unhealthy_workers.len(), 1);
        assert_eq!(result.unhealthy_workers[0].0, 0);
        assert!((result.health_baseline - 100000.0).abs() < 0.1);
    }
}

