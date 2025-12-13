//! 核心算法模块
//!
//! 包含速度计算的核心算法逻辑，独立于 SpeedCalculator 状态管理，便于单元测试。

use net_bytes::DownloadSpeed;
use std::num::NonZeroU64;
use std::time::Duration;

/// 线性回归所需的最小采样点数
pub(crate) const MIN_SAMPLES_FOR_REGRESSION: usize = 3;

/// 缓冲区大小的最小值（确保足够的采样点用于回归）
pub(crate) const MIN_BUFFER_SIZE: usize = MIN_SAMPLES_FOR_REGRESSION * 3;

/// 缓冲区大小的最大值（防止过度内存占用）
pub(crate) const MAX_BUFFER_SIZE: usize = 512;

#[derive(Clone, Copy, Debug)]
pub struct Sample {
    pub(crate) timestamp_ns: NonZeroU64,
    pub(crate) bytes: u64,
}

impl Sample {
    /// 创建采样点
    #[inline]
    pub fn new(timestamp_ns: u64, bytes: u64) -> Self {
        let timestamp_ns =
            unsafe { NonZeroU64::new(timestamp_ns).unwrap_or(NonZeroU64::new_unchecked(1)) };
        Self {
            timestamp_ns,
            bytes,
        }
    }
}

/// 使用 Theil-Sen 估计器进行鲁棒回归计算速度
///
/// Theil-Sen 估计器是一种非参数的鲁棒回归方法，对异常值不敏感。
/// 它计算所有点对斜率的中位数，而不是使用最小二乘法。
///
/// # 算法原理
///
/// 1. 计算所有点对 (i, j) 的斜率：slope_ij = (b_j - b_i) / (t_j - t_i)
/// 2. 返回所有斜率的中位数
/// 3. 对异常值的容忍度：最多可容忍 50% 的异常值
/// 4. 使用 i128 和 u64 保留纳秒精度，避免浮点精度丧失
///
/// # Arguments
///
/// * `samples` - 采样点列表 `(时间戳纳秒, 字节数)`
/// * `min_samples_for_regression` - 线性回归所需的最小采样点数
///
/// # Returns
///
/// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
pub(crate) fn theil_sen_regression(
    samples: &[(i128, u64)],
    min_samples_for_regression: usize,
) -> Option<DownloadSpeed> {
    let n = samples.len();

    // 采样点不足，无法进行回归
    if n < min_samples_for_regression {
        // 降级：使用最简单的平均速度
        if n >= 2 {
            let (t_first, b_first) = samples[0];
            let (t_last, b_last) = samples[n - 1];
            let delta_t_ns = t_last - t_first;
            let delta_b = b_last as i128 - b_first as i128;

            if delta_t_ns > 0 && delta_b >= 0 {
                // 允许零速度（delta_b = 0）的情况
                if let (Ok(delta_t), Ok(delta_b)) =
                    (u64::try_from(delta_t_ns), u64::try_from(delta_b))
                {
                    let duration = Duration::from_nanos(delta_t);
                    return Some(DownloadSpeed::new(delta_b, duration));
                }
            }
        }
        return None;
    }

    // 计算所有点对的斜率，预分配容量 n*(n-1)/2
    let mut speed_pairs = Vec::with_capacity(n * (n - 1) / 2);

    for i in 0..n {
        for j in (i + 1)..n {
            let (t_i, b_i) = samples[i];
            let (t_j, b_j) = samples[j];
            let delta_t_ns = t_j - t_i;
            let delta_b = b_j as i128 - b_i as i128;

            // 允许零速度（delta_b >= 0），但时间戳必须不同
            if delta_t_ns > 0
                && delta_b >= 0
                && let (Ok(delta_t), Ok(delta_b)) =
                    (u64::try_from(delta_t_ns), u64::try_from(delta_b))
            {
                speed_pairs.push((delta_t, delta_b));
            }
        }
    }

    // 如果没有有效的点对，返回 None
    if speed_pairs.is_empty() {
        return None;
    }

    // 按速度（斜率）排序，而不是按时间排序
    // 这样才能正确计算中位数速度
    speed_pairs.sort_unstable_by(|a, b| {
        let speed_a = (a.1 as f64) / (a.0 as f64);
        let speed_b = (b.1 as f64) / (b.0 as f64);
        speed_a
            .partial_cmp(&speed_b)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let median_idx = speed_pairs.len() / 2;
    let (delta_t, delta_b) = if speed_pairs.len() % 2 == 0 && median_idx > 0 {
        // 偶数个元素：取中间两个的平均值
        let (t1, b1) = speed_pairs[median_idx - 1];
        let (t2, b2) = speed_pairs[median_idx];
        ((t1 + t2) / 2, (b1 + b2) / 2)
    } else {
        // 奇数个元素：取中间元素
        speed_pairs[median_idx]
    };

    // 创建 DownloadSpeed 实例
    let duration = Duration::from_nanos(delta_t);
    Some(DownloadSpeed::new(delta_b, duration))
}

/// 两点差分法计算速度
///
/// 使用采样序列的首尾两点计算平均速度。
pub(crate) fn two_point_speed(samples: &[(i128, u64)]) -> Option<DownloadSpeed> {
    if samples.len() < 2 {
        return None;
    }

    let (t_first, b_first) = samples[0];
    let (t_last, b_last) = samples[samples.len() - 1];
    let delta_t_ns = t_last - t_first;
    let delta_b = b_last as i128 - b_first as i128;

    if delta_t_ns > 0
        && delta_b >= 0
        && let (Ok(delta_t), Ok(delta_b)) = (u64::try_from(delta_t_ns), u64::try_from(delta_b))
    {
        let duration = Duration::from_nanos(delta_t);
        return Some(DownloadSpeed::new(delta_b, duration));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_new() {
        let sample = Sample::new(1000, 500);
        assert_eq!(sample.timestamp_ns.get(), 1000);
        assert_eq!(sample.bytes, 500);
    }

    #[test]
    fn test_sample_new_zero_timestamp() {
        // 0 时间戳应该变成 1
        let sample = Sample::new(0, 100);
        assert_eq!(sample.timestamp_ns.get(), 1);
    }

    #[test]
    fn test_theil_sen_regression_constant_speed() {
        // 模拟恒定速度 1000 bytes/s
        let samples: Vec<(i128, u64)> = vec![
            (1, 0),
            (1_000_000_000, 1000),
            (2_000_000_000, 2000),
            (3_000_000_000, 3000),
            (4_000_000_000, 4000),
        ];

        let speed = theil_sen_regression(&samples, MIN_SAMPLES_FOR_REGRESSION);
        assert!(speed.is_some());
    }

    #[test]
    fn test_theil_sen_regression_with_outliers() {
        // 1000 bytes/s 恒定速度，带一个异常值
        let samples: Vec<(i128, u64)> = vec![
            (1, 0),
            (1_000_000_000, 1000),
            (2_000_000_000, 1_000_000), // 异常值
            (3_000_000_000, 3000),
            (4_000_000_000, 4000),
        ];

        let speed = theil_sen_regression(&samples, MIN_SAMPLES_FOR_REGRESSION);
        assert!(speed.is_some());
    }

    #[test]
    fn test_theil_sen_regression_insufficient_samples() {
        // 只有 2 个采样点，应该降级到平均速度
        let samples: Vec<(i128, u64)> = vec![(1, 0), (1_000_000_000, 1000)];

        let speed = theil_sen_regression(&samples, MIN_SAMPLES_FOR_REGRESSION);
        assert!(speed.is_some());
    }

    #[test]
    fn test_theil_sen_regression_single_sample() {
        // 只有 1 个采样点
        let samples: Vec<(i128, u64)> = vec![(1, 0)];

        let speed = theil_sen_regression(&samples, MIN_SAMPLES_FOR_REGRESSION);
        assert!(speed.is_none());
    }

    #[test]
    fn test_theil_sen_regression_empty() {
        let samples: Vec<(i128, u64)> = vec![];

        let speed = theil_sen_regression(&samples, MIN_SAMPLES_FOR_REGRESSION);
        assert!(speed.is_none());
    }

    #[test]
    fn test_two_point_speed_basic() {
        let samples: Vec<(i128, u64)> = vec![(0, 0), (1_000_000_000, 1000)];

        let speed = two_point_speed(&samples);
        assert!(speed.is_some());
    }

    #[test]
    fn test_two_point_speed_single_sample() {
        let samples: Vec<(i128, u64)> = vec![(0, 0)];

        let speed = two_point_speed(&samples);
        assert!(speed.is_none());
    }

    #[test]
    fn test_two_point_speed_zero_download() {
        // 时间过了但没有下载任何数据
        let samples: Vec<(i128, u64)> = vec![(0, 0), (1_000_000_000, 0)];

        let speed = two_point_speed(&samples);
        assert!(speed.is_some()); // 零速度应该是有效的
    }
}
