use net_bytes::{DownloadAcceleration, DownloadSpeed};
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct Sample {
    timestamp_ns: NonZeroU64,
    bytes: u64,
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

/// 线性回归所需的最小采样点数
const MIN_SAMPLES_FOR_REGRESSION: usize = 3;

/// 缓冲区大小的最小值（确保足够的采样点用于回归）
const MIN_BUFFER_SIZE: usize = MIN_SAMPLES_FOR_REGRESSION * 3;

/// 缓冲区大小的最大值（防止过度内存占用）
const MAX_BUFFER_SIZE: usize = 512;

/// 速度计算器（基于环形缓冲区和线性回归）
///
/// 使用固定大小的环形缓冲区保存最近 N 个采样点，通过最小二乘法线性回归计算速度。
/// 相比传统的两点差分法，线性回归提供更平滑的速度曲线，具有更强的抗噪声能力。
///
/// # 核心特性
///
/// - **环形缓冲区**：动态大小（根据时间窗口和采样间隔计算），自动覆盖旧数据
/// - **线性回归**：使用最小二乘法计算速度（斜率）
/// - **Send + Sync**：所有字段都是 Send，record_sample 需要 &mut self
#[derive(Debug, Clone)]
pub(crate) struct SpeedCalculator {
    /// 采样点缓冲区
    samples: VecDeque<Sample>,
    /// 缓冲区最大容量
    max_samples: usize,
    /// 线性回归所需的最小采样点数
    min_samples_for_regression: usize,
    /// 瞬时速度的时间窗口
    instant_speed_window: Duration,
    /// 窗口平均速度的时间窗口
    window_avg_duration: Duration,
    /// 下载开始时间（只初始化一次）
    start_time: Option<Instant>,
    /// 采样间隔（纳秒）
    sample_interval_ns: u64,
}

impl SpeedCalculator {
    /// 从速度配置创建速度计算器
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        let instant_speed_window = config.instant_speed_window();
        let window_avg_duration = config.window_avg_duration();
        let sample_interval = config.sample_interval();
        let buffer_size_margin = config.buffer_size_margin();

        // 计算所需的缓冲区大小
        let max_window_ns = instant_speed_window
            .as_nanos()
            .max(window_avg_duration.as_nanos());
        let sample_interval_ns = sample_interval.as_nanos();
        let required_samples = (max_window_ns / sample_interval_ns) as usize;

        // 优化的缓冲区大小计算
        let base_size = (required_samples as f64 * buffer_size_margin) as usize;
        let max_samples = base_size.max(MIN_BUFFER_SIZE).min(MAX_BUFFER_SIZE);

        Self {
            samples: VecDeque::with_capacity(max_samples),
            max_samples,
            min_samples_for_regression: MIN_SAMPLES_FOR_REGRESSION,
            instant_speed_window,
            window_avg_duration,
            start_time: None,
            sample_interval_ns: sample_interval.as_nanos() as u64,
        }
    }

    /// 记录采样点
    ///
    /// 根据配置的采样间隔自动判断是否需要记录采样点。
    ///
    /// # Arguments
    ///
    /// * `current_bytes` - 当前累计下载的总字节数
    pub(crate) fn record_sample(&mut self, current_bytes: u64) {
        // 第一次调用时初始化开始时间
        let start_time = *self.start_time.get_or_insert_with(Instant::now);

        // 自动采样逻辑：根据配置的采样间隔记录采样点
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;

        // 数据一致性检查
        if let Some(last_sample) = self.samples.back() {
            // 1. 如果当前字节数小于上次记录的字节数，说明这是滞后的数据，直接丢弃
            if current_bytes < last_sample.bytes {
                return;
            }

            // 2. 检查是否需要采样（距离上次采样超过配置的采样间隔）
            let last_sample_ns = last_sample.timestamp_ns.get();
            if current_elapsed_ns.saturating_sub(last_sample_ns) < self.sample_interval_ns {
                return;
            }
        }

        let sample = Sample::new(current_elapsed_ns, current_bytes);

        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    /// 读取最近的有效采样点（保留纳秒精度）
    fn read_recent_samples(&self) -> Vec<(i128, u64)> {
        self.samples
            .iter()
            .map(|sample| {
                let timestamp_ns = sample.timestamp_ns.get() as i128;
                (timestamp_ns, sample.bytes)
            })
            .collect()
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
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    fn theil_sen_regression(&self, samples: &[(i128, u64)]) -> Option<DownloadSpeed> {
        let n = samples.len();

        // 采样点不足，无法进行回归
        if n < self.min_samples_for_regression {
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

        // 计算所有点对的斜率
        let mut speed_pairs = Vec::new();

        for i in 0..n {
            for j in (i + 1)..n {
                let (t_i, b_i) = samples[i];
                let (t_j, b_j) = samples[j];
                let delta_t_ns = t_j - t_i;
                let delta_b = b_j as i128 - b_i as i128;

                // 允许零速度（delta_b >= 0），但时间戳必须不同
                if delta_t_ns > 0 && delta_b >= 0 {
                    if let (Ok(delta_t), Ok(delta_b)) =
                        (u64::try_from(delta_t_ns), u64::try_from(delta_b))
                    {
                        speed_pairs.push((delta_t, delta_b as u64));
                    }
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

    /// 读取最近时间窗口内的采样点（保留纳秒精度）
    ///
    /// # Arguments
    ///
    /// * `window` - 时间窗口
    /// * `start_time` - 下载开始时间
    ///
    /// # Returns
    ///
    /// `Vec<(时间戳纳秒, 字节数)>`
    #[inline]
    fn read_samples_in_window(&self, window: Duration, start_time: Instant) -> Vec<(i128, u64)> {
        let current_elapsed_ns = start_time.elapsed().as_nanos() as i128;
        let window_start_ns = current_elapsed_ns - window.as_nanos() as i128;

        let all_samples = self.read_recent_samples();

        // 只保留时间窗口内的采样点
        all_samples
            .into_iter()
            .filter(|(t, _)| *t >= window_start_ns)
            .collect()
    }

    /// 获取瞬时下载速度
    ///
    /// 基于瞬时速度时间窗口内的采样点，使用 Theil-Sen 鲁棒回归计算速度。
    /// 相比最小二乘法，Theil-Sen 估计器对网络波动和异常值更加鲁棒，
    /// 能够容忍最多 50% 的异常采样点。
    ///
    /// # Returns
    ///
    /// - `Some(DownloadSpeed)`: 如果成功计算出有效速度
    /// - `None`: 如果采样点不足或速度计算失败
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.get_start_time()?;
        let samples = self.read_samples_in_window(self.instant_speed_window, start_time);
        self.theil_sen_regression(&samples)
    }

    /// 获取窗口平均下载速度
    ///
    /// 基于窗口平均时间窗口内的采样点，使用 Theil-Sen 鲁棒回归计算速度。
    /// 相比最小二乘法，Theil-Sen 估计器对网络波动和异常值更加鲁棒，
    /// 能够容忍最多 50% 的异常采样点。
    ///
    /// # Returns
    ///
    /// - `Some(DownloadSpeed)`: 如果成功计算出有效速度
    /// - `None`: 如果采样点不足或速度计算失败
    pub fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.get_start_time()?;
        let samples = self.read_samples_in_window(self.window_avg_duration, start_time);
        self.theil_sen_regression(&samples)
    }

    /// 获取瞬时下载加速度（bytes/s²）
    ///
    /// 基于瞬时速度时间窗口内的采样点，计算速度的变化率。
    /// 正值表示加速，负值表示减速。
    ///
    /// # Returns
    ///
    /// - `Some(acceleration)`: 如果成功计算出有效加速度
    /// - `None`: 如果采样点不足
    pub fn get_instant_acceleration(&self) -> Option<DownloadAcceleration> {
        let samples =
            self.read_samples_in_window(self.instant_speed_window, self.get_start_time()?);
        if samples.len() < 4 {
            return None;
        }

        let n = samples.len();
        let mid = n / 2;

        // 前半段速度
        let (t_first, b_first) = samples[0];
        let (t_mid, b_mid) = samples[mid - 1];
        let delta_t1_ns = t_mid - t_first;
        let delta_b1 = b_mid as i128 - b_first as i128;

        let speed1 = if delta_t1_ns > 0 {
            (delta_b1 as f64 * 1_000_000_000.0) / (delta_t1_ns as f64)
        } else {
            0.0
        } as u64;

        // 后半段速度
        let (t_mid2, b_mid2) = samples[mid];
        let (t_last, b_last) = samples[n - 1];
        let delta_t2_ns = t_last - t_mid2;
        let delta_b2 = b_last as i128 - b_mid2 as i128;

        let speed2 = if delta_t2_ns > 0 {
            (delta_b2 as f64 * 1_000_000_000.0) / (delta_t2_ns as f64)
        } else {
            0.0
        } as u64;

        // 计算总时间
        let total_time_ns = t_last - t_first;
        if total_time_ns <= 0 {
            return None;
        }

        let duration = Duration::from_nanos(total_time_ns as u64);
        Some(DownloadAcceleration::new(speed1, speed2, duration))
    }

    /// 获取下载开始时间
    ///
    /// # Returns
    ///
    /// `Option<Instant>` - 下载开始时间，如果尚未初始化则返回 None
    #[inline]
    pub(crate) fn get_start_time(&self) -> Option<Instant> {
        self.start_time
    }

    /// 清空采样点缓冲区
    ///
    /// 清空所有采样点并重置开始时间，用于在新的下载任务开始时重置状态。
    /// 下次调用 `record_sample` 时会重新初始化开始时间。
    #[inline]
    pub(crate) fn clear_samples(&mut self) {
        self.samples.clear();
        self.start_time = None;
    }
}

#[cfg(test)]
mod tests;
