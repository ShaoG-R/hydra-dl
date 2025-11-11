#[cfg(test)]
mod tests;

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use portable_atomic::AtomicU128;
use smallring::atomic::{AtomicRingBuf, AtomicElement};

#[derive(Clone, Copy)]
pub struct Sample {
    timestamp_ns: NonZeroU64,
    bytes: u64,
}

impl Sample {
    /// 创建采样点
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - 时间戳（纳秒）
    /// * `bytes` - 字节数
    ///
    /// # Returns
    ///
    /// `Sample` - 创建的采样点
    #[inline]
    pub fn new(timestamp_ns: u64, bytes: u64) -> Self {
        let timestamp_ns = unsafe {
            NonZeroU64::new(timestamp_ns).unwrap_or(NonZeroU64::new_unchecked(1))
        };
        Self { timestamp_ns, bytes }
    }

    /// 从打包的 u128 数据创建采样点
    ///
    /// # Arguments
    ///
    /// * `packed` - 打包的 u128 数据
    ///
    /// # Returns
    ///
    /// `Sample` - 解包后的采样点
    #[inline]
    pub fn from_u128(packed: u128) -> Self {
        Self::new((packed >> 64) as u64, packed as u64)
    }

    /// 将采样点转换为打包的 u128 数据
    ///
    /// # Returns
    ///
    /// `u128` - 打包后的 u128 数据
    #[inline]
    pub fn into_u128(self) -> u128 {
        ((self.timestamp_ns.get() as u128) << 64) | (self.bytes as u128)
    }

}


/// 采样点数据
///
/// 使用 128 位原子类型存储时间戳和字节数，确保原子化读写。
///
/// # 数据布局
///
/// - 高 64 位：时间戳（纳秒）
/// - 低 64 位：字节数
#[derive(Debug)]
pub(crate) struct SampleRaw {
    /// 打包的数据：高 64 位为时间戳，低 64 位为字节数
    data: AtomicU128,
}

/// 为 SampleRaw 实现 AtomicElement trait
/// 
/// 这允许 SampleRaw 直接用于 AtomicRingBuf，而不需要额外的包装
impl AtomicElement for SampleRaw {
    type Primitive = Sample;
    
    #[inline]
    fn load(&self, order: Ordering) -> Self::Primitive {
        let packed = self.data.load(order);
        Sample::from_u128(packed)
    }
    
    #[inline]
    fn store(&self, val: Self::Primitive, order: Ordering) {
        let packed = val.into_u128();
        self.data.store(packed, order);
    }
    
    #[inline]
    fn swap(&self, val: Self::Primitive, order: Ordering) -> Self::Primitive {
        let new_packed = val.into_u128();
        let old_packed = self.data.swap(new_packed, order);
        Sample::from_u128(old_packed)
    }
}

/// 线性回归所需的最小采样点数
const MIN_SAMPLES_FOR_REGRESSION: usize = 3;

/// 速度计算器（基于环形缓冲区和线性回归）
/// 
/// 使用固定大小的环形缓冲区保存最近 N 个采样点，通过最小二乘法线性回归计算速度。
/// 相比传统的两点差分法，线性回归提供更平滑的速度曲线，具有更强的抗噪声能力。
/// 
/// # 核心特性
/// 
/// - **环形缓冲区**：动态大小（根据时间窗口和采样间隔计算），自动覆盖旧数据
/// - **线性回归**：使用最小二乘法计算速度（斜率）
/// - **完全无锁**：使用原子操作，支持高并发场景
/// - **Send + Sync**：所有字段都是原子类型或不可变引用
/// 
/// # 采样策略
/// 
/// 调用方应定期调用 `record_sample()` 记录采样点（间隔由 `sample_interval` 配置）。
/// 缓冲区满时会自动覆盖最旧的采样点。
/// 
/// # 速度计算
/// 
/// 使用最小二乘法线性回归：
/// ```text
/// 速度 = Σ[(t_i - t_avg)(b_i - b_avg)] / Σ[(t_i - t_avg)²]
/// ```
/// 
/// 当有效采样点少于 3 个时，降级使用平均速度。
/// 
/// # 线程安全
/// 
/// 所有方法都是线程安全的，可以被多个线程并发调用。使用 `AtomicU128` 保证
/// 每个采样点的时间戳和字节数原子化读写，不存在数据不一致的问题。
#[derive(Debug)]
pub(crate) struct SpeedCalculator {
    /// 环形缓冲区，存储最近的采样点
    ring_buffer: AtomicRingBuf<SampleRaw, 64, true>,
    /// 线性回归所需的最小采样点数
    min_samples_for_regression: usize,
    /// 瞬时速度的时间窗口
    instant_speed_window: Duration,
    /// 窗口平均速度的时间窗口
    window_avg_duration: Duration,
    /// 下载开始时间（只初始化一次）
    start_time: OnceLock<Instant>,
    /// 上次采样时间戳（纳秒，相对于 start_time）
    last_sample_timestamp_ns: AtomicU64,
    /// 采样间隔（纳秒）
    sample_interval_ns: u64,
}

impl SpeedCalculator {
    /// 从速度配置创建速度计算器
    /// 
    /// # Arguments
    /// 
    /// * `config` - 速度配置
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::config::SpeedConfig;
    /// 
    /// let config = SpeedConfig::default();
    /// let calculator = SpeedCalculator::from_config(&config);
    /// ```
    /// 
    /// # 缓冲区大小计算
    /// 
    /// 根据最大时间窗口和采样间隔计算所需的缓冲区大小：
    /// ```text
    /// buffer_size = max(instant_speed_window, window_avg_duration) / sample_interval * buffer_size_margin
    /// ```
    /// 
    /// 例如，对于 5 秒窗口、100ms 采样间隔、1.2 倍余量：
    /// - 理论需要：5000ms / 100ms = 50 个采样点
    /// - 实际分配：50 * 1.2 = 60 个采样点（增加 20% 余量）
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        let instant_speed_window = config.instant_speed_window();
        let window_avg_duration = config.window_avg_duration();
        let sample_interval = config.sample_interval();
        let buffer_size_margin = config.buffer_size_margin();
        
        // 计算所需的缓冲区大小
        let max_window_ns = instant_speed_window.as_nanos().max(window_avg_duration.as_nanos());
        let sample_interval_ns = sample_interval.as_nanos();
        let required_samples = (max_window_ns / sample_interval_ns) as usize;
        
        // 添加安全余量，并确保至少有 MIN_SAMPLES_FOR_REGRESSION * 2 个采样点
        let buffer_size = ((required_samples as f64 * buffer_size_margin) as usize)
            .max(MIN_SAMPLES_FOR_REGRESSION * 2);
        
        // 创建环形缓冲区
        let ring_buffer = AtomicRingBuf::new(buffer_size);
        
        Self {
            ring_buffer,
            min_samples_for_regression: MIN_SAMPLES_FOR_REGRESSION,
            instant_speed_window,
            window_avg_duration,
            start_time: OnceLock::new(),
            last_sample_timestamp_ns: AtomicU64::new(0),
            sample_interval_ns: sample_interval.as_nanos() as u64,
        }
    }

    /// 记录采样点（无锁并发写入）
    /// 
    /// 根据配置的采样间隔自动判断是否需要记录采样点。
    /// 多个线程可以并发调用此方法，通过原子操作保证线程安全。
    /// 
    /// # Arguments
    /// 
    /// * `current_bytes` - 当前累计下载的总字节数
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// let calculator = SpeedCalculator::from_config(&config);
    /// calculator.record_sample(1024 * 1024);
    /// ```
    pub(crate) fn record_sample(&self, current_bytes: u64) {
        // 第一次调用时初始化开始时间
        let start_time = *self.start_time.get_or_init(Instant::now);
        
        // 自动采样逻辑：根据配置的采样间隔记录采样点
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;
        let last_sample_ns = self.last_sample_timestamp_ns.load(Ordering::Relaxed);

        // 检查是否需要采样（距离上次采样超过配置的采样间隔，或者是首次采样）
        if last_sample_ns == 0 || current_elapsed_ns.saturating_sub(last_sample_ns) >= self.sample_interval_ns {
            let sample = Sample::new(current_elapsed_ns, current_bytes);

            // 尝试更新采样时间戳（使用 compare_exchange 避免重复采样）
            // 允许多个线程竞争，只有一个会成功，这样可以避免过度采样
            if self.last_sample_timestamp_ns.compare_exchange(
                last_sample_ns,
                sample.timestamp_ns.get(),
                Ordering::Relaxed,
                Ordering::Relaxed
            ).is_ok() {
                // 成功获取采样权限，记录采样点
                let _ = self.ring_buffer.push(sample, Ordering::Relaxed);
            }
        }
    }
    
    /// 读取最近的有效采样点
    /// 
    /// 从环形缓冲区读取所有有效的采样点（时间戳不为 0）。
    /// 返回的采样点按时间排序（最旧的在前，最新的在后）。
    /// 
    /// # Returns
    /// 
    /// `Vec<(时间戳秒, 字节数)>`
    fn read_recent_samples(&self) -> Vec<(f64, f64)> {
        // 使用 read_all 方法从环形缓冲区读取所有采样点
        let all_samples = self.ring_buffer.read_all(Ordering::Acquire);
        
        // 转换为 (时间戳秒, 字节数) 格式，并过滤掉时间戳为 0 的采样点
        let mut samples: Vec<(f64, f64)> = all_samples
            .into_iter()
            .map(|sample| {
                let timestamp_secs = sample.timestamp_ns.get() as f64 / 1_000_000_000.0;
                (timestamp_secs, sample.bytes as f64)
            })
            .collect();
        
        // 按时间戳排序（从旧到新）
        samples.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        samples
    }
    
    /// 使用最小二乘法线性回归计算速度
    /// 
    /// 基于采样点计算最佳拟合直线的斜率（速度），使用最小二乘法：
    /// 
    /// ```text
    /// 速度 = Σ[(t_i - t_avg)(b_i - b_avg)] / Σ[(t_i - t_avg)²]
    /// ```
    /// 
    /// # Arguments
    /// 
    /// * `samples` - 采样点列表 `(时间戳秒, 字节数)`
    /// 
    /// # Returns
    /// 
    /// 速度（bytes/s）。如果采样点不足或时间跨度为 0，返回 0.0。
    fn linear_regression(&self, samples: &[(f64, f64)]) -> f64 {
        let n = samples.len();
        
        // 采样点不足，无法进行回归
        if n < self.min_samples_for_regression {
            // 降级：使用最简单的平均速度
            if n >= 2 {
                let (t_first, b_first) = samples[0];
                let (t_last, b_last) = samples[n - 1];
                let delta_t = t_last - t_first;
                let delta_b = b_last - b_first;
                if delta_t > 0.0 {
                    return delta_b / delta_t;
                }
            }
            return 0.0;
        }
        
        // 计算平均值
        let sum_t: f64 = samples.iter().map(|(t, _)| t).sum();
        let sum_b: f64 = samples.iter().map(|(_, b)| b).sum();
        let t_avg = sum_t / n as f64;
        let b_avg = sum_b / n as f64;
        
        // 计算协方差和方差
        let mut cov_tb = 0.0;  // Σ[(t_i - t_avg)(b_i - b_avg)]
        let mut var_t = 0.0;   // Σ[(t_i - t_avg)²]
        
        for (t, b) in samples {
            let t_diff = t - t_avg;
            let b_diff = b - b_avg;
            cov_tb += t_diff * b_diff;
            var_t += t_diff * t_diff;
        }
        
        // 计算斜率（速度）
        if var_t > 0.0 {
            cov_tb / var_t
        } else {
            0.0
        }
    }
    
    /// 读取最近时间窗口内的采样点
    /// 
    /// # Arguments
    /// 
    /// * `window` - 时间窗口
    /// * `start_time` - 下载开始时间
    /// 
    /// # Returns
    /// 
    /// `Vec<(时间戳秒, 字节数)>`
    fn read_samples_in_window(&self, window: Duration, start_time: Instant) -> Vec<(f64, f64)> {
        let current_elapsed_secs = start_time.elapsed().as_secs_f64();
        let window_start_secs = current_elapsed_secs - window.as_secs_f64();
        
        let all_samples = self.read_recent_samples();
        
        // 只保留时间窗口内的采样点
        all_samples
            .into_iter()
            .filter(|(t, _)| *t >= window_start_secs)
            .collect()
    }
    
    /// 获取瞬时下载速度（bytes/s）
    /// 
    /// 基于瞬时速度时间窗口内的采样点，使用线性回归计算速度。
    /// 相比旧版本的两点差分法，线性回归提供更平滑、更稳定的速度估计。
    /// 
    /// # Returns
    /// 
    /// `(瞬时速度, 是否有效)`：
    /// - 瞬时速度：基于线性回归计算的速度（bytes/s）
    /// - 是否有效：true 表示有足够的采样点，false 表示采样点不足
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// let calculator = SpeedCalculator::from_config(&config);
    /// let (speed, valid) = calculator.get_instant_speed();
    /// if valid {
    ///     println!("瞬时速度: {:.2} MB/s", speed / 1024.0 / 1024.0);
    /// }
    /// ```
    pub(crate) fn get_instant_speed(&self) -> (f64, bool) {
        let start_time = self.start_time.get().copied().unwrap_or_else(Instant::now);
        // 读取瞬时速度窗口内的采样点
        let samples = self.read_samples_in_window(self.instant_speed_window, start_time);
        
        // 线性回归计算速度
        let speed = self.linear_regression(&samples);
        let valid = samples.len() >= self.min_samples_for_regression;
        
        (speed, valid)
    }

    /// 获取窗口平均下载速度（bytes/s）
    /// 
    /// 基于窗口平均时间窗口内的采样点，使用线性回归计算速度。
    /// 相比旧版本的两点差分法，线性回归提供更平滑、更稳定的速度估计。
    /// 
    /// # Returns
    /// 
    /// `(窗口平均速度, 是否有效)`：
    /// - 窗口平均速度：基于线性回归计算的速度（bytes/s）
    /// - 是否有效：true 表示有足够的采样点，false 表示采样点不足
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// let calculator = SpeedCalculator::from_config(&config);
    /// let (speed, valid) = calculator.get_window_avg_speed();
    /// if valid {
    ///     println!("窗口平均速度: {:.2} MB/s", speed / 1024.0 / 1024.0);
    /// }
    /// ```
    pub(crate) fn get_window_avg_speed(&self) -> (f64, bool) {
        let start_time = self.start_time.get().copied().unwrap_or_else(Instant::now);
        // 读取窗口平均速度窗口内的采样点
        let samples = self.read_samples_in_window(self.window_avg_duration, start_time);
        
        // 线性回归计算速度
        let speed = self.linear_regression(&samples);
        let valid = samples.len() >= self.min_samples_for_regression;
        
        (speed, valid)
    }
}