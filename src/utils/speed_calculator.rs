use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use portable_atomic::AtomicU128;

/// 线性回归所需的最小采样点数
const MIN_SAMPLES_FOR_REGRESSION: usize = 3;

/// 采样点
/// 
/// 包含一个时间戳和对应的累计字节数，用于速度计算
/// 
/// # 线程安全
/// 
/// 使用 `AtomicU128` 保证时间戳和字节数的读写完全原子化，避免数据不一致。
/// 
/// # 数据布局
/// 
/// - 高 64 位：时间戳（纳秒，相对于 start_time）
/// - 低 64 位：累计下载字节数
#[derive(Debug)]
struct Sample {
    /// 原子化存储：高 64 位为 timestamp_ns，低 64 位为 bytes
    data: AtomicU128,
}

impl Sample {
    /// 创建新的采样点（初始值为 0）
    fn new() -> Self {
        Self {
            data: AtomicU128::new(0),
        }
    }
    
    /// 读取采样点数据（原子化快照）
    /// 
    /// # Returns
    /// 
    /// `Option<(时间戳纳秒, 字节数)>`，如果时间戳为 0 则返回 None
    fn read(&self) -> Option<(u64, u64)> {
        let packed = self.data.load(Ordering::Relaxed);
        
        // 解包：高 64 位为 timestamp_ns，低 64 位为 bytes
        let timestamp_ns = (packed >> 64) as u64;
        let bytes = packed as u64;
        
        if timestamp_ns == 0 {
            return None;
        }
        
        Some((timestamp_ns, bytes))
    }
    
    /// 写入采样点数据（原子化写入）
    fn write(&self, timestamp_ns: u64, bytes: u64) {
        // 打包：高 64 位为 timestamp_ns，低 64 位为 bytes
        let packed = ((timestamp_ns as u128) << 64) | (bytes as u128);
        self.data.store(packed, Ordering::Relaxed);
    }
}

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
    samples: Box<[Sample]>,
    /// 写入索引（单调递增，通过模运算映射到环形缓冲区）
    write_index: AtomicU64,
    /// 线性回归所需的最小采样点数
    min_samples_for_regression: usize,
    /// 瞬时速度的时间窗口
    instant_speed_window: Duration,
    /// 窗口平均速度的时间窗口
    window_avg_duration: Duration,
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
        let mut samples = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            samples.push(Sample::new());
        }
        
        Self {
            samples: samples.into_boxed_slice(),
            write_index: AtomicU64::new(0),
            min_samples_for_regression: MIN_SAMPLES_FOR_REGRESSION,
            instant_speed_window,
            window_avg_duration,
        }
    }

    /// 记录采样点（无锁并发写入）
    /// 
    /// 将当前的时间戳和字节数写入环形缓冲区。多个线程可以并发调用此方法，
    /// 通过 fetch_add 原子操作获取不同的写入位置。
    /// 
    /// # Arguments
    /// 
    /// * `current_bytes` - 当前累计下载的总字节数
    /// * `start_time` - 下载开始时间
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// let calculator = SpeedCalculator::new(
    ///     Duration::from_secs(1),
    ///     Duration::from_secs(5),
    /// );
    /// 
    /// let start_time = Instant::now();
    /// calculator.record_sample(1024 * 1024, start_time);
    /// ```
    pub(crate) fn record_sample(&self, current_bytes: u64, start_time: Instant) {
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;
        
        // 原子获取写入位置（单调递增）
        let index = self.write_index.fetch_add(1, Ordering::Relaxed);
        
        // 映射到环形缓冲区（模运算）
        let slot_index = (index % self.samples.len() as u64) as usize;
        let slot = &self.samples[slot_index];
        
        // 原子化写入采样点（时间戳和字节数同时更新）
        slot.write(current_elapsed_ns, current_bytes);
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
        let mut samples = Vec::with_capacity(self.samples.len());
        
        // 读取所有采样点
        for sample in self.samples.iter() {
            if let Some((timestamp_ns, bytes)) = sample.read() {
                // 转换为秒
                let timestamp_secs = timestamp_ns as f64 / 1_000_000_000.0;
                samples.push((timestamp_secs, bytes as f64));
            }
        }
        
        // 按时间戳排序（从旧到新）
        samples.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
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
    /// # Arguments
    /// 
    /// * `start_time` - 下载开始时间
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
    /// let calculator = SpeedCalculator::new(
    ///     Duration::from_secs(1),
    ///     Duration::from_secs(5),
    /// );
    /// 
    /// let start_time = Instant::now();
    /// let (speed, valid) = calculator.get_instant_speed(start_time);
    /// if valid {
    ///     println!("瞬时速度: {:.2} MB/s", speed / 1024.0 / 1024.0);
    /// }
    /// ```
    pub(crate) fn get_instant_speed(&self, start_time: Instant) -> (f64, bool) {
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
    /// # Arguments
    /// 
    /// * `start_time` - 下载开始时间
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
    /// let calculator = SpeedCalculator::new(
    ///     Duration::from_secs(1),
    ///     Duration::from_secs(5),
    /// );
    /// 
    /// let start_time = Instant::now();
    /// let (speed, valid) = calculator.get_window_avg_speed(start_time);
    /// if valid {
    ///     println!("窗口平均速度: {:.2} MB/s", speed / 1024.0 / 1024.0);
    /// }
    /// ```
    pub(crate) fn get_window_avg_speed(&self, start_time: Instant) -> (f64, bool) {
        // 读取窗口平均速度窗口内的采样点
        let samples = self.read_samples_in_window(self.window_avg_duration, start_time);
        
        // 线性回归计算速度
        let speed = self.linear_regression(&samples);
        let valid = samples.len() >= self.min_samples_for_regression;
        
        (speed, valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // 测试辅助常量
    const TEST_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
    const TEST_BUFFER_MARGIN: f64 = 1.2;

    #[test]
    fn test_sample_creation_and_read() {
        let sample = Sample::new();
        
        // 初始状态应该返回 None
        assert!(sample.read().is_none());
        
        // 写入数据后应该能读取
        sample.write(1000, 2048);
        let data = sample.read();
        assert!(data.is_some());
        assert_eq!(data.unwrap(), (1000, 2048));
    }

    #[test]
    fn test_ring_buffer_creation() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        // 验证缓冲区大小（5秒窗口 / 100ms采样 * 1.2 = 60）
        let expected_size = ((5_000 / 100) as f64 * TEST_BUFFER_MARGIN) as usize;
        assert_eq!(calculator.samples.len(), expected_size);
        assert_eq!(calculator.write_index.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_sample() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        
        // 记录第一个采样点
        calculator.record_sample(1024, start_time);
        assert_eq!(calculator.write_index.load(Ordering::Relaxed), 1);
        
        // 记录第二个采样点
        thread::sleep(Duration::from_millis(10));
        calculator.record_sample(2048, start_time);
        assert_eq!(calculator.write_index.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        let buffer_size = calculator.samples.len();
        
        // 写入超过缓冲区大小的采样点
        for i in 0..(buffer_size + 5) {
            calculator.record_sample((i * 1024) as u64, start_time);
            if i < buffer_size {
                thread::sleep(Duration::from_micros(100));
            }
        }
        
        // 验证写入索引继续递增
        assert_eq!(
            calculator.write_index.load(Ordering::Relaxed),
            (buffer_size + 5) as u64
        );
    }

    #[test]
    fn test_linear_regression_with_perfect_line() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        // 完美的线性数据：速度 = 1000 bytes/s
        let samples = vec![
            (0.0, 0.0),
            (1.0, 1000.0),
            (2.0, 2000.0),
            (3.0, 3000.0),
            (4.0, 4000.0),
        ];
        
        let speed = calculator.linear_regression(&samples);
        
        // 应该准确计算出速度 1000 bytes/s
        assert!((speed - 1000.0).abs() < 0.1, "速度应该接近 1000, 实际: {}", speed);
    }

    #[test]
    fn test_linear_regression_with_noise() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        // 带噪声的线性数据：速度约 1000 bytes/s
        let samples = vec![
            (0.0, 0.0),
            (1.0, 950.0),   // -50
            (2.0, 2100.0),  // +100
            (3.0, 2900.0),  // -100
            (4.0, 4050.0),  // +50
        ];
        
        let speed = calculator.linear_regression(&samples);
        
        // 线性回归应该给出接近 1000 的结果
        assert!(speed > 900.0 && speed < 1100.0, "速度应该接近 1000, 实际: {}", speed);
    }

    #[test]
    fn test_linear_regression_insufficient_samples() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        // 只有 2 个采样点，少于最小要求
        let samples = vec![
            (0.0, 0.0),
            (1.0, 1000.0),
        ];
        
        let speed = calculator.linear_regression(&samples);
        
        // 应该降级使用平均速度
        assert!((speed - 1000.0).abs() < 0.1, "速度应该是 1000, 实际: {}", speed);
    }

    #[test]
    fn test_linear_regression_zero_variance() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        // 所有采样点时间相同（方差为 0）
        let samples = vec![
            (1.0, 100.0),
            (1.0, 200.0),
            (1.0, 300.0),
        ];
        
        let speed = calculator.linear_regression(&samples);
        
        // 应该返回 0
        assert_eq!(speed, 0.0);
    }

    #[test]
    fn test_instant_speed_with_samples() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(200))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(25).unwrap()) // 5秒
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        
        // 记录多个采样点，模拟 1 MB/s 的速度
        for i in 0..5 {
            let elapsed_ms = i * 50;
            thread::sleep(Duration::from_millis(50));
            let bytes = (elapsed_ms as u64 * 1024 * 1024) / 1000; // 1 MB/s
            calculator.record_sample(bytes, start_time);
        }
        
        // 获取瞬时速度（最近 200ms 窗口）
        let (speed, valid) = calculator.get_instant_speed(start_time);
        
        if valid {
            let speed_mbs = speed / 1024.0 / 1024.0;
            // 速度应该接近 1 MB/s
            assert!(
                speed_mbs > 0.5 && speed_mbs < 1.5,
                "速度应该接近 1 MB/s, 实际: {:.2} MB/s",
                speed_mbs
            );
        }
    }

    #[test]
    fn test_window_avg_speed_with_samples() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        
        // 记录多个采样点
        for i in 0..5 {
            thread::sleep(Duration::from_millis(50));
            let bytes = i as u64 * 100 * 1024; // 变化的字节数
            calculator.record_sample(bytes, start_time);
        }
        
        // 获取窗口平均速度
        let (speed, valid) = calculator.get_window_avg_speed(start_time);
        
        if valid {
            assert!(speed > 0.0, "速度应该大于 0");
        }
    }

    #[test]
    fn test_concurrent_record_sample() {
        use std::sync::Arc;
        use crate::config::SpeedConfigBuilder;
        
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = Arc::new(SpeedCalculator::from_config(&config));
        
        let start_time = Instant::now();
        let mut handles = vec![];
        
        // 启动 4 个线程，每个记录 10 个采样点
        for thread_id in 0..4 {
            let calc = Arc::clone(&calculator);
            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let bytes = (thread_id * 10 + i) * 1024;
                    calc.record_sample(bytes as u64, start_time);
                    thread::sleep(Duration::from_micros(100));
                }
            });
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        // 验证所有采样点都已记录
        assert_eq!(calculator.write_index.load(Ordering::Relaxed), 40);
    }

    #[test]
    fn test_read_samples_in_window() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(100))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(50).unwrap()) // 5秒
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        
        // 记录一些采样点
        for i in 0..5 {
            thread::sleep(Duration::from_millis(30));
            calculator.record_sample((i * 1024) as u64, start_time);
        }
        
        // 读取最近 100ms 窗口内的采样点
        let samples = calculator.read_samples_in_window(Duration::from_millis(100), start_time);
        
        // 应该只包含最近的采样点
        assert!(samples.len() > 0, "应该有采样点");
        assert!(samples.len() <= 5, "采样点数量应该合理");
    }

    #[test]
    fn test_backward_compatibility() {
        // 验证 API 仍然可用
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        
        // 记录一些采样点
        calculator.record_sample(1024, start_time);
        thread::sleep(Duration::from_millis(50));
        calculator.record_sample(2048, start_time);
        
        // API 应该正常工作
        let (_speed, _valid) = calculator.get_instant_speed(start_time);
        let (_speed2, _valid2) = calculator.get_window_avg_speed(start_time);
    }

    #[test]
    fn test_buffer_size_calculation() {
        use crate::config::SpeedConfigBuilder;
        
        // 测试 1: 小窗口
        let config1 = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(500))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(2).unwrap()) // 1秒
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calc1 = SpeedCalculator::from_config(&config1);
        // max(500ms, 1000ms) = 1000ms
        // 1000ms / 100ms = 10 采样点
        // 10 * 1.2 = 12 采样点
        assert_eq!(calc1.samples.len(), 12);
        
        // 测试 2: 大窗口
        let config2 = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(2))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap()) // 10秒
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calc2 = SpeedCalculator::from_config(&config2);
        // max(2s, 10s) = 10s
        // 10000ms / 100ms = 100 采样点
        // 100 * 1.2 = 120 采样点
        assert_eq!(calc2.samples.len(), 120);
        
        // 测试 3: 极小窗口（确保至少有 MIN_SAMPLES_FOR_REGRESSION * 2）
        let config3 = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(10))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap()) // 50ms
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calc3 = SpeedCalculator::from_config(&config3);
        // max(10ms, 50ms) = 50ms
        // 50ms / 100ms = 0.5 采样点 -> 0
        // 0 * 1.2 = 0，但会被限制为 MIN_SAMPLES_FOR_REGRESSION * 2 = 6
        assert_eq!(calc3.samples.len(), MIN_SAMPLES_FOR_REGRESSION * 2);
    }

    #[test]
    fn test_buffer_size_sufficient_for_window() {
        // 验证缓冲区大小足以容纳整个时间窗口的数据
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(8))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap()) // 8秒, 2秒中取最大
            .sample_interval(TEST_SAMPLE_INTERVAL)
            .buffer_size_margin(TEST_BUFFER_MARGIN)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        let start_time = Instant::now();
        
        // 在 8 秒窗口内，以 100ms 间隔记录采样点
        // 理论上需要 80 个采样点
        // 实际分配 80 * 1.2 = 96 个
        for i in 0..85 {
            calculator.record_sample((i * 1024) as u64, start_time);
            thread::sleep(Duration::from_micros(100));
        }
        
        // 验证缓冲区足够大
        let expected_min_size = (8000 / 100) as usize; // 至少 80 个
        assert!(calculator.samples.len() >= expected_min_size,
                "缓冲区大小 {} 应该至少为 {}", calculator.samples.len(), expected_min_size);
    }
}

