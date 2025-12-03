mod core;

use net_bytes::DownloadSpeed;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub use core::Sample;
use core::{theil_sen_regression, two_point_speed, MAX_BUFFER_SIZE, MIN_BUFFER_SIZE, MIN_SAMPLES_FOR_REGRESSION};

/// 速度计算器配置参数
///
/// 内部共享的配置参数，用于 Recording 和 Active 状态
#[derive(Debug, Clone)]
struct SpeedCalculatorConfig {
    /// 缓冲区最大容量
    max_samples: usize,
    /// 线性回归所需的最小采样点数
    min_samples_for_regression: usize,
    /// 瞬时速度的时间窗口
    instant_speed_window: Duration,
    /// 窗口平均速度的时间窗口
    window_avg_duration: Duration,
    /// 采样间隔（纳秒）
    sample_interval_ns: u64,
}

impl SpeedCalculatorConfig {
    fn from_config(config: &crate::config::SpeedConfig) -> Self {
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
            max_samples,
            min_samples_for_regression: MIN_SAMPLES_FOR_REGRESSION,
            instant_speed_window,
            window_avg_duration,
            sample_interval_ns: sample_interval.as_nanos() as u64,
        }
    }
}

/// 速度计算器 - 记录状态（未激活）
///
/// 在首次采样前的状态，`start_time` 为 `Option`。
/// 当成功采样时，转换为 `SpeedCalculatorActive` 状态。
///
/// # 类型状态模式
///
/// - `SpeedCalculatorRecording`：记录状态，用于采样
/// - `SpeedCalculatorActive`：激活状态，用于计算速度（速度返回无 `Option`）
#[derive(Debug, Clone)]
pub(crate) struct SpeedCalculatorRecording {
    /// 采样点缓冲区
    samples: VecDeque<Sample>,
    /// 配置参数
    config: SpeedCalculatorConfig,
    /// 下载开始时间（尚未初始化）
    start_time: Option<Instant>,
}

/// 速度计算器 - 激活状态
///
/// 成功采样后的状态，`start_time` 无 `Option` 包装。
/// 所有速度计算方法返回非 `Option` 的 `DownloadSpeed`。
///
/// # 类型状态模式
///
/// 激活状态保证 `start_time` 存在，因此速度计算一定有效。
#[derive(Debug, Clone)]
pub(crate) struct SpeedCalculatorActive {
    /// 采样点缓冲区
    samples: VecDeque<Sample>,
    /// 配置参数
    config: SpeedCalculatorConfig,
    /// 下载开始时间（已初始化，无 Option）
    start_time: Instant,
}

impl SpeedCalculatorRecording {
    /// 从速度配置创建速度计算器（记录状态）
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        let calc_config = SpeedCalculatorConfig::from_config(config);
        Self {
            samples: VecDeque::with_capacity(calc_config.max_samples),
            config: calc_config,
            start_time: None,
        }
    }

    /// 记录采样点
    ///
    /// 根据配置的采样间隔自动判断是否需要记录采样点。
    ///
    /// # Arguments
    ///
    /// * `current_bytes` - 当前累计下载的总字节数
    ///
    /// # Returns
    ///
    /// - `Some(SpeedCalculatorActive)`: 成功采样，返回激活状态的计算器
    /// - `None`: 跳过采样（未达到采样间隔）
    #[inline]
    pub(crate) fn record_sample(&mut self, current_bytes: u64) -> Option<SpeedCalculatorActive> {
        // 第一次调用时初始化开始时间
        let start_time = *self.start_time.get_or_insert_with(Instant::now);
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;

        // 检查是否需要采样（距离上次采样超过配置的采样间隔）
        if let Some(last_sample) = self.samples.back() {
            if current_elapsed_ns.saturating_sub(last_sample.timestamp_ns.get())
                < self.config.sample_interval_ns
            {
                return None;
            }
        }

        // 环形缓冲区：满时移除最旧的采样点
        if self.samples.len() >= self.config.max_samples {
            self.samples.pop_front();
        }
        self.samples
            .push_back(Sample::new(current_elapsed_ns, current_bytes));

        // 返回激活状态的计算器
        Some(SpeedCalculatorActive {
            samples: self.samples.clone(),
            config: self.config.clone(),
            start_time,
        })
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

impl SpeedCalculatorActive {
    /// 采样点迭代器（保留纳秒精度）
    #[inline]
    fn iter_samples(&self) -> impl Iterator<Item = (i128, u64)> + '_ {
        self.samples
            .iter()
            .map(|s| (s.timestamp_ns.get() as i128, s.bytes))
    }

    /// 读取最近时间窗口内的采样点（保留纳秒精度）
    #[inline]
    fn read_samples_in_window(&self, window: Duration) -> Vec<(i128, u64)> {
        let window_start_ns =
            self.start_time.elapsed().as_nanos() as i128 - window.as_nanos() as i128;
        self.iter_samples()
            .filter(|(t, _)| *t >= window_start_ns)
            .collect()
    }

    /// 获取瞬时下载速度
    ///
    /// 使用分级回退策略：
    /// 1. Theil-Sen 回归（≥3 采样点）
    /// 2. 两点差分法（≥2 采样点）  
    /// 3. 全局平均速度（≥1 采样点）
    ///
    /// # Returns
    ///
    /// `DownloadSpeed` - 速度值（保证有效，无 Option）
    pub fn get_instant_speed(&self) -> DownloadSpeed {
        let samples = self.read_samples_in_window(self.config.instant_speed_window);
        
        // 优先级 1: Theil-Sen 回归
        if let Some(speed) = theil_sen_regression(&samples, self.config.min_samples_for_regression) {
            return speed;
        }
        
        // 优先级 2: 两点差分法（窗口内）
        if let Some(speed) = two_point_speed(&samples) {
            return speed;
        }
        
        // 优先级 3: 全局平均速度
        self.get_global_avg_speed()
    }

    /// 获取窗口平均下载速度
    ///
    /// 使用分级回退策略：
    /// 1. Theil-Sen 回归（≥3 采样点）
    /// 2. 两点差分法（≥2 采样点）
    /// 3. 全局平均速度（≥1 采样点）
    ///
    /// # Returns
    ///
    /// `DownloadSpeed` - 速度值（保证有效，无 Option）
    pub fn get_window_avg_speed(&self) -> DownloadSpeed {
        let samples = self.read_samples_in_window(self.config.window_avg_duration);
        
        // 优先级 1: Theil-Sen 回归
        if let Some(speed) = theil_sen_regression(&samples, self.config.min_samples_for_regression) {
            return speed;
        }
        
        // 优先级 2: 两点差分法（窗口内）
        if let Some(speed) = two_point_speed(&samples) {
            return speed;
        }
        
        // 优先级 3: 全局平均速度
        self.get_global_avg_speed()
    }
    
    /// 全局平均速度（从 start_time 到最新采样点）
    ///
    /// 这是最可靠的回退方法，start_time 已保证存在。
    ///
    /// # Returns
    ///
    /// `DownloadSpeed` - 全局平均速度（保证有效，无 Option）
    pub fn get_global_avg_speed(&self) -> DownloadSpeed {
        let elapsed = self.start_time.elapsed();
        
        // 如果有采样点，使用最后一个采样点的字节数
        let bytes = self.samples.back().map(|s| s.bytes).unwrap_or(0);
        
        // 即使 elapsed 很小，DownloadSpeed::new 也能处理
        DownloadSpeed::new(bytes, elapsed)
    }
}

#[cfg(test)]
mod tests;
