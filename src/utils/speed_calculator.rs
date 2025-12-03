mod core;

use net_bytes::DownloadSpeed;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub use core::Sample;
use core::{theil_sen_regression, two_point_speed, MAX_BUFFER_SIZE, MIN_BUFFER_SIZE, MIN_SAMPLES_FOR_REGRESSION};

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
    ///
    /// # Returns
    ///
    /// `true` 表示成功采样，`false` 表示跳过（未达到采样间隔）
    #[inline]
    pub(crate) fn record_sample(&mut self, current_bytes: u64) -> bool {
        // 第一次调用时初始化开始时间
        let start_time = *self.start_time.get_or_insert_with(Instant::now);
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;

        // 检查是否需要采样（距离上次采样超过配置的采样间隔）
        if let Some(last_sample) = self.samples.back() {
            if current_elapsed_ns.saturating_sub(last_sample.timestamp_ns.get())
                < self.sample_interval_ns
            {
                return false;
            }
        }

        // 环形缓冲区：满时移除最旧的采样点
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples
            .push_back(Sample::new(current_elapsed_ns, current_bytes));
        true
    }

    /// 采样点迭代器（保留纳秒精度）
    #[inline]
    fn iter_samples(&self) -> impl Iterator<Item = (i128, u64)> + '_ {
        self.samples
            .iter()
            .map(|s| (s.timestamp_ns.get() as i128, s.bytes))
    }


    /// 读取最近时间窗口内的采样点（保留纳秒精度）
    #[inline]
    fn read_samples_in_window(&self, window: Duration, start_time: Instant) -> Vec<(i128, u64)> {
        let window_start_ns =
            start_time.elapsed().as_nanos() as i128 - window.as_nanos() as i128;
        self.iter_samples()
            .filter(|(t, _)| *t >= window_start_ns)
            .collect()
    }

    /// 获取瞬时下载速度（带质量标记）
    ///
    /// 使用分级回退策略：
    /// 1. Theil-Sen 回归（≥3 采样点）
    /// 2. 两点差分法（≥2 采样点）  
    /// 3. 全局平均速度（≥1 采样点）
    ///
    /// # Returns
    ///
    /// - `Some(DownloadSpeed)`: 速度值
    /// - `None`: 如果 start_time 不存在或无采样点
    pub fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.get_start_time()?;
        let samples = self.read_samples_in_window(self.instant_speed_window, start_time);
        
        // 优先级 1: Theil-Sen 回归
        if let Some(speed) = theil_sen_regression(&samples, self.min_samples_for_regression) {
            return Some(speed);
        }
        
        // 优先级 2: 两点差分法（窗口内）
        if let Some(speed) = two_point_speed(&samples) {
            return Some(speed);
        }
        
        // 优先级 3: 全局平均速度
        self.get_global_avg_speed()
    }

    /// 获取窗口平均下载速度（带质量标记）
    ///
    /// 使用分级回退策略：
    /// 1. Theil-Sen 回归（≥3 采样点）
    /// 2. 两点差分法（≥2 采样点）
    /// 3. 全局平均速度（≥1 采样点）
    ///
    /// # Returns
    ///
    /// - `Some(DownloadSpeed)`: 速度值
    /// - `None`: 如果 start_time 不存在或无采样点
    pub fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.get_start_time()?;
        let samples = self.read_samples_in_window(self.window_avg_duration, start_time);
        
        // 优先级 1: Theil-Sen 回归
        if let Some(speed) = theil_sen_regression(&samples, self.min_samples_for_regression) {
            return Some(speed);
        }
        
        // 优先级 2: 两点差分法（窗口内）
        if let Some(speed) = two_point_speed(&samples) {
            return Some(speed);
        }
        
        // 优先级 3: 全局平均速度
        self.get_global_avg_speed()
    }
    
    /// 全局平均速度（从 start_time 到最新采样点）
    ///
    /// 这是最可靠的回退方法，只要有 start_time 和任意采样点就能计算。
    ///
    /// # Returns
    ///
    /// - `Some(DownloadSpeed)`: 全局平均速度
    /// - `None`: 如果 start_time 不存在或无采样点
    pub fn get_global_avg_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.start_time?;
        let last_sample = self.samples.back()?;
        let elapsed = start_time.elapsed();
        
        if elapsed.is_zero() {
            return None;
        }
        
        let speed = DownloadSpeed::new(last_sample.bytes, elapsed);
        Some(speed)
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
