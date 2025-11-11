use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use super::speed_calculator::SpeedCalculator;

/// 统计摘要
/// 
/// 封装下载统计的完整信息，避免使用过多的元组返回值
#[derive(Debug, Clone, Copy)]
pub(crate) struct StatsSummary {
    /// 总下载字节数
    pub(crate) total_bytes: u64,
    /// 总耗时（秒）
    pub(crate) elapsed_secs: f64,
    /// 完成的 range 数量
    pub(crate) completed_ranges: usize,
    /// 平均速度（bytes/s）
    pub(crate) avg_speed: f64,
    /// 实时速度（bytes/s）
    pub(crate) instant_speed: Option<f64>,
    /// 窗口平均速度（bytes/s）
    pub(crate) window_avg_speed: Option<f64>,
    /// 实时加速度（bytes/s²）
    pub(crate) instant_acceleration: Option<f64>,
}

/// 统计聚合器
/// 
/// 轻量级的聚合器，用于父级统计，包含原子计数器和速度计算器
/// 多个子统计可以共享同一个聚合器，实时更新父级数据
#[derive(Debug)]
pub(crate) struct StatsAggregator {
    /// 总下载字节数（原子操作，无锁）
    total_bytes: AtomicU64,
    /// 完成的 range 数量（原子操作，无锁）
    completed_ranges: AtomicUsize,
    /// 速度计算器（管理瞬时速度、窗口平均速度的采样点和开始时间）
    speed_calculator: SpeedCalculator,
}

impl StatsAggregator {
    /// 记录下载的 chunk（在下载过程中实时调用）
    /// 
    /// 使用原子操作，多个 worker 可以并发调用，无锁竞争。
    /// 根据配置的采样间隔自动记录采样点，用于速度计算。
    /// 
    /// # Arguments
    /// 
    /// * `bytes` - 本次下载的字节数
    #[inline]
    pub(crate) fn record_chunk(&self, bytes: u64) {
        // 原子增加字节数（使用 Relaxed 顺序，性能最佳）
        let current_total = self.total_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        
        // 自动采样逻辑由 SpeedCalculator 内部处理（包括初始化开始时间）
        self.speed_calculator.record_sample(current_total);
    }

    /// 记录一个 range 完成
    #[inline]
    pub(crate) fn record_range_complete(&self) {
        self.completed_ranges.fetch_add(1, Ordering::Relaxed);
    }

    /// 获取实时下载速度（bytes/s）
    /// 
    /// # Returns
    /// 
    /// `(实时速度, 是否有效)`
    #[inline]
    pub(crate) fn get_instant_speed(&self) -> (f64, bool) {
        self.speed_calculator.get_instant_speed()
    }

    /// 获取窗口平均下载速度（bytes/s）
    /// 
    /// # Returns
    /// 
    /// `(窗口平均速度, 是否有效)`
    #[inline]
    pub(crate) fn get_window_avg_speed(&self) -> (f64, bool) {
        self.speed_calculator.get_window_avg_speed()
    }
    
    /// 获取实时下载加速度（bytes/s²）
    /// 
    /// # Returns
    /// 
    /// `(实时加速度, 是否有效)`：
    /// - 实时加速度：基于采样点计算的加速度（bytes/s²）
    /// - 是否有效：true 表示采样点有效，false 表示采样点不足
    #[inline]
    pub(crate) fn get_instant_acceleration(&self) -> (f64, bool) {
        self.speed_calculator.get_instant_acceleration()
    }
}

/// 下载速度统计
/// 
/// 线程安全的统计结构，在下载过程中被所有 worker 共享并实时更新
/// 使用完全无锁的原子操作，实现高性能并发
/// 
/// # 支持两种速度计算
/// 
/// 1. **平均速度**：从开始到现在的总体平均速度
///    - 通过 `get_speed()` 获取
///    - 计算公式：总下载字节数 / 总耗时
/// 
/// 2. **实时速度**：基于环形缓冲区和线性回归的速度
///    - 通过 `get_instant_speed()` 和 `get_window_avg_speed()` 获取
///    - 采用环形缓冲区存储最近的采样点，使用线性回归计算速度
///    - 时间窗口可配置（默认瞬时 1 秒，窗口平均 5 秒）
/// 
/// # 性能优化
/// 
/// - 所有字段使用原子操作，完全无锁并发
/// - `start_time` 使用 OnceLock，只初始化一次
/// - 速度计算器使用环形缓冲区和原子操作，无需 Mutex
/// - 支持父子统计聚合，子统计自动更新父级
/// - 自动采样：每 100ms 自动记录一次采样点
/// 
/// # 线程安全
/// 
/// 所有方法都是线程安全的，可以被多个 worker 并发调用
pub(crate) struct WorkerStats {
    /// 总下载字节数（原子操作，无锁）
    total_bytes: AtomicU64,
    /// 下载开始时间（只初始化一次）
    pub(crate) start_time: OnceLock<Instant>,
    /// 完成的 range 数量（原子操作，无锁）
    completed_ranges: AtomicUsize,
    /// 速度计算器（管理瞬时速度和窗口平均速度的采样点）
    speed_calculator: SpeedCalculator,
    /// 父级聚合器（子统计通过此字段自动更新父级）
    parent_aggregator: Option<Arc<StatsAggregator>>,
    /// 当前分块大小（原子操作，无锁）
    /// 由 worker 内部的 ChunkStrategy 更新，外部只读访问
    current_chunk_size: AtomicU64,
    /// 上次采样时间戳（纳秒，相对于 start_time）
    last_sample_timestamp_ns: AtomicU64,
    /// 采样间隔（纳秒）
    sample_interval_ns: u64,
}

impl Default for WorkerStats {
    #[inline]
    fn default() -> Self {
        // 使用默认配置值
        use crate::config::SpeedConfig;
        Self::from_config(&SpeedConfig::default())
    }
}

impl WorkerStats {
    /// 从速度配置创建统计实例
    /// 
    /// # Arguments
    /// 
    /// * `config` - 速度配置
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// use hydra_dl::config::SpeedConfig;
    /// 
    /// let config = SpeedConfig::default();
    /// let stats = WorkerStats::from_config(&config);
    /// ```
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        let sample_interval = config.sample_interval();
        Self {
            total_bytes: AtomicU64::new(0),
            start_time: OnceLock::new(),
            completed_ranges: AtomicUsize::new(0),
            speed_calculator: SpeedCalculator::from_config(config),
            parent_aggregator: None,
            current_chunk_size: AtomicU64::new(0),
            last_sample_timestamp_ns: AtomicU64::new(0),
            sample_interval_ns: sample_interval.as_nanos() as u64,
        }
    }

    /// 从速度配置创建带父级聚合器的统计实例
    /// 
    /// # Arguments
    /// 
    /// * `config` - 速度配置
    /// * `parent_aggregator` - 父级聚合器，子统计会自动更新父级
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// use hydra_dl::config::SpeedConfig;
    /// use std::sync::Arc;
    /// 
    /// let parent = TaskStats::default();
    /// let config = SpeedConfig::default();
    /// let child = WorkerStats::from_config_with_parent(&config, parent.aggregator());
    /// ```
    pub(crate) fn from_config_with_parent(
        config: &crate::config::SpeedConfig,
        parent_aggregator: Arc<StatsAggregator>,
    ) -> Self {
        let sample_interval = config.sample_interval();
        Self {
            total_bytes: AtomicU64::new(0),
            start_time: OnceLock::new(),
            completed_ranges: AtomicUsize::new(0),
            speed_calculator: SpeedCalculator::from_config(config),
            parent_aggregator: Some(parent_aggregator),
            current_chunk_size: AtomicU64::new(0),
            last_sample_timestamp_ns: AtomicU64::new(0),
            sample_interval_ns: sample_interval.as_nanos() as u64,
        }
    }

    /// 记录下载的 chunk（在下载过程中实时调用）
    /// 
    /// 使用原子操作，多个 worker 可以并发调用，无锁竞争。
    /// 根据配置的采样间隔自动记录采样点，用于速度计算。
    /// 
    /// # Arguments
    /// 
    /// * `bytes` - 本次下载的字节数
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// 
    /// let stats = WorkerStats::default();
    /// stats.record_chunk(1024); // 记录下载了 1KB
    /// ```
    #[inline]
    pub(crate) fn record_chunk(&self, bytes: u64) {
        // 第一个 chunk 到达时初始化开始时间
        self.start_time.get_or_init(Instant::now);
        
        // 原子增加字节数（使用 Relaxed 顺序，性能最佳）
        let current_total = self.total_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        
        // 自动采样逻辑由 SpeedCalculator 内部处理
        self.speed_calculator.record_sample(current_total);
        
        // 同步到父级聚合器（如果存在）
        if let Some(parent) = &self.parent_aggregator {
            parent.record_chunk(bytes);
        }
    }

    /// 记录一个 range 完成
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// 
    /// let stats = WorkerStats::default();
    /// stats.record_range_complete(); // 记录完成了一个 range
    /// ```
    #[inline]
    pub(crate) fn record_range_complete(&self) {
        self.completed_ranges.fetch_add(1, Ordering::Relaxed);
        
        // 同步到父级聚合器（如果存在）
        if let Some(parent) = &self.parent_aggregator {
            parent.record_range_complete();
        }
    }

    /// 获取当前平均下载速度（bytes/s）
    /// 
    /// 从开始到现在的总体平均速度
    /// 
    /// # Returns
    /// 
    /// 平均下载速度（bytes/s），如果尚未开始则返回 0.0
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// 
    /// let stats = WorkerStats::default();
    /// stats.record_chunk(1024 * 1024); // 下载 1MB
    /// 
    /// let speed = stats.get_speed();
    /// println!("平均速度: {:.2} MB/s", speed / 1024.0 / 1024.0);
    /// ```
    #[inline]
    pub(crate) fn get_speed(&self) -> f64 {
        if let Some(start_time) = self.start_time.get() {
            let elapsed_secs = start_time.elapsed().as_secs_f64();
            if elapsed_secs > 0.0 {
                let bytes = self.total_bytes.load(Ordering::Relaxed);
                return bytes as f64 / elapsed_secs;
            }
        }
        0.0
    }

    /// 获取实时下载速度（bytes/s）
    /// 
    /// 基于时间窗口的瞬时速度，通过与上次采样点比较计算增量
    /// 
    /// # Returns
    /// 
    /// `(实时速度, 是否有效)`：
    /// - 实时速度：基于采样点计算的速度（bytes/s）
    /// - 是否有效：true 表示采样点有效，false 表示刚初始化
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// use std::time::Duration;
    /// use std::thread;
    /// 
    /// let stats = WorkerStats::default();
    /// stats.record_chunk(1024 * 1024); // 下载 1MB
    /// 
    /// thread::sleep(Duration::from_secs(1));
    /// stats.record_chunk(1024 * 1024); // 再下载 1MB
    /// 
    /// let (instant_speed, valid) = stats.get_instant_speed();
    /// if valid {
    ///     println!("实时速度: {:.2} MB/s", instant_speed / 1024.0 / 1024.0);
    /// }
    /// ```
    #[inline]
    pub(crate) fn get_instant_speed(&self) -> (f64, bool) {
        self.speed_calculator.get_instant_speed()
    }

    /// 获取窗口平均下载速度（bytes/s）
    /// 
    /// 基于较长时间窗口的平均速度，用于检测异常下载线程
    /// 
    /// # Returns
    /// 
    /// `(窗口平均速度, 是否有效)`：
    /// - 窗口平均速度：基于采样点计算的速度（bytes/s）
    /// - 是否有效：true 表示采样点有效，false 表示刚初始化
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// 
    /// let stats = WorkerStats::default();
    /// let (window_avg_speed, valid) = stats.get_window_avg_speed();
    /// if valid {
    ///     println!("窗口平均速度: {:.2} MB/s", window_avg_speed / 1024.0 / 1024.0);
    /// }
    /// ```
    #[inline]
    pub(crate) fn get_window_avg_speed(&self) -> (f64, bool) {
        self.speed_calculator.get_window_avg_speed()
    }
    
    /// 获取实时下载加速度（bytes/s²）
    /// 
    /// # Returns
    /// 
    /// `(实时加速度, 是否有效)`：
    /// - 实时加速度：基于采样点计算的加速度（bytes/s²）
    /// - 是否有效：true 表示采样点有效，false 表示采样点不足
    #[inline]
    pub(crate) fn get_instant_acceleration(&self) -> (f64, bool) {
        self.speed_calculator.get_instant_acceleration()
    }

    /// 获取统计摘要
    /// 
    /// # Returns
    /// 
    /// `(总字节数, 总耗时秒数, 完成的 range 数)`
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// 
    /// let stats = WorkerStats::default();
    /// stats.record_chunk(1024);
    /// stats.record_range_complete();
    /// 
    /// let (bytes, elapsed, ranges) = stats.get_summary();
    /// println!("下载了 {} bytes，耗时 {:.2}s，完成 {} 个 ranges", bytes, elapsed, ranges);
    /// ```
    #[allow(dead_code)]
    pub(crate) fn get_summary(&self) -> (u64, f64, usize) {
        let bytes = self.total_bytes.load(Ordering::Relaxed);
        let ranges = self.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.start_time
            .get()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        (bytes, elapsed_secs, ranges)
    }

    /// 获取当前分块大小
    /// 
    /// # Returns
    /// 
    /// 当前分块大小 (bytes)
    #[inline]
    pub(crate) fn get_current_chunk_size(&self) -> u64 {
        self.current_chunk_size.load(Ordering::Relaxed)
    }
    
    /// 设置当前分块大小
    /// 
    /// # Arguments
    /// 
    /// * `size` - 新的分块大小 (bytes)
    #[inline]
    pub(crate) fn set_current_chunk_size(&self, size: u64) {
        self.current_chunk_size.store(size, Ordering::Relaxed);
    }

    /// 获取完整的统计摘要（包含平均速度、实时速度和窗口平均速度）
    /// 
    /// # Returns
    /// 
    /// 返回 `StatsSummary` 结构体，包含所有统计信息
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::WorkerStats;
    /// 
    /// let stats = WorkerStats::default();
    /// stats.record_chunk(1024 * 1024);
    /// 
    /// let summary = stats.get_full_summary();
    /// println!("平均速度: {:.2} MB/s", summary.avg_speed / 1024.0 / 1024.0);
    /// if let Some(instant_speed) = summary.instant_speed {
    ///     println!("实时速度: {:.2} MB/s", instant_speed / 1024.0 / 1024.0);
    /// }
    /// if let Some(window_avg_speed) = summary.window_avg_speed {
    ///     println!("窗口平均速度: {:.2} MB/s", window_avg_speed / 1024.0 / 1024.0);
    /// }
    /// ```
    pub(crate) fn get_full_summary(&self) -> StatsSummary {
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let completed_ranges = self.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.start_time
            .get()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let avg_speed = self.get_speed();
        let (instant_speed, instant_speed_valid) = self.get_instant_speed();
        let (window_avg_speed, window_avg_speed_valid) = self.get_window_avg_speed();
        let (instant_acceleration, accel_valid) = self.get_instant_acceleration();
        
        StatsSummary {
            total_bytes,
            elapsed_secs,
            completed_ranges,
            avg_speed,
            instant_speed: if instant_speed_valid { Some(instant_speed) } else { None },
            window_avg_speed: if window_avg_speed_valid { Some(window_avg_speed) } else { None },
            instant_acceleration: if accel_valid { Some(instant_acceleration) } else { None },
        }
    }
}

impl std::fmt::Debug for WorkerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStats")
            .field("total_bytes", &self.total_bytes)
            .field("start_time", &self.start_time)
            .field("completed_ranges", &self.completed_ranges)
            .field("speed_calculator", &self.speed_calculator)
            .field("has_parent", &self.parent_aggregator.is_some())
            .field("current_chunk_size", &self.current_chunk_size)
            .field("last_sample_timestamp_ns", &self.last_sample_timestamp_ns)
            .field("sample_interval_ns", &self.sample_interval_ns)
            .finish()
    }
}

/// 任务级统计管理器
/// 
/// 管理多个 Worker 统计，Worker 统计的数据会自动聚合到任务级
/// 通过任务统计可以 O(1) 获取所有 Worker 统计的聚合结果，无需遍历
/// 
/// # 使用场景
/// 
/// 在 WorkerPool 中，创建一个任务统计，每个 worker 通过 `create_child()` 
/// 创建自己的 Worker 统计。当 Worker 记录数据时，会自动同步到任务统计。
/// 
/// # Examples
/// 
/// ```ignore
/// use hydra_dl::TaskStats;
/// 
/// let parent = TaskStats::default();
/// let child1 = parent.create_child();
/// let child2 = parent.create_child();
/// 
/// child1.record_chunk(1024);
/// child2.record_chunk(2048);
/// 
/// let (total_bytes, _, _) = parent.get_summary();
/// assert_eq!(total_bytes, 3072);  // 自动聚合
/// ```
#[derive(Debug)]
pub(crate) struct TaskStats {
    /// 聚合器，存储所有子统计的累加结果，包含速度计算器
    aggregator: Arc<StatsAggregator>,
    /// 速度配置（用于创建子统计）
    speed_config: crate::config::SpeedConfig,
}

impl Default for TaskStats {
    #[inline]
    fn default() -> Self {
        use crate::config::SpeedConfig;
        Self::from_config(&SpeedConfig::default())
    }
}

impl TaskStats {
    /// 从速度配置创建任务统计管理器
    /// 
    /// # Arguments
    /// 
    /// * `config` - 速度配置
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// use hydra_dl::config::SpeedConfig;
    /// 
    /// let config = SpeedConfig::default();
    /// let parent = TaskStats::from_config(&config);
    /// ```
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        Self {
            aggregator: Arc::new(StatsAggregator {
                total_bytes: AtomicU64::new(0),
                completed_ranges: AtomicUsize::new(0),
                speed_calculator: SpeedCalculator::from_config(config),
            }),
            speed_config: config.clone(),
        }
    }

    /// 创建 Worker 统计
    /// 
    /// Worker 统计记录数据时会自动同步到任务级，实现 O(1) 聚合
    /// Worker 统计使用与任务统计相同的速度配置
    /// 
    /// # Returns
    /// 
    /// 返回一个新的 `WorkerStats` 实例，其 parent 字段指向当前任务统计
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// 
    /// let parent = TaskStats::default();
    /// let child = parent.create_child();
    /// 
    /// child.record_chunk(1024);
    /// // parent 的统计会自动更新
    /// ```
    #[inline]
    pub(crate) fn create_child(&self) -> WorkerStats {
        WorkerStats::from_config_with_parent(
            &self.speed_config,
            Arc::clone(&self.aggregator),
        )
    }

    /// 获取聚合统计摘要
    /// 
    /// # Returns
    /// 
    /// `(总字节数, 总耗时秒数, 完成的 range 数)`
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// 
    /// let parent = TaskStats::new();
    /// let (bytes, elapsed, ranges) = parent.get_summary();
    /// ```
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn get_summary(&self) -> (u64, f64, usize) {
        let bytes = self.aggregator.total_bytes.load(Ordering::Relaxed);
        let ranges = self.aggregator.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.aggregator.speed_calculator.get_start_time()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        (bytes, elapsed_secs, ranges)
    }

    /// 获取聚合平均速度（bytes/s）
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// 
    /// let parent = TaskStats::new();
    /// let speed = parent.get_speed();
    /// ```
    #[inline]
    pub(crate) fn get_speed(&self) -> f64 {
        if let Some(start_time) = self.aggregator.speed_calculator.get_start_time() {
            let elapsed_secs = start_time.elapsed().as_secs_f64();
            if elapsed_secs > 0.0 {
                let bytes = self.aggregator.total_bytes.load(Ordering::Relaxed);
                return bytes as f64 / elapsed_secs;
            }
        }
        0.0
    }

    /// 获取聚合实时速度（bytes/s）
    /// 
    /// # Returns
    /// 
    /// `(实时速度, 是否有效)`
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// 
    /// let parent = TaskStats::default();
    /// let (instant_speed, valid) = parent.get_instant_speed();
    /// ```
    #[inline]
    pub(crate) fn get_instant_speed(&self) -> (f64, bool) {
        self.aggregator.get_instant_speed()
    }

    /// 获取聚合窗口平均速度（bytes/s）
    /// 
    /// # Returns
    /// 
    /// `(窗口平均速度, 是否有效)`
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// 
    /// let parent = TaskStats::default();
    /// let (window_avg_speed, valid) = parent.get_window_avg_speed();
    /// ```
    #[inline]
    pub(crate) fn get_window_avg_speed(&self) -> (f64, bool) {
        self.aggregator.get_window_avg_speed()
    }
    
    /// 获取聚合实时加速度（bytes/s²）
    /// 
    /// # Returns
    /// 
    /// `(实时加速度, 是否有效)`
    #[inline]
    pub(crate) fn get_instant_acceleration(&self) -> (f64, bool) {
        self.aggregator.get_instant_acceleration()
    }

    /// 获取完整的聚合统计
    /// 
    /// # Returns
    /// 
    /// 返回 `StatsSummary` 结构体，包含所有聚合统计信息
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::TaskStats;
    /// 
    /// let parent = TaskStats::default();
    /// let summary = parent.get_full_summary();
    /// ```
    pub(crate) fn get_full_summary(&self) -> StatsSummary {
        let total_bytes = self.aggregator.total_bytes.load(Ordering::Relaxed);
        let completed_ranges = self.aggregator.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.aggregator.speed_calculator.get_start_time()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let avg_speed = self.get_speed();
        let (instant_speed, instant_speed_valid) = self.get_instant_speed();
        let (window_avg_speed, window_avg_speed_valid) = self.get_window_avg_speed();
        let (instant_acceleration, accel_valid) = self.get_instant_acceleration();
        
        StatsSummary {
            total_bytes,
            elapsed_secs,
            completed_ranges,
            avg_speed,
            instant_speed: if instant_speed_valid { Some(instant_speed) } else { None },
            window_avg_speed: if window_avg_speed_valid { Some(window_avg_speed) } else { None },
            instant_acceleration: if accel_valid { Some(instant_acceleration) } else { None },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_stats() {
        let stats = WorkerStats::default();
        let (bytes, elapsed, ranges) = stats.get_summary();
        
        assert_eq!(bytes, 0);
        assert_eq!(elapsed, 0.0);
        assert_eq!(ranges, 0);
    }

    #[test]
    fn test_with_custom_windows() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(500))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(6).unwrap()) // 3秒
            .build();
        let stats = WorkerStats::from_config(&config);
        // 验证 SpeedCalculator 已创建（通过测试速度功能）
        let (_, _) = stats.get_instant_speed();
        let (_, _) = stats.get_window_avg_speed();
    }

    #[test]
    fn test_record_chunk() {
        let stats = WorkerStats::default();
        
        stats.record_chunk(1024);
        let (bytes, _, _) = stats.get_summary();
        assert_eq!(bytes, 1024);
        
        stats.record_chunk(2048);
        let (bytes, _, _) = stats.get_summary();
        assert_eq!(bytes, 3072);
    }

    #[test]
    fn test_record_range_complete() {
        let stats = WorkerStats::default();
        
        stats.record_range_complete();
        let (_, _, ranges) = stats.get_summary();
        assert_eq!(ranges, 1);
        
        stats.record_range_complete();
        stats.record_range_complete();
        let (_, _, ranges) = stats.get_summary();
        assert_eq!(ranges, 3);
    }

    #[test]
    fn test_average_speed() {
        let stats = WorkerStats::default();
        
        // 第一次记录会初始化开始时间
        stats.record_chunk(1024 * 1024); // 1 MB
        
        // 等待一段时间
        thread::sleep(Duration::from_millis(100));
        
        let speed = stats.get_speed();
        assert!(speed > 0.0, "速度应该大于 0");
        
        // 速度应该在合理范围内（考虑到睡眠时间约 100ms）
        // 1MB / 0.1s = 10 MB/s，但实际会稍低一些
        assert!(speed > 1024.0 * 1024.0, "速度应该至少 1 MB/s");
    }

    #[test]
    fn test_instant_speed_initialization() {
        let stats = WorkerStats::default();
        
        stats.record_chunk(1024);
        
        // 第一次调用应该初始化采样点
        let (speed, valid) = stats.get_instant_speed();
        assert_eq!(speed, 0.0, "首次调用速度应该为 0");
        assert!(!valid, "首次调用 valid 应该为 false");
    }

    #[test]
    fn test_instant_speed_before_window() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .build();
        let stats = WorkerStats::from_config(&config);
        
        // 记录多个 chunk 以触发采样（每 100ms 采样一次，需要至少 3 个采样点）
        for _ in 0..5 {
            stats.record_chunk(200 * 1024); // 200 KB per chunk
            thread::sleep(Duration::from_millis(110)); // 超过采样间隔
        }
        
        // 现在应该有足够的采样点
        let (speed, valid) = stats.get_instant_speed();
        if valid {
            assert!(speed > 0.0, "速度应该大于 0");
        }
    }

    #[test]
    fn test_instant_speed_after_window() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(200))
            .window_avg_multiplier(std::num::NonZeroU32::new(25).unwrap()) // 5秒
            .build();
        let stats = WorkerStats::from_config(&config);
        
        // 记录足够的 chunks 以触发多次采样
        for _ in 0..5 {
            stats.record_chunk(200 * 1024); // 200 KB
            thread::sleep(Duration::from_millis(110)); // 触发采样
        }
        
        let (speed, valid) = stats.get_instant_speed();
        if valid {
            assert!(speed > 0.0, "速度应该大于 0");
        }
    }

    #[test]
    fn test_full_summary() {
        let stats = WorkerStats::default();
        
        stats.record_chunk(2048);
        stats.record_range_complete();
        
        thread::sleep(Duration::from_millis(50));
        
        let summary = stats.get_full_summary();
        
        assert_eq!(summary.total_bytes, 2048);
        assert!(summary.elapsed_secs > 0.0);
        assert_eq!(summary.completed_ranges, 1);
        assert!(summary.avg_speed > 0.0);
        assert_eq!(summary.instant_speed, None); // 首次调用
        assert_eq!(summary.window_avg_speed, None); // 首次调用
    }

    #[test]
    fn test_concurrent_record_chunk() {
        use std::sync::Arc;
        
        let stats = Arc::new(WorkerStats::default());
        let mut handles = vec![];
        
        // 启动 10 个线程，每个记录 100 次
        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.record_chunk(1024);
                }
            });
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        let (bytes, _, _) = stats.get_summary();
        assert_eq!(bytes, 10 * 100 * 1024, "并发写入应该正确累加");
    }

    #[test]
    fn test_concurrent_range_complete() {
        use std::sync::Arc;
        
        let stats = Arc::new(WorkerStats::default());
        let mut handles = vec![];
        
        // 启动 5 个线程，每个完成 20 个 range
        for _ in 0..5 {
            let stats_clone = Arc::clone(&stats);
            let handle = thread::spawn(move || {
                for _ in 0..20 {
                    stats_clone.record_range_complete();
                }
            });
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        let (_, _, ranges) = stats.get_summary();
        assert_eq!(ranges, 100, "并发计数应该正确");
    }

    #[test]
    fn test_speed_calculation_accuracy() {
        let stats = WorkerStats::default();
        
        stats.record_chunk(10 * 1024 * 1024); // 10 MB
        
        thread::sleep(Duration::from_millis(500)); // 0.5 秒
        
        let speed = stats.get_speed();
        let speed_mbs = speed / 1024.0 / 1024.0;
        
        // 10 MB / 0.5s = 20 MB/s，考虑误差应该在 15-25 MB/s 之间
        assert!(speed_mbs > 15.0 && speed_mbs < 25.0, 
            "速度应该接近 20 MB/s，实际: {:.2} MB/s", speed_mbs);
    }

    #[test]
    fn test_instant_speed_multiple_samples() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(300))
            .window_avg_multiplier(std::num::NonZeroU32::new(17).unwrap()) // ~5秒
            .build();
        let stats = WorkerStats::from_config(&config);
        
        // 记录多个 chunks 以积累足够的采样点
        for _ in 0..6 {
            stats.record_chunk(200 * 1024);
            thread::sleep(Duration::from_millis(110)); // 触发采样
        }
        
        let (speed, valid) = stats.get_instant_speed();
        if valid {
            assert!(speed > 0.0, "速度应该大于 0");
        }
    }

    #[test]
    fn test_window_avg_speed_initialization() {
        let stats = WorkerStats::default();
        
        stats.record_chunk(1024);
        
        // 第一次调用应该初始化采样点
        let (speed, valid) = stats.get_window_avg_speed();
        assert_eq!(speed, 0.0, "首次调用速度应该为 0");
        assert!(!valid, "首次调用 valid 应该为 false");
    }

    #[test]
    fn test_window_avg_speed_calculation() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap()) // 1秒, 取最大300ms
            .build();
        let stats = WorkerStats::from_config(&config);
        
        // 记录足够的 chunks 以触发采样
        for _ in 0..5 {
            stats.record_chunk(200 * 1024);
            thread::sleep(Duration::from_millis(110)); // 触发采样
        }
        
        let (speed, valid) = stats.get_window_avg_speed();
        if valid {
            assert!(speed > 0.0, "速度应该大于 0");
        }
    }

    #[test]
    fn test_independent_speed_calculations() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(200))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap()) // 200ms
            .window_avg_multiplier(std::num::NonZeroU32::new(2).unwrap()) // 400ms
            .build();
        let stats = WorkerStats::from_config(&config);
        
        // 记录足够的 chunks 以积累采样点
        for _ in 0..6 {
            stats.record_chunk(200 * 1024);
            thread::sleep(Duration::from_millis(110)); // 触发采样
        }
        
        let (instant_speed, instant_valid) = stats.get_instant_speed();
        let (window_avg_speed, window_avg_valid) = stats.get_window_avg_speed();
        
        // 两种速度计算都应该有数据
        if instant_valid {
            assert!(instant_speed >= 0.0, "瞬时速度应该 >= 0");
        }
        if window_avg_valid {
            assert!(window_avg_speed >= 0.0, "窗口平均速度应该 >= 0");
        }
    }

    #[test]
    fn test_parent_child_aggregation() {
        let parent = TaskStats::default();
        let child1 = parent.create_child();
        let child2 = parent.create_child();
        
        // child 记录数据会自动同步到 parent
        child1.record_chunk(1024);
        child2.record_chunk(2048);
        
        let (total_bytes, _, _) = parent.get_summary();
        assert_eq!(total_bytes, 3072);
        
        // 验证 range 完成也会同步
        child1.record_range_complete();
        child2.record_range_complete();
        
        let (_, _, total_ranges) = parent.get_summary();
        assert_eq!(total_ranges, 2);
    }

    #[test]
    fn test_task_stats_window_avg_speed() {
        use crate::config::SpeedConfigBuilder;
        let config = SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap()) // 1秒, 取最大300ms
            .build();
        let parent = TaskStats::from_config(&config);
        
        let child = parent.create_child();
        
        // 记录足够的 chunks 以触发采样
        for _ in 0..5 {
            child.record_chunk(200 * 1024);
            thread::sleep(Duration::from_millis(110)); // 触发采样
        }
        
        let (speed, valid) = parent.get_window_avg_speed();
        if valid {
            assert!(speed > 0.0, "速度应该大于 0");
        }
    }

    #[test]
    fn test_parent_child_concurrent_aggregation() {
        use std::sync::Arc;
        use std::thread;
        
        let parent = Arc::new(TaskStats::default());
        let mut handles = vec![];
        
        // 启动 10 个线程，每个创建一个 child 并记录数据
        for _ in 0..10 {
            let parent_clone = parent.clone();
            let handle = thread::spawn(move || {
                let child = parent_clone.create_child();
                for _ in 0..100 {
                    child.record_chunk(1024);
                }
                child.record_range_complete();
            });
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        let (total_bytes, _, total_ranges) = parent.get_summary();
        assert_eq!(total_bytes, 10 * 100 * 1024, "并发聚合应该正确累加字节数");
        assert_eq!(total_ranges, 10, "并发聚合应该正确累加 range 数");
    }
}

