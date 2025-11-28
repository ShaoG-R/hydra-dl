use super::speed_calculator::SpeedCalculator;
use lfrlock::LfrLock;
use net_bytes::{DownloadAcceleration, DownloadSpeed};

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
    /// 平均速度
    pub(crate) avg_speed: Option<DownloadSpeed>,
    /// 实时速度
    pub(crate) instant_speed: Option<DownloadSpeed>,
    /// 窗口平均速度
    pub(crate) window_avg_speed: Option<DownloadSpeed>,
    /// 实时加速度
    pub(crate) instant_acceleration: Option<DownloadAcceleration>,
}

#[derive(Debug, Clone)]
struct StatsAggregatorInner {
    total_bytes: u64,
    completed_ranges: usize,
    speed_calculator: SpeedCalculator,
}

/// 统计聚合器
///
/// 轻量级的聚合器，用于父级统计，包含原子计数器和速度计算器
/// 多个子统计可以共享同一个聚合器，实时更新父级数据
#[derive(Debug, Clone)]
pub(crate) struct StatsAggregator {
    inner: LfrLock<StatsAggregatorInner>,
}

impl StatsAggregator {
    /// 记录下载的 chunk（在下载过程中实时调用）
    ///
    /// 使用原子操作，多个 worker 可以并发调用，无锁竞争。
    ///
    /// 根据配置的采样间隔自动记录采样点，用于速度计算。
    ///
    /// # Arguments
    ///
    /// * `bytes` - 本次下载的字节数
    #[inline]
    pub(crate) fn record_chunk(&self, bytes: u64) {
        let mut inner = self.inner.write();
        inner.total_bytes += bytes;

        let total_bytes = inner.total_bytes;
        inner.speed_calculator.record_sample(total_bytes);
    }

    /// 记录一个 range 完成
    #[inline]
    pub(crate) fn record_range_complete(&self) {
        let mut inner = self.inner.write();
        inner.completed_ranges += 1;
    }

    /// 获取实时下载速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    #[inline]
    pub(crate) fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        let inner = self.inner.read();
        inner.speed_calculator.get_instant_speed()
    }

    /// 获取窗口平均下载速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    #[inline]
    pub(crate) fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        let inner = self.inner.read();
        inner.speed_calculator.get_window_avg_speed()
    }

    /// 获取实时下载加速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadAcceleration)` 如果加速度计算有效，否则返回 `None`
    ///
    /// 注意：需要至少3个采样点才能计算加速度
    #[inline]
    pub(crate) fn get_instant_acceleration(&self) -> Option<DownloadAcceleration> {
        let inner = self.inner.read();
        inner.speed_calculator.get_instant_acceleration()
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
/// - `start_time` 委托给 SpeedCalculator 管理
/// - 速度计算器使用环形缓冲区和原子操作，无需 Mutex
/// - 支持父子统计聚合，子统计自动更新父级
/// - 自动采样：每 100ms 自动记录一次采样点
///
/// # 线程安全
///
/// 所有方法都是线程安全的，可以被多个 worker 并发调用
#[derive(Clone)]
pub(crate) struct WorkerStats {
    /// 总下载字节数
    total_bytes: u64,
    /// 完成的 range 数量
    completed_ranges: usize,
    /// 速度计算器（管理瞬时速度和窗口平均速度的采样点）
    speed_calculator: SpeedCalculator,
    /// 父级聚合器（子统计通过此字段自动更新父级）
    parent_aggregator: Option<StatsAggregator>,
    /// 当前分块大小
    /// 由 worker 内部的 ChunkStrategy 更新，外部只读访问
    current_chunk_size: u64,
    /// worker 是否正在执行任务
    /// 由 worker 内部在 execute 开始和结束时更新
    is_active: bool,
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
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        Self {
            total_bytes: 0,
            completed_ranges: 0,
            speed_calculator: SpeedCalculator::from_config(config),
            parent_aggregator: None,
            current_chunk_size: 0,
            is_active: false,
        }
    }

    /// 从速度配置创建带父级聚合器的统计实例
    pub(crate) fn from_config_with_parent(
        config: &crate::config::SpeedConfig,
        parent_aggregator: StatsAggregator,
    ) -> Self {
        Self {
            total_bytes: 0,
            completed_ranges: 0,
            speed_calculator: SpeedCalculator::from_config(config),
            parent_aggregator: Some(parent_aggregator),
            current_chunk_size: 0,
            is_active: false,
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
    #[inline]
    pub(crate) fn record_chunk(&mut self, bytes: u64) {
        // 原子增加字节数（使用 Relaxed 顺序，性能最佳）
        self.total_bytes += bytes;
        let current_total = self.total_bytes;

        // 自动采样逻辑由 SpeedCalculator 内部处理
        self.speed_calculator.record_sample(current_total);

        // 同步到父级聚合器（如果存在）
        if let Some(parent) = &self.parent_aggregator {
            parent.record_chunk(bytes);
        }
    }

    /// 记录一个 range 完成
    #[inline]
    pub(crate) fn record_range_complete(&mut self) {
        self.completed_ranges += 1;

        // 同步到父级聚合器（如果存在）
        if let Some(parent) = &self.parent_aggregator {
            parent.record_range_complete();
        }
    }

    /// 获取当前平均下载速度
    ///
    /// 从开始到现在的总体平均速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    #[inline]
    pub(crate) fn get_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.speed_calculator.get_start_time()?;
        let elapsed = start_time.elapsed();
        if elapsed.as_secs_f64() > 0.0 {
            let bytes = self.total_bytes;
            Some(DownloadSpeed::new(bytes, elapsed))
        } else {
            None
        }
    }

    /// 获取实时下载速度
    ///
    /// 基于时间窗口的瞬时速度，通过与上次采样点比较计算增量
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    pub(crate) fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        self.speed_calculator.get_instant_speed()
    }

    /// 获取窗口平均下载速度
    ///
    /// 基于较长时间窗口的平均速度，用于检测异常下载线程
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    pub(crate) fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        self.speed_calculator.get_window_avg_speed()
    }

    /// 获取实时下载加速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadAcceleration)` 如果加速度计算有效，否则返回 `None`
    ///
    /// 注意：需要至少3个采样点才能计算加速度
    #[inline]
    pub(crate) fn get_instant_acceleration(&self) -> Option<DownloadAcceleration> {
        self.speed_calculator.get_instant_acceleration()
    }

    /// 获取统计摘要
    ///
    /// # Returns
    ///
    /// `(总字节数, 总耗时秒数, 完成的 range 数)`
    #[allow(dead_code)]
    pub(crate) fn get_summary(&self) -> (u64, f64, usize) {
        let bytes = self.total_bytes;
        let ranges = self.completed_ranges;
        let elapsed_secs = self
            .speed_calculator
            .get_start_time()
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
        self.current_chunk_size
    }

    /// 设置当前分块大小
    ///
    /// # Arguments
    ///
    /// * `size` - 新的分块大小 (bytes)
    #[inline]
    pub(crate) fn set_current_chunk_size(&mut self, size: u64) {
        self.current_chunk_size = size;
    }

    /// 清空采样点缓冲区
    ///
    /// 清空速度计算器中的所有采样点并重置开始时间。
    /// 用于在新的下载任务开始时重置统计状态，避免旧数据影响速度计算。
    #[inline]
    pub(crate) fn clear_samples(&mut self) {
        self.speed_calculator.clear_samples();
    }

    /// 设置 worker 活跃状态
    ///
    /// # Arguments
    ///
    /// * `active` - true 表示 worker 正在执行任务，false 表示空闲
    #[inline]
    pub(crate) fn set_active(&mut self, active: bool) {
        self.is_active = active;
    }

    /// 获取 worker 活跃状态
    ///
    /// # Returns
    ///
    /// true 表示 worker 正在执行任务，false 表示空闲
    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.is_active
    }

    /// 获取完整的统计摘要（包含平均速度、实时速度和窗口平均速度）
    ///
    /// # Returns
    ///
    /// 返回 `StatsSummary` 结构体，包含所有统计信息
    pub(crate) fn get_full_summary(&self) -> StatsSummary {
        let total_bytes = self.total_bytes;
        let completed_ranges = self.completed_ranges;
        let elapsed_secs = self
            .speed_calculator
            .get_start_time()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        StatsSummary {
            total_bytes,
            elapsed_secs,
            completed_ranges,
            avg_speed: self.get_speed(),
            instant_speed: self.get_instant_speed(),
            window_avg_speed: self.get_window_avg_speed(),
            instant_acceleration: self.get_instant_acceleration(),
        }
    }
}

impl std::fmt::Debug for WorkerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStats")
            .field("total_bytes", &self.total_bytes)
            .field("completed_ranges", &self.completed_ranges)
            .field("speed_calculator", &self.speed_calculator)
            .field("has_parent", &self.parent_aggregator.is_some())
            .field("current_chunk_size", &self.current_chunk_size)
            .field("is_active", &self.is_active)
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
#[derive(Debug, Clone)]
pub(crate) struct TaskStats {
    /// 聚合器，存储所有子统计的累加结果，包含速度计算器
    aggregator: StatsAggregator,
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
            aggregator: StatsAggregator {
                inner: LfrLock::new(StatsAggregatorInner {
                    total_bytes: 0,
                    completed_ranges: 0,
                    speed_calculator: SpeedCalculator::from_config(config),
                }),
            },
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
        WorkerStats::from_config_with_parent(&self.speed_config, self.aggregator.clone())
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
        let inner = self.aggregator.inner.read();
        let bytes = inner.total_bytes;
        let ranges = inner.completed_ranges;
        let elapsed_secs = inner
            .speed_calculator
            .get_start_time()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        (bytes, elapsed_secs, ranges)
    }

    /// 获取聚合平均速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use hydra_dl::TaskStats;
    ///
    /// let parent = TaskStats::default();
    /// if let Some(speed) = parent.get_speed() {
    ///     println!("平均速度: {}", speed);
    /// }
    /// ```
    #[inline]
    pub(crate) fn get_speed(&self) -> Option<DownloadSpeed> {
        let inner = self.aggregator.inner.read();
        let start_time = inner.speed_calculator.get_start_time()?;
        let elapsed = start_time.elapsed();
        if elapsed.as_secs_f64() > 0.0 {
            let bytes = inner.total_bytes;
            Some(DownloadSpeed::new(bytes, elapsed))
        } else {
            None
        }
    }

    /// 获取聚合实时速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use hydra_dl::TaskStats;
    ///
    /// let parent = TaskStats::default();
    /// if let Some(speed) = parent.get_instant_speed() {
    ///     println!("实时速度: {}", speed);
    /// }
    /// ```
    #[inline]
    pub(crate) fn get_instant_speed(&self) -> Option<DownloadSpeed> {
        self.aggregator.get_instant_speed()
    }

    /// 获取聚合窗口平均速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use hydra_dl::TaskStats;
    ///
    /// let parent = TaskStats::default();
    /// if let Some(speed) = parent.get_window_avg_speed() {
    ///     println!("窗口平均速度: {}", speed);
    /// }
    /// ```
    #[inline]
    pub(crate) fn get_window_avg_speed(&self) -> Option<DownloadSpeed> {
        self.aggregator.get_window_avg_speed()
    }

    /// 获取聚合实时加速度
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadAcceleration)` 如果加速度计算有效，否则返回 `None`
    ///
    /// 注意：需要至少3个采样点才能计算加速度
    #[inline]
    pub(crate) fn get_instant_acceleration(&self) -> Option<DownloadAcceleration> {
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
    /// if let Some(speed) = summary.avg_speed {
    ///     println!("平均速度: {}", speed);
    /// }
    /// ```
    pub(crate) fn get_full_summary(&self) -> StatsSummary {
        let inner = self.aggregator.inner.read();
        let total_bytes = inner.total_bytes;
        let completed_ranges = inner.completed_ranges;
        let elapsed_secs = inner
            .speed_calculator
            .get_start_time()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        StatsSummary {
            total_bytes,
            elapsed_secs,
            completed_ranges,
            avg_speed: self.get_speed(),
            instant_speed: self.get_instant_speed(),
            window_avg_speed: self.get_window_avg_speed(),
            instant_acceleration: self.get_instant_acceleration(),
        }
    }
}
