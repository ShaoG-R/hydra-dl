use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// 统计聚合器
/// 
/// 轻量级的聚合器，用于父级统计，只包含原子计数器
/// 多个子统计可以共享同一个聚合器，实时更新父级数据
#[derive(Debug)]
pub(crate) struct StatsAggregator {
    /// 总下载字节数（原子操作，无锁）
    total_bytes: AtomicU64,
    /// 完成的 range 数量（原子操作，无锁）
    completed_ranges: AtomicUsize,
    /// 下载开始时间（只初始化一次）
    start_time: OnceLock<Instant>,
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
/// 2. **实时速度**：基于时间窗口的瞬时速度
///    - 通过 `get_instant_speed()` 获取
///    - 采用简单采样法：记录最近一次采样点，计算增量
///    - 时间窗口可配置（默认 1 秒）
/// 
/// # 性能优化
/// 
/// - 所有字段使用原子操作，完全无锁并发
/// - `start_time` 使用 OnceLock，只初始化一次
/// - `last_sample_*` 使用原子操作存储采样点，无需 Mutex
/// - 支持父子统计聚合，子统计自动更新父级
/// 
/// # 线程安全
/// 
/// 所有方法都是线程安全的，可以被多个 worker 并发调用
pub(crate) struct DownloadStats {
    /// 总下载字节数（原子操作，无锁）
    total_bytes: AtomicU64,
    /// 下载开始时间（只初始化一次）
    pub(crate) start_time: OnceLock<Instant>,
    /// 完成的 range 数量（原子操作，无锁）
    completed_ranges: AtomicUsize,
    /// 上一次采样点的时间戳（纳秒，相对于 start_time）
    last_sample_timestamp_ns: AtomicU64,
    /// 上一次采样点的字节数
    last_sample_bytes: AtomicU64,
    /// 实时速度的时间窗口（默认 1 秒）
    instant_speed_window: Duration,
    /// 父级聚合器（子统计通过此字段自动更新父级）
    parent_aggregator: Option<Arc<StatsAggregator>>,
    /// 当前分块大小（原子操作，无锁）
    /// 由 worker 内部的 ChunkStrategy 更新，外部只读访问
    current_chunk_size: AtomicU64,
}

impl Default for DownloadStats {
    fn default() -> Self {
        Self::with_window(Duration::from_secs(1))
    }
}

impl DownloadStats {
    /// 创建新的统计实例并指定时间窗口
    /// 
    /// # Arguments
    /// 
    /// * `instant_speed_window` - 实时速度的时间窗口，用于计算瞬时速度
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStats;
    /// use std::time::Duration;
    /// 
    /// // 使用 500ms 的时间窗口
    /// let stats = DownloadStats::with_window(Duration::from_millis(500));
    /// ```
    pub(crate) fn with_window(instant_speed_window: Duration) -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            start_time: OnceLock::new(),
            completed_ranges: AtomicUsize::new(0),
            last_sample_timestamp_ns: AtomicU64::new(0),
            last_sample_bytes: AtomicU64::new(0),
            instant_speed_window,
            parent_aggregator: None,
            current_chunk_size: AtomicU64::new(0),
        }
    }

    /// 创建带父级聚合器的统计实例
    /// 
    /// # Arguments
    /// 
    /// * `instant_speed_window` - 实时速度的时间窗口
    /// * `parent_aggregator` - 父级聚合器，子统计会自动更新父级
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStats;
    /// use std::sync::Arc;
    /// 
    /// let parent = DownloadStatsParent::default();
    /// let child = parent.create_child();
    /// 
    /// child.record_chunk(1024);
    /// let (bytes, _, _) = parent.get_summary();
    /// assert_eq!(bytes, 1024);
    /// ```
    pub(crate) fn with_parent(
        instant_speed_window: Duration,
        parent_aggregator: Arc<StatsAggregator>,
    ) -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            start_time: OnceLock::new(),
            completed_ranges: AtomicUsize::new(0),
            last_sample_timestamp_ns: AtomicU64::new(0),
            last_sample_bytes: AtomicU64::new(0),
            instant_speed_window,
            parent_aggregator: Some(parent_aggregator),
            current_chunk_size: AtomicU64::new(0),
        }
    }

    /// 记录下载的 chunk（在下载过程中实时调用）
    /// 
    /// 使用原子操作，多个 worker 可以并发调用，无锁竞争
    /// 
    /// # Arguments
    /// 
    /// * `bytes` - 本次下载的字节数
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStats;
    /// 
    /// let stats = DownloadStats::default();
    /// stats.record_chunk(1024); // 记录下载了 1KB
    /// ```
    pub(crate) fn record_chunk(&self, bytes: u64) {
        // 第一个 chunk 到达时初始化开始时间
        self.start_time.get_or_init(|| Instant::now());
        
        // 原子增加字节数（使用 Relaxed 顺序，性能最佳）
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        
        // 同步到父级聚合器（如果存在）
        if let Some(parent) = &self.parent_aggregator {
            parent.start_time.get_or_init(|| Instant::now());
            parent.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// 记录一个 range 完成
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStats;
    /// 
    /// let stats = DownloadStats::default();
    /// stats.record_range_complete(); // 记录完成了一个 range
    /// ```
    pub(crate) fn record_range_complete(&self) {
        self.completed_ranges.fetch_add(1, Ordering::Relaxed);
        
        // 同步到父级聚合器（如果存在）
        if let Some(parent) = &self.parent_aggregator {
            parent.completed_ranges.fetch_add(1, Ordering::Relaxed);
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
    /// use hydra_dl::DownloadStats;
    /// 
    /// let stats = DownloadStats::default();
    /// stats.record_chunk(1024 * 1024); // 下载 1MB
    /// 
    /// let speed = stats.get_speed();
    /// println!("平均速度: {:.2} MB/s", speed / 1024.0 / 1024.0);
    /// ```
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
    /// 如果距离上次采样时间超过配置的时间窗口，则更新采样点
    /// 
    /// # 采样策略
    /// 
    /// - 首次调用时初始化采样点
    /// - 如果距离上次采样时间 >= 时间窗口，则：
    ///   1. 计算与上次采样点的字节差和时间差
    ///   2. 计算实时速度 = 字节差 / 时间差
    ///   3. 更新采样点为当前时刻
    /// - 如果距离上次采样时间 < 时间窗口，则：
    ///   1. 使用当前数据与上次采样点计算速度（估算值）
    ///   2. 不更新采样点
    /// 
    /// # Returns
    /// 
    /// `(实时速度, 是否有效)`：
    /// - 实时速度：基于采样点计算的速度（bytes/s）
    /// - 是否有效：true 表示采样点有效，false 表示刚初始化（elapsed < 10ms）
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStats;
    /// use std::time::Duration;
    /// use std::thread;
    /// 
    /// let stats = DownloadStats::default();
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
    pub(crate) fn get_instant_speed(&self) -> (f64, bool) {
        let now = Instant::now();
        let current_bytes = self.total_bytes.load(Ordering::Relaxed);
        
        // 获取采样点（原子读取）
        let last_timestamp_ns = self.last_sample_timestamp_ns.load(Ordering::Relaxed);
        let last_bytes = self.last_sample_bytes.load(Ordering::Relaxed);
        
        // 获取开始时间，如果还未初始化则使用当前时间
        let start_time = self.start_time.get().copied().unwrap_or(now);
        
        // 首次调用：初始化采样点
        if last_timestamp_ns == 0 {
            let elapsed_ns = start_time.elapsed().as_nanos() as u64;
            self.last_sample_timestamp_ns.store(elapsed_ns, Ordering::Relaxed);
            self.last_sample_bytes.store(current_bytes, Ordering::Relaxed);
            return (0.0, false);
        }
        
        // 计算距离上次采样的时间
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;
        let elapsed_ns = current_elapsed_ns.saturating_sub(last_timestamp_ns);
        let elapsed = Duration::from_nanos(elapsed_ns);
        
        // 如果超过时间窗口，更新采样点并计算速度
        if elapsed >= self.instant_speed_window {
            let bytes_diff = current_bytes.saturating_sub(last_bytes);
            let elapsed_secs = elapsed.as_secs_f64();
            let instant_speed = if elapsed_secs > 0.0 {
                bytes_diff as f64 / elapsed_secs
            } else {
                0.0
            };
            
            // 原子更新采样点（无锁，可能有多个线程同时更新，但不影响正确性）
            self.last_sample_timestamp_ns.store(current_elapsed_ns, Ordering::Relaxed);
            self.last_sample_bytes.store(current_bytes, Ordering::Relaxed);
            
            (instant_speed, true)
        } else {
            // 时间窗口未到，返回估算速度
            let bytes_diff = current_bytes.saturating_sub(last_bytes);
            let elapsed_secs = elapsed.as_secs_f64();
            let instant_speed = if elapsed_secs > 0.0 {
                bytes_diff as f64 / elapsed_secs
            } else {
                0.0
            };
            
            // 如果是刚初始化（elapsed 接近 0），返回 false
            (instant_speed, elapsed_secs > 0.01)
        }
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
    /// use hydra_dl::DownloadStats;
    /// 
    /// let stats = DownloadStats::default();
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
    pub(crate) fn get_current_chunk_size(&self) -> u64 {
        self.current_chunk_size.load(Ordering::Relaxed)
    }
    
    /// 设置当前分块大小
    /// 
    /// # Arguments
    /// 
    /// * `size` - 新的分块大小 (bytes)
    pub(crate) fn set_current_chunk_size(&self, size: u64) {
        self.current_chunk_size.store(size, Ordering::Relaxed);
    }

    /// 获取完整的统计摘要（包含平均速度和实时速度）
    /// 
    /// # Returns
    /// 
    /// `(总字节数, 总耗时秒数, 完成的 range 数, 平均速度 bytes/s, 实时速度 bytes/s, 实时速度是否有效)`
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStats;
    /// 
    /// let stats = DownloadStats::default();
    /// stats.record_chunk(1024 * 1024);
    /// 
    /// let (bytes, elapsed, ranges, avg_speed, instant_speed, valid) = stats.get_full_summary();
    /// println!("平均速度: {:.2} MB/s", avg_speed / 1024.0 / 1024.0);
    /// if valid {
    ///     println!("实时速度: {:.2} MB/s", instant_speed / 1024.0 / 1024.0);
    /// }
    /// ```
    pub(crate) fn get_full_summary(&self) -> (u64, f64, usize, f64, f64, bool) {
        let bytes = self.total_bytes.load(Ordering::Relaxed);
        let ranges = self.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.start_time
            .get()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let avg_speed = self.get_speed();
        let (instant_speed, instant_valid) = self.get_instant_speed();
        
        (bytes, elapsed_secs, ranges, avg_speed, instant_speed, instant_valid)
    }
}

impl std::fmt::Debug for DownloadStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadStats")
            .field("total_bytes", &self.total_bytes)
            .field("start_time", &self.start_time)
            .field("completed_ranges", &self.completed_ranges)
            .field("last_sample_timestamp_ns", &self.last_sample_timestamp_ns)
            .field("last_sample_bytes", &self.last_sample_bytes)
            .field("instant_speed_window", &self.instant_speed_window)
            .field("has_parent", &self.parent_aggregator.is_some())
            .finish()
    }
}

/// 父级统计管理器
/// 
/// 管理多个子统计（child stats），子统计的数据会自动聚合到父级
/// 通过父级可以 O(1) 获取所有子统计的聚合结果，无需遍历
/// 
/// # 使用场景
/// 
/// 在 WorkerPool 中，创建一个 parent stats，每个 worker 通过 `create_child()` 
/// 创建自己的 child stats。当 child 记录数据时，会自动同步到 parent。
/// 
/// # Examples
/// 
/// ```ignore
/// use hydra_dl::DownloadStatsParent;
/// 
/// let parent = DownloadStatsParent::new();
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
pub(crate) struct DownloadStatsParent {
    /// 聚合器，存储所有子统计的累加结果
    aggregator: Arc<StatsAggregator>,
    /// 实时速度的时间窗口
    instant_speed_window: Duration,
    /// 父级自己的采样点（用于计算聚合的实时速度）
    last_sample_timestamp_ns: AtomicU64,
    last_sample_bytes: AtomicU64,
}

impl Default for DownloadStatsParent {
    fn default() -> Self {
        Self::with_window(Duration::from_secs(1))
    }
}

impl DownloadStatsParent {
    /// 创建新的父级统计管理器并指定时间窗口
    /// 
    /// # Arguments
    /// 
    /// * `instant_speed_window` - 实时速度的时间窗口
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStatsParent;
    /// use std::time::Duration;
    /// 
    /// let parent = DownloadStatsParent::default();
    /// ```
    pub(crate) fn with_window(instant_speed_window: Duration) -> Self {
        Self {
            aggregator: Arc::new(StatsAggregator {
                total_bytes: AtomicU64::new(0),
                completed_ranges: AtomicUsize::new(0),
                start_time: OnceLock::new(),
            }),
            instant_speed_window,
            last_sample_timestamp_ns: AtomicU64::new(0),
            last_sample_bytes: AtomicU64::new(0),
        }
    }

    /// 创建子统计
    /// 
    /// 子统计记录数据时会自动同步到父级，实现 O(1) 聚合
    /// 子统计使用与父级相同的时间窗口
    /// 
    /// # Returns
    /// 
    /// 返回一个新的 `DownloadStats` 实例，其 parent 字段指向当前父级
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStatsParent;
    /// 
    /// let parent = DownloadStatsParent::new();
    /// let child = parent.create_child();
    /// 
    /// child.record_chunk(1024);
    /// // parent 的统计会自动更新
    /// ```
    pub(crate) fn create_child(&self) -> DownloadStats {
        self.create_child_with_window(self.instant_speed_window)
    }

    /// 创建子统计并指定时间窗口
    /// 
    /// # Arguments
    /// 
    /// * `instant_speed_window` - 子统计的实时速度时间窗口
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStatsParent;
    /// use std::time::Duration;
    /// 
    /// let parent = DownloadStatsParent::new();
    /// let child = parent.create_child_with_window(Duration::from_millis(200));
    /// ```
    pub(crate) fn create_child_with_window(&self, instant_speed_window: Duration) -> DownloadStats {
        DownloadStats::with_parent(instant_speed_window, Arc::clone(&self.aggregator))
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
    /// use hydra_dl::DownloadStatsParent;
    /// 
    /// let parent = DownloadStatsParent::new();
    /// let (bytes, elapsed, ranges) = parent.get_summary();
    /// ```
    pub(crate) fn get_summary(&self) -> (u64, f64, usize) {
        let bytes = self.aggregator.total_bytes.load(Ordering::Relaxed);
        let ranges = self.aggregator.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.aggregator.start_time
            .get()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        (bytes, elapsed_secs, ranges)
    }

    /// 获取聚合平均速度（bytes/s）
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStatsParent;
    /// 
    /// let parent = DownloadStatsParent::new();
    /// let speed = parent.get_speed();
    /// ```
    pub(crate) fn get_speed(&self) -> f64 {
        if let Some(start_time) = self.aggregator.start_time.get() {
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
    /// use hydra_dl::DownloadStatsParent;
    /// 
    /// let parent = DownloadStatsParent::new();
    /// let (instant_speed, valid) = parent.get_instant_speed();
    /// ```
    pub(crate) fn get_instant_speed(&self) -> (f64, bool) {
        let now = Instant::now();
        let current_bytes = self.aggregator.total_bytes.load(Ordering::Relaxed);
        
        // 获取采样点（原子读取）
        let last_timestamp_ns = self.last_sample_timestamp_ns.load(Ordering::Relaxed);
        let last_bytes = self.last_sample_bytes.load(Ordering::Relaxed);
        
        // 获取开始时间，如果还未初始化则使用当前时间
        let start_time = self.aggregator.start_time.get().copied().unwrap_or(now);
        
        // 首次调用：初始化采样点
        if last_timestamp_ns == 0 {
            let elapsed_ns = start_time.elapsed().as_nanos() as u64;
            self.last_sample_timestamp_ns.store(elapsed_ns, Ordering::Relaxed);
            self.last_sample_bytes.store(current_bytes, Ordering::Relaxed);
            return (0.0, false);
        }
        
        // 计算距离上次采样的时间
        let current_elapsed_ns = start_time.elapsed().as_nanos() as u64;
        let elapsed_ns = current_elapsed_ns.saturating_sub(last_timestamp_ns);
        let elapsed = Duration::from_nanos(elapsed_ns);
        
        // 如果超过时间窗口，更新采样点并计算速度
        if elapsed >= self.instant_speed_window {
            let bytes_diff = current_bytes.saturating_sub(last_bytes);
            let elapsed_secs = elapsed.as_secs_f64();
            let instant_speed = if elapsed_secs > 0.0 {
                bytes_diff as f64 / elapsed_secs
            } else {
                0.0
            };
            
            // 原子更新采样点（无锁）
            self.last_sample_timestamp_ns.store(current_elapsed_ns, Ordering::Relaxed);
            self.last_sample_bytes.store(current_bytes, Ordering::Relaxed);
            
            (instant_speed, true)
        } else {
            // 时间窗口未到，返回估算速度
            let bytes_diff = current_bytes.saturating_sub(last_bytes);
            let elapsed_secs = elapsed.as_secs_f64();
            let instant_speed = if elapsed_secs > 0.0 {
                bytes_diff as f64 / elapsed_secs
            } else {
                0.0
            };
            
            // 如果是刚初始化（elapsed 接近 0），返回 false
            (instant_speed, elapsed_secs > 0.01)
        }
    }

    /// 获取完整的聚合统计
    /// 
    /// # Returns
    /// 
    /// `(总字节数, 总耗时秒数, 完成的 range 数, 平均速度 bytes/s, 实时速度 bytes/s, 实时速度是否有效)`
    /// 
    /// # Examples
    /// 
    /// ```ignore
    /// use hydra_dl::DownloadStatsParent;
    /// 
    /// let parent = DownloadStatsParent::new();
    /// let (bytes, elapsed, ranges, avg_speed, instant_speed, valid) = parent.get_full_summary();
    /// ```
    #[allow(dead_code)]
    pub(crate) fn get_full_summary(&self) -> (u64, f64, usize, f64, f64, bool) {
        let bytes = self.aggregator.total_bytes.load(Ordering::Relaxed);
        let ranges = self.aggregator.completed_ranges.load(Ordering::Relaxed);
        let elapsed_secs = self.aggregator.start_time
            .get()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let avg_speed = self.get_speed();
        let (instant_speed, instant_valid) = self.get_instant_speed();
        
        (bytes, elapsed_secs, ranges, avg_speed, instant_speed, instant_valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_stats() {
        let stats = DownloadStats::default();
        let (bytes, elapsed, ranges) = stats.get_summary();
        
        assert_eq!(bytes, 0);
        assert_eq!(elapsed, 0.0);
        assert_eq!(ranges, 0);
    }

    #[test]
    fn test_with_custom_window() {
        let stats = DownloadStats::with_window(Duration::from_millis(500));
        assert_eq!(stats.instant_speed_window, Duration::from_millis(500));
    }

    #[test]
    fn test_record_chunk() {
        let stats = DownloadStats::default();
        
        stats.record_chunk(1024);
        let (bytes, _, _) = stats.get_summary();
        assert_eq!(bytes, 1024);
        
        stats.record_chunk(2048);
        let (bytes, _, _) = stats.get_summary();
        assert_eq!(bytes, 3072);
    }

    #[test]
    fn test_record_range_complete() {
        let stats = DownloadStats::default();
        
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
        let stats = DownloadStats::default();
        
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
        let stats = DownloadStats::default();
        
        stats.record_chunk(1024);
        
        // 第一次调用应该初始化采样点
        let (speed, valid) = stats.get_instant_speed();
        assert_eq!(speed, 0.0, "首次调用速度应该为 0");
        assert!(!valid, "首次调用 valid 应该为 false");
    }

    #[test]
    fn test_instant_speed_before_window() {
        let stats = DownloadStats::with_window(Duration::from_secs(1));
        
        stats.record_chunk(1024 * 1024); // 1 MB
        
        // 第一次调用初始化采样点
        let (speed, valid) = stats.get_instant_speed();
        assert_eq!(speed, 0.0, "首次调用速度应该为 0");
        assert!(!valid, "首次调用 valid 应该为 false");
        
        // 等待一小段时间（小于时间窗口）
        thread::sleep(Duration::from_millis(100));
        
        stats.record_chunk(1024 * 1024); // 再下载 1 MB
        
        // 第二次调用应该计算出速度
        let (speed, valid) = stats.get_instant_speed();
        assert!(valid, "第二次调用应该是有效的");
        assert!(speed > 0.0, "速度应该大于 0");
    }

    #[test]
    fn test_instant_speed_after_window() {
        let stats = DownloadStats::with_window(Duration::from_millis(100));
        
        stats.record_chunk(1024 * 1024); // 1 MB
        
        // 第一次调用初始化
        stats.get_instant_speed();
        
        // 等待超过时间窗口
        thread::sleep(Duration::from_millis(150));
        
        stats.record_chunk(1024 * 1024); // 再下载 1 MB
        
        let (speed, valid) = stats.get_instant_speed();
        assert!(valid, "应该是有效的");
        assert!(speed > 0.0, "速度应该大于 0");
        
        // 速度应该接近 1MB / 0.15s ≈ 6.67 MB/s
        let speed_mbs = speed / 1024.0 / 1024.0;
        assert!(speed_mbs > 5.0 && speed_mbs < 10.0, 
            "速度应该在合理范围内，实际: {:.2} MB/s", speed_mbs);
    }

    #[test]
    fn test_full_summary() {
        let stats = DownloadStats::default();
        
        stats.record_chunk(2048);
        stats.record_range_complete();
        
        thread::sleep(Duration::from_millis(50));
        
        let (bytes, elapsed, ranges, avg_speed, instant_speed, valid) = stats.get_full_summary();
        
        assert_eq!(bytes, 2048);
        assert!(elapsed > 0.0);
        assert_eq!(ranges, 1);
        assert!(avg_speed > 0.0);
        assert_eq!(instant_speed, 0.0); // 首次调用
        assert!(!valid); // 首次调用
    }

    #[test]
    fn test_concurrent_record_chunk() {
        use std::sync::Arc;
        
        let stats = Arc::new(DownloadStats::default());
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
        
        let stats = Arc::new(DownloadStats::default());
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
        let stats = DownloadStats::default();
        
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
        let stats = DownloadStats::with_window(Duration::from_millis(50));
        
        // 第一次采样
        stats.record_chunk(1024 * 1024);
        let (speed1, valid1) = stats.get_instant_speed();
        assert_eq!(speed1, 0.0);
        assert!(!valid1);
        
        // 等待超过窗口
        thread::sleep(Duration::from_millis(60));
        stats.record_chunk(1024 * 1024);
        let (speed2, valid2) = stats.get_instant_speed();
        assert!(valid2);
        assert!(speed2 > 0.0);
        
        // 再等待超过窗口
        thread::sleep(Duration::from_millis(60));
        stats.record_chunk(1024 * 1024);
        let (speed3, valid3) = stats.get_instant_speed();
        assert!(valid3);
        assert!(speed3 > 0.0);
    }

    #[test]
    fn test_parent_child_aggregation() {
        let parent = DownloadStatsParent::default();
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
    fn test_parent_child_concurrent_aggregation() {
        use std::sync::Arc;
        use std::thread;
        
        let parent = Arc::new(DownloadStatsParent::default());
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

