use super::speed_calculator::SpeedCalculator;
use net_bytes::DownloadSpeed;

/// 速度统计快照
///
/// 封装所有速度相关的统计数据，一次性获取避免多次遍历
/// 注意：进度相关的 written_bytes 由 ExecutorStats 单独管理
#[derive(Debug, Clone, Copy, Default)]
pub struct SpeedStats {
    /// 当前分块大小 (bytes)
    pub current_chunk_size: u64,
    /// 平均速度（从开始到现在）
    pub avg_speed: Option<DownloadSpeed>,
    /// 实时速度（基于短时间窗口）
    pub instant_speed: Option<DownloadSpeed>,
    /// 窗口平均速度（基于较长时间窗口）
    pub window_avg_speed: Option<DownloadSpeed>,
}

impl SpeedStats {
    /// 创建空的速度统计
    pub fn empty() -> Self {
        Self::default()
    }
}


/// Worker 速度统计
///
/// 专注于速度计算，跟踪 total_bytes（包含重试字节）用于速度计算。
/// 进度相关的 written_bytes 由 ExecutorStats 单独管理。
#[derive(Clone)]
pub(crate) struct WorkerStats {
    /// 总下载字节数（包括重试的重复字节，用于速度计算）
    total_bytes: u64,
    /// 速度计算器（管理瞬时速度和窗口平均速度的采样点）
    speed_calculator: SpeedCalculator,
    /// 当前分块大小
    /// 由 worker 内部的 ChunkStrategy 更新，外部只读访问
    current_chunk_size: u64,
    /// Worker 生命周期开始时间（用于计算整体平均速度）
    /// 不随 clear_samples() 重置，在第一次记录数据时初始化
    worker_start_time: Option<std::time::Instant>,
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
            speed_calculator: SpeedCalculator::from_config(config),
            current_chunk_size: 0,
            worker_start_time: None,
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
    /// # Returns
    ///
    /// `true` 表示成功采样，`false` 表示跳过
    #[inline]
    pub(crate) fn record_chunk(&mut self, bytes: u64) -> bool {
        // 第一次记录时初始化 worker 开始时间（用于计算整体平均速度）
        if self.worker_start_time.is_none() {
            self.worker_start_time = Some(std::time::Instant::now());
        }

        self.total_bytes += bytes;
        let sampled = self.speed_calculator.record_sample(self.total_bytes);
        sampled
    }

    /// 获取当前平均下载速度
    ///
    /// 从 Worker 开始工作到现在的总体平均速度
    /// 使用 worker_start_time 而不是 speed_calculator 的 start_time，
    /// 因为后者会在每次任务开始时重置
    ///
    /// # Returns
    ///
    /// 返回 `Some(DownloadSpeed)` 如果速度计算有效，否则返回 `None`
    #[inline]
    pub(crate) fn get_speed(&self) -> Option<DownloadSpeed> {
        let start_time = self.worker_start_time?;
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

    /// 获取所有速度统计的快照
    ///
    /// 一次性获取所有速度相关数据，避免多次调用
    #[inline]
    pub(crate) fn get_speed_stats(&self) -> SpeedStats {
        SpeedStats {
            current_chunk_size: self.current_chunk_size,
            avg_speed: self.get_speed(),
            instant_speed: self.speed_calculator.get_instant_speed(),
            window_avg_speed: self.speed_calculator.get_window_avg_speed(),
        }
    }

    /// 获取总下载字节数（包括重试的重复字节）
    ///
    /// # Returns
    ///
    /// 总传输字节数（用于速度计算）
    #[inline]
    pub(crate) fn get_total_bytes(&self) -> u64 {
        self.total_bytes
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
}

impl std::fmt::Debug for WorkerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStats")
            .field("total_bytes", &self.total_bytes)
            .field("speed_calculator", &self.speed_calculator)
            .field("current_chunk_size", &self.current_chunk_size)
            .finish()
    }
}
