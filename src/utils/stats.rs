use super::speed_calculator::{SpeedCalculatorActive, SpeedCalculatorRecording};
use net_bytes::DownloadSpeed;
use std::time::Instant;

/// 速度统计快照
///
/// 封装所有速度相关的统计数据，一次性获取避免多次遍历
/// 注意：进度相关的 written_bytes 由 ExecutorStats 单独管理
///
/// 速度字段不再使用 `Option`，因为只有在成功采样后才会创建 `WorkerStatsActive`。
#[derive(Debug, Clone, Copy)]
pub struct SpeedStats {
    /// 当前分块大小 (bytes)
    pub current_chunk_size: u64,
    /// 平均速度（从开始到现在）
    pub avg_speed: DownloadSpeed,
    /// 实时速度（基于短时间窗口）
    pub instant_speed: DownloadSpeed,
    /// 窗口平均速度（基于较长时间窗口）
    pub window_avg_speed: DownloadSpeed,
}

/// Worker 速度统计 - 记录状态（未激活）
///
/// 专注于采样记录，跟踪 total_bytes（包含重试字节）用于速度计算。
/// 进度相关的 written_bytes 由 ExecutorStats 单独管理。
///
/// # 类型状态模式
///
/// - `WorkerStatsRecording`：记录状态，用于采样
/// - `WorkerStatsActive`：激活状态，用于计算速度（速度返回无 `Option`）
#[derive(Clone)]
pub(crate) struct WorkerStatsRecording {
    /// 总下载字节数（包括重试的重复字节，用于速度计算）
    total_bytes: u64,
    /// 速度计算器（管理瞬时速度和窗口平均速度的采样点）
    speed_calculator: SpeedCalculatorRecording,
    /// 当前分块大小
    /// 由 worker 内部的 ChunkStrategy 更新，外部只读访问
    current_chunk_size: u64,
    /// Worker 生命周期开始时间（用于计算整体平均速度）
    /// 不随 clear_samples() 重置，在第一次记录数据时初始化
    worker_start_time: Option<Instant>,
}

/// Worker 速度统计 - 激活状态
///
/// 成功采样后的状态，`worker_start_time` 无 `Option` 包装。
/// 所有速度计算方法返回非 `Option` 的 `DownloadSpeed`。
///
/// # 类型状态模式
///
/// 激活状态保证 `worker_start_time` 存在，因此速度计算一定有效。
#[derive(Clone)]
pub(crate) struct WorkerStatsActive {
    /// 总下载字节数（包括重试的重复字节，用于速度计算）
    total_bytes: u64,
    /// 速度计算器（激活状态）
    speed_calculator: SpeedCalculatorActive,
    /// Worker 生命周期开始时间（已初始化，无 Option）
    worker_start_time: Instant,
}

impl Default for WorkerStatsRecording {
    #[inline]
    fn default() -> Self {
        // 使用默认配置值
        use crate::config::SpeedConfig;
        Self::from_config(&SpeedConfig::default())
    }
}

impl WorkerStatsRecording {
    /// 从速度配置创建统计实例（记录状态）
    pub(crate) fn from_config(config: &crate::config::SpeedConfig) -> Self {
        Self {
            total_bytes: 0,
            speed_calculator: SpeedCalculatorRecording::from_config(config),
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
    /// - `Some(WorkerStatsActive)`: 成功采样，返回激活状态的统计
    /// - `None`: 跳过采样（未达到采样间隔）
    #[inline]
    pub(crate) fn record_chunk(&mut self, bytes: u64) -> Option<WorkerStatsActive> {
        // 第一次记录时初始化 worker 开始时间（用于计算整体平均速度）
        let worker_start_time = *self.worker_start_time.get_or_insert_with(Instant::now);

        self.total_bytes += bytes;

        // 尝试采样，如果成功则返回激活状态
        let speed_calculator_active = self.speed_calculator.record_sample(self.total_bytes)?;

        Some(WorkerStatsActive {
            total_bytes: self.total_bytes,
            speed_calculator: speed_calculator_active,
            worker_start_time,
        })
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

impl WorkerStatsActive {
    /// 获取当前平均下载速度
    ///
    /// 从 Worker 开始工作到现在的总体平均速度
    ///
    /// # Returns
    ///
    /// `DownloadSpeed` - 速度值（保证有效，无 Option）
    #[inline]
    pub(crate) fn get_speed(&self) -> DownloadSpeed {
        let elapsed = self.worker_start_time.elapsed();
        DownloadSpeed::new(self.total_bytes, elapsed)
    }

    /// 获取实时下载速度
    ///
    /// 基于时间窗口的瞬时速度，通过与上次采样点比较计算增量
    ///
    /// # Returns
    ///
    /// `DownloadSpeed` - 速度值（保证有效，无 Option）
    pub(crate) fn get_instant_speed(&self) -> DownloadSpeed {
        self.speed_calculator.get_instant_speed()
    }

    /// 获取窗口平均下载速度
    ///
    /// 基于较长时间窗口的平均速度，用于检测异常下载线程
    ///
    /// # Returns
    ///
    /// `DownloadSpeed` - 速度值（保证有效，无 Option）
    pub(crate) fn get_window_avg_speed(&self) -> DownloadSpeed {
        self.speed_calculator.get_window_avg_speed()
    }

    /// 获取所有速度统计的快照
    ///
    /// 一次性获取所有速度相关数据，避免多次调用
    ///
    /// # Returns
    ///
    /// `SpeedStats` - 速度统计快照（所有字段保证有效，无 Option）
    #[inline]
    pub(crate) fn get_speed_stats(&self, current_chunk_size: u64) -> SpeedStats {
        SpeedStats {
            current_chunk_size,
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
}

impl std::fmt::Debug for WorkerStatsRecording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStatsRecording")
            .field("total_bytes", &self.total_bytes)
            .field("speed_calculator", &self.speed_calculator)
            .field("current_chunk_size", &self.current_chunk_size)
            .finish()
    }
}

impl std::fmt::Debug for WorkerStatsActive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStatsActive")
            .field("total_bytes", &self.total_bytes)
            .field("speed_calculator", &self.speed_calculator)
            .finish()
    }
}
