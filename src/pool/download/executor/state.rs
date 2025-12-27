//! 任务状态机
//!
//! 状态转换路径：Pending -> Started -> Running -> Ended

use crate::utils::stats::WorkerStatsActive;
use std::time::{Duration, Instant};

// ==========================================
// 独立的状态结构体定义
// ==========================================

/// Started 状态数据封装
#[derive(Debug, Clone)]
pub struct StartedState {
    start_time: Instant,
    current_chunk_size: u64,
}

impl StartedState {
    /// 获取任务启动时间
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// 获取当前分块大小
    pub fn current_chunk_size(&self) -> u64 {
        self.current_chunk_size
    }
}

/// Running 状态数据封装
#[derive(Debug, Clone)]
pub struct RunningState {
    start_time: Instant,
    stats: WorkerStatsActive,
    current_chunk_size: u64,
}

impl RunningState {
    /// 获取统计数据
    pub fn stats(&self) -> &WorkerStatsActive {
        &self.stats
    }

    /// 获取当前分块大小
    pub fn current_chunk_size(&self) -> u64 {
        self.current_chunk_size
    }

    /// 设置统计数据
    fn set_stats(&mut self, stats: WorkerStatsActive) {
        self.stats = stats;
    }
}

/// Ended 状态数据封装
#[derive(Debug, Clone)]
pub struct EndedState {
    consumed_time: Duration,
}

impl EndedState {
    /// 获取消耗时间
    pub fn consumed_time(&self) -> Duration {
        self.consumed_time
    }
}

// ==========================================
// TaskState 枚举
// ==========================================

/// 任务状态（对外暴露，不包含 Pending）
#[derive(Debug, Clone)]
pub enum TaskState {
    /// Started 状态
    Started(StartedState),
    /// Running 状态
    Running(RunningState),
    /// Ended 状态
    Ended(EndedState),
}

impl Default for TaskState {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskState {
    pub fn new() -> Self {
        Self::Started(StartedState {
            start_time: Instant::now(),
            current_chunk_size: 0,
        })
    }

    /// 获取运行中的统计数据（Running 状态）
    pub fn stats(&self) -> Option<&WorkerStatsActive> {
        match self {
            TaskState::Running(s) => Some(&s.stats),
            _ => None,
        }
    }
}

// ==========================================
// 内部状态机实现
// ==========================================

/// 初始状态
#[derive(Debug)]
pub struct Pending;

/// 活跃状态（Started 或 Running）
#[derive(Debug)]
pub enum Active {
    Started(StartedState),
    Running(RunningState),
}

/// 内部任务状态（泛型状态机）
#[derive(Debug)]
pub(crate) struct TaskInternalState<S> {
    pub state: S,
}

impl TaskInternalState<Pending> {
    /// 创建新的内部状态（Pending）
    pub fn new() -> Self {
        Self { state: Pending }
    }

    /// 转换到 Started 状态
    pub fn transition_to_started(self, current_chunk_size: u64) -> TaskInternalState<Active> {
        let started = StartedState {
            start_time: Instant::now(),
            current_chunk_size,
        };
        TaskInternalState {
            state: Active::Started(started),
        }
    }
}

impl TaskInternalState<Active> {
    /// 转换到 Running 状态或更新统计数据
    ///
    /// 如果当前是 Started，则转换为 Running。
    /// 如果当前是 Running，则更新统计数据。
    pub fn transition_to_running(&mut self, stats: WorkerStatsActive) {
        match &mut self.state {
            Active::Started(started) => {
                let running = RunningState {
                    start_time: started.start_time,
                    stats,
                    current_chunk_size: started.current_chunk_size,
                };
                self.state = Active::Running(running);
            }
            Active::Running(running) => {
                running.set_stats(stats);
            }
        }
    }
    /// 转换到 Ended 状态
    pub fn transition_to_ended(self) -> TaskInternalState<EndedState> {
        let start_time = match &self.state {
            Active::Started(s) => s.start_time,
            Active::Running(s) => s.start_time,
        };

        let state_struct = EndedState {
            consumed_time: start_time.elapsed(),
        };

        TaskInternalState {
            state: state_struct,
        }
    }

    /// 获取当前状态对应的 TaskState
    pub fn as_task_state(&self) -> TaskState {
        match &self.state {
            Active::Started(s) => TaskState::Started(s.clone()),
            Active::Running(s) => TaskState::Running(s.clone()),
        }
    }
}

impl TaskInternalState<EndedState> {
    pub fn consumed_time(&self) -> Duration {
        self.state.consumed_time
    }

    pub fn into_task_state(self) -> TaskState {
        TaskState::Ended(self.state)
    }
}

impl Default for TaskInternalState<Pending> {
    fn default() -> Self {
        Self::new()
    }
}
