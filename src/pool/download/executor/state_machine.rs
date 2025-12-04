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
}

impl StartedState {
    /// 获取任务启动时间
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// 转换为通用 TaskState
    pub fn into_task_state(self) -> TaskState {
        TaskState::Started(self)
    }
}

/// Running 状态数据封装
#[derive(Debug, Clone)]
pub struct RunningState {
    start_time: Instant,
    stats: WorkerStatsActive,
}

impl RunningState {
    /// 获取任务启动时间
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// 获取统计数据
    pub fn stats(&self) -> &WorkerStatsActive {
        &self.stats
    }

    /// 转换为通用 TaskState
    pub fn into_task_state(self) -> TaskState {
        TaskState::Running(self)
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

    /// 转换为通用 TaskState
    pub fn into_task_state(self) -> TaskState {
        TaskState::Ended(self)
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

impl TaskState {
    pub fn new() -> Self {
        Self::Started(StartedState {
            start_time: Instant::now(),
        })
    }

    /// 获取任务启动时间（Started 或 Running 状态）
    pub fn start_time(&self) -> Option<Instant> {
        match self {
            TaskState::Started(s) => Some(s.start_time),
            TaskState::Running(s) => Some(s.start_time),
            _ => None,
        }
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

/// 内部任务状态（包含 Pending）
#[derive(Debug)]
pub(crate) enum TaskInternalState {
    /// 初始状态
    Pending,
    /// 活跃状态
    Active(TaskState),
}

impl TaskInternalState {
    /// 创建新的内部状态（Pending）
    pub fn new() -> Self {
        Self::Pending
    }

    /// 转换到 Started 状态，并返回 StartedState
    pub fn transition_to_started(&mut self) -> StartedState {
        let state_struct = StartedState {
            start_time: Instant::now(),
        };

        *self = TaskInternalState::Active(TaskState::Started(state_struct.clone()));
        state_struct
    }

    /// 转换到 Running 状态，并返回 RunningState
    pub fn transition_to_running(&mut self, stats: WorkerStatsActive) -> Option<RunningState> {
        // 只有从 Started 状态才能首次进入 Running
        if let TaskInternalState::Active(TaskState::Started(started)) = self {
            let state_struct = RunningState {
                start_time: started.start_time,
                stats,
            };

            *self = TaskInternalState::Active(TaskState::Running(state_struct.clone()));
            Some(state_struct)
        } else {
            None
        }
    }

    /// 更新统计数据，并返回最新的 RunningState
    pub fn update_stats(&mut self, new_stats: WorkerStatsActive) -> Option<RunningState> {
        // 只有在 Running 状态下才能更新
        if let TaskInternalState::Active(TaskState::Running(running)) = self {
            let state_struct = RunningState {
                start_time: running.start_time,
                stats: new_stats,
            };

            *self = TaskInternalState::Active(TaskState::Running(state_struct.clone()));
            Some(state_struct)
        } else {
            None
        }
    }

    /// 转换到 Ended 状态，并返回 EndedState
    pub fn transition_to_ended(&mut self) -> Option<EndedState> {
        let elapsed = match self {
            TaskInternalState::Active(TaskState::Started(s)) => s.start_time.elapsed(),
            TaskInternalState::Active(TaskState::Running(s)) => s.start_time.elapsed(),
            _ => return None,
        };

        let state_struct = EndedState {
            consumed_time: elapsed,
        };

        *self = TaskInternalState::Active(TaskState::Ended(state_struct.clone()));
        Some(state_struct)
    }

    /// 检查是否为 Started 状态
    pub fn is_started(&self) -> bool {
        matches!(self, TaskInternalState::Active(TaskState::Started(_)))
    }

    /// 检查是否为 Running 状态
    pub fn is_running(&self) -> bool {
        matches!(self, TaskInternalState::Active(TaskState::Running(_)))
    }
}

impl Default for TaskInternalState {
    fn default() -> Self {
        Self::new()
    }
}
