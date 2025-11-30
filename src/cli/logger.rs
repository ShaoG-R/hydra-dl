use indicatif::ProgressBar;
use log::{Level, LevelFilter, Metadata, Record};
use tokio::sync::mpsc;

/// 控制指令
#[derive(Debug)]
enum ControlCommand {
    Set(ProgressBar),
    Clear,
}

/// 日志控制器
///
/// 持有控制通道的发送端，不再依赖全局变量。
/// 需要在 main 中初始化后传递给 runner。
#[derive(Clone)] // Sender 很轻量，可以随意 Clone
pub struct LogController {
    tx: mpsc::Sender<ControlCommand>,
}

impl LogController {
    /// 设置进度条 (Async)
    pub async fn set_progress_bar(&self, progress_bar: ProgressBar) {
        let _ = self.tx.send(ControlCommand::Set(progress_bar)).await;
    }

    /// 清除进度条 (Async)
    pub async fn clear_progress_bar(&self) {
        let _ = self.tx.send(ControlCommand::Clear).await;
    }
}

/// 自定义 Logger
///
/// 直接持有发送端，不再查找全局变量
struct TokioLogger {
    tx: mpsc::UnboundedSender<String>,
}

impl log::Log for TokioLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let level_str = match record.level() {
            Level::Error => "\x1b[31m[ERROR]\x1b[0m",
            Level::Warn => "\x1b[33m[WARN]\x1b[0m",
            Level::Info => "\x1b[32m[INFO]\x1b[0m",
            Level::Debug => "\x1b[36m[DEBUG]\x1b[0m",
            Level::Trace => "\x1b[90m[TRACE]\x1b[0m",
        };

        let msg = format!("{} {}", level_str, record.args());

        // 直接使用 self.tx 发送，无需全局查找
        let _ = self.tx.send(msg);
    }

    fn flush(&self) {}
}

/// 后台 Actor 逻辑 (保持不变)
async fn logger_actor(
    mut log_rx: mpsc::UnboundedReceiver<String>,
    mut ctrl_rx: mpsc::Receiver<ControlCommand>,
) {
    let mut current_pb: Option<ProgressBar> = None;

    loop {
        tokio::select! {
            Some(msg) = log_rx.recv() => {
                if let Some(ref pb) = current_pb {
                    pb.println(msg);
                } else {
                    eprintln!("{}", msg);
                }
            }
            Some(cmd) = ctrl_rx.recv() => {
                match cmd {
                    ControlCommand::Set(pb) => current_pb = Some(pb),
                    ControlCommand::Clear => current_pb = None,
                }
            }
            else => break,
        }
    }
}

/// 初始化日志系统
///
/// 返回一个 Controller 实例，调用者需要将其传递给需要控制进度条的模块
pub fn init_logger(level: LevelFilter) -> Result<LogController, log::SetLoggerError> {
    let (log_tx, log_rx) = mpsc::unbounded_channel();
    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);

    // 1. 启动 Actor
    tokio::spawn(logger_actor(log_rx, ctrl_rx));

    // 2. 创建 Logger 实例
    let logger = TokioLogger { tx: log_tx };

    // 3. 关键点：使用 Box::leak 将 logger 提升为 'static 生命周期
    // 这样满足了 log::set_logger 的要求，但不需要定义全局 static 变量
    let static_logger = Box::leak(Box::new(logger));

    log::set_logger(static_logger)?;
    log::set_max_level(level);

    // 4. 返回控制器
    Ok(LogController { tx: ctrl_tx })
}
