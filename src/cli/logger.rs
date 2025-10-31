use indicatif::ProgressBar;
use log::{Level, LevelFilter, Metadata, Record};
use parking_lot::RwLock;

/// Logger 状态管理器
/// 
/// 封装进度条引用的存储和访问逻辑，提供简洁的 API
struct LoggerState {
    progress_bar: RwLock<Option<ProgressBar>>,
}

impl LoggerState {
    /// 创建新的 logger 状态（用于静态初始化）
    #[inline]
    const fn new() -> Self {
        Self {
            progress_bar: RwLock::new(None),
        }
    }

    /// 设置进度条引用
    #[inline]
    fn set_progress_bar(&self, progress_bar: ProgressBar) {
        *self.progress_bar.write() = Some(progress_bar);
    }

    /// 清除进度条引用
    #[inline]
    fn clear(&self) {
        *self.progress_bar.write() = None;
    }

    /// 输出日志消息（自动选择输出方式）
    #[inline]
    fn println(&self, message: String) {
        let guard = self.progress_bar.read();
        if let Some(progress_bar) = guard.as_ref() {
            // 通过进度条的 println 输出，不会破坏进度条显示
            progress_bar.println(message);
        } else {
            // 如果没有进度条，直接输出到 stderr
            eprintln!("{}", message);
        }
    }
}

/// 全局 logger 状态
static LOGGER_STATE: LoggerState = LoggerState::new();

/// 自定义 Logger，与 indicatif 进度条集成
struct IndicatifLogger;

impl log::Log for IndicatifLogger {
    #[inline]
    fn enabled(&self, metadata: &Metadata) -> bool {
        // 启用所有级别的日志（具体级别由 log::set_max_level 控制）
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        // 格式化日志消息
        let level_str = match record.level() {
            Level::Error => "\x1b[31m[ERROR]\x1b[0m", // 红色
            Level::Warn => "\x1b[33m[WARN]\x1b[0m",   // 黄色
            Level::Info => "\x1b[32m[INFO]\x1b[0m",   // 绿色
            Level::Debug => "\x1b[36m[DEBUG]\x1b[0m", // 青色
            Level::Trace => "\x1b[90m[TRACE]\x1b[0m", // 灰色
        };

        let message = format!("{} {}", level_str, record.args());

        // 通过 logger 状态输出消息
        LOGGER_STATE.println(message);
    }

    #[inline]
    fn flush(&self) {
        // 不需要特殊的 flush 操作
    }
}

/// 初始化日志系统
///
/// # Arguments
/// * `level` - 日志级别过滤器
///
/// # Example
/// ```
/// use log::LevelFilter;
/// init_logger(LevelFilter::Info);
/// ```
#[inline]
pub fn init_logger(level: LevelFilter) -> Result<(), log::SetLoggerError> {
    // 设置静态 logger
    log::set_logger(&IndicatifLogger)?;
    log::set_max_level(level);

    Ok(())
}

/// 设置进度条引用，使日志输出通过进度条显示
///
/// # Arguments
/// * `progress_bar` - 进度条实例
#[inline]
pub fn set_progress_bar(progress_bar: ProgressBar) {
    LOGGER_STATE.set_progress_bar(progress_bar);
}

/// 清除进度条引用
#[inline]
pub fn clear_progress_bar() {
    LOGGER_STATE.clear();
}

