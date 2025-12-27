use clap::Parser;

mod logger;
mod progress;
mod runner;
pub mod utils;

pub use logger::{LogController, init_logger};

/// CLI 错误类型
#[derive(thiserror::Error, Debug)]
pub enum CliError {
    /// 下载错误
    #[error(transparent)]
    Download(#[from] crate::DownloadError),

    /// URL 解析错误
    #[error("URL 解析失败: {0}")]
    UrlParse(#[from] url::ParseError),

    /// URL 解码错误
    #[error("URL 解码失败: {0}")]
    UrlDecode(String),

    /// IO 错误
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    /// 路径错误
    #[error("{0}")]
    Path(String),

    /// 通用错误
    #[error("{0}")]
    Other(String),
}

/// CLI 结果类型
pub type Result<T> = std::result::Result<T, CliError>;

/// 高性能 Rust 下载器
#[derive(Parser, Debug)]
#[command(name = "hydra-dl")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// 下载 URL
    #[arg(value_name = "URL")]
    pub url: String,

    /// 保存目录（如不指定，保存到当前目录）
    #[arg(short, long, value_name = "DIRECTORY")]
    pub output: Option<String>,

    /// Worker 并发数
    #[arg(short = 'n', long, default_value = "4")]
    pub workers: u64,

    /// 初始 Worker 数量
    #[arg(long, default_value = "1")]
    pub initial_workers: u64,

    /// 初始分块大小（MB）
    #[arg(long, default_value = "5")]
    pub chunk_size: u64,

    /// 最小分块大小（MB）
    #[arg(long, default_value = "2")]
    pub min_chunk: u64,

    /// 最大分块大小（MB）
    #[arg(long, default_value = "50")]
    pub max_chunk: u64,

    /// 静默模式（不显示进度条）
    #[arg(short, long)]
    pub quiet: bool,

    /// 详细模式（显示每个 worker 的统计）
    #[arg(short, long)]
    pub verbose: bool,
}

/// 运行 CLI 程序
pub async fn run(cli: Cli, logger_ctrl: Option<LogController>) -> Result<()> {
    // 确定保存目录（默认为当前目录）
    let save_dir = cli.output.as_deref().unwrap_or(".");

    // 执行下载
    runner::execute_download(&cli, save_dir, logger_ctrl).await
}
