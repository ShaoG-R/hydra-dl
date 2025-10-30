use anyhow::Result;
use clap::Parser;

mod progress;
mod runner;
pub mod utils;

/// 高性能 Rust 下载器
#[derive(Parser, Debug)]
#[command(name = "rs-dn")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// 下载 URL
    #[arg(value_name = "URL")]
    pub url: String,

    /// 保存路径（如不指定，自动从 URL 提取文件名）
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<String>,

    /// Worker 并发数
    #[arg(short = 'n', long, default_value = "4")]
    pub workers: usize,

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
pub async fn run(cli: Cli) -> Result<()> {
    // 确定输出路径
    let output_path = match cli.output.clone() {
        Some(path) => path,
        None => utils::extract_filename_from_url(&cli.url)?,
    };

    // 验证输出路径
    let output_path = utils::validate_output_path(&output_path)?;

    // 执行下载
    runner::execute_download(&cli, output_path).await
}

