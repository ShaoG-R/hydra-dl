use clap::Parser;
use hydra_dl::cli;
use log::LevelFilter;

#[tokio::main]
async fn main() {
    #[cfg(not(target_env = "msvc"))]
    use mimalloc::MiMalloc;

    #[cfg(not(target_env = "msvc"))]
    #[global_allocator]
    static GLOBAL: MiMalloc = MiMalloc;

    // 解析命令行参数
    let cli = cli::Cli::parse();

    // 根据 CLI 模式初始化日志系统
    let log_level = if cli.quiet {
        LevelFilter::Warn // quiet 模式：只显示 WARN 和 ERROR
    } else {
        LevelFilter::Info // normal/verbose 模式：显示 INFO、WARN 和 ERROR
    };

    let logger_ctrl = cli::init_logger(log_level)
        .map_err(|e| {
            eprintln!("警告: 无法初始化日志系统: {}", e);
        })
        .ok();

    // 执行下载任务
    if let Err(e) = cli::run(cli, logger_ctrl).await {
        eprintln!("错误: {:?}", e);
        std::process::exit(1);
    }
}
