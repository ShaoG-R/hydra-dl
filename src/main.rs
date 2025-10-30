use clap::Parser;
use rs_dn::cli;

#[tokio::main]
async fn main() {
    // 解析命令行参数
    let cli = cli::Cli::parse();

    // 执行下载任务
    if let Err(e) = cli::run(cli).await {
        eprintln!("错误: {:?}", e);
        std::process::exit(1);
    }
}

