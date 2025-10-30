use anyhow::Result;
use log::info;
use std::path::PathBuf;

use crate::{download_ranged, DownloadConfig};
use super::Cli;
use super::progress::ProgressManager;
use super::utils::format_bytes;

/// 执行下载任务
///
/// 根据 CLI 参数构建配置并启动下载
pub async fn execute_download(cli: &Cli, output_path: PathBuf) -> Result<()> {
    // 构建下载配置
    let config = DownloadConfig::builder()
        .worker_count(cli.workers)
        .initial_chunk_size(cli.chunk_size * 1024 * 1024)
        .min_chunk_size(cli.min_chunk * 1024 * 1024)
        .max_chunk_size(cli.max_chunk * 1024 * 1024)
        .build();

    info!(
        "开始下载: {} -> {:?}",
        cli.url,
        output_path
    );
    info!(
        "配置: {} workers, 分块大小: {} ~ {} (初始: {})",
        config.worker_count(),
        format_bytes(config.min_chunk_size()),
        format_bytes(config.max_chunk_size()),
        format_bytes(config.initial_chunk_size()),
    );

    // 启动下载任务
    let mut handle = download_ranged(&cli.url, output_path.clone(), config).await?;

    if cli.quiet {
        // 静默模式：只等待完成
        handle.wait().await?;
        println!("下载完成: {:?}", output_path);
    } else {
        // 进度条模式
        let mut progress_manager = ProgressManager::new(cli.verbose);

        // 循环接收进度更新
        while let Some(progress) = handle.progress_receiver().recv().await {
            progress_manager.handle_progress(progress);
        }

        // 等待任务完成
        handle.wait().await?;

        // 确保进度条完成
        progress_manager.finish();
    }

    Ok(())
}


