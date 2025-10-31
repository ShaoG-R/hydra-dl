use log::info;

use crate::{download_ranged, DownloadConfig};
use super::{Cli, Result};
use super::progress::ProgressManager;
use super::utils::format_bytes;

/// 执行下载任务
///
/// 根据 CLI 参数构建配置并启动下载
pub async fn execute_download(cli: &Cli, save_dir: &str) -> Result<()> {
    // 构建下载配置
    let config = DownloadConfig::builder()
        .worker_count(cli.workers)
        .initial_chunk_size(cli.chunk_size * 1024 * 1024)
        .min_chunk_size(cli.min_chunk * 1024 * 1024)
        .max_chunk_size(cli.max_chunk * 1024 * 1024)
        .build();

    info!(
        "开始下载: {}",
        cli.url
    );
    info!(
        "配置: {} workers, 分块大小: {} ~ {} (初始: {})",
        config.worker_count(),
        format_bytes(config.min_chunk_size()),
        format_bytes(config.max_chunk_size()),
        format_bytes(config.initial_chunk_size()),
    );

    // 启动下载任务（会自动检测文件名）
    let (mut handle, save_path) = download_ranged(&cli.url, save_dir, config).await?;

    info!("保存路径: {:?}", save_path);

    if cli.quiet {
        // 静默模式：只等待完成
        handle.wait().await?;
        println!("下载完成: {:?}", save_path);
    } else {
        // 进度条模式
        let mut progress_manager = ProgressManager::new(cli.verbose);

        // 将进度条引用传给 logger，使日志输出不破坏进度条
        super::set_progress_bar(progress_manager.main_bar());

        // 循环接收进度更新
        while let Some(progress) = handle.progress_receiver().recv().await {
            progress_manager.handle_progress(progress);
        }

        // 等待任务完成
        handle.wait().await?;

        // 确保进度条完成
        progress_manager.finish();
        
        // 清除进度条引用
        super::clear_progress_bar();
        
        println!("文件已保存到: {:?}", save_path);
    }

    Ok(())
}


