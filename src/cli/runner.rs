use log::info;

use super::progress::ProgressManager;
use super::utils::format_bytes;
use super::{Cli, Result};
use crate::cli::LogController;
use crate::{DownloadConfig, download_ranged};

/// 执行下载任务
///
/// 根据 CLI 参数构建配置并启动下载
pub async fn execute_download(
    cli: &Cli,
    save_dir: &str,
    logger_ctrl: Option<LogController>,
) -> Result<()> {
    // 构建下载配置
    let config = DownloadConfig::builder()
        .concurrency(|c| c.worker_count(cli.workers))
        .chunk(|c| {
            c.initial_size(cli.chunk_size * 1024 * 1024)
                .min_size(cli.min_chunk * 1024 * 1024)
                .max_size(cli.max_chunk * 1024 * 1024)
        })
        .build();

    let size_standard = config.speed().size_standard();

    info!("开始下载: {}", cli.url);
    info!(
        "配置: {} workers, 分块大小: {} ~ {} (初始: {})",
        config.concurrency().worker_count(),
        format_bytes(config.chunk().min_size()),
        format_bytes(config.chunk().max_size()),
        format_bytes(config.chunk().initial_size()),
    );

    // 启动下载任务（会自动检测文件名）
    let (mut handle, save_path) = download_ranged(&cli.url, save_dir, config).await?;

    match logger_ctrl {
        Some(logger_ctrl) if !cli.quiet => {
            // 进度条模式
            let mut progress_manager = ProgressManager::new(cli.verbose, size_standard);

            // 将进度条引用传给 logger，使日志输出不破坏进度条
            logger_ctrl
                .set_progress_bar(progress_manager.main_bar())
                .await;

            // 循环接收进度更新
            while let Some(progress) = handle.progress_receiver().recv().await {
                progress_manager.handle_progress(progress);
            }

            // 等待任务完成
            handle.wait().await?;

            // 确保进度条完成
            progress_manager.finish();

            // 清除进度条引用
            logger_ctrl.clear_progress_bar().await;

            println!("文件已保存到: {:?}", save_path);
        }
        _ => {
            // 静默模式：只等待完成
            handle.wait().await?;
            println!("下载完成: {:?}", save_path);
        }
    }

    Ok(())
}
