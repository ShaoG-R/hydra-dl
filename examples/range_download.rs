use hydra_dl::{DownloadConfig, DownloadProgress, download_ranged};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), hydra_dl::DownloadError> {
    // 创建配置
    let config = DownloadConfig::builder()
        .progressive(|p| p.max_concurrent_downloads(8)) // 8 个并发 worker
        .chunk(
            |c| {
                c.initial_size(10 * 1024 * 1024) // 初始 10 MB
                    .min_size(5 * 1024 * 1024) // 最小 5 MB
                    .max_size(100 * 1024 * 1024)
            }, // 最大 100 MB
        )
        .build();

    // 开始下载
    let (mut handle, save_path) = download_ranged(
        "https://example.com/large_file.zip",
        PathBuf::from("."),
        config,
    )
    .await?;

    println!("文件将保存到: {:?}", save_path);

    // 监听进度
    while let Some(progress) = handle.progress_receiver().recv().await {
        match progress {
            DownloadProgress::Started {
                total_size,
                worker_count,
                ..
            } => {
                println!(
                    "开始下载，大小: {:.2} MB，{} 个 workers",
                    total_size.get() as f64 / 1024.0 / 1024.0,
                    worker_count
                );
            }
            DownloadProgress::Progress {
                percentage,
                avg_speed,
                executor_stats,
                ..
            } => {
                println!(
                    "进度: {:.1}%，速度: {:.2} MB/s，{} 个 workers 活跃",
                    percentage,
                    avg_speed.as_u64() as f64 / 1024.0 / 1024.0,
                    executor_stats.running_count()
                );
            }
            DownloadProgress::Completed {
                total_written_bytes,
                total_time,
                ..
            } => {
                println!(
                    "下载完成！{:.2} MB 用时 {:.2}s",
                    total_written_bytes as f64 / 1024.0 / 1024.0,
                    total_time
                );
            }
            DownloadProgress::Error { message } => {
                eprintln!("下载出错: {}", message);
            }
        }
    }

    // 等待下载完成
    handle.wait().await?;
    Ok(())
}
