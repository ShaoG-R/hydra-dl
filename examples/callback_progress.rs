use hydra_dl::{DownloadConfig, DownloadProgress, download_ranged};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), hydra_dl::DownloadError> {
    let config = DownloadConfig::default();

    let (handle, save_path) =
        download_ranged("https://example.com/file.zip", PathBuf::from("."), config).await?;

    println!("下载到: {:?}", save_path);

    // 使用回调函数监听进度并等待完成
    handle
        .wait_with_progress(|progress| {
            if let DownloadProgress::Progress {
                percentage,
                avg_speed,
                ..
            } = progress
            {
                println!(
                    "进度: {:.1}%，速度: {:.2} MB/s",
                    percentage,
                    avg_speed.as_u64() as f64 / 1024.0 / 1024.0
                );
            }
        })
        .await?;

    Ok(())
}
