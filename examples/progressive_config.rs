use hydra_dl::DownloadConfig;
use std::num::NonZeroU64;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 渐进式启动配置
    let config = DownloadConfig::builder()
        .progressive(
            |p| {
                p.max_concurrent_downloads(8) // 最大并发数 8
                    .initial_worker_count(NonZeroU64::new(2).unwrap()) // 初始启动 2 个
                    .min_speed_per_thread(NonZeroU64::new(1024 * 1024).unwrap())
            }, // 单线程速度需达 1MB/s 才会增加新 worker
        )
        .build();

    println!("Progressive Config Created Successfully:");
    println!(
        "Max Concurrent Downloads: {}",
        config.progressive().max_concurrent_downloads()
    );
    println!(
        "Initial Worker Count: {}",
        config.progressive().initial_worker_count()
    );
    println!(
        "Min Speed Per Thread: {} bytes/s",
        config.progressive().min_speed_per_thread()
    );

    // 常用单位常量示例 (from README lines 278-284)
    use hydra_dl::constants::*;
    let config2 = DownloadConfig::builder()
        .chunk(|c| c.initial_size(10 * MB)) // 10 MB
        .progressive(|p| p.min_speed_per_thread(NonZeroU64::new(5 * MB).unwrap())) // 5 MB/s
        .build();
    println!("\nConstants Config Created:");
    println!(
        "Initial Chunk Size: {} bytes",
        config2.chunk().initial_size()
    );
    println!(
        "Min Speed Per Thread: {} bytes/s",
        config2.progressive().min_speed_per_thread()
    );

    Ok(())
}
