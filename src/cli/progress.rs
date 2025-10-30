use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

use crate::download::DownloadProgress;
use super::utils::{format_bytes, format_speed, format_duration};

/// 进度条管理器
pub struct ProgressManager {
    /// 主进度条
    main_bar: ProgressBar,
    /// 多进度条管理器（用于详细模式）
    multi: Option<MultiProgress>,
    /// Worker 进度条列表（详细模式）
    worker_bars: Vec<ProgressBar>,
    /// 是否为详细模式
    verbose: bool,
}

impl ProgressManager {
    /// 创建新的进度条管理器
    ///
    /// # Arguments
    /// * `verbose` - 是否启用详细模式（显示每个 worker 的进度）
    pub fn new(verbose: bool) -> Self {
        let (main_bar, multi) = if verbose {
            // 详细模式：使用 MultiProgress
            let multi = MultiProgress::new();
            let main_bar = multi.add(ProgressBar::new(0));
            (main_bar, Some(multi))
        } else {
            // 普通模式：只显示主进度条
            let main_bar = ProgressBar::new(0);
            (main_bar, None)
        };

        // 设置主进度条样式
        main_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}")
                .expect("无效的进度条模板")
                .progress_chars("#>-"),
        );
        
        Self {
            main_bar,
            multi,
            worker_bars: Vec::new(),
            verbose,
        }
    }

    /// 处理下载进度更新
    pub fn handle_progress(&mut self, progress: DownloadProgress) {
        match progress {
            DownloadProgress::Started {
                total_size,
                worker_count,
                initial_chunk_size,
            } => {
                self.main_bar.set_length(total_size);
                self.main_bar.set_message(format!(
                    "{} workers, 初始分块: {}",
                    worker_count,
                    format_bytes(initial_chunk_size)
                ));

                // 详细模式：创建 worker 进度条
                if self.verbose {
                    if let Some(ref multi) = self.multi {
                        for i in 0..worker_count {
                            let worker_bar = multi.add(ProgressBar::new(0));
                            worker_bar.set_style(
                                ProgressStyle::default_bar()
                                    .template(&format!("  Worker #{}: {{msg}}", i))
                                    .expect("无效的进度条模板"),
                            );
                            worker_bar.set_message("等待任务...");
                            self.worker_bars.push(worker_bar);
                        }
                    }
                }
            }

            DownloadProgress::Progress {
                bytes_downloaded,
                total_size,
                percentage: _,
                avg_speed,
                instant_speed,
                current_chunk_size,
                worker_stats,
            } => {
                // 更新主进度条
                self.main_bar.set_position(bytes_downloaded);
                
                let speed_str = if let Some(instant) = instant_speed {
                    format!("{} (瞬时: {})", format_speed(avg_speed), format_speed(instant))
                } else {
                    format_speed(avg_speed)
                };

                // 计算 ETA
                let eta = if avg_speed > 0.0 {
                    let remaining_bytes = total_size.saturating_sub(bytes_downloaded) as f64;
                    let eta_secs = remaining_bytes / avg_speed;
                    format!(", ETA: {}", format_duration(eta_secs))
                } else {
                    String::new()
                };

                self.main_bar.set_message(format!(
                    "{}, 分块: {}{}", 
                    speed_str,
                    format_bytes(current_chunk_size),
                    eta
                ));

                // 详细模式：更新每个 worker 的进度条
                if self.verbose {
                    for (idx, stats) in worker_stats.iter().enumerate() {
                        if let Some(worker_bar) = self.worker_bars.get(idx) {
                            let instant_str = if let Some(instant) = stats.instant_speed {
                                format!(" ({})", format_speed(instant))
                            } else {
                                String::new()
                            };
                            
                            worker_bar.set_message(format!(
                                "{}, {} ranges{}",
                                format_bytes(stats.bytes),
                                stats.ranges,
                                instant_str
                            ));
                        }
                    }
                }
            }

            DownloadProgress::Completed {
                total_bytes,
                total_time,
                avg_speed,
                worker_stats,
            } => {
                // 完成主进度条
                self.main_bar.finish_with_message(format!(
                    "完成！{} in {}, 平均速度: {}",
                    format_bytes(total_bytes),
                    format_duration(total_time),
                    format_speed(avg_speed)
                ));

                // 详细模式：显示每个 worker 的最终统计
                if self.verbose {
                    for (idx, stats) in worker_stats.iter().enumerate() {
                        if let Some(worker_bar) = self.worker_bars.get(idx) {
                            worker_bar.finish_with_message(format!(
                                "完成：{}, {} ranges, 平均: {}",
                                format_bytes(stats.bytes),
                                stats.ranges,
                                format_speed(stats.avg_speed)
                            ));
                        }
                    }
                }
            }

            DownloadProgress::Error { message } => {
                self.main_bar.abandon_with_message(format!("错误: {}", message));
                
                // 详细模式：清理 worker 进度条
                if self.verbose {
                    for worker_bar in &self.worker_bars {
                        worker_bar.abandon();
                    }
                }
            }
        }
    }

    /// 完成并清理进度条
    pub fn finish(&self) {
        if !self.main_bar.is_finished() {
            self.main_bar.finish();
        }
    }
}

/// 创建简单的进度条（用于静默模式的替代）
#[allow(dead_code)]
pub fn create_spinner() -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .expect("无效的进度条模板"),
    );
    spinner.enable_steady_tick(Duration::from_millis(100));
    spinner
}

