use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use net_bytes::{
    FileSizeFormat, SizeStandard,
    rust_decimal::{Decimal, prelude::ToPrimitive},
};
use std::time::Duration;

use super::utils::{format_bytes, format_duration};
use crate::download::DownloadProgress;

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
    /// 文件大小标准
    size_standard: SizeStandard,
}

impl ProgressManager {
    /// 创建新的进度条管理器
    ///
    /// # Arguments
    /// * `verbose` - 是否启用详细模式（显示每个 worker 的进度）
    /// * `size_standard` - 文件大小标准
    pub fn new(verbose: bool, size_standard: SizeStandard) -> Self {
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
            size_standard,
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
                self.main_bar.set_length(total_size.get());
                self.main_bar.set_message(format!(
                    "{} workers, 初始分块: {}",
                    worker_count,
                    format_bytes(initial_chunk_size)
                ));

                // 详细模式：创建 worker 进度条
                if self.verbose
                    && let Some(ref multi) = self.multi
                {
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

            DownloadProgress::Progress {
                written_bytes,
                total_size,
                percentage: _,
                instant_speed,
                instant_acceleration,
                executor_stats,
                ..
            } => {
                // 更新主进度条
                self.main_bar.set_position(written_bytes);

                // 只在有实时速度时显示速度、加速度和 ETA
                let (speed_str, eta) = if let Some(instant) = instant_speed {
                    // 速度显示
                    let speed_display = instant.to_formatted(self.size_standard).to_string();
                    // 加速度显示（仅主进度条）
                    let accel_display = instant_acceleration
                        .map(|a| {
                            let v = a.as_i64();
                            if v > 0 {
                                format!(" ↑{}", a.to_formatted(self.size_standard))
                            } else if v < 0 {
                                format!(" ↓{}", a.to_formatted(self.size_standard))
                            } else {
                                String::new()
                            }
                        })
                        .unwrap_or_default();
                    let speed_display = format!("{}{}", speed_display, accel_display);
                    let eta_display = {
                        let remaining_bytes = total_size.get().saturating_sub(written_bytes);

                        // 使用加速度改进 ETA 预测
                        let eta_secs = instant_acceleration
                            .and_then(|accel| {
                                accel.predict_eta(instant.as_decimal(), remaining_bytes)
                            })
                            .and_then(|d| Decimal::to_u64(&d))
                            .unwrap_or_else(|| remaining_bytes / instant.as_u64());

                        format!(", ETA: {}", format_duration(eta_secs as f64))
                    };
                    (speed_display, eta_display)
                } else {
                    (String::new(), String::new())
                };

                // 计算分块大小范围（只统计运行中的 Executor）
                let chunk_sizes: Vec<u64> = executor_stats
                    .iter_running()
                    .map(|(_, s)| s.speed_stats.current_chunk_size)
                    .collect();
                let chunk_info = if !chunk_sizes.is_empty() {
                    let min_chunk = chunk_sizes.iter().min().copied().unwrap_or(0);
                    let max_chunk = chunk_sizes.iter().max().copied().unwrap_or(0);
                    if min_chunk == max_chunk {
                        format!(", 分块: {}", format_bytes(min_chunk))
                    } else {
                        format!(
                            ", 分块: {}~{}",
                            format_bytes(min_chunk),
                            format_bytes(max_chunk)
                        )
                    }
                } else {
                    String::new()
                };

                // 组合消息，避免空字符串开头
                let message = if speed_str.is_empty() {
                    format!("{}{}", chunk_info.trim_start_matches(", "), eta)
                } else {
                    format!("{}{}{}", speed_str, chunk_info, eta)
                };
                self.main_bar.set_message(message);

                // 详细模式：更新每个 worker 的进度条
                if self.verbose {
                    // 动态添加新的 worker 进度条（处理渐进式启动）
                    if let Some(ref multi) = self.multi {
                        while self.worker_bars.len() < executor_stats.len() {
                            let worker_id = self.worker_bars.len();
                            let worker_bar = multi.add(ProgressBar::new(0));
                            worker_bar.set_style(
                                ProgressStyle::default_bar()
                                    .template(&format!("  Worker #{}: {{msg}}", worker_id))
                                    .expect("无效的进度条模板"),
                            );
                            worker_bar.set_message("新启动...");
                            self.worker_bars.push(worker_bar);
                        }
                    }

                    // 更新所有运行中 worker 的进度条
                    for (idx, (_, running_stats)) in executor_stats.iter_running().enumerate() {
                        if let Some(worker_bar) = self.worker_bars.get(idx) {
                            let speed = running_stats.get_instant_speed()
                                .to_formatted(self.size_standard).to_string();
                            let msg = format!(
                                "{} {}",
                                format_bytes(running_stats.written_bytes),
                                speed
                            );
                            worker_bar.set_message(msg);
                        }
                    }
                }
            }

            DownloadProgress::Completed {
                total_written_bytes,
                total_time,
                avg_speed,
                executor_stats,
                ..
            } => {
                // 完成主进度条
                self.main_bar.finish_with_message(format!(
                    "完成！{} in {}, 平均速度: {}",
                    format_bytes(total_written_bytes),
                    format_duration(total_time),
                    avg_speed
                        .map(|s| s.to_formatted(self.size_standard).to_string())
                        .unwrap_or("N/A".to_string())
                ));

                // 详细模式：显示每个 worker 的最终统计
                if self.verbose {
                    // 确保所有 worker 都有进度条（处理渐进式启动的情况）
                    let total_workers = executor_stats.total_count() as usize;
                    if let Some(ref multi) = self.multi {
                        while self.worker_bars.len() < total_workers {
                            let worker_id = self.worker_bars.len();
                            let worker_bar = multi.add(ProgressBar::new(0));
                            worker_bar.set_style(
                                ProgressStyle::default_bar()
                                    .template(&format!("  Worker #{}: {{msg}}", worker_id))
                                    .expect("无效的进度条模板"),
                            );
                            self.worker_bars.push(worker_bar);
                        }
                    }

                    // 显示仍在运行的 worker 统计
                    for (idx, (_, running_stats)) in executor_stats.iter_running().enumerate() {
                        if let Some(worker_bar) = self.worker_bars.get(idx) {
                            let avg_speed_str = running_stats.get_avg_speed()
                                .to_formatted(self.size_standard).to_string();
                            worker_bar.finish_with_message(format!(
                                "完成：{}, 平均: {}",
                                format_bytes(running_stats.written_bytes),
                                avg_speed_str
                            ));
                        }
                    }

                    // 其余 worker 已完成（从 completed_stats 获取总计）
                    let completed = executor_stats.get_completed_stats();
                    if completed.count > 0 {
                        for idx in executor_stats.len()..self.worker_bars.len() {
                            if let Some(worker_bar) = self.worker_bars.get(idx) {
                                worker_bar.finish_with_message("已完成".to_string());
                            }
                        }
                    }
                }
            }

            DownloadProgress::Error { message } => {
                self.main_bar
                    .abandon_with_message(format!("错误: {}", message));

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

    /// 获取主进度条的引用（用于 logger 集成）
    pub fn main_bar(&self) -> ProgressBar {
        self.main_bar.clone()
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
