use crate::DownloadError;
use crate::download::DownloadTaskParams;
use crate::download::download_stats::{DownloadStats, DownloadStatsHandle};
use crate::pool::download::{DownloadWorkerHandle, DownloadWorkerPool, ExecutorResult};
use crate::utils::io_traits::HttpClient;
use crate::utils::writer::MmapWriter;
use crate::{
    download::{
        progress_reporter::{ProgressReporter, ProgressReporterParams},
        progressive::{ProgressiveLauncher, ProgressiveLauncherParams, WorkerLaunchRequest},
        worker_health_checker::{WorkerHealthChecker, WorkerHealthCheckerParams},
    },
};
use futures::stream::{FuturesUnordered, StreamExt};
use log::{error, info, warn};
use ranged_mmap::AllocatedRange;
use rustc_hash::FxHashMap;
use smr_swap::SmrSwap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

/// 下载任务执行器
///
/// 封装了下载任务的执行逻辑，使用辅助结构体管理进度报告
pub struct DownloadTask<C: HttpClient> {
    /// Worker 协程池
    pool: DownloadWorkerPool<C>,
    /// 文件写入器
    writer: MmapWriter,
    /// 进度报告器
    progress_reporter: ProgressReporter,
    /// 渐进式启动管理器
    progressive_launcher: ProgressiveLauncher,
    /// Worker 启动请求接收器（由 progressive_launcher 发送）
    launch_request_rx: mpsc::Receiver<WorkerLaunchRequest>,
    /// Worker 结果接收器集合（每个 worker 一个 oneshot）
    result_futures: FuturesUnordered<tokio::sync::oneshot::Receiver<ExecutorResult>>,
    /// Worker 句柄缓存（worker_id -> handle）
    worker_handles: SmrSwap<FxHashMap<u64, DownloadWorkerHandle>>,
    /// 健康检查器（actor 模式）
    health_checker: WorkerHealthChecker,
    /// 下载统计聚合器句柄（包含 actor 任务和关闭接口）
    stats_handle: DownloadStatsHandle,
    /// 永久失败的 ranges
    failed_ranges: Vec<(AllocatedRange, String)>,
}

impl<C: HttpClient + Clone> DownloadTask<C> {
    /// 创建新的下载任务
    pub(super) async fn new(params: DownloadTaskParams<C>) -> Self {
        // 解构参数
        let DownloadTaskParams {
            client,
            progress_sender,
            writer,
            allocator,
            url,
            total_size,
            config,
        } = params;

        // worker_handles 占位，稍后填充
        let mut swap = SmrSwap::new(FxHashMap::default());

        // 基准时间间隔：50ms
        let base_offset = std::time::Duration::from_millis(50);

        // 创建广播通道
        let (broadcast_tx, broadcast_rx) = tokio::sync::broadcast::channel(128);

        // 创建下载统计聚合器（订阅广播） - 必须在 ProgressiveLauncher 之前创建
        let (stats_handle, aggregated_stats_reader) = DownloadStats::spawn(broadcast_rx);

        // 创建渐进式启动管理器（actor 模式） - 偏移 0ms
        let (progressive_launcher, launch_request_rx) =
            ProgressiveLauncher::new(ProgressiveLauncherParams {
                config: Arc::clone(&config),
                aggregated_stats: aggregated_stats_reader.clone(),
                total_size: writer.total_size(),
                start_offset: std::time::Duration::ZERO,
            });

        // 第一批 worker 数量
        let initial_worker_count = progressive_launcher.initial_worker_count();

        info!("初始启动 {} 个 workers", initial_worker_count);

        // 创建健康检查器（actor 模式，订阅广播）
        let health_checker = WorkerHealthChecker::new(WorkerHealthCheckerParams {
            config: Arc::clone(&config),
            worker_handles: swap.local(),
            broadcast_rx: broadcast_tx.subscribe(),
        });

        // 创建 DownloadWorkerPool（只启动第一批 worker）
        let (pool, initial_handles, initial_result_rxs) = DownloadWorkerPool::new(
            client.clone(),
            initial_worker_count,
            writer.clone(),
            allocator,
            url.clone(),
            Arc::clone(&config),
            broadcast_tx,
        );

        // 创建进度报告器（使用配置的统计窗口作为更新间隔） - 偏移 100ms
        let progress_reporter = ProgressReporter::new(ProgressReporterParams {
            progress_sender,
            total_size,
            aggregated_stats: aggregated_stats_reader,
            update_interval: config.speed().instant_speed_window(),
            start_offset: base_offset * 2,
        });

        // 使用实际的 worker_id（从 handle 获取）填充 worker_handles
        // 使用 global_id 作为 HashMap 的 key
        let initial_worker_handles: FxHashMap<u64, _> = initial_handles
            .into_iter()
            .map(|handle| (handle.global_id(), handle))
            .collect();
        swap.store(initial_worker_handles);

        // 初始化 result_futures
        let result_futures: FuturesUnordered<_> = initial_result_rxs.into_iter().collect();

        Self {
            pool,
            writer,
            progress_reporter,
            progressive_launcher,
            launch_request_rx,
            result_futures,
            worker_handles: swap,
            health_checker,
            stats_handle,
            failed_ranges: Vec::new(),
        }
    }

    /// 等待所有 range 完成
    ///
    /// Worker 自主分配任务并重试，这里只监听结果
    /// 如果有任务达到最大重试次数，将终止下载并返回错误
    pub(super) async fn wait_for_completion(&mut self) -> crate::Result<Vec<(AllocatedRange, String)>> {
        // 事件循环：处理 worker 结果和启动请求
        loop {
            tokio::select! {
                // 处理 worker 启动请求
                Some(request) = self.launch_request_rx.recv() => {
                    
                    self.handle_launch_request(request).await;
                },
                // 处理 worker 结果（从 FuturesUnordered 中获取）
                Some(result) = self.result_futures.next() => {
                    match result {
                        Ok(ExecutorResult::Success { worker_id }) => {
                            info!("Worker #{} 完成任务", worker_id);
                            
                            // 检查是否所有写入完成
                            if self.writer.is_complete() {
                                info!("所有写入已完成");
                                break;
                            }
                        }
                        Ok(ExecutorResult::DownloadFailed { worker_id, failed_ranges }) => {
                            // Worker 有任务永久失败
                            for (range, error) in &failed_ranges {
                                let (start, end) = range.as_range_tuple();
                                error!(
                                    "Worker #{} Range {}..{} 永久失败: {}",
                                    worker_id, start, end, error
                                );
                            }
                            self.failed_ranges.extend(failed_ranges);
                            
                            info!("有任务永久失败，终止下载");
                            break;
                        }
                        Ok(ExecutorResult::WriteFailed { worker_id, range, error }) => {
                            // 写入失败是致命错误，立即终止
                            let (start, end) = range.as_range_tuple();
                            error!(
                                "Worker #{} Range {}..{} 写入失败: {}",
                                worker_id, start, end, error
                            );
                            return Err(DownloadError::Other(format!("写入失败: {}", error)));
                        }
                        Err(_) => {
                            // oneshot 通道被取消（worker 异常退出）
                            warn!("Worker result channel cancelled");
                            break;
                        }
                    }
                },
                else => {
                    error!("所有通道已关闭");
                    break;
                }
            }
        }

        // 返回失败的 ranges
        if self.failed_ranges.is_empty() {
            Ok(Vec::new())
        } else {
            let error_details: Vec<String> = self.failed_ranges
                .iter()
                .map(|(range, error)| {
                    let (start, end) = range.as_range_tuple();
                    format!("range {}..{}: {}", start, end, error)
                })
                .collect();

            let error_msg = format!(
                "有 {} 个任务达到最大重试次数后失败:\n  {}",
                self.failed_ranges.len(),
                error_details.join("\n  ")
            );

            Err(DownloadError::Other(error_msg))
        }
    }

    /// 处理 worker 启动请求（由 progressive_launcher actor 发送）
    async fn handle_launch_request(&mut self, request: WorkerLaunchRequest) {
        let WorkerLaunchRequest { count, stage } = request;

        self.execute_worker_launch(count, stage).await
    }

    /// 关闭并清理资源
    pub(super) async fn shutdown_and_cleanup(
        mut self,
        error_msg: Option<String>,
    ) -> crate::Result<()> {
        // 发送错误事件
        if let Some(ref msg) = error_msg {
            self.progress_reporter.send_error(msg).await;
        }

        // 关闭 workers（发送关闭信号）
        self.pool.shutdown().await;

        // 关闭 progress reporter actor
        self.progress_reporter.shutdown().await;

        // 关闭 progressive launcher actor 并等待其完全停止
        self.progressive_launcher.shutdown_and_wait().await;

        // 关闭 health checker actor 并等待其完全停止
        self.health_checker.shutdown_and_wait().await;

        // 关闭下载统计聚合器并等待其完全停止
        self.stats_handle.shutdown_and_wait().await;

        if let Some(msg) = error_msg {
            return Err(DownloadError::Other(msg));
        }

        Ok(())
    }

    /// 完成并清理资源
    pub(super) async fn finalize_and_cleanup(self, save_path: PathBuf) -> crate::Result<()> {
        // 发送完成统计（Actor 会自动从共享数据源获取统计）
        self.progress_reporter.send_completion().await;

        // 优雅关闭所有 workers（发送关闭信号，workers 会异步自动清理）
        let mut pool = self.pool;
        pool.shutdown().await;

        // 释放 pool（它持有 writer 的共享）
        drop(pool);

        // 关闭 progress reporter actor
        self.progress_reporter.shutdown().await;

        // 关闭 progressive launcher actor 并等待其完全停止
        // 这很重要，因为 actor 持有 written_bytes 的 Arc 引用
        // 必须等待它释放后才能 finalize writer
        self.progressive_launcher.shutdown_and_wait().await;

        // 关闭 health checker actor 并等待其完全停止
        self.health_checker.shutdown_and_wait().await;

        // 关闭下载统计聚合器并等待其完全停止
        self.stats_handle.shutdown_and_wait().await;

        // 完成写入
        self.writer.finalize()?;

        info!("Range 下载任务完成: {:?}", save_path);

        Ok(())
    }

    /// 获取当前活跃 worker 数量
    #[inline]
    pub fn worker_count(&self) -> u64 {
        self.pool.worker_count()
    }

    /// 获取进度报告器
    #[inline]
    pub(super) fn progress_reporter(&self) -> &ProgressReporter {
        &self.progress_reporter
    }

    /// 检查是否下载完成
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.writer.is_complete()
    }

    /// 执行 worker 启动（内部方法）
    async fn execute_worker_launch(&mut self, count: u64, stage: usize) {
        let current_worker_count = self.pool.worker_count();
        let next_target = current_worker_count + count;

        info!(
            "渐进式启动 - 第{}批: 启动 {} 个新 workers (总计 {} 个)",
            stage + 1,
            count,
            next_target
        );

        // 动态添加新 worker，更新 worker_handles 和 result_futures
        let (new_handles, new_result_rxs) = self.pool.add_workers(count);
        self.worker_handles.update(|handles| {
            let mut handles = handles.clone();
            for handle in &new_handles {
                // 使用 global_id 作为 HashMap 的 key
                handles.insert(handle.global_id(), handle.clone());
            }
            handles
        });

        // 添加新的 result receivers 到 FuturesUnordered
        for rx in new_result_rxs {
            self.result_futures.push(rx);
        }

        // Worker 会自动从 allocator 分配任务并开始下载，无需额外操作
    }
}
