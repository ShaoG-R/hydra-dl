//! Worker Pool 集成测试
//!
//! 本文件包含 WorkerPool 的复杂集成测试，使用 common.rs 中的测试套件
//! 这些测试验证边界情况、并发场景、失败场景和复杂交互

use hydra_dl::config::ConcurrencyDefaults;
use hydra_dl::pool::common::test_utils::*;
use hydra_dl::pool::common::WorkerPool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// ==================== 边界情况测试 ====================

#[tokio::test]
async fn test_create_pool_with_zero_workers() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(0);
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    assert_eq!(pool.worker_count(), 0);
    
    // 验证可以正常关闭
    let mut pool = pool;
    pool.shutdown();
    pool.wait_for_shutdown().await;
}

#[tokio::test]
async fn test_create_pool_with_max_workers() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(ConcurrencyDefaults::MAX_WORKER_COUNT);
    
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();
    assert_eq!(pool.worker_count(), ConcurrencyDefaults::MAX_WORKER_COUNT);
}

#[tokio::test]
async fn test_create_pool_exceeds_max_workers() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(ConcurrencyDefaults::MAX_WORKER_COUNT + 1);
    
    let result = WorkerPool::new(executor, contexts_with_stats);
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(matches!(e, hydra_dl::DownloadError::WorkerCountExceeded(_, _)));
    }
}

#[tokio::test]
async fn test_send_task_to_invalid_worker_id() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 尝试发送到超出范围的 worker_id
    let task = TestTask { id: 1, data: "test".to_string() };
    let result = pool.send_task(task, ConcurrencyDefaults::MAX_WORKER_COUNT).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), hydra_dl::DownloadError::WorkerNotFound(_)));
    
    // 尝试发送到不存在的 worker_id（在范围内但未创建）
    let task = TestTask { id: 2, data: "test".to_string() };
    let result = pool.send_task(task, 5).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), hydra_dl::DownloadError::WorkerNotFound(_)));
}

#[tokio::test]
async fn test_access_stats_of_invalid_worker() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 访问超出范围的 worker_id
    assert!(pool.worker_stats(ConcurrencyDefaults::MAX_WORKER_COUNT).is_none());
    
    // 访问不存在的 worker_id（在范围内但未创建）
    assert!(pool.worker_stats(10).is_none());
}

#[tokio::test]
async fn test_shutdown_worker_out_of_bounds() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let result = pool.shutdown_worker(ConcurrencyDefaults::MAX_WORKER_COUNT);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), hydra_dl::DownloadError::WorkerNotFound(_)));
}

// ==================== 单个 Worker 关闭测试 ====================

#[tokio::test]
async fn test_shutdown_single_worker() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(3);
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    assert_eq!(pool.worker_count(), 3);

    // 关闭 worker #1
    pool.shutdown_worker(1).unwrap();
    sleep(Duration::from_millis(50)).await;

    // 验证 worker 数量减少
    assert_eq!(pool.worker_count(), 2);

    // 验证该 worker 不可用
    let task = TestTask { id: 1, data: "test".to_string() };
    let result = pool.send_task(task, 1).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), hydra_dl::DownloadError::WorkerNotFound(1)));

    // 验证其他 worker 仍然正常工作
    let task = TestTask { id: 2, data: "test".to_string() };
    assert!(pool.send_task(task, 0).await.is_ok());
    
    let task = TestTask { id: 3, data: "test".to_string() };
    assert!(pool.send_task(task, 2).await.is_ok());
}

#[tokio::test]
async fn test_shutdown_nonexistent_worker() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let result = pool.shutdown_worker(5);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), hydra_dl::DownloadError::WorkerNotFound(5)));
}

// ==================== 动态添加 Worker 测试 ====================

#[tokio::test]
async fn test_add_workers_fills_gaps() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(3);
    let mut pool = WorkerPool::new(executor.clone(), contexts_with_stats).unwrap();

    assert_eq!(pool.worker_count(), 3);

    // 关闭 worker #1
    pool.shutdown_worker(1).unwrap();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.worker_count(), 2);

    // 关闭 worker #0
    pool.shutdown_worker(0).unwrap();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.worker_count(), 1);

    // 添加 2 个新 worker，应该填充到 #0 和 #1
    let new_contexts_with_stats = create_contexts_with_stats(2);
    pool.add_workers(new_contexts_with_stats).await.unwrap();
    assert_eq!(pool.worker_count(), 3);

    // 验证所有位置都可用
    let task0 = TestTask { id: 1, data: "test".to_string() };
    assert!(pool.send_task(task0, 0).await.is_ok());

    let task1 = TestTask { id: 2, data: "test".to_string() };
    assert!(pool.send_task(task1, 1).await.is_ok());

    let task2 = TestTask { id: 3, data: "test".to_string() };
    assert!(pool.send_task(task2, 2).await.is_ok());
}

#[tokio::test]
async fn test_add_workers_when_slots_full() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(ConcurrencyDefaults::MAX_WORKER_COUNT);
    
    let mut pool = WorkerPool::new(executor.clone(), contexts_with_stats).unwrap();
    
    // 尝试再添加一个 worker
    let new_contexts = create_contexts_with_stats(1);
    let result = pool.add_workers(new_contexts).await;
    assert!(result.is_err());
}

// ==================== 并发场景测试 ====================

#[tokio::test]
async fn test_concurrent_task_sending() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 并发发送多个任务
    let tasks = vec![
        TestTask { id: 1, data: "task1".to_string() },
        TestTask { id: 2, data: "task2".to_string() },
        TestTask { id: 3, data: "task3".to_string() },
    ];

    for task in tasks {
        pool.send_task(task, 0).await.unwrap();
    }

    // 接收所有结果
    let mut received = 0;
    while received < 3 {
        if pool.result_receiver().recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, 3);
}

#[tokio::test]
async fn test_multiple_workers_processing_concurrently() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(3);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 向不同的 workers 发送任务
    for worker_id in 0..3 {
        let task = TestTask { id: worker_id, data: format!("worker{}", worker_id) };
        pool.send_task(task, worker_id).await.unwrap();
    }

    // 接收所有结果
    let mut results = Vec::new();
    for _ in 0..3 {
        if let Some(result) = pool.result_receiver().recv().await {
            results.push(result);
        }
    }

    assert_eq!(results.len(), 3);
    
    // 验证每个 worker 都处理了任务
    let mut worker_ids: Vec<usize> = results.iter().map(|r| {
        match r {
            TestResult::Success { worker_id, .. } => *worker_id,
            _ => panic!("不应该失败"),
        }
    }).collect();
    worker_ids.sort();
    assert_eq!(worker_ids, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_concurrent_stats_access() {
    use tokio::task::JoinSet;
    
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let pool = Arc::new(WorkerPool::new(executor, contexts_with_stats).unwrap());

    let mut join_set = JoinSet::new();
    
    // 并发访问统计信息
    for _ in 0..10 {
        let pool_clone = Arc::clone(&pool);
        join_set.spawn(async move {
            if let Some(stats) = pool_clone.worker_stats(0) {
                stats.task_count.load(Ordering::SeqCst);
            }
        });
    }

    // 等待所有任务完成
    while join_set.join_next().await.is_some() {}
}

// ==================== 失败场景测试 ====================

#[tokio::test]
async fn test_executor_returns_failure() {
    let executor = Arc::new(FailingExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let task = TestTask { id: 1, data: "test".to_string() };
    pool.send_task(task, 0).await.unwrap();

    let result = pool.result_receiver().recv().await;
    assert!(result.is_some());
    
    match result.unwrap() {
        TestResult::Failed { error, .. } => {
            assert_eq!(error, "intentional failure");
        }
        TestResult::Success { .. } => panic!("应该返回失败"),
    }
}

// ==================== 复杂场景测试 ====================

#[tokio::test]
async fn test_round_robin_task_distribution() {
    let executor = Arc::new(TestExecutor);
    let worker_count = 3;
    let contexts_with_stats = create_contexts_with_stats(worker_count);
    
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 轮询发送任务给每个 worker
    let task_count = 9;
    for i in 0..task_count {
        let worker_id = i % worker_count;
        let task = TestTask { id: i, data: format!("task{}", i) };
        pool.send_task(task, worker_id).await.unwrap();
    }

    // 接收所有结果
    let mut results = Vec::new();
    for _ in 0..task_count {
        if let Some(result) = pool.result_receiver().recv().await {
            results.push(result);
        }
    }

    assert_eq!(results.len(), task_count);
    
    // 验证每个 worker 都处理了 3 个任务
    for worker_id in 0..worker_count {
        let stats = pool.worker_stats(worker_id).unwrap();
        assert_eq!(stats.task_count.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn test_sequential_tasks_to_single_worker() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let task_count = 5;
    
    // 顺序发送多个任务给同一个 worker
    for i in 0..task_count {
        let task = TestTask { id: i, data: format!("task{}", i) };
        pool.send_task(task, 0).await.unwrap();
    }

    // 接收所有结果并验证顺序
    let mut received_ids = Vec::new();
    for _ in 0..task_count {
        if let Some(result) = pool.result_receiver().recv().await {
            match result {
                TestResult::Success { task_id, .. } => received_ids.push(task_id),
                _ => panic!("不应该失败"),
            }
        }
    }

    // 验证所有任务都被处理
    assert_eq!(received_ids.len(), task_count);
    received_ids.sort();
    assert_eq!(received_ids, vec![0, 1, 2, 3, 4]);
    
    // 验证统计
    let stats = pool.worker_stats(0).unwrap();
    assert_eq!(stats.task_count.load(Ordering::SeqCst), task_count);
}

#[tokio::test]
async fn test_partial_shutdown_and_continue() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(3);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 关闭中间的 worker
    pool.shutdown_worker(1).unwrap();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.worker_count(), 2);

    // 继续使用剩余的 workers
    let task0 = TestTask { id: 1, data: "test".to_string() };
    pool.send_task(task0, 0).await.unwrap();
    
    let task2 = TestTask { id: 2, data: "test".to_string() };
    pool.send_task(task2, 2).await.unwrap();

    // 接收结果
    let mut received = 0;
    while received < 2 {
        if pool.result_receiver().recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, 2);
}

#[tokio::test]
async fn test_immediate_shutdown_after_creation() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(2);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 立即关闭
    pool.shutdown();
    pool.wait_for_shutdown().await;

    // 验证所有 workers 都已关闭
    assert_eq!(pool.worker_count(), 0);
}

// ==================== 上下文隔离和统计准确性测试 ====================

#[tokio::test]
async fn test_context_isolation_between_workers() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(2);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // Worker 0 处理 3 个任务
    for i in 0..3 {
        let task = TestTask { id: i, data: format!("w0_task{}", i) };
        pool.send_task(task, 0).await.unwrap();
    }

    // Worker 1 处理 2 个任务
    for i in 0..2 {
        let task = TestTask { id: i + 10, data: format!("w1_task{}", i) };
        pool.send_task(task, 1).await.unwrap();
    }

    // 接收所有结果
    for _ in 0..5 {
        let _ = pool.result_receiver().recv().await;
    }

    // 验证每个 worker 的统计独立
    let stats0 = pool.worker_stats(0).unwrap();
    let stats1 = pool.worker_stats(1).unwrap();
    
    assert_eq!(stats0.task_count.load(Ordering::SeqCst), 3);
    assert_eq!(stats1.task_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_stats_accumulation() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 发送多批任务
    for batch in 0..3 {
        for task_id in 0..2 {
            let task = TestTask { id: batch * 10 + task_id, data: format!("batch{}", batch) };
            pool.send_task(task, 0).await.unwrap();
        }
        
        // 接收本批次结果
        for _ in 0..2 {
            let _ = pool.result_receiver().recv().await;
        }
        
        // 验证累计统计
        let stats = pool.worker_stats(0).unwrap();
        assert_eq!(stats.task_count.load(Ordering::SeqCst), (batch + 1) * 2);
    }
}

#[tokio::test]
async fn test_worker_stats_persist_after_task_completion() {
    let executor = Arc::new(TestExecutor);
    let contexts_with_stats = create_contexts_with_stats(1);
    let mut pool = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 发送任务
    let task = TestTask { id: 1, data: "test".to_string() };
    pool.send_task(task, 0).await.unwrap();
    
    // 等待完成
    let _ = pool.result_receiver().recv().await;
    
    // 多次读取统计，验证一致性
    for _ in 0..5 {
        let stats = pool.worker_stats(0).unwrap();
        assert_eq!(stats.task_count.load(Ordering::SeqCst), 1);
        sleep(Duration::from_millis(10)).await;
    }
}

