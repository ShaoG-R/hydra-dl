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
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(0);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    assert_eq!(pool.worker_count(), 0);
    
    // 验证可以正常关闭
    let mut pool = pool;
    pool.shutdown();
    pool.wait_for_shutdown().await;
}

#[tokio::test]
async fn test_create_pool_with_max_workers() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(ConcurrencyDefaults::MAX_WORKER_COUNT);
    
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();
    assert_eq!(pool.worker_count(), ConcurrencyDefaults::MAX_WORKER_COUNT);
}

#[tokio::test]
async fn test_create_pool_exceeds_max_workers() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(ConcurrencyDefaults::MAX_WORKER_COUNT + 1);
    
    let result = WorkerPool::new(executor, contexts_with_stats);
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(matches!(e, hydra_dl::DownloadError::WorkerCountExceeded(_, _)));
    }
}

#[tokio::test]
async fn test_send_task_to_invalid_worker_id() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 尝试获取超出范围的 worker handle
    let handle = pool.get_worker(ConcurrencyDefaults::MAX_WORKER_COUNT);
    assert!(handle.is_none());
    
    // 尝试获取不存在的 worker handle（在范围内但未创建）
    let handle = pool.get_worker(5);
    assert!(handle.is_none());
}

#[tokio::test]
async fn test_access_stats_of_invalid_worker() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 访问超出范围的 worker_id
    let handle = pool.get_worker(ConcurrencyDefaults::MAX_WORKER_COUNT);
    assert!(handle.is_none());
    
    // 访问不存在的 worker_id（在范围内但未创建）
    let handle = pool.get_worker(10);
    assert!(handle.is_none());
}

#[tokio::test]
async fn test_shutdown_worker_out_of_bounds() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 尝试获取超出范围的 worker
    let handle = pool.get_worker(ConcurrencyDefaults::MAX_WORKER_COUNT);
    assert!(handle.is_none());
}

// ==================== 单个 Worker 关闭测试 ====================

#[tokio::test]
async fn test_shutdown_single_worker() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(3);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    assert_eq!(pool.worker_count(), 3);

    // 关闭 worker #1
    let handle = pool.get_worker(1).unwrap();
    handle.shutdown().unwrap();
    sleep(Duration::from_millis(50)).await;

    // 验证 worker 数量减少
    assert_eq!(pool.worker_count(), 2);

    // 验证该 worker 不可用
    let handle = pool.get_worker(1);
    assert!(handle.is_none());

    // 验证其他 worker 仍然正常工作
    let task = TestTask { id: 2, data: "test".to_string() };
    let handle0 = pool.get_worker(0).unwrap();
    assert!(handle0.send_task(task).await.is_ok());
    
    let task = TestTask { id: 3, data: "test".to_string() };
    let handle2 = pool.get_worker(2).unwrap();
    assert!(handle2.send_task(task).await.is_ok());
}

#[tokio::test]
async fn test_shutdown_nonexistent_worker() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 尝试获取不存在的 worker
    let handle = pool.get_worker(5);
    assert!(handle.is_none());
}

// ==================== 动态添加 Worker 测试 ====================

#[tokio::test]
async fn test_add_workers_fills_gaps() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(3);
    let (mut pool, _handles) = WorkerPool::new(executor.clone(), contexts_with_stats).unwrap();

    assert_eq!(pool.worker_count(), 3);

    // 关闭 worker #1
    let handle = pool.get_worker(1).unwrap();
    handle.shutdown().unwrap();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.worker_count(), 2);

    // 关闭 worker #0
    let handle = pool.get_worker(0).unwrap();
    handle.shutdown().unwrap();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.worker_count(), 1);

    // 添加 2 个新 worker，应该填充到 #0 和 #1
    let new_contexts_with_stats = create_contexts_with_stats(2);
    let _new_handles = pool.add_workers(new_contexts_with_stats).await.unwrap();
    assert_eq!(pool.worker_count(), 3);

    // 验证所有位置都可用
    let task0 = TestTask { id: 1, data: "test".to_string() };
    let handle0 = pool.get_worker(0).unwrap();
    assert!(handle0.send_task(task0).await.is_ok());

    let task1 = TestTask { id: 2, data: "test".to_string() };
    let handle1 = pool.get_worker(1).unwrap();
    assert!(handle1.send_task(task1).await.is_ok());

    let task2 = TestTask { id: 3, data: "test".to_string() };
    let handle2 = pool.get_worker(2).unwrap();
    assert!(handle2.send_task(task2).await.is_ok());
}

#[tokio::test]
async fn test_add_workers_when_slots_full() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(ConcurrencyDefaults::MAX_WORKER_COUNT);
    
    let (mut pool, _handles) = WorkerPool::new(executor.clone(), contexts_with_stats).unwrap();
    
    // 尝试再添加一个 worker
    let new_contexts = create_contexts_with_stats(1);
    let result = pool.add_workers(new_contexts).await;
    assert!(result.is_err());
}

// ==================== 并发场景测试 ====================

#[tokio::test]
async fn test_concurrent_task_sending() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 并发发送多个任务
    let tasks = vec![
        TestTask { id: 1, data: "task1".to_string() },
        TestTask { id: 2, data: "task2".to_string() },
        TestTask { id: 3, data: "task3".to_string() },
    ];

    let handle = pool.get_worker(0).unwrap();
    for task in tasks {
        handle.send_task(task).await.unwrap();
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
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(3);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 向不同的 workers 发送任务
    for worker_id in 0..3 {
        let task = TestTask { id: worker_id, data: format!("worker{}", worker_id) };
        let handle = pool.get_worker(worker_id).unwrap();
        handle.send_task(task).await.unwrap();
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
    
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();
    let pool = Arc::new(pool);

    let mut join_set = JoinSet::new();
    
    // 并发访问统计信息
    for _ in 0..10 {
        let pool_clone = Arc::clone(&pool);
        join_set.spawn(async move {
            if let Some(handle) = pool_clone.get_worker(0) {
                if let Some(stats) = handle.stats() {
                    stats.task_count.load(Ordering::SeqCst);
                }
            }
        });
    }

    // 等待所有任务完成
    while join_set.join_next().await.is_some() {}
}

// ==================== 失败场景测试 ====================

#[tokio::test]
async fn test_executor_returns_failure() {
    let executor = FailingExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let task = TestTask { id: 1, data: "test".to_string() };
    let handle = pool.get_worker(0).unwrap();
    handle.send_task(task).await.unwrap();

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
async fn test_partial_shutdown_and_continue() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(3);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 关闭中间的 worker
    let handle = pool.get_worker(1).unwrap();
    handle.shutdown().unwrap();
    sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.worker_count(), 2);

    // 继续使用剩余的 workers
    let task0 = TestTask { id: 1, data: "test".to_string() };
    let handle0 = pool.get_worker(0).unwrap();
    handle0.send_task(task0).await.unwrap();
    
    let task2 = TestTask { id: 2, data: "test".to_string() };
    let handle2 = pool.get_worker(2).unwrap();
    handle2.send_task(task2).await.unwrap();

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
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(2);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // 立即关闭
    pool.shutdown();
    pool.wait_for_shutdown().await;

    // 验证所有 workers 都已关闭
    assert_eq!(pool.worker_count(), 0);
}

// ==================== 上下文隔离和统计准确性测试 ====================

#[tokio::test]
async fn test_context_isolation_between_workers() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(2);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    // Worker 0 处理 3 个任务
    let handle0 = pool.get_worker(0).unwrap();
    for i in 0..3 {
        let task = TestTask { id: i, data: format!("w0_task{}", i) };
        handle0.send_task(task).await.unwrap();
    }

    // Worker 1 处理 2 个任务
    let handle1 = pool.get_worker(1).unwrap();
    for i in 0..2 {
        let task = TestTask { id: i + 10, data: format!("w1_task{}", i) };
        handle1.send_task(task).await.unwrap();
    }

    // 接收所有结果
    for _ in 0..5 {
        let _ = pool.result_receiver().recv().await;
    }

    // 验证每个 worker 的统计独立
    let stats0 = handle0.stats().unwrap();
    let stats1 = handle1.stats().unwrap();
    
    assert_eq!(stats0.task_count.load(Ordering::SeqCst), 3);
    assert_eq!(stats1.task_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_stats_accumulation() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handle = pool.get_worker(0).unwrap();

    // 发送多批任务
    for batch in 0..3 {
        for task_id in 0..2 {
            let task = TestTask { id: batch * 10 + task_id, data: format!("batch{}", batch) };
            handle.send_task(task).await.unwrap();
        }
        
        // 接收本批次结果
        for _ in 0..2 {
            let _ = pool.result_receiver().recv().await;
        }
        
        // 验证累计统计
        let stats = handle.stats().unwrap();
        assert_eq!(stats.task_count.load(Ordering::SeqCst), (batch + 1) * 2);
    }
}

#[tokio::test]
async fn test_worker_stats_persist_after_task_completion() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handle = pool.get_worker(0).unwrap();

    // 发送任务
    let task = TestTask { id: 1, data: "test".to_string() };
    handle.send_task(task).await.unwrap();
    
    // 等待完成
    let _ = pool.result_receiver().recv().await;
    
    // 多次读取统计，验证一致性
    for _ in 0..5 {
        let stats = handle.stats().unwrap();
        assert_eq!(stats.task_count.load(Ordering::SeqCst), 1);
        sleep(Duration::from_millis(10)).await;
    }
}

// ==================== WorkerHandle 高级功能测试 ====================

#[tokio::test]
async fn test_get_all_workers_via_handle() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(3);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handles = pool.workers();
    assert_eq!(handles.len(), 3);

    // 验证 worker IDs
    let ids: Vec<usize> = handles.iter().map(|h| h.worker_id()).collect();
    assert_eq!(ids, vec![0, 1, 2]);

    // 验证所有 workers 都是活跃的
    for handle in handles {
        assert!(handle.is_alive());
    }
}

#[tokio::test]
async fn test_handle_stats_access() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handle = pool.get_worker(0).unwrap();
    let task = TestTask { id: 1, data: "test".to_string() };
    
    handle.send_task(task).await.unwrap();
    let _ = pool.result_receiver().recv().await;

    // 通过 handle 获取统计信息
    let stats = handle.stats().unwrap();
    assert_eq!(stats.task_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_handle_shutdown_individual_worker() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(2);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handle = pool.get_worker(0).unwrap();
    assert!(handle.is_alive());

    // 通过 handle 关闭 worker
    handle.shutdown().unwrap();

    // 等待一会儿让 worker 退出
    sleep(Duration::from_millis(50)).await;

    // 验证 worker 已经不活跃
    assert!(!handle.is_alive());

    // 验证剩余的 worker 仍然活跃
    assert_eq!(pool.worker_count(), 1);
}

#[tokio::test]
async fn test_handle_clone_shares_worker() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (mut pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handle1 = pool.get_worker(0).unwrap();
    let handle2 = handle1.clone();

    // 两个 handle 应该指向同一个 worker
    assert_eq!(handle1.worker_id(), handle2.worker_id());
    assert_eq!(handle1.worker_id(), 0);

    // 通过第一个 handle 发送任务
    let task = TestTask { id: 1, data: "test".to_string() };
    handle1.send_task(task).await.unwrap();

    // 通过第二个 handle 获取统计信息
    let _ = pool.result_receiver().recv().await;
    let stats = handle2.stats().unwrap();
    assert_eq!(stats.task_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_handle_operations_after_worker_shutdown() {
    let executor = TestExecutor;
    let contexts_with_stats = create_contexts_with_stats(1);
    let (pool, _handles) = WorkerPool::new(executor, contexts_with_stats).unwrap();

    let handle = pool.get_worker(0).unwrap();
    handle.shutdown().unwrap();

    // 等待 worker 退出
    sleep(Duration::from_millis(50)).await;

    // 尝试发送任务应该失败
    let task = TestTask { id: 1, data: "test".to_string() };
    let result = handle.send_task(task).await;
    assert!(result.is_err());

    // 获取统计信息应该返回 None
    let stats = handle.stats();
    assert!(stats.is_none());
    
    // is_alive 应该返回 false
    assert!(!handle.is_alive());
}

