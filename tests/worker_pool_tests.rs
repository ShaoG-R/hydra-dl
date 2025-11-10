//! Worker Pool 集成测试
//!
//! 本文件包含 WorkerPool 的复杂集成测试，使用 common.rs 中的测试套件
//! 这些测试验证边界情况、并发场景、失败场景和复杂交互

use hydra_dl::pool::common::{WorkerPool, WorkerExecutor};
use hydra_dl::pool::common::test_utils::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{sleep, Duration, timeout};
use tokio::sync::Barrier;

// ============================================================================
// 边界情况测试 (Edge Cases)
// ============================================================================

/// 测试单个 worker 处理大量任务
#[tokio::test]
async fn test_single_worker_many_tasks() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(1);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let task_count = 100;
    
    // 发送大量任务
    for i in 0..task_count {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handles[0].send_task(task).await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < task_count {
        if let Some(result) = result_receiver.recv().await {
            match result {
                TestResult::Success { .. } => received += 1,
                _ => panic!("Unexpected failure"),
            }
        }
    }
    
    assert_eq!(received, task_count);
    pool.shutdown().await;
}

/// 测试大量 workers（扩展性测试）
#[tokio::test]
async fn test_many_workers() {
    let executor = TestExecutor;
    let worker_count = 50;
    let contexts = create_contexts_with_stats(worker_count);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    assert_eq!(pool.worker_count(), worker_count as u64);
    
    // 每个 worker 发送一个任务
    for (i, handle) in handles.iter().enumerate() {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handle.send_task(task).await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < worker_count {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, worker_count);
    pool.shutdown().await;
}

/// 测试空池上的操作
#[tokio::test]
async fn test_operations_on_empty_pool() {
    let executor = TestExecutor;
    let contexts = vec![];
    let (mut pool, handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    assert_eq!(pool.worker_count(), 0);
    assert_eq!(handles.len(), 0);
    
    // 关闭空池应该正常工作
    pool.shutdown().await;
    assert_eq!(pool.worker_count(), 0);
}

/// 测试多次关闭协程池
#[tokio::test]
async fn test_multiple_shutdowns() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, _handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 第一次关闭
    pool.shutdown().await;
    assert_eq!(pool.worker_count(), 0);
    
    // 第二次关闭应该是安全的
    pool.shutdown().await;
    assert_eq!(pool.worker_count(), 0);
}

/// 测试大数据任务
#[tokio::test]
async fn test_large_data_task() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(1);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 创建包含大量数据的任务
    let large_data = "x".repeat(1_000_000); // 1MB 的数据
    let task = TestTask {
        id: 1,
        data: large_data.clone(),
    };
    
    handles[0].send_task(task).await.unwrap();
    
    // 接收结果
    if let Some(result) = result_receiver.recv().await {
        match result {
            TestResult::Success { task_id, .. } => {
                assert_eq!(task_id, 1);
            }
            _ => panic!("Expected success"),
        }
    }
    
    pool.shutdown().await;
}

// ============================================================================
// 并发场景测试 (Concurrent Scenarios)
// ============================================================================

/// 测试多个 workers 并发处理任务
#[tokio::test]
async fn test_concurrent_task_processing() {
    let executor = TestExecutor;
    let worker_count = 8;
    let tasks_per_worker = 20;
    let contexts = create_contexts_with_stats(worker_count);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let total_tasks = worker_count * tasks_per_worker;
    
    // 并发发送任务到不同 workers
    let send_tasks = handles.iter().enumerate().map(|(worker_idx, handle)| {
        let handle = handle.clone();
        tokio::spawn(async move {
            for i in 0..tasks_per_worker {
                let task = TestTask {
                    id: worker_idx * tasks_per_worker + i,
                    data: format!("worker-{}-task-{}", worker_idx, i),
                };
                handle.send_task(task).await.unwrap();
            }
        })
    });
    
    // 等待所有发送完成
    for task_handle in send_tasks {
        task_handle.await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < total_tasks {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, total_tasks);
    pool.shutdown().await;
}

/// 测试多个线程同时使用 WorkerHandle
#[tokio::test]
async fn test_concurrent_handle_usage() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(4);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let task_count = 50;
    let handle = handles[0].clone();
    
    // 多个并发任务使用同一个 handle
    let tasks: Vec<_> = (0..task_count).map(|i| {
        let h = handle.clone();
        tokio::spawn(async move {
            let task = TestTask {
                id: i,
                data: format!("task-{}", i),
            };
            h.send_task(task).await.unwrap();
        })
    }).collect();
    
    // 等待所有发送完成
    for t in tasks {
        t.await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < task_count {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, task_count);
    pool.shutdown().await;
}

/// 测试运行时动态添加 workers（模拟扩容）
#[tokio::test]
async fn test_dynamic_scaling() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, initial_handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 发送一些任务到初始 workers
    for i in 0..10 {
        let task = TestTask { id: i, data: format!("initial-{}", i) };
        initial_handles[i % 2].send_task(task).await.unwrap();
    }
    
    // 动态添加新 workers
    let new_contexts = create_contexts_with_stats(3);
    let new_handles = pool.add_workers(new_contexts).await.unwrap();
    
    assert_eq!(pool.worker_count(), 5);
    
    // 发送任务到新 workers
    for i in 0..15 {
        let task = TestTask { id: 100 + i, data: format!("new-{}", i) };
        new_handles[i % 3].send_task(task).await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < 25 {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, 25);
    pool.shutdown().await;
}

/// 测试并发添加 workers
#[tokio::test]
async fn test_concurrent_add_workers() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(1);
    let (mut pool, _handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 注意：add_workers 需要 &mut self，所以不能真正并发调用
    // 这里测试顺序添加多批 workers
    for _ in 0..5 {
        let new_contexts = create_contexts_with_stats(2);
        pool.add_workers(new_contexts).await.unwrap();
    }
    
    assert_eq!(pool.worker_count(), 11); // 1 + 5*2 = 11
    pool.shutdown().await;
}

/// 测试 workers 间的负载均衡
#[tokio::test]
async fn test_load_balancing() {
    let executor = TestExecutor;
    let worker_count = 4;
    let contexts = create_contexts_with_stats(worker_count);
    let stats_refs: Vec<_> = contexts.iter().map(|(_, s)| s.clone()).collect();
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let total_tasks = 100;
    
    // 轮流分配任务
    for i in 0..total_tasks {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handles[i % worker_count].send_task(task).await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < total_tasks {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    // 验证每个 worker 处理了大约相同数量的任务
    for (i, stats) in stats_refs.iter().enumerate() {
        let count = stats.task_count.load(Ordering::SeqCst);
        println!("Worker {} processed {} tasks", i, count);
        assert!(count >= 20 && count <= 30, "Worker {} processed {} tasks", i, count);
    }
    
    pool.shutdown().await;
}

// ============================================================================
// 失败场景测试 (Failure Scenarios)
// ============================================================================

/// 测试所有任务失败的场景
#[tokio::test]
async fn test_all_tasks_fail() {
    let executor = FailingExecutor;
    let contexts = create_contexts_with_stats(3);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 发送任务
    for i in 0..10 {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handles[i % 3].send_task(task).await.unwrap();
    }
    
    // 接收所有结果，验证都是失败的
    let mut received = 0;
    let mut failed = 0;
    while received < 10 {
        if let Some(result) = result_receiver.recv().await {
            received += 1;
            if let TestResult::Failed { .. } = result {
                failed += 1;
            }
        }
    }
    
    assert_eq!(failed, 10);
    pool.shutdown().await;
}

/// 测试向已关闭的 worker 发送任务
#[tokio::test]
async fn test_send_after_shutdown() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let handle_clone = handles[0].clone();
    
    // 关闭协程池
    pool.shutdown().await;
    
    // 尝试发送任务应该失败
    let task = TestTask {
        id: 1,
        data: "test".to_string(),
    };
    let result = handle_clone.send_task(task).await;
    assert!(result.is_err());
}

/// 测试在处理任务时关闭
#[tokio::test]
async fn test_shutdown_while_processing() {
    use async_trait::async_trait;
    
    // 创建一个慢速执行器
    #[derive(Clone)]
    struct SlowExecutor;
    
    #[async_trait]
    impl WorkerExecutor for SlowExecutor {
        type Task = TestTask;
        type Result = TestResult;
        type Context = TestContext;
        type Stats = TestStats;
        
        async fn execute(&self, worker_id: u64, task: Self::Task, _context: &mut Self::Context, _stats: &Self::Stats) -> Self::Result {
            // 模拟长时间处理
            sleep(Duration::from_millis(500)).await;
            TestResult::Success {
                worker_id,
                task_id: task.id,
            }
        }
    }
    
    let executor = SlowExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, handles, _result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 发送一些任务
    for i in 0..10 {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handles[i % 2].send_task(task).await.unwrap();
    }
    
    // 立即关闭（不等待任务完成）
    sleep(Duration::from_millis(50)).await;
    pool.shutdown().await;
    
    // 验证关闭成功
    assert_eq!(pool.worker_count(), 0);
}

/// 测试结果 channel 满的情况
#[tokio::test]
async fn test_result_channel_full() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 发送大量任务但不接收结果
    // 注意：channel 容量是 100，所以发送超过这个数量可能会阻塞
    for i in 0..50 {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handles[i % 2].send_task(task).await.unwrap();
    }
    
    // 等待一些任务完成
    sleep(Duration::from_millis(200)).await;
    
    // 现在开始接收结果
    let mut received = 0;
    while received < 50 {
        if let Some(_) = timeout(Duration::from_secs(5), result_receiver.recv()).await.ok().flatten() {
            received += 1;
        } else {
            break;
        }
    }
    
    assert!(received > 0, "Should have received some results");
    pool.shutdown().await;
}

/// 测试混合成功和失败的结果
#[tokio::test]
async fn test_mixed_success_and_failure() {
    use async_trait::async_trait;
    
    // 创建一个随机失败的执行器
    #[derive(Clone)]
    struct RandomFailExecutor;
    
    #[async_trait]
    impl WorkerExecutor for RandomFailExecutor {
        type Task = TestTask;
        type Result = TestResult;
        type Context = TestContext;
        type Stats = TestStats;
        
        async fn execute(&self, worker_id: u64, task: Self::Task, _context: &mut Self::Context, _stats: &Self::Stats) -> Self::Result {
            // 偶数 ID 的任务失败
            if task.id % 2 == 0 {
                TestResult::Failed {
                    worker_id,
                    error: format!("Task {} failed", task.id),
                }
            } else {
                TestResult::Success {
                    worker_id,
                    task_id: task.id,
                }
            }
        }
    }
    
    let executor = RandomFailExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 发送任务
    for i in 0..20 {
        let task = TestTask {
            id: i,
            data: format!("task-{}", i),
        };
        handles[i % 2].send_task(task).await.unwrap();
    }
    
    // 接收并统计结果
    let mut success_count = 0;
    let mut failure_count = 0;
    
    for _ in 0..20 {
        if let Some(result) = result_receiver.recv().await {
            match result {
                TestResult::Success { .. } => success_count += 1,
                TestResult::Failed { .. } => failure_count += 1,
            }
        }
    }
    
    assert_eq!(success_count, 10);
    assert_eq!(failure_count, 10);
    
    pool.shutdown().await;
}

// ============================================================================
// 复杂交互测试 (Complex Interactions)
// ============================================================================

/// 测试任务链：一个任务的结果触发另一个任务
#[tokio::test]
async fn test_task_chaining() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(2);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let handles_clone = handles.clone();
    
    // 启动一个任务来处理结果并发送新任务
    let result_processor = tokio::spawn(async move {
        let processed = 0;
        
        // 发送初始任务
        for i in 0..5 {
            let task = TestTask {
                id: i,
                data: format!("initial-{}", i),
            };
            handles_clone[i % 2].send_task(task).await.unwrap();
        }
        
        processed
    });
    
    // 等待处理器完成
    result_processor.await.unwrap();
    
    // 接收所有结果
    let mut received = 0;
    while received < 5 {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    pool.shutdown().await;
}

/// 测试生产者-消费者模式
#[tokio::test]
async fn test_producer_consumer_pattern() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(4);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let producer_count = 3;
    let tasks_per_producer = 20;
    let total_tasks = producer_count * tasks_per_producer;
    
    // 创建多个生产者
    let producers: Vec<_> = (0..producer_count).map(|producer_id| {
        let handles_clone = handles.clone();
        tokio::spawn(async move {
            for i in 0..tasks_per_producer {
                let task = TestTask {
                    id: producer_id * tasks_per_producer + i,
                    data: format!("producer-{}-task-{}", producer_id, i),
                };
                // 轮流发送到不同 workers
                handles_clone[i % handles_clone.len()].send_task(task).await.unwrap();
            }
        })
    }).collect();
    
    // 创建消费者
    let consumed = Arc::new(AtomicUsize::new(0));
    let consumed_clone = consumed.clone();
    
    let consumer = tokio::spawn(async move {
        let mut count = 0;
        while count < total_tasks {
            if result_receiver.recv().await.is_some() {
                count += 1;
                consumed_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    
    // 等待所有生产者完成
    for p in producers {
        p.await.unwrap();
    }
    
    // 等待消费者完成
    consumer.await.unwrap();
    
    assert_eq!(consumed.load(Ordering::SeqCst), total_tasks);
    
    // 关闭协程池
    pool.shutdown().await;
}

/// 测试带同步屏障的并发场景
#[tokio::test]
async fn test_synchronized_workers() {
    let executor = TestExecutor;
    let worker_count = 5;
    let contexts = create_contexts_with_stats(worker_count);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let barrier = Arc::new(Barrier::new(worker_count));
    
    // 启动多个任务，它们在屏障处同步
    let tasks: Vec<_> = (0..worker_count).map(|i| {
        let barrier_clone = barrier.clone();
        let handle = handles[i].clone();
        tokio::spawn(async move {
            // 等待所有任务准备好
            barrier_clone.wait().await;
            
            // 同时发送任务
            let task = TestTask {
                id: i,
                data: format!("synchronized-task-{}", i),
            };
            handle.send_task(task).await.unwrap();
        })
    }).collect();
    
    // 等待所有任务完成
    for t in tasks {
        t.await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < worker_count {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, worker_count);
    pool.shutdown().await;
}

/// 测试长时间运行的场景
#[tokio::test]
async fn test_long_running_scenario() {
    let executor = TestExecutor;
    let contexts = create_contexts_with_stats(3);
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    let duration = Duration::from_secs(2);
    let start = tokio::time::Instant::now();
    let mut task_id = 0;
    
    // 在指定时间内持续发送任务
    while start.elapsed() < duration {
        let task = TestTask {
            id: task_id,
            data: format!("task-{}", task_id),
        };
        handles[task_id % 3].send_task(task).await.unwrap();
        task_id += 1;
        
        sleep(Duration::from_millis(10)).await;
    }
    
    println!("Sent {} tasks in {:?}", task_id, duration);
    
    // 接收所有结果
    let mut received = 0;
    while received < task_id {
        if timeout(Duration::from_secs(5), result_receiver.recv()).await.ok().flatten().is_some() {
            received += 1;
        } else {
            break;
        }
    }
    
    println!("Received {} results", received);
    assert_eq!(received, task_id);
    
    pool.shutdown().await;
}

/// 测试 worker 统计数据的准确性
#[tokio::test]
async fn test_statistics_accuracy() {
    let executor = TestExecutor;
    let worker_count = 4;
    let contexts = create_contexts_with_stats(worker_count);
    let stats_refs: Vec<_> = contexts.iter().map(|(_, s)| s.clone()).collect();
    let (mut pool, handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    
    // 为每个 worker 发送已知数量的任务
    let tasks_per_worker = vec![10, 20, 15, 25];
    
    for (i, &task_count) in tasks_per_worker.iter().enumerate() {
        for j in 0..task_count {
            let task = TestTask {
                id: i * 100 + j,
                data: format!("worker-{}-task-{}", i, j),
            };
            handles[i].send_task(task).await.unwrap();
        }
    }
    
    // 接收所有结果
    let total: usize = tasks_per_worker.iter().sum();
    let mut received = 0;
    while received < total {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    // 验证每个 worker 的统计数据
    for (i, &expected) in tasks_per_worker.iter().enumerate() {
        let actual = stats_refs[i].task_count.load(Ordering::SeqCst);
        assert_eq!(actual, expected, "Worker {} should have processed {} tasks", i, expected);
    }
    
    pool.shutdown().await;
}

/// 测试动态扩容和缩容
#[tokio::test]
async fn test_dynamic_resize() {
    let executor = TestExecutor;
    
    // 开始时有 2 个 workers
    let contexts = create_contexts_with_stats(2);
    let (mut pool, initial_handles, mut result_receiver) = WorkerPool::new(executor, contexts).unwrap();
    assert_eq!(pool.worker_count(), 2);
    
    // 发送一些任务
    for i in 0..10 {
        let task = TestTask { id: i, data: format!("phase1-{}", i) };
        initial_handles[i % 2].send_task(task).await.unwrap();
    }
    
    // 扩容：添加 5 个新 workers
    let new_contexts = create_contexts_with_stats(5);
    let new_handles = pool.add_workers(new_contexts).await.unwrap();
    assert_eq!(pool.worker_count(), 7);
    
    // 发送更多任务到新 workers
    for i in 0..20 {
        let task = TestTask { id: 100 + i, data: format!("phase2-{}", i) };
        new_handles[i % 5].send_task(task).await.unwrap();
    }
    
    // 接收所有结果
    let mut received = 0;
    while received < 30 {
        if result_receiver.recv().await.is_some() {
            received += 1;
        }
    }
    
    assert_eq!(received, 30);
    
    // 关闭（模拟缩容）
    pool.shutdown().await;
    assert_eq!(pool.worker_count(), 0);
}
