use crate::SpeedConfigBuilder;
use crate::config::SpeedConfig;
use crate::utils::speed_calculator::SpeedCalculator;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============================================================================
// Sample 结构测试
// ============================================================================

#[test]
fn test_sample_new() {
    use crate::utils::speed_calculator::Sample;
    
    let sample = Sample::new();
    
    // 新创建的 Sample 应该返回 None（时间戳为 0）
    assert_eq!(sample.read(), None);
}

#[test]
fn test_sample_write_and_read() {
    use crate::utils::speed_calculator::Sample;
    
    let sample = Sample::new();
    
    // 写入数据
    sample.write(1_000_000_000, 1024);
    
    // 读取数据
    let result = sample.read();
    assert_eq!(result, Some((1_000_000_000, 1024)));
}

#[test]
fn test_sample_overwrite() {
    use crate::utils::speed_calculator::Sample;
    
    let sample = Sample::new();
    
    // 第一次写入
    sample.write(1_000_000_000, 1024);
    assert_eq!(sample.read(), Some((1_000_000_000, 1024)));
    
    // 覆盖写入
    sample.write(2_000_000_000, 2048);
    assert_eq!(sample.read(), Some((2_000_000_000, 2048)));
}

#[test]
fn test_sample_zero_timestamp() {
    use crate::utils::speed_calculator::Sample;
    
    let sample = Sample::new();
    
    // 写入时间戳为 0 的数据
    sample.write(0, 1024);
    
    // 应该返回 None
    assert_eq!(sample.read(), None);
}

#[test]
fn test_sample_max_values() {
    use crate::utils::speed_calculator::Sample;
    
    let sample = Sample::new();
    
    // 写入最大值
    sample.write(u64::MAX, u64::MAX);
    
    // 读取应该成功
    assert_eq!(sample.read(), Some((u64::MAX, u64::MAX)));
}

#[test]
fn test_sample_atomic_write() {
    use crate::utils::speed_calculator::Sample;
    use std::sync::Arc;
    use std::thread;
    
    let sample = Arc::new(Sample::new());
    let mut handles = vec![];
    
    // 多线程并发写入
    for i in 0..10 {
        let sample_clone = Arc::clone(&sample);
        let handle = thread::spawn(move || {
            sample_clone.write((i + 1) * 1_000_000_000, (i + 1) * 1024);
        });
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    // 最终应该有一个有效的值（无论是哪个线程写入的）
    let result = sample.read();
    assert!(result.is_some());
    
    if let Some((ts, bytes)) = result {
        // 验证数据一致性：时间戳和字节数应该匹配（都来自同一次写入）
        assert!(ts > 0);
        assert!(bytes > 0);
        
        // 验证时间戳和字节数的关系（时间戳 / 1_000_000_000 应该等于 bytes / 1024）
        assert_eq!(ts / 1_000_000_000, bytes / 1024);
    }
}

// ============================================================================
// SpeedCalculator 基本功能测试
// ============================================================================

#[test]
fn test_speed_calculator_from_config() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 验证缓冲区大小
    // 默认配置：instant_speed_window = 1s, window_avg_duration = 5s, sample_interval = 100ms
    // 需要的采样点数 = max(1s, 5s) / 100ms = 50
    // 实际缓冲区大小 = 50 * 1.2 = 60
    // 但至少需要 MIN_SAMPLES_FOR_REGRESSION * 2 = 6 个采样点
    assert!(calculator.samples.len() >= 6);
    // 验证新增的字段存在
    assert_eq!(calculator.samples_written.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn test_speed_calculator_record_sample_basic() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录第一个采样点
    calculator.record_sample(1024);
    
    // 等待一小段时间以确保采样间隔
    thread::sleep(Duration::from_millis(150));
    
    // 记录第二个采样点
    calculator.record_sample(2048);
    
    // 读取采样点
    let samples = calculator.read_recent_samples();
    
    // 应该至少有 1 个采样点（第一个）
    assert!(samples.len() >= 1);
    // 验证 samples_written 计数器
    assert!(calculator.samples_written.load(std::sync::atomic::Ordering::Relaxed) >= 1);
}

#[test]
fn test_speed_calculator_record_sample_respects_interval() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(200))
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 快速记录多个采样点
    calculator.record_sample(1024);
    calculator.record_sample(2048);
    calculator.record_sample(3072);
    
    // 读取采样点（应该只有 1 个，因为采样间隔未到）
    let samples = calculator.read_recent_samples();
    assert_eq!(samples.len(), 1);
    // 验证 samples_written 计数器
    assert_eq!(calculator.samples_written.load(std::sync::atomic::Ordering::Relaxed), 1);
    
    // 等待采样间隔
    thread::sleep(Duration::from_millis(250));
    
    // 再次记录
    calculator.record_sample(4096);
    
    // 现在应该有 2 个采样点
    let samples = calculator.read_recent_samples();
    assert_eq!(samples.len(), 2);
    // 验证 samples_written 计数器
    assert_eq!(calculator.samples_written.load(std::sync::atomic::Ordering::Relaxed), 2);
}

#[test]
fn test_speed_calculator_ring_buffer_wrapping() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(10))
        .base_interval(Duration::from_millis(100))
        .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .buffer_size_margin(1.2)
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录足够多的采样点以触发环形缓冲区覆盖
    // 缓冲区大小 = max(100ms, 100ms) / 10ms * 1.2 = 12
    for i in 0..20 {
        calculator.record_sample((i + 1) * 1024);
        thread::sleep(Duration::from_millis(15));
    }
    
    // 读取采样点
    let samples = calculator.read_recent_samples();
    
    // 由于环形缓冲区覆盖，采样点数量不应超过缓冲区大小
    assert!(samples.len() <= calculator.samples.len());
    // 验证 samples_written 计数器
    assert_eq!(calculator.samples_written.load(std::sync::atomic::Ordering::Relaxed), 20);
    
    // 采样点应该按时间排序
    for i in 1..samples.len() {
        assert!(samples[i].0 > samples[i - 1].0);
    }
}

#[test]
fn test_speed_calculator_read_recent_samples_sorted() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(50))
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录多个采样点
    for i in 0..5 {
        calculator.record_sample((i + 1) * 1024);
        thread::sleep(Duration::from_millis(60));
    }
    
    let samples = calculator.read_recent_samples();
    
    // 验证排序：时间戳应该递增
    for i in 1..samples.len() {
        assert!(samples[i].0 > samples[i - 1].0, 
            "样本 {} 的时间戳 {} 应该大于样本 {} 的时间戳 {}", 
            i, samples[i].0, i - 1, samples[i - 1].0);
    }
    
    // 验证 samples_written 计数器
    assert_eq!(calculator.samples_written.load(std::sync::atomic::Ordering::Relaxed), 5);
}

// ============================================================================
// 线性回归测试
// ============================================================================

#[test]
fn test_linear_regression_insufficient_samples() {
    let config = SpeedConfigBuilder::new().build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 没有采样点
    let samples = vec![];
    let speed = calculator.linear_regression(&samples);
    assert_eq!(speed, 0.0);
    
    // 只有 1 个采样点
    let samples = vec![(1.0, 1024.0)];
    let speed = calculator.linear_regression(&samples);
    assert_eq!(speed, 0.0);
}

#[test]
fn test_linear_regression_two_samples() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 2 个采样点（降级为平均速度）
    // t=0s, bytes=0; t=1s, bytes=1024
    // 速度 = (1024 - 0) / (1.0 - 0.0) = 1024 bytes/s
    let samples = vec![
        (0.0, 0.0),
        (1.0, 1024.0),
    ];
    let speed = calculator.linear_regression(&samples);
    assert_eq!(speed, 1024.0);
}

#[test]
fn test_linear_regression_perfect_linear() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 完美线性关系：速度恒定为 1024 bytes/s
    let samples = vec![
        (0.0, 0.0),
        (1.0, 1024.0),
        (2.0, 2048.0),
        (3.0, 3072.0),
        (4.0, 4096.0),
    ];
    let speed = calculator.linear_regression(&samples);
    
    // 速度应该接近 1024.0
    assert!((speed - 1024.0).abs() < 0.1);
}

#[test]
fn test_linear_regression_with_noise() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 带噪声的线性关系（平均速度约 1024 bytes/s）
    let samples = vec![
        (0.0, 0.0),
        (1.0, 1000.0),
        (2.0, 2100.0),
        (3.0, 2950.0),
        (4.0, 4200.0),
    ];
    let speed = calculator.linear_regression(&samples);
    
    // 速度应该接近 1024.0（允许一定误差）
    assert!((speed - 1024.0).abs() < 100.0);
}

#[test]
fn test_linear_regression_zero_variance() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 所有采样点时间戳相同（方差为 0）
    let samples = vec![
        (1.0, 0.0),
        (1.0, 1024.0),
        (1.0, 2048.0),
    ];
    let speed = calculator.linear_regression(&samples);
    
    // 方差为 0 时应该返回 0
    assert_eq!(speed, 0.0);
}

// ============================================================================
// 速度计算测试
// ============================================================================

#[test]
fn test_get_instant_speed_no_samples() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    let (speed, valid) = calculator.get_instant_speed();
    
    // 没有采样点时应该返回无效
    assert!(!valid);
    assert_eq!(speed, 0.0);
}

#[test]
fn test_get_instant_speed_with_samples() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(50))
        .base_interval(Duration::from_secs(1))
        .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录多个采样点（模拟 1024 bytes/s 的速度）
    for i in 0..5 {
        calculator.record_sample((i + 1) * 1024 * 50 / 1000);
        thread::sleep(Duration::from_millis(60));
    }
    
    let (speed, valid) = calculator.get_instant_speed();
    
    // 应该有足够的采样点
    assert!(valid);
    
    // 速度应该在合理范围内（允许较大误差，因为实际时间可能有波动）
    assert!(speed > 0.0);
}

#[test]
fn test_get_window_avg_speed_no_samples() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    let (speed, valid) = calculator.get_window_avg_speed();
    
    // 没有采样点时应该返回无效
    assert!(!valid);
    assert_eq!(speed, 0.0);
}

#[test]
fn test_get_window_avg_speed_with_samples() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(50))
        .base_interval(Duration::from_secs(1))
        .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录多个采样点
    for i in 0..5 {
        calculator.record_sample((i + 1) * 1024 * 50 / 1000);
        thread::sleep(Duration::from_millis(60));
    }
    
    let (speed, valid) = calculator.get_window_avg_speed();
    
    // 应该有足够的采样点
    assert!(valid);
    
    // 速度应该大于 0
    assert!(speed > 0.0);
}

#[test]
fn test_speed_window_filtering() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(100))
        .base_interval(Duration::from_millis(100))
        .instant_window_multiplier(std::num::NonZeroU32::new(4).unwrap())
        .window_avg_multiplier(std::num::NonZeroU32::new(6).unwrap())
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录多个采样点
    for i in 0..8 {
        calculator.record_sample((i + 1) * 1024);
        thread::sleep(Duration::from_millis(120));
    }
    
    // 瞬时速度窗口（400ms）应该比窗口平均速度窗口（600ms）的采样点少
    let (instant_speed, instant_valid) = calculator.get_instant_speed();
    let (window_speed, window_valid) = calculator.get_window_avg_speed();
    
    // 两者都应该有效
    assert!(instant_valid);
    assert!(window_valid);
    
    // 速度都应该大于 0
    assert!(instant_speed > 0.0);
    assert!(window_speed > 0.0);
}

// ============================================================================
// 并发安全测试
// ============================================================================

#[test]
fn test_concurrent_record_sample() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(10))
        .build();
    let calculator = Arc::new(SpeedCalculator::from_config(&config));
    let mut handles = vec![];
    
    // 启动多个线程并发记录采样点
    for thread_id in 0..4 {
        let calculator_clone = Arc::clone(&calculator);
        let handle = thread::spawn(move || {
            for i in 0..10 {
                calculator_clone.record_sample((thread_id * 10 + i + 1) * 1024);
                thread::sleep(Duration::from_millis(15));
            }
        });
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    // 读取采样点
    let samples = calculator.read_recent_samples();
    
    // 应该有多个采样点
    assert!(samples.len() > 0);
    
    // 所有采样点应该按时间排序
    for i in 1..samples.len() {
        assert!(samples[i].0 > samples[i - 1].0);
    }
}

#[test]
fn test_concurrent_read_write() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(10))
        .build();
    let calculator = Arc::new(SpeedCalculator::from_config(&config));
    let mut handles = vec![];
    
    // 写入线程
    for thread_id in 0..2 {
        let calculator_clone = Arc::clone(&calculator);
        let handle = thread::spawn(move || {
            for i in 0..20 {
                calculator_clone.record_sample((thread_id * 20 + i + 1) * 1024);
                thread::sleep(Duration::from_millis(15));
            }
        });
        handles.push(handle);
    }
    
    // 读取线程
    for _ in 0..2 {
        let calculator_clone = Arc::clone(&calculator);
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                let _samples = calculator_clone.read_recent_samples();
                let _instant = calculator_clone.get_instant_speed();
                let _window = calculator_clone.get_window_avg_speed();
                thread::sleep(Duration::from_millis(30));
            }
        });
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    // 最终读取应该成功
    let samples = calculator.read_recent_samples();
    assert!(samples.len() > 0);
}

#[test]
fn test_concurrent_speed_calculation() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(20))
        .build();
    let calculator = Arc::new(SpeedCalculator::from_config(&config));
    
    // 先记录一些采样点
    for i in 0..10 {
        calculator.record_sample((i + 1) * 1024);
        thread::sleep(Duration::from_millis(25));
    }
    
    let mut handles = vec![];
    
    // 多个线程并发计算速度
    for _ in 0..4 {
        let calculator_clone = Arc::clone(&calculator);
        let handle = thread::spawn(move || {
            for _ in 0..5 {
                let (_instant_speed, _instant_valid) = calculator_clone.get_instant_speed();
                let (_window_speed, _window_valid) = calculator_clone.get_window_avg_speed();
                thread::sleep(Duration::from_millis(10));
            }
        });
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    // 验证最终状态
    let (speed, valid) = calculator.get_instant_speed();
    assert!(valid);
    assert!(speed >= 0.0);
}

// ============================================================================
// 边界条件测试
// ============================================================================

#[test]
fn test_very_small_buffer() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(100))
        .base_interval(Duration::from_millis(100))
        .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .buffer_size_margin(0.5)  // 非常小的余量
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 缓冲区应该至少有 MIN_SAMPLES_FOR_REGRESSION * 2 个采样点
    assert!(calculator.samples.len() >= 6);
}

#[test]
fn test_very_large_buffer() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(10))
        .base_interval(Duration::from_secs(10))
        .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .window_avg_multiplier(std::num::NonZeroU32::new(1).unwrap())
        .buffer_size_margin(2.0)
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 缓冲区大小 = 10s / 10ms * 2.0 = 2000
    assert!(calculator.samples.len() >= 1000);
}

#[test]
fn test_zero_bytes_download() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(50))
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录多个字节数为 0 的采样点（模拟没有下载进度）
    for _ in 0..5 {
        calculator.record_sample(0);
        thread::sleep(Duration::from_millis(60));
    }
    
    let (speed, valid) = calculator.get_instant_speed();
    
    // 应该有足够的采样点
    assert!(valid);
    
    // 速度应该为 0（没有下载进度）
    assert_eq!(speed, 0.0);
}

#[test]
fn test_constant_bytes_download() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(50))
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 记录多个字节数相同的采样点（下载已完成，没有新的进度）
    for _ in 0..5 {
        calculator.record_sample(1024 * 1024);
        thread::sleep(Duration::from_millis(60));
    }
    
    let (speed, valid) = calculator.get_instant_speed();
    
    // 应该有足够的采样点
    assert!(valid);
    
    // 速度应该接近 0（字节数不变）
    assert!(speed.abs() < 1.0);
}

#[test]
fn test_very_fast_download() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(50))
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 模拟非常快的下载速度（100 MB/s）
    for i in 0..5 {
        calculator.record_sample((i + 1) * 100 * 1024 * 1024 * 50 / 1000);
        thread::sleep(Duration::from_millis(60));
    }
    
    let (speed, valid) = calculator.get_instant_speed();
    
    // 应该有足够的采样点
    assert!(valid);
    
    // 速度应该非常大
    assert!(speed > 1024.0 * 1024.0);  // > 1 MB/s
}

#[test]
fn test_sample_interval_enforcement() {
    let config = SpeedConfigBuilder::new()
        .sample_interval(Duration::from_millis(100))
        .build();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 在采样间隔内快速调用多次
    calculator.record_sample(1024);
    calculator.record_sample(2048);
    calculator.record_sample(3072);
    calculator.record_sample(4096);
    
    // 应该只记录一个采样点
    let samples = calculator.read_recent_samples();
    assert_eq!(samples.len(), 1);
    
    // 等待采样间隔
    thread::sleep(Duration::from_millis(120));
    calculator.record_sample(5120);
    
    // 现在应该有 2 个采样点
    let samples = calculator.read_recent_samples();
    assert_eq!(samples.len(), 2);
}

// ============================================================================
// 配置测试
// ============================================================================

#[test]
fn test_different_window_configurations() {
    // 测试不同的时间窗口配置
    let configs = vec![
        SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(500))
            .instant_window_multiplier(std::num::NonZeroU32::new(1).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(2).unwrap())
            .build(),
        SpeedConfigBuilder::new()
            .base_interval(Duration::from_secs(1))
            .instant_window_multiplier(std::num::NonZeroU32::new(2).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(5).unwrap())
            .build(),
        SpeedConfigBuilder::new()
            .base_interval(Duration::from_millis(100))
            .instant_window_multiplier(std::num::NonZeroU32::new(2).unwrap())
            .window_avg_multiplier(std::num::NonZeroU32::new(3).unwrap())
            .build(),
    ];
    
    for config in configs {
        let calculator = SpeedCalculator::from_config(&config);
        
        // 验证缓冲区创建成功
        assert!(calculator.samples.len() >= 6);
        
        // 验证速度计算能正常工作
        let (_instant, _) = calculator.get_instant_speed();
        let (_window, _) = calculator.get_window_avg_speed();
    }
}

#[test]
fn test_different_sample_intervals() {
    let intervals = vec![
        Duration::from_millis(10),
        Duration::from_millis(50),
        Duration::from_millis(100),
        Duration::from_millis(500),
        Duration::from_secs(1),
    ];
    
    for interval in intervals {
        let config = SpeedConfigBuilder::new()
            .sample_interval(interval)
            .build();
        let calculator = SpeedCalculator::from_config(&config);
        
        // 记录一个采样点
        calculator.record_sample(1024);
        
        // 等待短暂时间确保采样点写入完成
        thread::sleep(Duration::from_millis(1));
        
        // 验证采样间隔执行
        let samples_before = calculator.read_recent_samples();
        
        // 在间隔内再次记录（应该被忽略）
        calculator.record_sample(2048);
        let samples_after = calculator.read_recent_samples();
        
        assert_eq!(samples_before.len(), samples_after.len());
    }
}
