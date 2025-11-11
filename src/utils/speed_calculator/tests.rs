use crate::SpeedConfigBuilder;
use crate::config::SpeedConfig;
use crate::utils::speed_calculator::SpeedCalculator;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
    assert!(calculator.ring_buffer.capacity() >= 6);
    // 验证缓冲区为空
    assert_eq!(calculator.ring_buffer.len(), 0);
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
    
    // 等待采样间隔
    thread::sleep(Duration::from_millis(250));
    
    // 再次记录
    calculator.record_sample(4096);
    
    // 现在应该有 2 个采样点
    let samples = calculator.read_recent_samples();
    assert_eq!(samples.len(), 2);
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
    assert!(samples.len() <= calculator.ring_buffer.capacity());
    
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
    assert!(calculator.ring_buffer.capacity() >= 6);
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
    
    // 缓冲区大小被限制在 MAX_BUFFER_SIZE (512)
    // 实际计算：10s / 10ms * 2.0 = 2000，但被限制为 512
    assert!(calculator.ring_buffer.capacity() >= 256);
    assert!(calculator.ring_buffer.capacity() <= 512);
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

// 鲁棒回归测试（Theil-Sen 估计器）
// ============================================================================

#[test]
fn test_theil_sen_perfect_linear() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 完美线性关系：速度恒定为 1024 bytes/s
    // 时间戳以纳秒为单位
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 1024u64),
        (2_000_000_000i128, 2048u64),
        (3_000_000_000i128, 3072u64),
        (4_000_000_000i128, 4096u64),
    ];
    let speed = calculator.theil_sen_regression(&samples);
    
    // 速度应该接近 1024.0
    assert!((speed - 1024.0).abs() < 0.1);
}


// ============================================================================
// 加速度计算测试
// ============================================================================

#[test]
fn test_calculate_acceleration_constant_speed() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 恒定速度：加速度应该为 0
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 1024u64),
        (2_000_000_000i128, 2048u64),
        (3_000_000_000i128, 3072u64),
        (4_000_000_000i128, 4096u64),
    ];
    
    let acceleration = calculator.calculate_acceleration(&samples);
    
    // 加速度应该接近 0（恒定速度）
    assert!(acceleration.abs() < 10.0);
}

#[test]
fn test_calculate_acceleration_accelerating() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 加速：前半段速度 256 bytes/s，后半段速度 768 bytes/s
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 256u64),
        (2_000_000_000i128, 512u64),
        (3_000_000_000i128, 1280u64),
        (4_000_000_000i128, 2048u64),
    ];
    
    let acceleration = calculator.calculate_acceleration(&samples);
    
    // 加速度应该为正
    assert!(acceleration > 0.0);
}

#[test]
fn test_calculate_acceleration_decelerating() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 减速：速度从 1536 bytes/s 减少到 512 bytes/s
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 1536u64),
        (2_000_000_000i128, 2048u64),
        (3_000_000_000i128, 2560u64),
        (4_000_000_000i128, 3072u64),
    ];
    
    let acceleration = calculator.calculate_acceleration(&samples);
    
    // 加速度应该为负
    assert!(acceleration < 0.0);
}

#[test]
fn test_calculate_acceleration_insufficient_samples() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 只有 3 个采样点（需要至少 4 个）
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 1024u64),
        (2_000_000_000i128, 2048u64),
    ];
    
    let acceleration = calculator.calculate_acceleration(&samples);
    
    // 应该返回 0
    assert_eq!(acceleration, 0.0);
}

#[test]
fn test_theil_sen_multiple_outliers() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 包含多个异常值的数据（最多 50% 异常值）
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 1024u64),
        (2_000_000_000i128, 2048u64),
        (3_000_000_000i128, 3072u64),
        (4_000_000_000i128, 4096u64),
        (5_000_000_000i128, 15000u64),  // 异常值
        (6_000_000_000i128, 6144u64),
        (7_000_000_000i128, 7168u64),
        (8_000_000_000i128, 20000u64),  // 异常值
        (9_000_000_000i128, 9216u64),
    ];
    
    let theil_sen_speed = calculator.theil_sen_regression(&samples);
    
    // 速度应该接近 1024（即使有 20% 的异常值）
    assert!((theil_sen_speed - 1024.0).abs() < 200.0);
}

#[test]
fn test_theil_sen_insufficient_samples() {
    let config = SpeedConfig::default();
    let calculator = SpeedCalculator::from_config(&config);
    
    // 只有 2 个采样点
    let samples = vec![
        (0i128, 0u64),
        (1_000_000_000i128, 1024u64),
    ];
    let speed = calculator.theil_sen_regression(&samples);
    
    // 应该降级为平均速度
    assert_eq!(speed, 1024.0);
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
        assert!(calculator.ring_buffer.capacity() >= 6);
        
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
