//! SpeedCalculator 集成测试
//!
//! 核心算法测试已移至 `core` 子模块。
//!
//! 这些测试验证类型状态模式：
//! - `SpeedCalculatorRecording`: 记录状态，用于采样
//! - `SpeedCalculatorActive`: 激活状态，用于计算速度

use super::*;
use super::core::{MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, MIN_SAMPLES_FOR_REGRESSION};
use crate::config::{SpeedConfig, SpeedConfigBuilder};
use std::time::Duration;

fn create_test_config() -> SpeedConfig {
    SpeedConfigBuilder::new()
        .base_interval(Duration::from_secs(1))
        .sample_interval(Duration::from_millis(1)) // 1ms 采样间隔，方便测试
        .build()
}

#[test]
fn test_initialization() {
    let config = create_test_config();
    let calculator = SpeedCalculatorRecording::from_config(&config);

    // Check buffer size constraints via config
    assert!(calculator.config.max_samples >= MIN_BUFFER_SIZE);
    assert!(calculator.config.max_samples <= MAX_BUFFER_SIZE);
    assert!(calculator.samples.capacity() >= calculator.config.max_samples);

    // Check defaults
    assert_eq!(
        calculator.config.min_samples_for_regression,
        MIN_SAMPLES_FOR_REGRESSION
    );
    assert!(calculator.start_time.is_none());
    assert!(calculator.samples.is_empty());
}

#[test]
fn test_record_sample_returns_active() {
    let config = create_test_config();
    let mut calculator = SpeedCalculatorRecording::from_config(&config);

    // 第一次采样应该成功，返回 SpeedCalculatorActive
    let active = calculator.record_sample(100);
    assert!(active.is_some());
    
    // 验证 Recording 状态被更新
    assert!(calculator.start_time.is_some());
    assert!(!calculator.samples.is_empty());
    assert_eq!(calculator.samples.back().unwrap().bytes, 100);
}

#[test]
fn test_record_sample_skip_due_to_interval() {
    let config = SpeedConfigBuilder::new()
        .base_interval(Duration::from_secs(1))
        .sample_interval(Duration::from_secs(1)) // 1秒采样间隔
        .build();
    let mut calculator = SpeedCalculatorRecording::from_config(&config);

    // 第一次采样应该成功
    let active1 = calculator.record_sample(100);
    assert!(active1.is_some());

    // 立即第二次采样应该被跳过（因为间隔不足 1 秒）
    let active2 = calculator.record_sample(200);
    assert!(active2.is_none()); // 应该返回 None

    // 采样点应该还是第一个
    assert_eq!(calculator.samples.len(), 1);
    assert_eq!(calculator.samples.back().unwrap().bytes, 100);
}

#[test]
fn test_active_get_instant_speed() {
    let config = create_test_config();
    let mut calculator = SpeedCalculatorRecording::from_config(&config);

    // 记录多个采样点
    for i in 1..=5 {
        // 等待一点时间确保采样间隔
        std::thread::sleep(Duration::from_millis(2));
        let _ = calculator.record_sample(i * 1000);
    }

    // 获取最后一次采样的激活状态
    std::thread::sleep(Duration::from_millis(2));
    let active = calculator.record_sample(6000);
    assert!(active.is_some());

    let active = active.unwrap();
    
    // 激活状态应该能够计算速度（无 Option）
    let instant_speed = active.get_instant_speed();
    let window_avg_speed = active.get_window_avg_speed();
    let global_avg_speed = active.get_global_avg_speed();
    
    // 验证速度值是有效的（非零，因为有数据传输）
    assert!(instant_speed.as_u64() > 0 || global_avg_speed.as_u64() > 0);
    assert!(window_avg_speed.as_u64() > 0 || global_avg_speed.as_u64() > 0);
}

#[test]
fn test_clear_samples() {
    let config = create_test_config();
    let mut calculator = SpeedCalculatorRecording::from_config(&config);

    // 记录一些采样点
    let _ = calculator.record_sample(100);
    std::thread::sleep(Duration::from_millis(2));
    let _ = calculator.record_sample(200);

    assert!(!calculator.samples.is_empty());
    assert!(calculator.start_time.is_some());

    // 清空采样点
    calculator.clear_samples();

    assert!(calculator.samples.is_empty());
    assert!(calculator.start_time.is_none());
}