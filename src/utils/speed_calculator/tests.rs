//! SpeedCalculator 集成测试
//!
//! 核心算法测试已移至 `core` 子模块。

use super::*;
use super::core::{MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, MIN_SAMPLES_FOR_REGRESSION};
use crate::config::{SpeedConfig, SpeedConfigBuilder};
use std::time::{Duration, Instant};

fn create_test_config() -> SpeedConfig {
    SpeedConfigBuilder::new()
        .base_interval(Duration::from_secs(1))
        .sample_interval(Duration::from_millis(100))
        .build()
}

#[test]
fn test_initialization() {
    let config = create_test_config();
    let calculator = SpeedCalculator::from_config(&config);

    // Check buffer size constraints
    assert!(calculator.max_samples >= MIN_BUFFER_SIZE);
    assert!(calculator.max_samples <= MAX_BUFFER_SIZE);
    assert!(calculator.samples.capacity() >= calculator.max_samples);

    // Check defaults
    assert_eq!(
        calculator.min_samples_for_regression,
        MIN_SAMPLES_FOR_REGRESSION
    );
    assert!(calculator.start_time.is_none());
    assert!(calculator.samples.is_empty());
}

#[test]
fn test_record_sample_basic() {
    let config = create_test_config();
    let mut calculator = SpeedCalculator::from_config(&config);

    // Record first sample
    calculator.record_sample(100);
    assert!(calculator.start_time.is_some());
    assert!(!calculator.samples.is_empty());
    assert_eq!(calculator.samples.back().unwrap().bytes, 100);

    let first_sample_ts = calculator.samples.back().unwrap().timestamp_ns;

    // Record second sample immediately (should be skipped due to interval being 100ms)
    // Note: In a real run, execution is fast enough that elapsed < 100ms.
    calculator.record_sample(200);

    // Should still be the first sample because interval check failed
    assert_eq!(calculator.samples.back().unwrap().bytes, 100);
    assert_eq!(
        calculator.samples.back().unwrap().timestamp_ns,
        first_sample_ts
    );
}

#[test]
fn test_get_instant_speed() {
    let config = create_test_config();
    let mut calculator = SpeedCalculator::from_config(&config);

    // Mock start time: 10 seconds ago
    let start_time = Instant::now() - Duration::from_secs(10);
    calculator.start_time = Some(start_time);

    // Populate buffer with samples relative to start_time
    // Current elapsed is ~10s (10_000_000_000 ns)
    // Instant window is 1s.
    // We need samples in range [9s, 10s].

    // Sample 1: 8.5s (outside window)
    calculator
        .samples
        .push_back(Sample::new(8_500_000_000, 8500));

    // Sample 2: 9.2s (inside window)
    calculator
        .samples
        .push_back(Sample::new(9_200_000_000, 9200));

    // Sample 3: 9.5s (inside window)
    calculator
        .samples
        .push_back(Sample::new(9_500_000_000, 9500));

    // Sample 4: 9.8s (inside window)
    calculator
        .samples
        .push_back(Sample::new(9_800_000_000, 9800));

    // Sample 5: 10.0s (current)
    calculator
        .samples
        .push_back(Sample::new(10_000_000_000, 10000));

    // Window contains 4 samples: 9.2, 9.5, 9.8, 10.0
    // This is >= MIN_SAMPLES_FOR_REGRESSION (3)

    let speed = calculator.get_instant_speed();
    assert!(speed.is_some());
}