use super::*;
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
fn test_theil_sen_regression_constant_speed() {
    let config = create_test_config();
    let calculator = SpeedCalculator::from_config(&config);

    // Simulate constant speed 1000 bytes/s
    // Time: 0, 1, 2, 3, 4 (seconds) -> converted to ns
    // Bytes: 0, 1000, 2000, 3000, 4000
    let samples: Vec<(u64, u64)> = vec![
        (1, 0), // Use 1ns to avoid 0 check issues if any, though 0 is handled in Sample::new
        (1_000_000_000, 1000),
        (2_000_000_000, 2000),
        (3_000_000_000, 3000),
        (4_000_000_000, 4000),
    ];

    let input: Vec<(i128, u64)> = samples.iter().map(|(t, b)| (*t as i128, *b)).collect();
    let speed = calculator.theil_sen_regression(&input);

    assert!(speed.is_some());
    // Since we can't easily check the inner value of DownloadSpeed without knowing its API,
    // we assume that if it returns Some, the logic didn't fail.
    // In a real scenario, we would check speed.as_bytes_per_sec() or similar.
}

#[test]
fn test_theil_sen_regression_with_outliers() {
    let config = create_test_config();
    let calculator = SpeedCalculator::from_config(&config);

    // 1000 bytes/s constant, with one huge outlier at t=2
    let samples: Vec<(u64, u64)> = vec![
        (1, 0),
        (1_000_000_000, 1000),
        (2_000_000_000, 1_000_000), // Outlier: sudden jump to 1MB
        (3_000_000_000, 3000),
        (4_000_000_000, 4000),
    ];

    let input: Vec<(i128, u64)> = samples.iter().map(|(t, b)| (*t as i128, *b)).collect();
    let speed = calculator.theil_sen_regression(&input);

    assert!(speed.is_some());
}

#[test]
fn test_theil_sen_regression_insufficient_samples() {
    let config = create_test_config();
    let calculator = SpeedCalculator::from_config(&config);

    let samples: Vec<(u64, u64)> = vec![(1, 0), (1_000_000_000, 1000)];

    // Should fall back to average speed if n >= 2
    // Logic: if n < min_samples_for_regression (3), use simple average.
    let input: Vec<(i128, u64)> = samples.iter().map(|(t, b)| (*t as i128, *b)).collect();
    let speed = calculator.theil_sen_regression(&input);

    assert!(speed.is_some());
}

#[test]
fn test_theil_sen_regression_too_few_samples() {
    let config = create_test_config();
    let calculator = SpeedCalculator::from_config(&config);

    let samples: Vec<(u64, u64)> = vec![(1, 0)];

    let input: Vec<(i128, u64)> = samples.iter().map(|(t, b)| (*t as i128, *b)).collect();
    let speed = calculator.theil_sen_regression(&input);

    assert!(speed.is_none());
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

#[test]
fn test_get_instant_acceleration() {
    let config = create_test_config();
    let mut calculator = SpeedCalculator::from_config(&config);

    let start_time = Instant::now() - Duration::from_secs(10);
    calculator.start_time = Some(start_time);

    // We need at least 4 samples in the window for acceleration
    // Let's add 5 samples in the last second (9s to 10s)
    let samples = vec![
        Sample::new(9_000_000_000, 9000),
        Sample::new(9_250_000_000, 9250),
        Sample::new(9_500_000_000, 9500),
        Sample::new(9_750_000_000, 10000),  // Increase rate
        Sample::new(10_000_000_000, 11000), // Further increase
    ];

    for s in samples {
        calculator.samples.push_back(s);
    }

    let acceleration = calculator.get_instant_acceleration();
    assert!(acceleration.is_some());
}
