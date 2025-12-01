//! RangeWriter 集成测试
//!
//! 测试 RangeWriter 在实际场景中的使用

use bytes::Bytes;
use hydra_dl::utils::writer::MmapWriter;
use std::num::NonZeroU64;
use std::path::PathBuf;
use tempfile::tempdir;
use tokio::fs;
use tokio::io::AsyncReadExt;

/// 测试模拟真实下载场景：多个并发 worker 乱序写入 Range
#[tokio::test]
async fn test_concurrent_writes_simulation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("concurrent_test.dat");

    // 使用 40KB 文件，分成 10 个 4K 对齐的 Range
    let range_size = 4096u64;
    let range_count = 10;
    let total_size = range_size * range_count as u64;

    let (writer, mut allocator) =
        MmapWriter::new(file_path.clone(), NonZeroU64::new(total_size).unwrap()).unwrap();

    // 预先分配所有 Range
    let mut ranges = Vec::new();
    for i in 0..range_count {
        let range = allocator.allocate(NonZeroU64::new(range_size).unwrap()).unwrap();
        ranges.push((i, range));
    }

    // 模拟并发写入（实际按顺序执行，但模拟乱序接收）
    let write_order = vec![3, 1, 7, 0, 5, 9, 2, 4, 6, 8];

    for &range_id in &write_order {
        let range = ranges[range_id].1;
        let data = vec![(range_id + 1) as u8; range.len() as usize];
        writer.write_range(range, data.as_ref()).unwrap();
    }

    assert!(writer.is_complete());
    writer.finalize().unwrap();

    // 验证文件内容
    let mut file = fs::File::open(&file_path).await.unwrap();
    let mut content = Vec::new();
    file.read_to_end(&mut content).await.unwrap();

    assert_eq!(content.len(), total_size as usize);

    // 验证每个 Range 的内容
    for i in 0..range_count {
        let start = (i as u64 * range_size) as usize;
        let end = start + range_size as usize;
        let expected_value = (i + 1) as u8;
        assert!(content[start..end].iter().all(|&b| b == expected_value));
    }
}

/// 测试部分 Range 写入后的状态
#[tokio::test]
async fn test_partial_completion() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("partial_test.dat");

    // 使用 4K 对齐的大小
    let range_size = 4096u64;
    let total_size = range_size * 5; // 20KB

    let (writer, mut allocator) =
        MmapWriter::new(file_path.clone(), NonZeroU64::new(total_size).unwrap()).unwrap();

    // 分配 5 个 4K Range
    let range0 = allocator
        .allocate(NonZeroU64::new(range_size).unwrap())
        .unwrap();
    let range1 = allocator
        .allocate(NonZeroU64::new(range_size).unwrap())
        .unwrap();
    let range2 = allocator
        .allocate(NonZeroU64::new(range_size).unwrap())
        .unwrap();
    let range3 = allocator
        .allocate(NonZeroU64::new(range_size).unwrap())
        .unwrap();
    let range4 = allocator
        .allocate(NonZeroU64::new(range_size).unwrap())
        .unwrap();

    // 只写入部分 Range
    writer
        .write_range(range0, vec![1u8; range_size as usize].as_ref())
        .unwrap();
    writer
        .write_range(range2, vec![3u8; range_size as usize].as_ref())
        .unwrap();
    writer
        .write_range(range4, vec![5u8; range_size as usize].as_ref())
        .unwrap();

    assert!(!writer.is_complete());

    let (written, total_bytes) = writer.progress();
    assert_eq!(written, range_size * 3);
    assert_eq!(total_bytes, total_size);

    // 完成剩余的 Range
    writer
        .write_range(range1, vec![2u8; range_size as usize].as_ref())
        .unwrap();
    writer
        .write_range(range3, vec![4u8; range_size as usize].as_ref())
        .unwrap();

    assert!(writer.is_complete());
    writer.finalize().unwrap();
}

/// 测试边界情况：单个 Range
#[tokio::test]
async fn test_single_range() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("single_range.dat");

    let total_size = 1000u64;
    let (writer, mut allocator) =
        MmapWriter::new(file_path.clone(), NonZeroU64::new(total_size).unwrap()).unwrap();

    let range = allocator
        .allocate(NonZeroU64::new(total_size).unwrap())
        .unwrap();
    let data = vec![42u8; total_size as usize];
    writer.write_range(range, data.as_ref()).unwrap();

    assert!(writer.is_complete());
    writer.finalize().unwrap();

    // 验证文件内容
    let content = fs::read(&file_path).await.unwrap();
    assert_eq!(content.len(), total_size as usize);
    assert!(content.iter().all(|&b| b == 42));
}

/// 测试不均匀的 Range 大小
#[tokio::test]
async fn test_uneven_ranges() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("uneven_ranges.dat");

    // 使用 12KB 文件，分成 3 个 4K 对齐的 Range
    let range_size = 4096u64;
    let total_size = range_size * 3; // 12KB

    let (writer, mut allocator) =
        MmapWriter::new(file_path.clone(), NonZeroU64::new(total_size).unwrap()).unwrap();

    // 分配 3 个 4K Range
    let range0 = allocator.allocate(NonZeroU64::new(range_size).unwrap()).unwrap();
    let range1 = allocator.allocate(NonZeroU64::new(range_size).unwrap()).unwrap();
    let range2 = allocator.allocate(NonZeroU64::new(range_size).unwrap()).unwrap();

    writer.write_range(range0, vec![1u8; range_size as usize].as_ref()).unwrap();
    writer.write_range(range1, vec![2u8; range_size as usize].as_ref()).unwrap();
    writer.write_range(range2, vec![3u8; range_size as usize].as_ref()).unwrap();

    assert!(writer.is_complete());
    writer.finalize().unwrap();

    // 验证文件内容
    let content = fs::read(&file_path).await.unwrap();
    assert_eq!(content.len(), total_size as usize);
    assert_eq!(&content[0..range_size as usize], &vec![1u8; range_size as usize][..]);
    assert_eq!(&content[range_size as usize..range_size as usize * 2], &vec![2u8; range_size as usize][..]);
    assert_eq!(&content[range_size as usize * 2..total_size as usize], &vec![3u8; range_size as usize][..]);
}

/// 测试大量 4K 对齐的 Range
#[tokio::test]
async fn test_many_small_ranges() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("many_ranges.dat");

    // 使用 100 个 4K Range
    let range_count = 100;
    let range_size = 4096u64;
    let total_size = range_count as u64 * range_size;

    let (writer, mut allocator) =
        MmapWriter::new(file_path.clone(), NonZeroU64::new(total_size).unwrap()).unwrap();

    // 分配并写入所有 Range
    for i in 0..range_count {
        let range = allocator
            .allocate(NonZeroU64::new(range_size).unwrap())
            .unwrap();
        let data = Bytes::from(vec![(i % 256) as u8; range_size as usize]);
        writer.write_range(range, data.as_ref()).unwrap();
    }

    assert!(writer.is_complete());
    writer.finalize().unwrap();

    let metadata = fs::metadata(&file_path).await.unwrap();
    assert_eq!(metadata.len(), total_size);
}

/// 测试错误处理：无效路径
#[tokio::test]
async fn test_invalid_path() {
    // 尝试在不存在的目录中创建文件
    let invalid_path = PathBuf::from("/nonexistent/path/file.dat");
    let result = MmapWriter::new(invalid_path, NonZeroU64::new(1000).unwrap());
    assert!(result.is_err());
}

/// 测试进度追踪的准确性
#[tokio::test]
async fn test_progress_tracking() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("progress.dat");

    // 使用 4K 对齐的 Range
    let range_size = 4096u64;
    let range_count = 10;
    let total_size = range_size * range_count as u64;

    let (writer, mut allocator) =
        MmapWriter::new(file_path.clone(), NonZeroU64::new(total_size).unwrap()).unwrap();

    // 逐步写入并检查进度
    for i in 0..range_count {
        let (written_before, _) = writer.progress();
        assert_eq!(written_before, i as u64 * range_size);

        let range = allocator
            .allocate(NonZeroU64::new(range_size).unwrap())
            .unwrap();
        writer
            .write_range(range, vec![i as u8; range_size as usize].as_ref())
            .unwrap();

        let (written_after, _) = writer.progress();
        assert_eq!(written_after, (i + 1) as u64 * range_size);
    }

    assert!(writer.is_complete());
    writer.finalize().unwrap();
}
