//! RangeWriter 集成测试
//! 
//! 测试 RangeWriter 在实际场景中的使用

use bytes::Bytes;
use rs_dn::utils::range_writer::RangeWriter;
use rs_dn::utils::io_traits::TokioFileSystem;
use std::path::PathBuf;
use tempfile::tempdir;
use tokio::fs;
use tokio::io::AsyncReadExt;

/// 测试模拟真实下载场景：多个并发 worker 乱序写入 Range
#[tokio::test]
async fn test_concurrent_writes_simulation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("concurrent_test.dat");
    
    // 模拟 1MB 文件，分成 10 个 Range
    let total_size = 1024 * 1024u64;
    let range_count = 10;
    let range_size = total_size / range_count as u64;

    let fs = TokioFileSystem::default();
    
    let (writer, mut allocator) = RangeWriter::new(&fs, file_path.clone(), total_size).await.unwrap();
    
    // 预先分配所有 Range
    let mut ranges = Vec::new();
    for i in 0..range_count {
        let size = if i == range_count - 1 {
            allocator.remaining()
        } else {
            range_size
        };
        ranges.push((i, allocator.allocate(size).unwrap()));
    }
    
    // 模拟并发写入（实际按顺序执行，但模拟乱序接收）
    let write_order = vec![3, 1, 7, 0, 5, 9, 2, 4, 6, 8];
    
    for &range_id in &write_order {
        let range = ranges[range_id].1;
        let data = Bytes::from(vec![(range_id + 1) as u8; range.len() as usize]);
        writer.write_range(range, data).await.unwrap();
    }
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
    
    // 验证文件内容
    let mut file = fs::File::open(&file_path).await.unwrap();
    let mut content = Vec::new();
    file.read_to_end(&mut content).await.unwrap();
    
    assert_eq!(content.len(), total_size as usize);
    
    // 验证每个 Range 的内容
    for i in 0..range_count {
        let start = (i as u64 * range_size) as usize;
        let end = if i == range_count - 1 {
            total_size as usize
        } else {
            start + range_size as usize
        };
        let expected_value = (i + 1) as u8;
        assert!(content[start..end].iter().all(|&b| b == expected_value));
    }
}

/// 测试部分 Range 写入后的状态
#[tokio::test]
async fn test_partial_completion() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("partial_test.dat");
    
    let total_size = 500u64;
    let range_size = 100u64;
    
    let fs = TokioFileSystem::default();
    let (writer, mut allocator) = RangeWriter::new(&fs, file_path.clone(), total_size).await.unwrap();
    
    // 分配 5 个 Range
    let range0 = allocator.allocate(range_size).unwrap();
    let range1 = allocator.allocate(range_size).unwrap();
    let range2 = allocator.allocate(range_size).unwrap();
    let range3 = allocator.allocate(range_size).unwrap();
    let range4 = allocator.allocate(range_size).unwrap();
    
    // 只写入部分 Range
    writer.write_range(range0, Bytes::from(vec![1u8; range_size as usize])).await.unwrap();
    writer.write_range(range2, Bytes::from(vec![3u8; range_size as usize])).await.unwrap();
    writer.write_range(range4, Bytes::from(vec![5u8; range_size as usize])).await.unwrap();
    
    assert!(!writer.is_complete());
    
    let (written, total_bytes) = writer.progress();
    assert_eq!(written, 300);
    assert_eq!(total_bytes, 500);
    
    // 完成剩余的 Range
    writer.write_range(range1, Bytes::from(vec![2u8; range_size as usize])).await.unwrap();
    writer.write_range(range3, Bytes::from(vec![4u8; range_size as usize])).await.unwrap();
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
}

/// 测试边界情况：单个 Range
#[tokio::test]
async fn test_single_range() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("single_range.dat");
    
    let total_size = 1000u64;
    let fs = TokioFileSystem::default();
    let (writer, mut allocator) = RangeWriter::new(&fs, file_path.clone(), total_size).await.unwrap();
    
    let range = allocator.allocate(total_size).unwrap();
    let data = Bytes::from(vec![42u8; total_size as usize]);
    writer.write_range(range, data).await.unwrap();
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
    
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
    
    // 总大小不能被 Range 数量整除
    let total_size = 1000u64;

    let fs = TokioFileSystem::default();
    
    let (writer, mut allocator) = RangeWriter::new(&fs, file_path.clone(), total_size).await.unwrap();
    
    // 分配不均匀的 Range
    let range0 = allocator.allocate(333).unwrap();  // 0-332 (333 bytes)
    let range1 = allocator.allocate(333).unwrap();  // 333-665 (333 bytes)
    let range2 = allocator.allocate(334).unwrap();  // 666-999 (334 bytes)
    
    writer.write_range(range0, Bytes::from(vec![1u8; 333])).await.unwrap();
    writer.write_range(range1, Bytes::from(vec![2u8; 333])).await.unwrap();
    writer.write_range(range2, Bytes::from(vec![3u8; 334])).await.unwrap();
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
    
    // 验证文件内容
    let content = fs::read(&file_path).await.unwrap();
    assert_eq!(content.len(), total_size as usize);
    assert_eq!(&content[0..333], &vec![1u8; 333][..]);
    assert_eq!(&content[333..666], &vec![2u8; 333][..]);
    assert_eq!(&content[666..1000], &vec![3u8; 334][..]);
}

/// 测试空文件
#[tokio::test]
async fn test_empty_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("empty.dat");
    let fs = TokioFileSystem::default();
    let (writer, _allocator) = RangeWriter::new(&fs, file_path.clone(), 0).await.unwrap();
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
    
    let metadata = fs::metadata(&file_path).await.unwrap();
    assert_eq!(metadata.len(), 0);
}

/// 测试大量小 Range
#[tokio::test]
async fn test_many_small_ranges() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("many_ranges.dat");
    
    let range_count = 1000;
    let range_size = 100u64;
    let total_size = range_count as u64 * range_size;

    let fs = TokioFileSystem::default();
    
    let (writer, mut allocator) = RangeWriter::new(&fs, file_path.clone(), total_size).await.unwrap();
    
    // 分配并写入所有 Range
    for i in 0..range_count {
        let range = allocator.allocate(range_size).unwrap();
        let data = Bytes::from(vec![(i % 256) as u8; range_size as usize]);
        writer.write_range(range, data).await.unwrap();
    }
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
    
    let metadata = fs::metadata(&file_path).await.unwrap();
    assert_eq!(metadata.len(), total_size);
}

/// 测试错误处理：无效路径
#[tokio::test]
async fn test_invalid_path() {
    // 尝试在不存在的目录中创建文件
    let invalid_path = PathBuf::from("/nonexistent/path/file.dat");
    let fs = TokioFileSystem::default();
    let result = RangeWriter::new(&fs, invalid_path, 1000).await;
    assert!(result.is_err());
}

/// 测试进度追踪的准确性
#[tokio::test]
async fn test_progress_tracking() {
    let fs = TokioFileSystem::default();
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("progress.dat");
    
    let total_size = 1000u64;
    let range_count = 10;
    let range_size = total_size / range_count as u64;
    
    let (writer, mut allocator) = RangeWriter::new(&fs, file_path.clone(), total_size).await.unwrap();
    
    // 逐步写入并检查进度
    for i in 0..range_count {
        let (written_before, _) = writer.progress();
        assert_eq!(written_before, i as u64 * range_size);
        
        let range = allocator.allocate(range_size).unwrap();
        writer.write_range(range, Bytes::from(vec![i as u8; range_size as usize])).await.unwrap();
        
        let (written_after, _) = writer.progress();
        assert_eq!(written_after, (i + 1) as u64 * range_size);
    }
    
    assert!(writer.is_complete());
    writer.finalize().await.unwrap();
}

