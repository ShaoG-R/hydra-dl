//! Worker 槽位位掩码管理
//!
//! 使用原子位掩码 (`AtomicU32`) 高效管理最多 32 个 worker 槽位的空闲状态。
//! 
//! # 核心思想
//! 
//! - **位表示**: 每个 bit 代表一个槽位，1 表示已占用，0 表示空闲
//! - **快速查找**: 使用 `trailing_zeros()` 快速找到第一个空闲位
//! - **原子操作**: 使用 CAS (Compare-And-Swap) 保证并发安全
//! 
//! # 位掩码示例
//! 
//! ```text
//! 0b00000000_00000000_00000000_00000111
//!   ↑                              ↑↑↑
//!   槽位 31                         槽位 2,1,0 已占用
//! ```
//! 
//! # 使用示例
//! 
//! ```ignore
//! let mask = WorkerMask::new(3); // 初始占用槽位 0, 1, 2
//! 
//! // 分配新槽位
//! let id = mask.allocate().unwrap(); // 返回 3
//! 
//! // 释放槽位
//! mask.free(1);
//! 
//! // 再次分配会复用空闲槽位
//! let id = mask.allocate().unwrap(); // 返回 1（最小的空闲位置）
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use crate::{DownloadError, Result};

/// Worker 槽位最大数量（受限于 u32 的位数）
pub const MAX_WORKERS: usize = 32;

/// Worker 槽位位掩码管理器
/// 
/// 使用一个 `AtomicU32` 来追踪所有 worker 槽位的占用状态。
/// 位为 1 表示已占用，位为 0 表示空闲。
/// 
/// # 并发安全
/// 
/// 所有操作都使用原子 CAS 操作，保证多线程并发安全。
pub struct WorkerMask {
    /// 位掩码：每个 bit 代表一个槽位，1 = 已占用，0 = 空闲
    mask: AtomicU32,
}

impl WorkerMask {
    /// 创建新的 worker 位掩码
    /// 
    /// # Arguments
    /// 
    /// - `initial_count`: 初始已占用的槽位数量（从索引 0 开始连续占用）
    /// 
    /// # Returns
    /// 
    /// 新创建的 `WorkerMask` 实例
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// let mask = WorkerMask::new(3);
    /// // 槽位 0, 1, 2 已占用，3-31 空闲
    /// assert_eq!(mask.count(), 3);
    /// ```
    pub fn new(initial_count: usize) -> Result<Self> {
        if initial_count > MAX_WORKERS {
            return Err(DownloadError::WorkerCountExceeded(initial_count, MAX_WORKERS));
        }

        // 计算初始掩码：前 initial_count 位为 1
        let initial_mask = if initial_count == 0 {
            0u32
        } else if initial_count >= MAX_WORKERS {
            u32::MAX // 所有位都为 1
        } else {
            // (1 << initial_count) - 1 生成前 n 位为 1 的掩码
            (1u32 << initial_count) - 1
        };
        
        Ok(Self {
            mask: AtomicU32::new(initial_mask),
        })
    }
    
    /// 分配一个空闲的槽位
    /// 
    /// 使用 CAS 循环找到并原子地标记第一个空闲位（索引最小）。
    /// 
    /// # Returns
    /// 
    /// - `Ok(worker_id)`: 成功分配，返回槽位索引
    /// - `Err(DownloadError::WorkerPoolFull)`: 所有槽位都已占用
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// let mask = WorkerMask::new(2);
    /// let id = mask.allocate().unwrap();
    /// assert_eq!(id, 2); // 下一个可用的位置
    /// ```
    pub fn allocate(&self) -> Result<usize> {
        loop {
            // 读取当前掩码
            let current = self.mask.load(Ordering::Acquire);
            
            // 检查是否已满（所有位都为 1）
            if current == u32::MAX {
                return Err(DownloadError::WorkerPoolFull(MAX_WORKERS));
            }
            
            // 找到第一个为 0 的位（最低位开始）
            // trailing_ones() 返回从最低位开始连续 1 的个数
            // 即第一个 0 的位置
            let free_bit = current.trailing_ones() as usize;
            
            // 双重检查（理论上不会发生，但保险起见）
            if free_bit >= MAX_WORKERS {
                return Err(DownloadError::WorkerPoolFull(MAX_WORKERS));
            }
            
            // 计算新掩码：将该位设置为 1
            let new_mask = current | (1u32 << free_bit);
            
            // CAS 操作：只有在掩码未被其他线程修改时才更新
            match self.mask.compare_exchange(
                current,
                new_mask,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(free_bit),
                Err(_) => continue, // CAS 失败，重试
            }
        }
    }
    
    /// 释放指定的槽位
    /// 
    /// 将指定位设置为 0，表示该槽位空闲可用。
    /// 
    /// # Arguments
    /// 
    /// - `worker_id`: 要释放的槽位索引
    /// 
    /// # Returns
    /// 
    /// - `Ok(())`: 成功释放
    /// - `Err(DownloadError::InvalidWorkerId)`: worker_id 超出范围
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// let mask = WorkerMask::new(3);
    /// mask.free(1).unwrap();
    /// assert!(!mask.is_allocated(1));
    /// ```
    #[inline]
    pub fn free(&self, worker_id: usize) -> Result<()> {
        if worker_id >= MAX_WORKERS {
            return Err(DownloadError::InvalidWorkerId(worker_id, MAX_WORKERS));
        }
        
        loop {
            let current = self.mask.load(Ordering::Acquire);
            
            // 计算新掩码：将该位清零
            let new_mask = current & !(1u32 << worker_id);
            
            // CAS 操作
            match self.mask.compare_exchange(
                current,
                new_mask,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue, // CAS 失败，重试
            }
        }
    }
    
    /// 检查指定槽位是否已分配
    /// 
    /// # Arguments
    /// 
    /// - `worker_id`: 槽位索引
    /// 
    /// # Returns
    /// 
    /// `true` 表示已占用，`false` 表示空闲
    /// 
    /// # Panics
    /// 
    /// 如果 `worker_id >= MAX_WORKERS` 则 panic
    #[inline]
    #[allow(dead_code)]
    pub fn is_allocated(&self, worker_id: usize) -> bool {
        assert!(worker_id < MAX_WORKERS, "worker_id {} 超出范围 [0, {})", worker_id, MAX_WORKERS);
        
        let current = self.mask.load(Ordering::Acquire);
        (current & (1u32 << worker_id)) != 0
    }
    
    /// 计算当前已占用的槽位数量
    /// 
    /// # Returns
    /// 
    /// 已占用槽位的总数
    #[inline]
    pub fn count(&self) -> usize {
        let current = self.mask.load(Ordering::Acquire);
        current.count_ones() as usize
    }
    
    /// 检查是否已满（所有槽位都已占用）
    #[inline]
    #[allow(dead_code)]
    pub fn is_full(&self) -> bool {
        self.mask.load(Ordering::Acquire) == u32::MAX
    }
    
    /// 检查是否为空（所有槽位都空闲）
    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.mask.load(Ordering::Acquire) == 0
    }
    
    /// 获取当前掩码的原始值（用于调试）
    #[inline]
    pub fn raw_mask(&self) -> u32 {
        self.mask.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for WorkerMask {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mask = self.raw_mask();
        write!(f, "WorkerMask {{ mask: 0b{:032b}, count: {} }}", mask, self.count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    
    #[test]
    fn test_new_empty() {
        let mask = WorkerMask::new(0).unwrap();
        assert_eq!(mask.count(), 0);
        assert_eq!(mask.raw_mask(), 0);
        assert!(mask.is_empty());
        assert!(!mask.is_full());
    }
    
    #[test]
    fn test_new_with_initial_count() {
        let mask = WorkerMask::new(5).unwrap();
        assert_eq!(mask.count(), 5);
        assert_eq!(mask.raw_mask(), 0b00011111);
        assert!(!mask.is_empty());
        assert!(!mask.is_full());
        
        // 前 5 个槽位应该被占用
        for i in 0..5 {
            assert!(mask.is_allocated(i), "槽位 {} 应该被占用", i);
        }
        
        // 后续槽位应该空闲
        for i in 5..MAX_WORKERS {
            assert!(!mask.is_allocated(i), "槽位 {} 应该空闲", i);
        }
    }
    
    #[test]
    fn test_new_full() {
        let mask = WorkerMask::new(MAX_WORKERS).unwrap();
        assert_eq!(mask.count(), MAX_WORKERS);
        assert_eq!(mask.raw_mask(), u32::MAX);
        assert!(!mask.is_empty());
        assert!(mask.is_full());
    }
    
    #[test]
    fn test_allocate_sequential() {
        let mask = WorkerMask::new(0).unwrap();
        
        // 连续分配应该返回递增的索引
        for expected_id in 0..10 {
            let id = mask.allocate().unwrap();
            assert_eq!(id, expected_id);
            assert!(mask.is_allocated(id));
        }
        
        assert_eq!(mask.count(), 10);
    }
    
    #[test]
    fn test_allocate_full() {
        let mask = WorkerMask::new(MAX_WORKERS).unwrap();
        
        // 所有槽位已满，应该返回错误
        let result = mask.allocate();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DownloadError::WorkerPoolFull(MAX_WORKERS)));
    }
    
    #[test]
    fn test_free_and_reallocate() {
        let mask = WorkerMask::new(5).unwrap();
        
        // 释放槽位 2
        mask.free(2).unwrap();
        assert!(!mask.is_allocated(2));
        assert_eq!(mask.count(), 4);
        
        // 下次分配应该复用槽位 2（最小的空闲位置）
        let id = mask.allocate().unwrap();
        assert_eq!(id, 2);
        assert_eq!(mask.count(), 5);
    }
    
    #[test]
    fn test_free_multiple_and_reallocate_min() {
        let mask = WorkerMask::new(10).unwrap();
        
        // 释放多个槽位
        mask.free(3).unwrap();
        mask.free(7).unwrap();
        mask.free(1).unwrap();
        
        assert_eq!(mask.count(), 7);
        
        // 分配应该返回最小的空闲位置
        assert_eq!(mask.allocate().unwrap(), 1);
        assert_eq!(mask.allocate().unwrap(), 3);
        assert_eq!(mask.allocate().unwrap(), 7);
        assert_eq!(mask.allocate().unwrap(), 10); // 新的位置
        
        assert_eq!(mask.count(), 11);
    }
    
    #[test]
    fn test_free_invalid_id() {
        let mask = WorkerMask::new(5).unwrap();
        
        // 释放超出范围的 ID 应该返回错误
        let result = mask.free(MAX_WORKERS);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DownloadError::InvalidWorkerId(_, _)));
    }
    
    #[test]
    #[should_panic(expected = "超出范围")]
    fn test_is_allocated_invalid_id() {
        let mask = WorkerMask::new(5).unwrap();
        let _ = mask.is_allocated(MAX_WORKERS); // 应该 panic
    }
    
    #[test]
    fn test_concurrent_allocations() {
        let mask = Arc::new(WorkerMask::new(0).unwrap());
        let mut handles = vec![];
        
        // 10 个线程同时分配
        for _ in 0..10 {
            let mask_clone = Arc::clone(&mask);
            let handle = thread::spawn(move || {
                mask_clone.allocate().unwrap()
            });
            handles.push(handle);
        }
        
        // 收集所有分配的 ID
        let mut allocated_ids: Vec<usize> = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        
        // 验证：所有 ID 应该唯一且在有效范围内
        allocated_ids.sort();
        assert_eq!(allocated_ids.len(), 10);
        assert_eq!(allocated_ids, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(mask.count(), 10);
    }
    
    #[test]
    fn test_concurrent_allocate_and_free() {
        let mask = Arc::new(WorkerMask::new(5).unwrap());
        let mut handles = vec![];
        
        // 5 个线程释放，5 个线程分配
        for i in 0..5 {
            let mask_clone = Arc::clone(&mask);
            let handle = thread::spawn(move || {
                mask_clone.free(i).unwrap();
            });
            handles.push(handle);
        }
        
        for _ in 0..5 {
            let mask_clone = Arc::clone(&mask);
            let handle = thread::spawn(move || {
                mask_clone.allocate().unwrap();
            });
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        // 最终应该还是 5 个已分配
        assert_eq!(mask.count(), 5);
    }
    
    #[test]
    fn test_allocate_until_full() {
        let mask = WorkerMask::new(0).unwrap();
        
        // 分配所有槽位
        for _ in 0..MAX_WORKERS {
            assert!(mask.allocate().is_ok());
        }
        
        assert!(mask.is_full());
        assert_eq!(mask.count(), MAX_WORKERS);
        
        // 下一次分配应该失败
        assert!(mask.allocate().is_err());
    }
    
    #[test]
    fn test_debug_format() {
        let mask = WorkerMask::new(3).unwrap();
        let debug_str = format!("{:?}", mask);
        assert!(debug_str.contains("WorkerMask"));
        assert!(debug_str.contains("count: 3"));
    }
    
    #[test]
    fn test_complex_pattern() {
        let mask = WorkerMask::new(8).unwrap();
        
        // 初始掩码: 0b11111111 (前8位都是1)
        assert_eq!(mask.raw_mask() & 0xFF, 0b11111111);
        
        // 创建一个复杂的分配模式：释放奇数位
        mask.free(1).unwrap();
        mask.free(3).unwrap();
        mask.free(5).unwrap();
        mask.free(7).unwrap();
        
        // 掩码应该是: 0b01010101 (0x55) - 偶数位已占用，奇数位空闲
        // bit 0,2,4,6 = 1 (已占用)
        // bit 1,3,5,7 = 0 (空闲)
        assert_eq!(mask.raw_mask() & 0xFF, 0b01010101);
        assert_eq!(mask.count(), 4);
        
        // 按顺序重新分配（应该先填充奇数位）
        assert_eq!(mask.allocate().unwrap(), 1);
        assert_eq!(mask.allocate().unwrap(), 3);
        assert_eq!(mask.allocate().unwrap(), 5);
        assert_eq!(mask.allocate().unwrap(), 7);
        assert_eq!(mask.allocate().unwrap(), 8); // 新位置
        
        assert_eq!(mask.count(), 9);
    }
    
    #[test]
    fn test_edge_case_bit_31() {
        let mask = WorkerMask::new(31).unwrap();
        
        // 只剩最后一个槽位
        assert_eq!(mask.count(), 31);
        assert_eq!(mask.allocate().unwrap(), 31);
        
        // 现在应该满了
        assert!(mask.is_full());
        assert!(mask.allocate().is_err());
        
        // 释放最后一位
        mask.free(31).unwrap();
        assert_eq!(mask.allocate().unwrap(), 31);
    }
}

