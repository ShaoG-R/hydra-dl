//! 高性能无锁环形缓冲区
//!
//! 基于 portable_atomic 的 AtomicU128 实现的环形缓冲区，
//! 用于存储采样点数据，支持多线程并发写入和读取。

use portable_atomic::AtomicU128;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

/// 采样点数据
///
/// 使用 128 位原子类型存储时间戳和字节数，确保原子化读写。
///
/// # 数据布局
///
/// - 高 64 位：时间戳（纳秒）
/// - 低 64 位：字节数
#[derive(Debug)]
pub struct Sample {
    /// 打包的数据：高 64 位为时间戳，低 64 位为字节数
    data: AtomicU128,
}

impl Sample {
    /// 创建新的采样点（初始值为 0）
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            data: AtomicU128::new(0),
        }
    }

    /// 读取采样点数据（原子化读取）
    ///
    /// # Returns
    ///
    /// `Option<(时间戳纳秒, 字节数)>`，如果时间戳为 0 则返回 None
    #[inline]
    pub(crate) fn read(&self) -> Option<(u64, u64)> {
        let packed = self.data.load(Ordering::Acquire);

        // 解包：高 64 位是时间戳，低 64 位是字节数
        let timestamp_ns = (packed >> 64) as u64;
        let bytes = packed as u64;

        if timestamp_ns == 0 {
            return None;
        }

        Some((timestamp_ns, bytes))
    }

    /// 写入采样点数据（原子化写入）
    #[inline]
    pub(crate) fn write(&self, timestamp_ns: u64, bytes: u64) {
        // 打包：高 64 位是时间戳，低 64 位是字节数
        let packed = ((timestamp_ns as u128) << 64) | (bytes as u128);
        self.data.store(packed, Ordering::Release);
    }
}

/// 存储后端 - 栈或堆
///
/// 参考 smallvec 设计，使用栈存储优化小容量场景
#[derive(Debug)]
enum Storage<const N: usize> {
    /// 栈存储（容量 ≤ N）
    Stack([MaybeUninit<Sample>; N]),
    
    /// 堆存储（容量 > N）
    Heap(Box<[Sample]>),
}

/// 无锁环形缓冲区
///
/// 用于存储采样点的固定大小环形缓冲区，支持多线程并发读写。
/// 参考 smallring 的设计，使用 2 的幂次容量以优化取模运算。
///
/// # 核心特性
///
/// - **固定大小**：初始化时确定大小（向上取整到 2 的幂次）
/// - **自动覆盖**：缓冲区满时自动覆盖最旧的数据
/// - **完全无锁**：使用原子操作实现，支持高并发场景
/// - **Send + Sync**：线程安全，可在多线程间共享
///
/// # 工作原理
///
/// 使用单调递增的写入索引，通过位掩码映射到实际的环形缓冲区位置：
/// ```text
/// physical_index = write_index & mask
/// ```
///
/// 这种设计避免了索引回绕的复杂性，同时保持了 O(1) 的写入性能。
#[derive(Debug)]
pub(crate) struct RingBuffer<const N: usize = 32> {
    /// 采样点缓冲区（栈或堆）
    storage: Storage<N>,
    
    /// 实际容量（2 的幂次）
    capacity: usize,
    
    /// 掩码，用于快速取模运算（capacity - 1）
    mask: usize,
    
    /// 写入索引（单调递增）
    write_idx: AtomicU64,
    
    /// 已写入的样本总数（可能大于缓冲区大小）
    samples_written: AtomicU64,
}

impl<const N: usize> RingBuffer<N> {
    /// 创建指定容量的环形缓冲区
    ///
    /// 容量会向上取整到最接近的 2 的幂次，以优化取模运算。
    /// 当容量 ≤ N 时使用栈存储，否则使用堆存储。
    ///
    /// # Arguments
    ///
    /// * `capacity` - 期望的缓冲区容量
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let buffer = RingBuffer::<32>::new(100);
    /// // 实际容量为 128 (2^7)，使用堆存储
    /// assert_eq!(buffer.capacity(), 128);
    /// ```
    pub(crate) fn new(capacity: usize) -> Self {
        // 向上取整到 2 的幂次
        let actual_capacity = capacity.next_power_of_two();
        let mask = actual_capacity - 1;

        let storage = if actual_capacity <= N {
            // 使用栈存储
            // SAFETY: MaybeUninit 不需要初始化，我们立即初始化每个元素
            let mut stack_array: [MaybeUninit<Sample>; N] = unsafe {
                MaybeUninit::uninit().assume_init()
            };
            
            // 初始化实际使用的元素
            for i in 0..actual_capacity {
                stack_array[i] = MaybeUninit::new(Sample::new());
            }
            
            Storage::Stack(stack_array)
        } else {
            // 使用堆存储
            let mut samples = Vec::with_capacity(actual_capacity);
            for _ in 0..actual_capacity {
                samples.push(Sample::new());
            }
            Storage::Heap(samples.into_boxed_slice())
        };

        Self {
            storage,
            capacity: actual_capacity,
            mask,
            write_idx: AtomicU64::new(0),
            samples_written: AtomicU64::new(0),
        }
    }

    /// 获取缓冲区容量
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// 获取已写入的样本总数
    ///
    /// 注意：这个值可能大于缓冲区容量，表示总共写入的样本数（包括被覆盖的）
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn total_written(&self) -> u64 {
        self.samples_written.load(Ordering::Relaxed)
    }

    /// 获取指定索引的采样点引用（内部方法）
    #[inline]
    fn get_sample(&self, index: usize) -> &Sample {
        debug_assert!(index < self.capacity);
        match &self.storage {
            Storage::Stack(arr) => unsafe {
                // SAFETY: index < capacity，且已初始化
                arr[index].assume_init_ref()
            },
            Storage::Heap(boxed) => &boxed[index],
        }
    }

    /// 写入采样点（无锁并发写入）
    ///
    /// 多个线程可以并发调用此方法，每个线程获得唯一的写入位置。
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - 时间戳（纳秒）
    /// * `bytes` - 字节数
    #[inline]
    pub(crate) fn push(&self, timestamp_ns: u64, bytes: u64) {
        // 原子获取写入位置（单调递增）
        let index = self.write_idx.fetch_add(1, Ordering::Relaxed);

        // 使用位掩码快速映射到环形缓冲区
        let slot_index = (index as usize) & self.mask;
        let slot = self.get_sample(slot_index);

        // 原子化写入采样点
        slot.write(timestamp_ns, bytes);

        // 更新已写入样本数
        self.samples_written.fetch_add(1, Ordering::Relaxed);
    }

    /// 读取所有有效采样点
    ///
    /// 返回缓冲区中所有有效的采样点（时间戳不为 0），按时间排序。
    ///
    /// # Returns
    ///
    /// `Vec<(时间戳纳秒, 字节数)>`
    pub(crate) fn read_all(&self) -> Vec<(u64, u64)> {
        let total_written = self.samples_written.load(Ordering::Relaxed);
        let samples_to_read = total_written.min(self.capacity as u64);
        let mut samples = Vec::with_capacity(samples_to_read as usize);

        // 只读取实际写入的样本
        for i in 0..samples_to_read {
            let index = (i as usize) & self.mask;
            let sample = self.get_sample(index);
            if let Some((timestamp_ns, bytes)) = sample.read() {
                samples.push((timestamp_ns, bytes));
            }
        }

        // 按时间戳排序（从旧到新）
        samples.sort_unstable_by_key(|&(ts, _)| ts);

        samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capacity_rounding() {
        // 测试容量向上取整到 2 的幂次
        assert_eq!(RingBuffer::<32>::new(5).capacity(), 8);
        assert_eq!(RingBuffer::<32>::new(8).capacity(), 8);
        assert_eq!(RingBuffer::<32>::new(9).capacity(), 16);
        assert_eq!(RingBuffer::<32>::new(100).capacity(), 128);
        assert_eq!(RingBuffer::<32>::new(1000).capacity(), 1024);
    }

    #[test]
    fn test_basic_push_and_read() {
        let buffer = RingBuffer::<32>::new(10);

        buffer.push(1_000_000_000, 1024);
        buffer.push(2_000_000_000, 2048);

        let samples = buffer.read_all();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], (1_000_000_000, 1024));
        assert_eq!(samples[1], (2_000_000_000, 2048));
    }

    #[test]
    fn test_wrapping() {
        let buffer = RingBuffer::<32>::new(4); // 实际容量为 4

        // 写入超过容量的数据
        for i in 0..10 {
            buffer.push((i + 1) * 1_000_000_000, (i + 1) * 1024);
        }

        let samples = buffer.read_all();
        
        // 采样点数量不应超过容量
        assert!(samples.len() <= buffer.capacity());
        
        // 总写入数应该是 10
        assert_eq!(buffer.total_written(), 10);
    }

    #[test]
    fn test_sorted_output() {
        let buffer = RingBuffer::<32>::new(10);

        // 乱序写入
        buffer.push(3_000_000_000, 3072);
        buffer.push(1_000_000_000, 1024);
        buffer.push(5_000_000_000, 5120);
        buffer.push(2_000_000_000, 2048);

        let samples = buffer.read_all();

        // 验证排序
        for i in 1..samples.len() {
            assert!(samples[i].0 > samples[i - 1].0);
        }
    }

    #[test]
    fn test_concurrent_push() {
        use std::sync::Arc;
        use std::thread;

        let buffer = Arc::new(RingBuffer::<32>::new(100));
        let mut handles = vec![];

        // 多线程并发写入
        for thread_id in 0..4 {
            let buffer_clone = Arc::clone(&buffer);
            let handle = thread::spawn(move || {
                for i in 0..25 {
                    let timestamp = (thread_id * 25 + i + 1) * 1_000_000;
                    let bytes = (thread_id * 25 + i + 1) * 1024;
                    buffer_clone.push(timestamp, bytes);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(buffer.total_written(), 100);
        
        let samples = buffer.read_all();
        assert_eq!(samples.len(), 100);
    }

    

    #[test]
    fn test_stack_storage() {
        // 测试小容量使用栈存储（容量 ≤ 32）
        let buffer: RingBuffer<32> = RingBuffer::<32>::new(16);
        assert_eq!(buffer.capacity(), 16);
        
        // 验证栈存储的功能
        buffer.push(1_000_000_000, 1024);
        buffer.push(2_000_000_000, 2048);
        buffer.push(3_000_000_000, 3072);
        
        let samples = buffer.read_all();
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0], (1_000_000_000, 1024));
        assert_eq!(samples[1], (2_000_000_000, 2048));
        assert_eq!(samples[2], (3_000_000_000, 3072));
        
        // 验证存储类型
        assert!(matches!(buffer.storage, Storage::Stack(_)));
    }

    #[test]
    fn test_heap_storage() {
        // 测试大容量使用堆存储（容量 > 32）
        let buffer: RingBuffer<32> = RingBuffer::new(64);
        assert_eq!(buffer.capacity(), 64);
        
        // 验证堆存储的功能
        for i in 0..50 {
            buffer.push((i + 1) * 1_000_000, (i + 1) * 100);
        }
        
        let samples = buffer.read_all();
        assert_eq!(samples.len(), 50);
        assert_eq!(buffer.total_written(), 50);
        
        // 验证存储类型
        assert!(matches!(buffer.storage, Storage::Heap(_)));
    }

    #[test]
    fn test_stack_heap_threshold() {
        // 测试栈/堆切换的临界点
        
        // 容量 = 32 时应该使用栈存储
        let stack_buffer: RingBuffer<32> = RingBuffer::new(32);
        assert_eq!(stack_buffer.capacity(), 32);
        assert!(matches!(stack_buffer.storage, Storage::Stack(_)));
        
        // 容量 = 64 时应该使用堆存储
        let heap_buffer: RingBuffer<32> = RingBuffer::new(64);
        assert_eq!(heap_buffer.capacity(), 64);
        assert!(matches!(heap_buffer.storage, Storage::Heap(_)));
    }

    #[test]
    fn test_custom_stack_size() {
        // 测试自定义栈大小阈值
        
        // 使用更大的栈阈值（64）
        let buffer: RingBuffer<64> = RingBuffer::new(48);
        assert_eq!(buffer.capacity(), 64); // 向上取整到 2 的幂次
        assert!(matches!(buffer.storage, Storage::Stack(_)));
        
        // 超过栈阈值使用堆
        let buffer: RingBuffer<64> = RingBuffer::new(128);
        assert_eq!(buffer.capacity(), 128);
        assert!(matches!(buffer.storage, Storage::Heap(_)));
    }

    #[test]
    fn test_stack_storage_wrapping() {
        // 测试栈存储的环形覆盖功能
        let buffer: RingBuffer<32> = RingBuffer::new(8);
        
        // 写入超过容量的数据
        for i in 0..20 {
            buffer.push((i + 1) * 1_000_000, (i + 1) * 100);
        }
        
        let samples = buffer.read_all();
        assert!(samples.len() <= buffer.capacity());
        assert_eq!(buffer.total_written(), 20);
        assert!(matches!(buffer.storage, Storage::Stack(_)));
    }

    #[test]
    fn test_small_capacity_with_default_threshold() {
        // 测试默认阈值（N = 32）的行为
        
        // 使用默认泛型参数
        let buffer = RingBuffer::<32>::new(5); // 实际容量为 8
        assert_eq!(buffer.capacity(), 8);
        
        buffer.push(1_000_000_000, 512);
        buffer.push(2_000_000_000, 1024);
        
        let samples = buffer.read_all();
        assert_eq!(samples.len(), 2);
        
        // 验证默认使用栈存储
        assert!(matches!(buffer.storage, Storage::Stack(_)));
    }
}
