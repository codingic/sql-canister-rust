//! Memory management module
//!
//! Provides configurable memory allocation for SQLite.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Memory statistics
#[derive(Debug, Default)]
pub struct MemoryStats {
    /// Total bytes allocated
    pub total_allocated: AtomicU64,
    /// Peak bytes allocated
    pub peak_allocated: AtomicU64,
    /// Number of allocations
    pub allocation_count: AtomicU64,
    /// Number of deallocations
    pub deallocation_count: AtomicU64,
}

impl MemoryStats {
    /// Create new memory statistics
    pub fn new() -> Self {
        MemoryStats::default()
    }

    /// Record an allocation
    pub fn record_alloc(&self, size: u64) {
        let total = self.total_allocated.fetch_add(size, Ordering::SeqCst) + size;
        self.allocation_count.fetch_add(1, Ordering::SeqCst);

        // Update peak
        loop {
            let current_peak = self.peak_allocated.load(Ordering::SeqCst);
            if total <= current_peak {
                break;
            }
            if self.peak_allocated.compare_exchange_weak(
                current_peak,
                total,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
            {
                break;
            }
        }
    }

    /// Record a deallocation
    pub fn record_dealloc(&self, size: u64) {
        self.total_allocated.fetch_sub(size, Ordering::SeqCst);
        self.deallocation_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Get current memory usage
    pub fn current_usage(&self) -> u64 {
        self.total_allocated.load(Ordering::SeqCst)
    }

    /// Get peak memory usage
    pub fn peak_usage(&self) -> u64 {
        self.peak_allocated.load(Ordering::SeqCst)
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.total_allocated.store(0, Ordering::SeqCst);
        self.peak_allocated.store(0, Ordering::SeqCst);
        self.allocation_count.store(0, Ordering::SeqCst);
        self.deallocation_count.store(0, Ordering::SeqCst);
    }
}

/// Global memory statistics
static MEMORY_STATS: MemoryStats = MemoryStats {
    total_allocated: AtomicU64::new(0),
    peak_allocated: AtomicU64::new(0),
    allocation_count: AtomicU64::new(0),
    deallocation_count: AtomicU64::new(0),
};

/// Get global memory statistics
pub fn memory_stats() -> &'static MemoryStats {
    &MEMORY_STATS
}

/// Get current memory usage
pub fn memory_used() -> u64 {
    MEMORY_STATS.current_usage()
}

/// Get peak memory usage
pub fn memory_highwater(reset: bool) -> u64 {
    let peak = MEMORY_STATS.peak_usage();
    if reset {
        MEMORY_STATS
            .peak_allocated
            .store(MEMORY_STATS.current_usage(), Ordering::SeqCst);
    }
    peak
}

/// Memory allocator configuration
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Soft heap limit
    pub soft_heap_limit: u64,
    /// Hard heap limit
    pub hard_heap_limit: u64,
    /// Enable statistics
    pub enable_stats: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        MemoryConfig {
            soft_heap_limit: u64::MAX,
            hard_heap_limit: u64::MAX,
            enable_stats: true,
        }
    }
}

/// Memory allocator
pub struct MemoryAllocator {
    config: MemoryConfig,
}

impl MemoryAllocator {
    /// Create a new memory allocator
    pub fn new(config: MemoryConfig) -> Self {
        MemoryAllocator { config }
    }

    /// Allocate memory
    pub fn alloc(&self, size: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::null_mut();
        }

        // Check limits
        if size as u64 > self.config.hard_heap_limit {
            return std::ptr::null_mut();
        }

        unsafe {
            let layout = Layout::from_size_align(size, 8).unwrap();
            let ptr = System.alloc(layout);

            if !ptr.is_null() && self.config.enable_stats {
                MEMORY_STATS.record_alloc(size as u64);
            }

            ptr
        }
    }

    /// Free memory
    pub fn free(&self, ptr: *mut u8, size: usize) {
        if ptr.is_null() || size == 0 {
            return;
        }

        unsafe {
            let layout = Layout::from_size_align(size, 8).unwrap();
            System.dealloc(ptr, layout);

            if self.config.enable_stats {
                MEMORY_STATS.record_dealloc(size as u64);
            }
        }
    }

    /// Reallocate memory
    pub fn realloc(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
        if ptr.is_null() {
            return self.alloc(new_size);
        }

        if new_size == 0 {
            self.free(ptr, old_size);
            return std::ptr::null_mut();
        }

        // Check limits
        if new_size as u64 > self.config.hard_heap_limit {
            return std::ptr::null_mut();
        }

        unsafe {
            let old_layout = Layout::from_size_align(old_size, 8).unwrap();
            let new_ptr = System.realloc(ptr, old_layout, new_size);

            if !new_ptr.is_null() && self.config.enable_stats {
                if old_size > 0 {
                    MEMORY_STATS.record_dealloc(old_size as u64);
                }
                MEMORY_STATS.record_alloc(new_size as u64);
            }

            new_ptr
        }
    }

    /// Set soft heap limit
    pub fn set_soft_heap_limit(&mut self, limit: u64) {
        self.config.soft_heap_limit = limit;
    }

    /// Set hard heap limit
    pub fn set_hard_heap_limit(&mut self, limit: u64) {
        self.config.hard_heap_limit = limit;
    }
}

impl Default for MemoryAllocator {
    fn default() -> Self {
        Self::new(MemoryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_stats() {
        let stats = MemoryStats::new();

        stats.record_alloc(100);
        assert_eq!(stats.current_usage(), 100);

        stats.record_alloc(50);
        assert_eq!(stats.current_usage(), 150);

        stats.record_dealloc(100);
        assert_eq!(stats.current_usage(), 50);

        assert_eq!(stats.peak_usage(), 150);
    }

    #[test]
    fn test_memory_used() {
        let _ = memory_used();
        let _ = memory_highwater(false);
    }

    #[test]
    fn test_memory_allocator() {
        let allocator = MemoryAllocator::default();

        let ptr = allocator.alloc(100);
        assert!(!ptr.is_null());

        allocator.free(ptr, 100);
    }

    #[test]
    fn test_memory_allocator_zero() {
        let allocator = MemoryAllocator::default();

        let ptr = allocator.alloc(0);
        assert!(ptr.is_null());
    }
}
