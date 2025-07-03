import os
import time
import threading
import weakref
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
import psutil
import json
from multiprocessing import shared_memory, Lock, Value, Array
import numpy as np


@dataclass
class BufferInfo:
    """Information about a shared memory buffer."""
    buffer_id: str
    size_bytes: int
    created_at: float
    last_used: float
    access_count: int = 0
    is_allocated: bool = False
    process_id: Optional[int] = None
    task_id: Optional[str] = None
    reference_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BufferAllocation:
    """Represents an allocated buffer."""
    buffer_id: str
    shared_memory: shared_memory.SharedMemory
    size_bytes: int
    allocated_at: float
    process_id: int
    task_id: str


class BufferManager:
    """
    Manages a pool of shared memory buffers for multiprocessing pipelines.
    Provides thread-safe and process-safe buffer allocation and release.
    Tracks buffer usage and stats for real-time monitoring.
    """
    num_buffers: int
    buffer_shape: Tuple[int, ...]
    dtype: Any
    buffer_size: int
    lock: Lock
    thread_lock: threading.Lock
    buffers: List[shared_memory.SharedMemory]
    buffer_status: Dict[int, str]
    buffer_ids: List[int]
    usage_history: List[Tuple[float, int, int, int]]

    def __init__(self, num_buffers: int, buffer_shape: Tuple[int, ...], dtype: Any = np.float32) -> None:
        """
        Args:
            num_buffers: Number of shared memory buffers to pre-allocate.
            buffer_shape: Shape of each buffer (e.g., (N, M)).
            dtype: Data type of the buffer (default: np.float32).
        """
        self.num_buffers = num_buffers
        self.buffer_shape = buffer_shape
        self.dtype = dtype
        self.buffer_size = int(np.prod(buffer_shape)) * np.dtype(dtype).itemsize
        self.lock = Lock()  # Process-safe lock
        self.thread_lock = threading.Lock()  # Thread-safe lock
        self.buffers: List[shared_memory.SharedMemory] = []
        self.buffer_status: Dict[int, str] = {}  # 'active' or 'free'
        self.buffer_ids: List[int] = list(range(num_buffers))
        self.usage_history: List[Tuple[float, int, int, int]] = []  # (timestamp, active_count, free_count, total_count)
        self._init_buffers()

    def _init_buffers(self) -> None:
        for i in range(self.num_buffers):
            shm = shared_memory.SharedMemory(create=True, size=self.buffer_size)
            self.buffers.append(shm)
            self.buffer_status[i] = 'free'

    def acquire_buffer(self) -> Optional[Tuple[int, np.ndarray]]:
        """
        Acquire a free buffer. Returns (buffer_id, numpy_array) or None if unavailable.
        Thread-safe and process-safe.
        """
        with self.lock:
            for buf_id in self.buffer_ids:
                if self.buffer_status[buf_id] == 'free':
                    self.buffer_status[buf_id] = 'active'
                    shm = self.buffers[buf_id]
                    arr = np.ndarray(self.buffer_shape, dtype=self.dtype, buffer=shm.buf)
                    self._record_usage()
                    return buf_id, arr
            return None  # No free buffer

    def release_buffer(self, buffer_id: int) -> None:
        """
        Release a buffer back to the pool.
        Thread-safe and process-safe.
        """
        with self.lock:
            if self.buffer_status.get(buffer_id) == 'active':
                self.buffer_status[buffer_id] = 'free'
                self._record_usage()

    def get_buffer(self, buffer_id: int) -> np.ndarray:
        """
        Get a numpy array view of a buffer by ID.
        """
        shm = self.buffers[buffer_id]
        return np.ndarray(self.buffer_shape, dtype=self.dtype, buffer=shm.buf)

    def get_stats(self) -> Dict[str, int]:
        """
        Returns current buffer stats.
        """
        with self.lock:
            active = sum(1 for status in self.buffer_status.values() if status == 'active')
            free = self.num_buffers - active
            return {
                'active_buffers': active,
                'free_buffers': free,
                'total_buffers': self.num_buffers
            }

    def get_usage_history(self, window_sec: int = 60) -> List[Tuple[float, int]]:
        """
        Returns buffer usage history for the last window_sec seconds.
        """
        cutoff = time.time() - window_sec
        return [(t, c) for t, c in self.usage_history if t >= cutoff]

    def get_full_usage_history(self, window_sec: int = 60) -> List[Tuple[float, int, int, int]]:
        """
        Returns buffer usage history as (timestamp, active, free, total) for the last window_sec seconds.
        """
        cutoff = time.time() - window_sec
        return [(t, a, f, tot) for t, a, f, tot in self.usage_history if t >= cutoff]

    def _record_usage(self) -> None:
        active = sum(1 for status in self.buffer_status.values() if status == 'active')
        free = self.num_buffers - active
        total = self.num_buffers
        self.usage_history.append((time.time(), active, free, total))
        # Keep history manageable
        if len(self.usage_history) > 1000:
            self.usage_history = self.usage_history[-1000:]

    def cleanup(self) -> None:
        """
        Release all shared memory resources.
        """
        for shm in self.buffers:
            try:
                shm.close()
                shm.unlink()
            except Exception:
                pass

    def __del__(self) -> None:
        self.cleanup()


class BufferPool:
    """A pool of reusable buffers with automatic sizing."""
    
    def __init__(self, initial_size: int = 5, max_size: int = 50, 
                 buffer_size_mb: int = 100, growth_factor: float = 1.5):
        self.initial_size = initial_size
        self.max_size = max_size
        self.buffer_size = buffer_size_mb * 1024 * 1024
        self.growth_factor = growth_factor
        
        self.buffer_manager = BufferManager(
            num_buffers=initial_size,
            buffer_shape=(initial_size,),
            dtype=np.float32
        )
        
        self.current_size = initial_size
        self.allocation_failures = 0
    
    def get_buffer(self, process_id: int, task_id: str) -> Optional[BufferAllocation]:
        """Get a buffer from the pool, growing if necessary."""
        allocation = self.buffer_manager.acquire_buffer()
        
        if allocation is None:
            self.allocation_failures += 1
            
            # Try to grow the pool
            if self.current_size < self.max_size:
                self._grow_pool()
                allocation = self.buffer_manager.acquire_buffer()
        
        return allocation
    
    def return_buffer(self, buffer_id: str, process_id: int):
        """Return a buffer to the pool."""
        self.buffer_manager.release_buffer(buffer_id)
    
    def _grow_pool(self):
        """Grow the buffer pool."""
        new_size = min(int(self.current_size * self.growth_factor), self.max_size)
        
        if new_size > self.current_size:
            print(f"Growing buffer pool from {self.current_size} to {new_size}")
            
            # Create new buffer manager with larger size
            old_manager = self.buffer_manager
            self.buffer_manager = BufferManager(
                num_buffers=new_size,
                buffer_shape=(new_size,),
                dtype=np.float32
            )
            
            # Transfer existing allocations
            for i in range(self.current_size):
                self.buffer_manager.buffers[i] = old_manager.buffers[i]
            
            self.current_size = new_size
            
            # Shutdown old manager
            old_manager.cleanup()
    
    def get_pool_stats(self) -> Dict:
        """Get pool statistics."""
        manager_stats = self.buffer_manager.get_stats()
        
        return {
            **manager_stats,
            "pool_current_size": self.current_size,
            "pool_max_size": self.max_size,
            "pool_initial_size": self.initial_size,
            "allocation_failures": self.allocation_failures,
            "growth_factor": self.growth_factor
        }
    
    def shutdown(self):
        """Shutdown the buffer pool."""
        self.buffer_manager.cleanup() 