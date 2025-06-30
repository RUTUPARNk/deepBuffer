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
    """Manages shared memory buffers with reference counting and reuse capabilities."""
    
    def __init__(self, max_buffers: int = 20, buffer_size_mb: int = 100, 
                 enable_reuse: bool = True, cleanup_interval: float = 30.0):
        self.max_buffers = max_buffers
        self.buffer_size = buffer_size_mb * 1024 * 1024  # Convert to bytes
        self.enable_reuse = enable_reuse
        self.cleanup_interval = cleanup_interval
        
        # Buffer tracking
        self.buffers: Dict[str, BufferInfo] = {}
        self.allocated_buffers: Dict[str, BufferAllocation] = {}
        self.available_buffers: deque = deque()
        
        # Thread safety
        self.lock = threading.Lock()
        self.cleanup_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            "total_allocations": 0,
            "total_deallocations": 0,
            "buffer_reuses": 0,
            "memory_leaks_prevented": 0,
            "cleanup_cycles": 0
        }
        
        # Initialize buffer pool
        self._initialize_buffer_pool()
        
        # Start cleanup thread
        self.cleanup_running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self.cleanup_thread.start()
    
    def _initialize_buffer_pool(self):
        """Initialize the shared memory buffer pool."""
        print(f"Initializing buffer pool: {self.max_buffers} buffers of {self.buffer_size / (1024*1024):.1f}MB each")
        
        for i in range(self.max_buffers):
            buffer_id = f"buffer_{i}"
            
            # Create shared memory block
            try:
                shm = shared_memory.SharedMemory(create=True, size=self.buffer_size, name=buffer_id)
                
                # Create buffer info
                buffer_info = BufferInfo(
                    buffer_id=buffer_id,
                    size_bytes=self.buffer_size,
                    created_at=time.time(),
                    last_used=time.time(),
                    is_allocated=False
                )
                
                self.buffers[buffer_id] = buffer_info
                self.available_buffers.append(buffer_id)
                
                print(f"Created buffer: {buffer_id} ({self.buffer_size / (1024*1024):.1f}MB)")
                
            except Exception as e:
                print(f"Failed to create buffer {buffer_id}: {e}")
    
    def allocate_buffer(self, process_id: int, task_id: str, 
                       required_size: Optional[int] = None) -> Optional[BufferAllocation]:
        """Allocate a buffer for use."""
        with self.lock:
            # Check if we have available buffers
            if not self.available_buffers:
                if self.enable_reuse:
                    # Try to reclaim unused buffers
                    self._reclaim_unused_buffers()
                
                if not self.available_buffers:
                    print("No available buffers")
                    return None
            
            # Get buffer ID
            buffer_id = self.available_buffers.popleft()
            
            # Check if buffer exists
            if buffer_id not in self.buffers:
                print(f"Buffer {buffer_id} not found in buffer pool")
                return None
            
            # Get shared memory
            try:
                shm = shared_memory.SharedMemory(name=buffer_id)
            except Exception as e:
                print(f"Failed to access shared memory for buffer {buffer_id}: {e}")
                return None
            
            # Update buffer info
            buffer_info = self.buffers[buffer_id]
            buffer_info.is_allocated = True
            buffer_info.process_id = process_id
            buffer_info.task_id = task_id
            buffer_info.last_used = time.time()
            buffer_info.access_count += 1
            buffer_info.reference_count = 1
            
            # Create allocation
            allocation = BufferAllocation(
                buffer_id=buffer_id,
                shared_memory=shm,
                size_bytes=self.buffer_size,
                allocated_at=time.time(),
                process_id=process_id,
                task_id=task_id
            )
            
            self.allocated_buffers[buffer_id] = allocation
            self.stats["total_allocations"] += 1
            
            print(f"Allocated buffer {buffer_id} to process {process_id} for task {task_id}")
            return allocation
    
    def release_buffer(self, buffer_id: str, process_id: int):
        """Release a buffer back to the pool."""
        with self.lock:
            if buffer_id not in self.allocated_buffers:
                print(f"Buffer {buffer_id} not allocated")
                return
            
            allocation = self.allocated_buffers[buffer_id]
            
            # Verify ownership
            if allocation.process_id != process_id:
                print(f"Process {process_id} cannot release buffer {buffer_id} owned by process {allocation.process_id}")
                return
            
            # Close shared memory
            try:
                allocation.shared_memory.close()
            except Exception as e:
                print(f"Error closing shared memory for buffer {buffer_id}: {e}")
            
            # Update buffer info
            buffer_info = self.buffers[buffer_id]
            buffer_info.is_allocated = False
            buffer_info.process_id = None
            buffer_info.task_id = None
            buffer_info.reference_count = 0
            
            # Return to available pool
            if self.enable_reuse:
                self.available_buffers.append(buffer_id)
                self.stats["buffer_reuses"] += 1
                print(f"Buffer {buffer_id} returned to pool for reuse")
            else:
                # Mark for cleanup
                buffer_info.reference_count = -1  # Mark as unreferenced
            
            # Remove from allocated buffers
            del self.allocated_buffers[buffer_id]
            self.stats["total_deallocations"] += 1
    
    def increment_reference(self, buffer_id: str):
        """Increment reference count for a buffer."""
        with self.lock:
            if buffer_id in self.buffers:
                self.buffers[buffer_id].reference_count += 1
                self.buffers[buffer_id].last_used = time.time()
    
    def decrement_reference(self, buffer_id: str):
        """Decrement reference count for a buffer."""
        with self.lock:
            if buffer_id in self.buffers:
                buffer_info = self.buffers[buffer_id]
                buffer_info.reference_count = max(0, buffer_info.reference_count - 1)
                
                # If reference count reaches 0 and buffer is not allocated, mark for cleanup
                if buffer_info.reference_count == 0 and not buffer_info.is_allocated:
                    buffer_info.reference_count = -1  # Mark as unreferenced
    
    def _reclaim_unused_buffers(self):
        """Reclaim buffers that are no longer in use."""
        current_time = time.time()
        reclaimed_count = 0
        
        # Find buffers that haven't been used recently
        for buffer_id, buffer_info in self.buffers.items():
            if (buffer_info.is_allocated and 
                buffer_info.reference_count == 0 and
                current_time - buffer_info.last_used > 60.0):  # 60 seconds timeout
                
                # Force release
                if buffer_id in self.allocated_buffers:
                    allocation = self.allocated_buffers[buffer_id]
                    try:
                        allocation.shared_memory.close()
                    except:
                        pass
                    
                    del self.allocated_buffers[buffer_id]
                    buffer_info.is_allocated = False
                    buffer_info.process_id = None
                    buffer_info.task_id = None
                    
                    self.available_buffers.append(buffer_id)
                    reclaimed_count += 1
                    self.stats["memory_leaks_prevented"] += 1
        
        if reclaimed_count > 0:
            print(f"Reclaimed {reclaimed_count} unused buffers")
    
    def _cleanup_worker(self):
        """Background worker for periodic cleanup."""
        while self.cleanup_running:
            try:
                time.sleep(self.cleanup_interval)
                self._perform_cleanup()
            except Exception as e:
                print(f"Error in cleanup worker: {e}")
    
    def _perform_cleanup(self):
        """Perform periodic cleanup of unreferenced buffers."""
        with self.cleanup_lock:
            current_time = time.time()
            cleanup_count = 0
            
            # Find buffers marked for cleanup
            buffers_to_cleanup = []
            for buffer_id, buffer_info in self.buffers.items():
                if buffer_info.reference_count == -1:
                    buffers_to_cleanup.append(buffer_id)
            
            # Clean up buffers
            for buffer_id in buffers_to_cleanup:
                try:
                    # Try to unlink shared memory
                    shm = shared_memory.SharedMemory(name=buffer_id)
                    shm.close()
                    shm.unlink()
                    
                    # Remove from tracking
                    del self.buffers[buffer_id]
                    cleanup_count += 1
                    
                except Exception as e:
                    print(f"Error cleaning up buffer {buffer_id}: {e}")
            
            if cleanup_count > 0:
                print(f"Cleaned up {cleanup_count} unreferenced buffers")
                self.stats["cleanup_cycles"] += 1
    
    def get_buffer_info(self, buffer_id: str) -> Optional[BufferInfo]:
        """Get information about a specific buffer."""
        with self.lock:
            return self.buffers.get(buffer_id)
    
    def get_all_buffer_info(self) -> Dict[str, BufferInfo]:
        """Get information about all buffers."""
        with self.lock:
            return self.buffers.copy()
    
    def get_manager_stats(self) -> Dict:
        """Get comprehensive manager statistics."""
        with self.lock:
            total_buffers = len(self.buffers)
            allocated_buffers = sum(1 for info in self.buffers.values() if info.is_allocated)
            available_buffers = len(self.available_buffers)
            
            # Calculate memory usage
            total_memory_mb = total_buffers * self.buffer_size / (1024 * 1024)
            allocated_memory_mb = allocated_buffers * self.buffer_size / (1024 * 1024)
            
            # Calculate buffer utilization
            utilization_percent = (allocated_buffers / total_buffers * 100) if total_buffers > 0 else 0
            
            return {
                "total_buffers": total_buffers,
                "allocated_buffers": allocated_buffers,
                "available_buffers": available_buffers,
                "total_memory_mb": total_memory_mb,
                "allocated_memory_mb": allocated_memory_mb,
                "utilization_percent": utilization_percent,
                "buffer_size_mb": self.buffer_size / (1024 * 1024),
                "enable_reuse": self.enable_reuse,
                "cleanup_interval": self.cleanup_interval,
                "stats": self.stats.copy()
            }
    
    def get_process_buffers(self, process_id: int) -> List[str]:
        """Get all buffers allocated to a specific process."""
        with self.lock:
            return [
                buffer_id for buffer_id, info in self.buffers.items()
                if info.process_id == process_id and info.is_allocated
            ]
    
    def get_task_buffers(self, task_id: str) -> List[str]:
        """Get all buffers associated with a specific task."""
        with self.lock:
            return [
                buffer_id for buffer_id, info in self.buffers.items()
                if info.task_id == task_id and info.is_allocated
            ]
    
    def set_buffer_metadata(self, buffer_id: str, metadata: Dict[str, Any]):
        """Set metadata for a buffer."""
        with self.lock:
            if buffer_id in self.buffers:
                self.buffers[buffer_id].metadata.update(metadata)
    
    def get_buffer_metadata(self, buffer_id: str) -> Dict[str, Any]:
        """Get metadata for a buffer."""
        with self.lock:
            if buffer_id in self.buffers:
                return self.buffers[buffer_id].metadata.copy()
            return {}
    
    def export_buffer_state(self) -> Dict:
        """Export current buffer state for persistence."""
        with self.lock:
            return {
                "buffers": {
                    buffer_id: {
                        "size_bytes": info.size_bytes,
                        "created_at": info.created_at,
                        "last_used": info.last_used,
                        "access_count": info.access_count,
                        "is_allocated": info.is_allocated,
                        "process_id": info.process_id,
                        "task_id": info.task_id,
                        "reference_count": info.reference_count,
                        "metadata": info.metadata
                    }
                    for buffer_id, info in self.buffers.items()
                },
                "stats": self.stats.copy(),
                "export_timestamp": time.time()
            }
    
    def import_buffer_state(self, state: Dict):
        """Import buffer state from persistence."""
        with self.lock:
            if "buffers" in state:
                for buffer_id, info_data in state["buffers"].items():
                    if buffer_id not in self.buffers:
                        # Create buffer info if it doesn't exist
                        buffer_info = BufferInfo(
                            buffer_id=buffer_id,
                            size_bytes=info_data["size_bytes"],
                            created_at=info_data["created_at"],
                            last_used=info_data["last_used"],
                            access_count=info_data["access_count"],
                            is_allocated=info_data["is_allocated"],
                            process_id=info_data["process_id"],
                            task_id=info_data["task_id"],
                            reference_count=info_data["reference_count"],
                            metadata=info_data.get("metadata", {})
                        )
                        self.buffers[buffer_id] = buffer_info
            
            if "stats" in state:
                self.stats.update(state["stats"])
    
    def shutdown(self):
        """Shutdown the buffer manager and cleanup all resources."""
        print("Shutting down Buffer Manager...")
        
        self.cleanup_running = False
        
        # Wait for cleanup thread
        if self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5)
        
        # Cleanup all buffers
        with self.lock:
            for buffer_id, buffer_info in self.buffers.items():
                try:
                    # Close shared memory if allocated
                    if buffer_info.is_allocated and buffer_id in self.allocated_buffers:
                        allocation = self.allocated_buffers[buffer_id]
                        allocation.shared_memory.close()
                    
                    # Unlink shared memory
                    shm = shared_memory.SharedMemory(name=buffer_id)
                    shm.close()
                    shm.unlink()
                    
                except Exception as e:
                    print(f"Error cleaning up buffer {buffer_id}: {e}")
        
        print("Buffer Manager shutdown complete")


class BufferPool:
    """A pool of reusable buffers with automatic sizing."""
    
    def __init__(self, initial_size: int = 5, max_size: int = 50, 
                 buffer_size_mb: int = 100, growth_factor: float = 1.5):
        self.initial_size = initial_size
        self.max_size = max_size
        self.buffer_size = buffer_size_mb * 1024 * 1024
        self.growth_factor = growth_factor
        
        self.buffer_manager = BufferManager(
            max_buffers=initial_size,
            buffer_size_mb=buffer_size_mb,
            enable_reuse=True
        )
        
        self.current_size = initial_size
        self.allocation_failures = 0
    
    def get_buffer(self, process_id: int, task_id: str) -> Optional[BufferAllocation]:
        """Get a buffer from the pool, growing if necessary."""
        allocation = self.buffer_manager.allocate_buffer(process_id, task_id)
        
        if allocation is None:
            self.allocation_failures += 1
            
            # Try to grow the pool
            if self.current_size < self.max_size:
                self._grow_pool()
                allocation = self.buffer_manager.allocate_buffer(process_id, task_id)
        
        return allocation
    
    def return_buffer(self, buffer_id: str, process_id: int):
        """Return a buffer to the pool."""
        self.buffer_manager.release_buffer(buffer_id, process_id)
    
    def _grow_pool(self):
        """Grow the buffer pool."""
        new_size = min(int(self.current_size * self.growth_factor), self.max_size)
        
        if new_size > self.current_size:
            print(f"Growing buffer pool from {self.current_size} to {new_size}")
            
            # Create new buffer manager with larger size
            old_manager = self.buffer_manager
            self.buffer_manager = BufferManager(
                max_buffers=new_size,
                buffer_size_mb=self.buffer_size // (1024 * 1024),
                enable_reuse=True
            )
            
            # Transfer existing allocations
            for buffer_id, allocation in old_manager.allocated_buffers.items():
                self.buffer_manager.allocated_buffers[buffer_id] = allocation
            
            self.current_size = new_size
            
            # Shutdown old manager
            old_manager.shutdown()
    
    def get_pool_stats(self) -> Dict:
        """Get pool statistics."""
        manager_stats = self.buffer_manager.get_manager_stats()
        
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
        self.buffer_manager.shutdown() 