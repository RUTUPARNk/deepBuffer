import threading
import queue
import numpy as np
from multiprocessing import shared_memory
from typing import Optional, Tuple, Dict, Any, List
import time

class TensorBufferManager:
    """
    Manages a pool of pre-allocated NumPy tensors in shared memory.
    Ensures thread/process-safe, exclusive buffer ownership and atomic handoff.
    """
    def __init__(self, num_buffers: int, tensor_shape: Tuple[int, ...], dtype: np.dtype = np.float32):
        self.num_buffers = num_buffers
        self.tensor_shape = tensor_shape
        self.dtype = dtype
        self.buffer_size = int(np.prod(tensor_shape)) * np.dtype(dtype).itemsize
        self.lock = threading.Lock()
        self.available = queue.Queue(maxsize=num_buffers)
        self.in_use: Dict[int, str] = {}  # buffer_id -> owner (thread name)
        self.buffers: List[shared_memory.SharedMemory] = []
        self._init_buffers()

    def _init_buffers(self) -> None:
        for i in range(self.num_buffers):
            shm = shared_memory.SharedMemory(create=True, size=self.buffer_size)
            self.buffers.append(shm)
            self.available.put(i)

    def acquire_tensor(self, timeout: Optional[float] = None) -> Tuple[int, np.ndarray]:
        """
        Acquire a tensor buffer for exclusive use. Returns (buffer_id, numpy_array).
        Raises queue.Empty if no buffer is available within timeout.
        """
        buffer_id = self.available.get(timeout=timeout)
        with self.lock:
            self.in_use[buffer_id] = threading.current_thread().name
        shm = self.buffers[buffer_id]
        arr = np.ndarray(self.tensor_shape, dtype=self.dtype, buffer=shm.buf)
        return buffer_id, arr

    def release_tensor(self, buffer_id: int) -> None:
        """
        Release a tensor buffer back to the pool. Only the owning thread may release.
        """
        with self.lock:
            owner = self.in_use.get(buffer_id)
            if owner != threading.current_thread().name:
                raise RuntimeError(f"Thread {threading.current_thread().name} cannot release buffer {buffer_id} owned by {owner}")
            del self.in_use[buffer_id]
        self.available.put(buffer_id)

    def get_buffer_state(self) -> Dict[str, int]:
        """
        Returns the current state of the buffer pool.
        """
        with self.lock:
            return {
                "in_use": len(self.in_use),
                "available": self.available.qsize(),
                "total": self.num_buffers
            }

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

    def __del__(self):
        self.cleanup()

    def create_numpy_array(self, buffer_id: int, shape: Tuple[int, ...], dtype: str) -> np.ndarray:
        """
        Return a numpy array view for the given buffer_id, shape, and dtype.
        """
        shm = self.buffers[buffer_id]
        return np.ndarray(shape, dtype=dtype, buffer=shm.buf)

    def get_buffer_info(self) -> Dict:
        """
        Return buffer pool info for dashboard and stats.
        """
        return {
            "total_buffers": self.num_buffers,
            "available_buffers": self.available.qsize(),
            "buffer_size_mb": (self.buffer_size / (1024 * 1024)),
            "buffer_ids": list(range(self.num_buffers)),
            "metadata_count": 0
        }

    def acquire_batch(self, n: int, timeout: Optional[float] = None) -> List[Tuple[int, np.ndarray]]:
        """
        Atomically acquire a batch of n tensor buffers. Returns list of (buffer_id, numpy_array).
        Raises queue.Empty if not enough buffers are available within timeout.
        """
        batch = []
        start = time.time()
        for _ in range(n):
            remaining = None if timeout is None else max(0, timeout - (time.time() - start))
            buf_id, arr = self.acquire_tensor(timeout=remaining)
            batch.append((buf_id, arr))
        return batch

    def release_batch(self, buffer_ids: List[int]) -> None:
        """
        Release a batch of tensor buffers back to the pool.
        """
        for buf_id in buffer_ids:
            self.release_tensor(buf_id) 