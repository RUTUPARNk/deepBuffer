import pytest
import threading
import time
from file_writer.tensor_buffer_manager import TensorBufferManager

@pytest.fixture
def tensor_manager():
    mgr = TensorBufferManager(num_buffers=8, tensor_shape=(128, 128), dtype=float)
    yield mgr
    mgr.cleanup()

def test_buffer_exhaustion_and_recycling(tensor_manager):
    """Test that all buffers can be acquired and released without leaks."""
    acquired = []
    for _ in range(tensor_manager.num_buffers):
        buf_id, arr = tensor_manager.acquire_tensor(timeout=1)
        acquired.append(buf_id)
    # All buffers should be in use
    state = tensor_manager.get_buffer_state()
    assert state['in_use'] == tensor_manager.num_buffers
    assert state['available'] == 0
    # Release all
    for buf_id in acquired:
        tensor_manager.release_tensor(buf_id)
    state = tensor_manager.get_buffer_state()
    assert state['in_use'] == 0
    assert state['available'] == tensor_manager.num_buffers

def test_exclusive_ownership_enforced(tensor_manager):
    """Test that only the owning thread can release a buffer."""
    buf_id, arr = tensor_manager.acquire_tensor()
    # Try to release from another thread
    def try_release():
        with pytest.raises(RuntimeError):
            tensor_manager.release_tensor(buf_id)
    t = threading.Thread(target=try_release)
    t.start()
    t.join()
    # Release from correct thread
    tensor_manager.release_tensor(buf_id)
    state = tensor_manager.get_buffer_state()
    assert state['in_use'] == 0

def test_stress_handoff(tensor_manager):
    """Stress test rapid acquire/release with many threads."""
    N = 1000
    errors = []
    def worker():
        try:
            buf_id, arr = tensor_manager.acquire_tensor(timeout=2)
            time.sleep(0.001)
            tensor_manager.release_tensor(buf_id)
        except Exception as e:
            errors.append(e)
    threads = [threading.Thread(target=worker) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    state = tensor_manager.get_buffer_state()
    assert state['in_use'] == 0
    assert state['available'] == tensor_manager.num_buffers

def test_shared_memory_cleanup():
    """Test that shared memory is cleaned up after manager deletion."""
    mgr = TensorBufferManager(num_buffers=2, tensor_shape=(32, 32), dtype=float)
    bufs = [shm.name for shm in mgr.buffers]
    mgr.cleanup()
    # After cleanup, shared memory segments should not exist
    for name in bufs:
        with pytest.raises(FileNotFoundError):
            shared_memory.SharedMemory(name=name) 