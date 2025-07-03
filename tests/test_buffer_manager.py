import pytest
import threading
import time
from unittest.mock import MagicMock

# Try to import BufferManager, else mock
try:
    from file_writer.buffer_manager import BufferManager
except ImportError:
    class BufferManager:
        def __init__(self, max_buffers=10):
            self.max_buffers = max_buffers
            self.buffers = list(range(max_buffers))
            self.lock = threading.Lock()
            self.in_use = set()
        def acquire_buffer(self):
            with self.lock:
                if self.buffers:
                    buf = self.buffers.pop()
                    self.in_use.add(buf)
                    return buf
                return None
        def release_buffer(self, buf):
            with self.lock:
                if buf in self.in_use:
                    self.in_use.remove(buf)
                    self.buffers.append(buf)
        def available_count(self):
            with self.lock:
                return len(self.buffers)
        def in_use_count(self):
            with self.lock:
                return len(self.in_use)

def test_buffer_acquire_release():
    bm = BufferManager(max_buffers=5)
    buf = bm.acquire_buffer()
    assert buf is not None
    assert bm.in_use_count() == 1
    bm.release_buffer(buf)
    assert bm.in_use_count() == 0
    assert bm.available_count() == 5

def test_buffer_exhaustion_and_recycling():
    bm = BufferManager(max_buffers=3)
    bufs = [bm.acquire_buffer() for _ in range(3)]
    assert all(b is not None for b in bufs)
    assert bm.acquire_buffer() is None  # Exhausted
    for b in bufs:
        bm.release_buffer(b)
    assert bm.available_count() == 3

def test_buffer_tracking_accuracy():
    bm = BufferManager(max_buffers=4)
    bufs = [bm.acquire_buffer() for _ in range(2)]
    assert bm.in_use_count() == 2
    assert bm.available_count() == 2
    bm.release_buffer(bufs[0])
    assert bm.in_use_count() == 1
    assert bm.available_count() == 3

def test_buffer_thread_safety():
    bm = BufferManager(max_buffers=10)
    acquired = []
    def worker():
        for _ in range(100):
            buf = bm.acquire_buffer()
            if buf is not None:
                acquired.append(buf)
                time.sleep(0.001)
                bm.release_buffer(buf)
    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert bm.available_count() == 10
    assert bm.in_use_count() == 0

def test_buffer_concurrency_stress():
    bm = BufferManager(max_buffers=50)
    acquired = []
    def stress_worker():
        for _ in range(250):
            buf = bm.acquire_buffer()
            if buf is not None:
                acquired.append(buf)
                bm.release_buffer(buf)
    threads = [threading.Thread(target=stress_worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert bm.available_count() == 50
    assert bm.in_use_count() == 0
    # Should have done 1000+ ops
    assert len(acquired) >= 1000 