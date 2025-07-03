import pytest
import tempfile
import os
import shutil
from typing import Generator, List
import threading
import time

@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for test files."""
    test_dir = tempfile.mkdtemp()
    yield test_dir
    shutil.rmtree(test_dir)

@pytest.fixture
def mock_process_ids() -> List[int]:
    """Return a list of mock process IDs."""
    return [1001, 1002, 1003]

@pytest.fixture
def mock_thread_ids() -> List[int]:
    """Return a list of mock thread IDs."""
    return [101, 102, 103]

@pytest.fixture
def mock_heartbeat_data():
    """Return mock heartbeat data for process testing."""
    return {
        1001: time.time(),
        1002: time.time(),
        1003: time.time()
    }

@pytest.fixture
def mock_system_state():
    """Return mock system state data."""
    return {
        "active_tasks": ["task1", "task2"],
        "buffer_usage": {1001: 2, 1002: 3},
        "process_health": {1001: "alive", 1002: "alive"},
        "file_write_progress": {"file1.txt": 0.5}
    }

class MockThread(threading.Thread):
    """Mock thread class for testing."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exception = None
        self._return = None

    def run(self):
        try:
            if self._target:
                self._return = self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise self.exception
        return self._return

@pytest.fixture
def mock_thread():
    """Return a mock thread class for testing."""
    return MockThread 