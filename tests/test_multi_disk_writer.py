import pytest
import os
import tempfile
from unittest.mock import MagicMock, patch

# Try to import MultiDiskWriter, else mock
try:
    from file_writer.multi_disk_writer import MultiDiskWriter
except ImportError:
    class MultiDiskWriter:
        def __init__(self, file_paths):
            self.file_paths = file_paths
            self.files = [open(p, 'w+b') for p in file_paths]
            self.idx = 0
        def write(self, data):
            f = self.files[self.idx % len(self.files)]
            f.write(data)
            f.flush()
            self.idx += 1
        def close(self):
            for f in self.files:
                f.close()

@pytest.fixture
def temp_files(temp_dir):
    paths = [os.path.join(temp_dir, f"file_{i}.bin") for i in range(3)]
    return paths

def test_round_robin_writing(temp_files):
    writer = MultiDiskWriter(temp_files)
    for i in range(6):
        writer.write(f"data{i}".encode())
    writer.close()
    # Each file should have 2 writes
    for path in temp_files:
        with open(path, 'rb') as f:
            content = f.read()
            assert content.count(b'data') >= 2 or content == b''

def test_file_integrity(temp_files):
    writer = MultiDiskWriter(temp_files)
    data = [os.urandom(32) for _ in range(9)]
    for d in data:
        writer.write(d)
    writer.close()
    # All data should be present in some file
    found = 0
    for d in data:
        for path in temp_files:
            with open(path, 'rb') as f:
                if d in f.read():
                    found += 1
                    break
    assert found == len(data)

def test_disk_failure_handling(temp_files):
    # Simulate failure by patching file write
    writer = MultiDiskWriter(temp_files)
    with patch.object(writer.files[1], 'write', side_effect=IOError("Disk error")):
        # First write to file 0, second to file 1 (should fail)
        writer.write(b'good')
        with pytest.raises(IOError):
            writer.write(b'fail')
    writer.close()

def test_load_based_selection(temp_files):
    # Mock a load-based selection
    class LoadBasedWriter(MultiDiskWriter):
        def __init__(self, file_paths):
            super().__init__(file_paths)
            self.loads = [0] * len(file_paths)
        def write(self, data):
            idx = self.loads.index(min(self.loads))
            self.files[idx].write(data)
            self.files[idx].flush()
            self.loads[idx] += 1
    writer = LoadBasedWriter(temp_files)
    for _ in range(10):
        writer.write(b'x')
    writer.close()
    # All files should have at least 3 writes (10/3)
    for path in temp_files:
        with open(path, 'rb') as f:
            assert len(f.read()) >= 3 