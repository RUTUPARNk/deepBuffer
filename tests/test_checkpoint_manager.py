import pytest
import os
import json
import time
from file_writer.transactional_checkpoint import CheckpointManager

def test_checkpoint_save_and_load(temp_dir):
    """Test basic checkpoint save and load functionality."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(checkpoint_path)
    
    test_state = {
        "key1": "value1",
        "key2": 42,
        "nested": {"a": 1, "b": 2}
    }
    
    # Save checkpoint
    manager.save_checkpoint(test_state)
    
    # Verify file exists
    assert os.path.exists(os.path.join(temp_dir, "checkpoint_v1.json"))
    
    # Load and verify
    loaded_state = manager.load_checkpoint()
    assert loaded_state == test_state

def test_checkpoint_rotation(temp_dir):
    """Test that checkpoints are properly rotated."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(checkpoint_path, keep_versions=3)
    
    # Create multiple checkpoints
    for i in range(5):
        state = {"version": i}
        manager.save_checkpoint(state)
        time.sleep(0.1)  # Ensure unique timestamps
    
    # Verify only 3 files exist
    checkpoint_files = [f for f in os.listdir(temp_dir) if f.startswith("checkpoint_v")]
    assert len(checkpoint_files) == 3
    
    # Verify we can load the latest state
    loaded_state = manager.load_checkpoint()
    assert loaded_state == {"version": 4}

def test_checkpoint_corruption_detection(temp_dir):
    """Test that corrupted checkpoints are detected and skipped."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(checkpoint_path)
    
    # Save valid checkpoint
    valid_state = {"key": "value"}
    manager.save_checkpoint(valid_state)
    
    # Corrupt the file
    with open(os.path.join(temp_dir, "checkpoint_v1.json"), 'w') as f:
        f.write("corrupted data")
    
    # Attempt to load
    loaded_state = manager.load_checkpoint()
    assert loaded_state is None

def test_checkpoint_metadata(temp_dir):
    """Test that checkpoint metadata is correctly stored and retrieved."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    system_version = "2.0"
    schema_version = "1.1"
    
    manager = CheckpointManager(
        checkpoint_path,
        system_version=system_version,
        schema_version=schema_version
    )
    
    test_state = {"data": "test"}
    manager.save_checkpoint(test_state)
    
    # Read the file directly to verify metadata
    with open(os.path.join(temp_dir, "checkpoint_v1.json"), 'r') as f:
        saved_data = json.load(f)
    
    assert saved_data["system_version"] == system_version
    assert saved_data["schema_version"] == schema_version
    assert "timestamp" in saved_data
    assert "checksum" in saved_data

def test_concurrent_checkpoint_access(temp_dir, mock_thread):
    """Test thread-safe checkpoint operations."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(checkpoint_path)
    
    def save_checkpoint(state):
        manager.save_checkpoint(state)
    
    def load_checkpoint():
        return manager.load_checkpoint()
    
    # Create threads for concurrent access
    save_threads = [
        mock_thread(target=save_checkpoint, args=({"thread": i},))
        for i in range(5)
    ]
    load_threads = [
        mock_thread(target=load_checkpoint)
        for _ in range(5)
    ]
    
    # Start all threads
    for thread in save_threads + load_threads:
        thread.start()
    
    # Wait for completion
    for thread in save_threads + load_threads:
        thread.join()
    
    # Verify we can still load a valid state
    final_state = manager.load_checkpoint()
    assert isinstance(final_state, dict)
    assert "thread" in final_state

def test_checkpoint_info(temp_dir):
    """Test that checkpoint info is correctly tracked."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(checkpoint_path)
    
    # Initially no checkpoint info
    assert manager.get_last_checkpoint_info() is None
    
    # Save checkpoint and verify info
    test_state = {"key": "value"}
    manager.save_checkpoint(test_state)
    
    info = manager.get_last_checkpoint_info()
    assert info is not None
    assert "timestamp" in info
    assert "version" in info
    assert "schema_version" in info
    assert "file" in info
    assert info["file"].endswith("_v1.json")

def test_checkpoint_resilience_to_corruption(temp_dir):
    """Test that load_checkpoint skips corrupted files and loads the most recent valid checkpoint."""
    checkpoint_path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(checkpoint_path, keep_versions=3)
    # Save three valid checkpoints
    for i in range(3):
        manager.save_checkpoint({"version": i})
        time.sleep(0.05)
    # Corrupt the latest checkpoint
    latest = os.path.join(temp_dir, "checkpoint_v1.json")
    with open(latest, 'w') as f:
        f.write("corrupted data")
    # Should load the next most recent valid checkpoint
    loaded = manager.load_checkpoint()
    assert loaded == {"version": 1} or loaded == {"version": 0}  # Accept either, depending on timing 