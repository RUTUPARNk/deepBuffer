import pytest
import os
import time
from unittest.mock import MagicMock, patch
from file_writer.process_supervisor import ProcessSupervisor
from file_writer.logger import ProcessLogger

@pytest.fixture
def mock_logger():
    """Create a mock process logger."""
    return MagicMock(spec=ProcessLogger)

def test_process_supervisor_init(temp_dir, mock_process_ids, mock_logger):
    """Test process supervisor initialization."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=1.0,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    
    assert supervisor.process_ids == mock_process_ids
    assert supervisor.heartbeat_interval == 1.0
    assert not supervisor.running
    assert isinstance(supervisor.heartbeats, dict)
    for pid in mock_process_ids:
        assert pid in supervisor.heartbeats

def test_process_heartbeat_monitoring(temp_dir, mock_process_ids, mock_logger):
    """Test heartbeat monitoring and failure detection."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    
    # Start supervisor
    supervisor.start()
    time.sleep(0.2)  # Let monitoring start
    
    # Update heartbeat for one process
    supervisor.update_heartbeat(mock_process_ids[0])
    
    # Wait for failure detection
    time.sleep(0.3)
    
    # Verify failure handling was called for other processes
    assert mock_logger.log_error.call_count >= 2  # Should log errors for 2 failed processes
    
    supervisor.stop()

def test_process_recovery(temp_dir, mock_process_ids, mock_logger):
    """Test process recovery after failure."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    
    # Mock checkpoint data
    test_state = {
        "active_tasks": ["task1"],
        "buffer_usage": {mock_process_ids[0]: 2},
        "process_health": {mock_process_ids[0]: "alive"}
    }
    supervisor.system_state = test_state.copy()
    supervisor.save_checkpoint()
    
    # Create new supervisor (simulating restart)
    new_supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    
    # Verify state was restored
    assert new_supervisor.system_state["active_tasks"] == test_state["active_tasks"]
    assert new_supervisor.system_state["buffer_usage"] == test_state["buffer_usage"]

def test_checkpoint_callback(temp_dir, mock_process_ids, mock_logger):
    """Test checkpoint callback functionality."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    callback_called = False
    
    def checkpoint_callback():
        nonlocal callback_called
        callback_called = True
    
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path,
        checkpoint_interval=0.2,
        checkpoint_callback=checkpoint_callback
    )
    
    supervisor.start()
    time.sleep(0.3)  # Wait for callback
    supervisor.stop()
    
    assert callback_called

@patch('psutil.Process')
def test_process_failure_handling(mock_process, temp_dir, mock_process_ids, mock_logger):
    """Test handling of process failures."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    
    # Mock process failure
    mock_process.side_effect = Exception("Process not found")
    
    # Simulate failure detection
    supervisor._handle_failure(mock_process_ids[0])
    
    # Verify failure was logged
    mock_logger.log_error.assert_called_with(
        'ERROR',
        f'Process {mock_process_ids[0]} unresponsive, attempting restart'
    )
    
    # Verify system state was updated
    assert supervisor.system_state["process_health"][mock_process_ids[0]] == "restarted"

def test_soft_shutdown(temp_dir, mock_process_ids, mock_logger):
    """Test soft shutdown functionality."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    
    supervisor.start()
    time.sleep(0.2)
    
    # Perform soft shutdown
    supervisor.soft_shutdown()
    
    # Verify final checkpoint was saved
    assert not supervisor.running
    mock_logger.log_error.assert_called_with('INFO', 'Supervisor initiating soft shutdown')

def test_process_crash_and_recovery(temp_dir, mock_process_ids, mock_logger):
    """Simulate process crash and validate automatic recovery from checkpoint."""
    checkpoint_path = os.path.join(temp_dir, "supervisor.json")
    supervisor = ProcessSupervisor(
        process_ids=mock_process_ids,
        heartbeat_interval=0.1,
        logger=mock_logger,
        checkpoint_path=checkpoint_path
    )
    # Save a known state
    test_state = {"active_tasks": ["crash_task"], "buffer_usage": {mock_process_ids[0]: 1}, "process_health": {mock_process_ids[0]: "alive"}}
    supervisor.system_state = test_state.copy()
    supervisor.save_checkpoint()
    # Simulate process crash by making heartbeat very old
    with supervisor.lock:
        supervisor.heartbeats[mock_process_ids[0]] = time.time() - 100
    # Call supervise loop once (simulate)
    supervisor._handle_failure(mock_process_ids[0])
    # After recovery, process should be marked as restarted and state restored
    assert supervisor.system_state["process_health"][mock_process_ids[0]] == "restarted"
    assert supervisor.system_state["active_tasks"] == ["crash_task"] 