import pytest
import os
import time
import json
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from file_writer.logger import ProcessLogger, ThreadLogger
from file_writer.dashboard_server import DashboardServer

@pytest.fixture
def process_logger():
    """Create a process logger instance."""
    return ProcessLogger(max_history=100)

@pytest.fixture
def dashboard_server(process_logger):
    """Create a dashboard server instance."""
    return DashboardServer(process_logger, host="127.0.0.1", port=8000)

@pytest.fixture
def test_client(dashboard_server):
    """Create a test client for the dashboard server."""
    return TestClient(dashboard_server.app)

def test_thread_activity_logging(process_logger):
    """Test thread activity logging."""
    thread_id = 101
    task_id = "test_task_1"
    
    # Log thread start
    process_logger.log_thread_start(thread_id, task_id)
    
    # Simulate some work
    time.sleep(0.1)
    
    # Log thread end
    process_logger.log_thread_end(thread_id, task_id)
    
    # Get thread stats
    stats = process_logger.get_thread_stats(thread_id)
    
    assert stats["thread_id"] == thread_id
    assert stats["total_activities"] == 1
    assert stats["total_duration"] > 0
    assert "idle_percentage" in stats

def test_process_activity_logging(process_logger):
    """Test process activity logging."""
    process_id = 1001
    process_name = "test_process"
    task_id = "test_task_1"
    
    # Log process start
    process_logger.log_process_start(process_id, process_name, task_id)
    
    # Log some metrics
    process_logger.log_buffer_usage(process_id, 5)
    process_logger.log_queue_size(process_id, 10)
    process_logger.log_throughput(process_id, 100.0)
    process_logger.log_compression_ratio(process_id, 0.7)
    process_logger.log_write_speed(process_id, 50.0)
    
    # Simulate work
    time.sleep(0.1)
    
    # Log process end
    process_logger.log_process_end(process_id, task_id)
    
    # Get process stats
    stats = process_logger.get_process_stats(process_id)
    
    assert stats["process_id"] == process_id
    assert stats["process_name"] == process_name
    assert stats["total_activities"] == 1
    assert stats["current_buffer_usage"] == 5
    assert stats["current_queue_size"] == 10
    assert stats["current_throughput"] == 100.0
    assert stats["current_compression_ratio"] == 0.7
    assert stats["current_write_speed"] == 50.0

def test_system_metrics_collection(process_logger):
    """Test system-wide metrics collection."""
    # Collect metrics
    metrics = process_logger.collect_system_metrics()
    
    assert metrics is not None
    assert metrics.timestamp > 0
    assert isinstance(metrics.total_cpu_usage, float)
    assert isinstance(metrics.total_memory_usage, float)
    assert isinstance(metrics.disk_read_bytes, int)
    assert isinstance(metrics.disk_write_bytes, int)
    assert isinstance(metrics.network_bytes_sent, int)
    assert isinstance(metrics.network_bytes_recv, int)

def test_error_logging(process_logger):
    """Test error logging functionality."""
    process_id = 1001
    
    # Log different types of messages
    process_logger.log_error("INFO", "Test info message", process_id)
    process_logger.log_error("WARNING", "Test warning message", process_id)
    process_logger.log_error("ERROR", "Test error message", process_id)
    
    # Get WebSocket update data
    update_data = process_logger.get_websocket_update_data()
    
    assert "error_logs" in update_data
    assert len(update_data["error_logs"]) == 3
    assert update_data["error_logs"][-1]["level"] == "ERROR"
    assert update_data["error_logs"][-1]["message"] == "Test error message"

def test_dashboard_api_endpoints(test_client):
    """Test dashboard API endpoints."""
    # Test stats endpoint
    response = test_client.get("/api/stats")
    assert response.status_code == 200
    stats = response.json()
    assert "timestamp" in stats
    
    # Test system state endpoint
    response = test_client.get("/api/system-state")
    assert response.status_code == 200
    state = response.json()
    assert isinstance(state, dict)
    
    # Test historical data endpoint
    response = test_client.get("/api/historical-data")
    assert response.status_code == 200
    historical = response.json()
    assert "timestamps" in historical
    assert "compression_ratios" in historical
    assert "throughput" in historical

def test_checkpoint_event_logging(process_logger, temp_dir):
    """Test checkpoint event logging."""
    # Log checkpoint event
    event_info = {
        "checkpoint_path": os.path.join(temp_dir, "test_checkpoint.json"),
        "version": "1.0",
        "timestamp": time.time()
    }
    process_logger.log_checkpoint_event("save", event_info)
    
    # Get WebSocket update data
    update_data = process_logger.get_websocket_update_data()
    
    assert "checkpoint_event" in update_data
    assert update_data["checkpoint_event"]["event"] == "save"
    assert update_data["checkpoint_event"]["info"] == event_info

def test_log_export(process_logger, temp_dir):
    """Test log export functionality."""
    process_id = 1001
    process_name = "test_process"
    
    # Generate some log data
    process_logger.log_process_start(process_id, process_name)
    process_logger.log_buffer_usage(process_id, 5)
    process_logger.log_error("INFO", "Test message")
    process_logger.log_process_end(process_id)
    
    # Export logs
    export_path = os.path.join(temp_dir, "test_logs.json")
    process_logger.export_process_logs(export_path)
    
    # Verify exported file
    assert os.path.exists(export_path)
    with open(export_path, 'r') as f:
        exported_data = json.load(f)
    
    assert "stats" in exported_data
    assert "process_activities" in exported_data
    assert "error_logs" in exported_data
    assert str(process_id) in exported_data["current_buffer_usage"]

@pytest.mark.asyncio
async def test_websocket_connection(dashboard_server):
    """Test WebSocket connection and updates."""
    with TestClient(dashboard_server.app) as client:
        with client.websocket_connect("/ws/status") as websocket:
            # Receive initial data
            data = websocket.receive_json()
            assert "timestamp" in data
            assert "pipeline_stats" in data
            assert "system_metrics" in data
            
            # Test that updates are received
            data = websocket.receive_json()
            assert "timestamp" in data
            assert data["timestamp"] > 0 