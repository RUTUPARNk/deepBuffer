#!/usr/bin/env python3
"""
Test script for the Shared Memory Pipeline Dashboard

This script demonstrates all dashboard features including:
- Real-time monitoring
- System controls
- Performance metrics
- Error logging
- WebSocket updates
"""

import time
import threading
import random
import json
import requests
import websocket
from logger import ProcessLogger
from dashboard_server import create_dashboard_server


class DashboardTester:
    """Test class for dashboard functionality."""
    
    def __init__(self, dashboard_host="localhost", dashboard_port=8000):
        self.dashboard_host = dashboard_host
        self.dashboard_port = dashboard_port
        self.base_url = f"http://{dashboard_host}:{dashboard_port}"
        self.ws_url = f"ws://{dashboard_host}:{dashboard_port}/ws/status"
        
        # Initialize logger
        self.logger = ProcessLogger()
        
        # Create dashboard server
        self.dashboard_server = create_dashboard_server(
            self.logger, 
            host="0.0.0.0", 
            port=dashboard_port
        )
        
        # Test data
        self.test_processes = {
            "producer_0": {"type": "producer", "id": 0},
            "producer_1": {"type": "producer", "id": 1},
            "compressor_0": {"type": "compressor", "id": 2},
            "compressor_1": {"type": "compressor", "id": 3},
            "writer_0": {"type": "writer", "id": 4},
            "writer_1": {"type": "writer", "id": 5}
        }
        
        self.running = False
    
    def start_dashboard(self):
        """Start the dashboard server in a separate thread."""
        print("Starting dashboard server...")
        self.dashboard_thread = threading.Thread(
            target=self.dashboard_server.run,
            daemon=True
        )
        self.dashboard_thread.start()
        
        # Wait for server to start
        time.sleep(3)
        print(f"Dashboard available at: {self.base_url}")
    
    def simulate_process_activity(self):
        """Simulate process activity for testing."""
        print("Starting process activity simulation...")
        self.running = True
        
        def simulate_process(process_name, process_id):
            while self.running:
                try:
                    # Simulate task processing
                    task_id = f"task_{int(time.time() * 1000)}"
                    
                    # Log process start
                    self.logger.log_process_start(process_id, process_name, task_id)
                    
                    # Simulate processing time
                    processing_time = random.uniform(0.5, 2.0)
                    time.sleep(processing_time)
                    
                    # Update metrics based on process type
                    if "producer" in process_name:
                        self.logger.log_throughput(process_id, random.uniform(10, 50))
                        self.logger.log_buffer_usage(process_id, random.randint(1, 5))
                        self.logger.log_queue_size(process_id, random.randint(0, 10))
                        
                    elif "compressor" in process_name:
                        self.logger.log_throughput(process_id, random.uniform(8, 40))
                        self.logger.log_compression_ratio(process_id, random.uniform(0.3, 0.8))
                        self.logger.log_queue_size(process_id, random.randint(0, 8))
                        
                    elif "writer" in process_name:
                        self.logger.log_throughput(process_id, random.uniform(5, 30))
                        self.logger.log_write_speed(process_id, random.uniform(10, 100))
                        self.logger.log_buffer_usage(process_id, random.randint(0, 2))
                    
                    # Log process end
                    self.logger.log_process_end(process_id, task_id)
                    
                    # Simulate idle time
                    idle_time = random.uniform(0.1, 1.0)
                    time.sleep(idle_time)
                    
                except Exception as e:
                    print(f"Error in process {process_name}: {e}")
                    time.sleep(1)
        
        # Start simulation threads
        self.simulation_threads = []
        for process_name, process_info in self.test_processes.items():
            thread = threading.Thread(
                target=simulate_process,
                args=(process_name, process_info["id"]),
                daemon=True
            )
            thread.start()
            self.simulation_threads.append(thread)
    
    def simulate_error_logs(self):
        """Simulate error logs for testing."""
        print("Starting error log simulation...")
        
        def generate_errors():
            while self.running:
                try:
                    # Generate random errors
                    error_types = ["INFO", "WARNING", "ERROR"]
                    error_messages = [
                        "Queue depth approaching limit",
                        "Buffer pool expanded",
                        "Process restarted due to timeout",
                        "Compression ratio below threshold",
                        "Disk I/O performance degraded",
                        "Memory usage high",
                        "Network connection lost",
                        "Task processing delayed"
                    ]
                    
                    error_type = random.choice(error_types)
                    error_message = random.choice(error_messages)
                    process_id = random.choice(list(self.test_processes.values()))["id"]
                    
                    self.logger.log_error(error_type, error_message, process_id)
                    
                    # Wait between errors
                    time.sleep(random.uniform(5, 15))
                    
                except Exception as e:
                    print(f"Error generating error logs: {e}")
                    time.sleep(5)
        
        error_thread = threading.Thread(target=generate_errors, daemon=True)
        error_thread.start()
    
    def test_rest_api(self):
        """Test REST API endpoints."""
        print("Testing REST API endpoints...")
        
        try:
            # Test stats endpoint
            response = requests.get(f"{self.base_url}/api/stats")
            if response.status_code == 200:
                print("✅ Stats endpoint working")
                stats = response.json()
                print(f"   Pipeline stats: {stats.get('pipeline_stats', {}).get('total_processes', 0)} processes")
            else:
                print(f"❌ Stats endpoint failed: {response.status_code}")
            
            # Test system state endpoint
            response = requests.get(f"{self.base_url}/api/system-state")
            if response.status_code == 200:
                print("✅ System state endpoint working")
                state = response.json()
                print(f"   System state: {state}")
            else:
                print(f"❌ System state endpoint failed: {response.status_code}")
            
            # Test historical data endpoint
            response = requests.get(f"{self.base_url}/api/historical-data")
            if response.status_code == 200:
                print("✅ Historical data endpoint working")
                data = response.json()
                print(f"   Historical data points: {len(data.get('timestamps', []))}")
            else:
                print(f"❌ Historical data endpoint failed: {response.status_code}")
                
        except Exception as e:
            print(f"❌ REST API test failed: {e}")
    
    def test_control_api(self):
        """Test control API endpoints."""
        print("Testing control API endpoints...")
        
        try:
            # Test pause command
            response = requests.post(
                f"{self.base_url}/api/control",
                json={"type": "pause"}
            )
            if response.status_code == 200:
                print("✅ Pause command working")
                result = response.json()
                print(f"   Result: {result}")
            else:
                print(f"❌ Pause command failed: {response.status_code}")
            
            # Test resume command
            response = requests.post(
                f"{self.base_url}/api/control",
                json={"type": "resume"}
            )
            if response.status_code == 200:
                print("✅ Resume command working")
                result = response.json()
                print(f"   Result: {result}")
            else:
                print(f"❌ Resume command failed: {response.status_code}")
            
            # Test scale compressors command
            response = requests.post(
                f"{self.base_url}/api/control",
                json={"type": "scale_compressors", "count": 3}
            )
            if response.status_code == 200:
                print("✅ Scale compressors command working")
                result = response.json()
                print(f"   Result: {result}")
            else:
                print(f"❌ Scale compressors command failed: {response.status_code}")
            
            # Test adjust buffer size command
            response = requests.post(
                f"{self.base_url}/api/control",
                json={"type": "adjust_buffer_size", "size_mb": 150}
            )
            if response.status_code == 200:
                print("✅ Adjust buffer size command working")
                result = response.json()
                print(f"   Result: {result}")
            else:
                print(f"❌ Adjust buffer size command failed: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Control API test failed: {e}")
    
    def test_websocket(self):
        """Test WebSocket connection."""
        print("Testing WebSocket connection...")
        
        try:
            # Create WebSocket connection
            ws = websocket.create_connection(self.ws_url)
            
            # Receive a few messages
            for i in range(5):
                message = ws.recv()
                data = json.loads(message)
                print(f"✅ WebSocket message {i+1}: {len(data)} bytes")
                
                # Check message structure
                if "timestamp" in data and "pipeline_stats" in data:
                    print(f"   Message structure valid")
                else:
                    print(f"   Message structure invalid")
                
                time.sleep(1)
            
            ws.close()
            print("✅ WebSocket test completed")
            
        except Exception as e:
            print(f"❌ WebSocket test failed: {e}")
    
    def run_comprehensive_test(self, duration=60):
        """Run comprehensive dashboard test."""
        print("=" * 60)
        print("SHARED MEMORY PIPELINE DASHBOARD TEST")
        print("=" * 60)
        
        # Start dashboard
        self.start_dashboard()
        
        # Start simulations
        self.simulate_process_activity()
        self.simulate_error_logs()
        
        # Wait for data to accumulate
        print("Waiting for data to accumulate...")
        time.sleep(10)
        
        # Test REST API
        print("\n" + "=" * 40)
        print("TESTING REST API")
        print("=" * 40)
        self.test_rest_api()
        
        # Test control API
        print("\n" + "=" * 40)
        print("TESTING CONTROL API")
        print("=" * 40)
        self.test_control_api()
        
        # Test WebSocket
        print("\n" + "=" * 40)
        print("TESTING WEBSOCKET")
        print("=" * 40)
        self.test_websocket()
        
        # Run for specified duration
        print(f"\nRunning dashboard test for {duration} seconds...")
        print(f"Dashboard available at: {self.base_url}")
        print("Press Ctrl+C to stop")
        
        try:
            time.sleep(duration)
        except KeyboardInterrupt:
            print("\nStopping test...")
        
        # Stop simulations
        self.running = False
        
        # Export logs
        print("Exporting logs...")
        self.logger.export_process_logs("dashboard_test_logs.json")
        
        print("Test completed!")


def main():
    """Main test function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Shared Memory Pipeline Dashboard")
    parser.add_argument("--host", default="localhost", help="Dashboard host")
    parser.add_argument("--port", type=int, default=8000, help="Dashboard port")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
    
    args = parser.parse_args()
    
    # Create and run tester
    tester = DashboardTester(args.host, args.port)
    tester.run_comprehensive_test(args.duration)


if __name__ == "__main__":
    main() 