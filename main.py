#!/usr/bin/env python3
"""
Shared Memory Pipeline Controller - Main Entry Point

This is the main controller that starts and manages the shared memory pipeline
with producer, compressor, and writer processes using multiprocessing.shared_memory.
"""

import asyncio
import threading
import time
import signal
import sys
import argparse
from typing import Optional, Dict, List
import os
import json

# Import the shared memory pipeline components
from shared_memory_pipeline import SharedMemoryPipeline
from buffer_manager import BufferManager, BufferPool
from logger import ProcessLogger
from dashboard_server import create_dashboard_server


class SharedMemoryPipelineController:
    """Main controller for the shared memory pipeline."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.running = False
        
        # Initialize components
        self._init_components()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _init_components(self):
        """Initialize all pipeline components."""
        print("Initializing Shared Memory Pipeline Controller...")
        
        # Process logger
        self.process_logger = ProcessLogger()
        
        # Buffer manager
        print("Initializing buffer manager...")
        self.buffer_manager = BufferManager(
            max_buffers=self.config.get("max_buffers", 20),
            buffer_size_mb=self.config.get("buffer_size_mb", 100),
            enable_reuse=self.config.get("enable_buffer_reuse", True),
            cleanup_interval=self.config.get("cleanup_interval", 30.0)
        )
        
        # Shared memory pipeline
        print("Initializing shared memory pipeline...")
        self.pipeline = SharedMemoryPipeline(
            num_producers=self.config.get("num_producers", 2),
            num_compressors=self.config.get("num_compressors", 2),
            num_writers=self.config.get("num_writers", 2),
            max_buffers=self.config.get("max_buffers", 20),
            buffer_size_mb=self.config.get("buffer_size_mb", 100),
            output_dir=self.config.get("output_dir", "output")
        )
        
        # Dashboard server
        self.dashboard_server = create_dashboard_server(
            self.process_logger,
            host=self.config.get("dashboard_host", "0.0.0.0"),
            port=self.config.get("dashboard_port", 8000)
        )
        
        # Dashboard thread
        self.dashboard_thread: Optional[threading.Thread] = None
        
        # Task generation thread
        self.task_generation_thread: Optional[threading.Thread] = None
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.shutdown()
    
    def start(self):
        """Start the shared memory pipeline controller."""
        print("Starting Shared Memory Pipeline Controller...")
        print(f"Dashboard will be available at: http://{self.config.get('dashboard_host', '0.0.0.0')}:{self.config.get('dashboard_port', 8000)}")
        
        self.running = True
        
        # Start the pipeline
        print("Starting shared memory pipeline...")
        self.pipeline.start()
        
        # Start dashboard server
        print("Starting dashboard server...")
        self.dashboard_thread = threading.Thread(
            target=self._run_dashboard_server,
            daemon=True
        )
        self.dashboard_thread.start()
        
        # Start task generation
        print("Starting task generation...")
        self.task_generation_thread = threading.Thread(
            target=self._generate_tasks,
            daemon=True
        )
        self.task_generation_thread.start()
        
        print("Shared Memory Pipeline Controller started successfully!")
        print("Press Ctrl+C to stop the application")
        
        # Keep the main thread alive
        try:
            while self.running:
                time.sleep(1)
                
                # Periodic status updates
                if int(time.time()) % 30 == 0:  # Every 30 seconds
                    self._print_status_summary()
                    
        except KeyboardInterrupt:
            print("\nReceived keyboard interrupt, shutting down...")
            self.shutdown()
    
    def _run_dashboard_server(self):
        """Run the dashboard server in a separate thread."""
        try:
            self.dashboard_server.run()
        except Exception as e:
            print(f"Dashboard server error: {e}")
    
    def _generate_tasks(self):
        """Generate tasks for the pipeline."""
        print("Task generation started...")
        
        task_id = 0
        
        while self.running:
            try:
                # Generate different types of tasks
                task_types = ["random", "identity", "zeros"]
                matrix_sizes = [100, 200, 300, 500, 1000]
                
                for size in matrix_sizes:
                    for task_type in task_types:
                        if not self.running:
                            break
                        
                        task = {
                            "task_id": f"task_{task_id}",
                            "size": size,
                            "type": task_type,
                            "priority": "normal",
                            "created_at": time.time()
                        }
                        
                        self.pipeline.add_task(task)
                        task_id += 1
                        
                        # Log task creation
                        self.process_logger.log_task_created(
                            task_id=task["task_id"],
                            task_type=task_type,
                            matrix_size=size,
                            process_id=0  # Main process
                        )
                        
                        # Small delay between tasks
                        time.sleep(0.5)
                
                # Wait before next batch
                time.sleep(10)
                
            except Exception as e:
                print(f"Error in task generation: {e}")
                time.sleep(5)
        
        print("Task generation stopped")
    
    def _print_status_summary(self):
        """Print periodic status summary."""
        print("\n" + "="*60)
        print("SHARED MEMORY PIPELINE STATUS SUMMARY")
        print("="*60)
        
        # Pipeline status
        pipeline_stats = self.pipeline.get_pipeline_stats()
        print(f"Pipeline Status: {pipeline_stats['pipeline_status']}")
        print(f"Total Processes: {pipeline_stats['total_processes']}")
        print(f"Active Processes: {pipeline_stats['active_processes']}")
        
        # Buffer status
        buffer_info = pipeline_stats['buffer_info']
        print(f"Total Buffers: {buffer_info['total_buffers']}")
        print(f"Available Buffers: {buffer_info['available_buffers']}")
        print(f"Buffer Size: {buffer_info['buffer_size_mb']:.1f}MB")
        
        # Queue status
        queue_sizes = pipeline_stats['queue_sizes']
        print(f"Task Queue: {queue_sizes['task_queue']}")
        print(f"Producer-Compressor Queue: {queue_sizes['producer_compressor_queue']}")
        print(f"Compressor-Writer Queue: {queue_sizes['compressor_writer_queue']}")
        print(f"Buffer Return Queue: {queue_sizes['buffer_return_queue']}")
        
        # Process statistics
        if pipeline_stats['producer_stats']:
            total_produced = sum(stat['matrices_produced'] for stat in pipeline_stats['producer_stats'])
            print(f"Total Matrices Produced: {total_produced}")
        
        if pipeline_stats['compressor_stats']:
            total_compressed = sum(stat['matrices_compressed'] for stat in pipeline_stats['compressor_stats'])
            avg_ratio = sum(stat['avg_compression_ratio'] for stat in pipeline_stats['compressor_stats']) / len(pipeline_stats['compressor_stats'])
            print(f"Total Matrices Compressed: {total_compressed}")
            print(f"Average Compression Ratio: {avg_ratio:.3f}")
        
        if pipeline_stats['writer_stats']:
            total_written = sum(stat['files_written'] for stat in pipeline_stats['writer_stats'])
            total_bytes = sum(stat['total_bytes_written'] for stat in pipeline_stats['writer_stats'])
            print(f"Total Files Written: {total_written}")
            print(f"Total Bytes Written: {total_bytes / (1024*1024):.1f}MB")
        
        # Buffer manager statistics
        buffer_stats = self.buffer_manager.get_manager_stats()
        print(f"Buffer Utilization: {buffer_stats['utilization_percent']:.1f}%")
        print(f"Buffer Reuses: {buffer_stats['stats']['buffer_reuses']}")
        print(f"Memory Leaks Prevented: {buffer_stats['stats']['memory_leaks_prevented']}")
        
        print("="*60)
    
    def shutdown(self):
        """Shutdown the controller gracefully."""
        print("Shutting down Shared Memory Pipeline Controller...")
        self.running = False
        
        # Stop the pipeline
        if hasattr(self, 'pipeline'):
            print("Stopping shared memory pipeline...")
            self.pipeline.stop()
        
        # Stop buffer manager
        if hasattr(self, 'buffer_manager'):
            print("Stopping buffer manager...")
            self.buffer_manager.shutdown()
        
        # Wait for threads
        if self.dashboard_thread and self.dashboard_thread.is_alive():
            print("Waiting for dashboard server to stop...")
        
        if self.task_generation_thread and self.task_generation_thread.is_alive():
            print("Waiting for task generation to stop...")
        
        print("Shared Memory Pipeline Controller shutdown complete.")
        sys.exit(0)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Shared Memory Pipeline Controller")
    
    # Pipeline configuration
    parser.add_argument("--num-producers", type=int, default=2, help="Number of producer processes (default: 2)")
    parser.add_argument("--num-compressors", type=int, default=2, help="Number of compressor processes (default: 2)")
    parser.add_argument("--num-writers", type=int, default=2, help="Number of writer processes (default: 2)")
    
    # Buffer configuration
    parser.add_argument("--max-buffers", type=int, default=20, help="Maximum number of shared memory buffers (default: 20)")
    parser.add_argument("--buffer-size-mb", type=int, default=100, help="Size of each buffer in MB (default: 100)")
    parser.add_argument("--enable-buffer-reuse", action="store_true", help="Enable buffer reuse (default: True)")
    parser.add_argument("--cleanup-interval", type=float, default=30.0, help="Buffer cleanup interval in seconds (default: 30.0)")
    
    # Dashboard configuration
    parser.add_argument("--dashboard-host", default="0.0.0.0", help="Dashboard server host (default: 0.0.0.0)")
    parser.add_argument("--dashboard-port", type=int, default=8000, help="Dashboard server port (default: 8000)")
    
    # Output configuration
    parser.add_argument("--output-dir", default="output", help="Output directory for compressed files (default: output)")
    
    args = parser.parse_args()
    
    # Build configuration
    config = {
        "num_producers": args.num_producers,
        "num_compressors": args.num_compressors,
        "num_writers": args.num_writers,
        "max_buffers": args.max_buffers,
        "buffer_size_mb": args.buffer_size_mb,
        "enable_buffer_reuse": args.enable_buffer_reuse,
        "cleanup_interval": args.cleanup_interval,
        "dashboard_host": args.dashboard_host,
        "dashboard_port": args.dashboard_port,
        "output_dir": args.output_dir
    }
    
    # Create and start the controller
    controller = SharedMemoryPipelineController(config)
    
    try:
        controller.start()
    except Exception as e:
        print(f"Controller error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 