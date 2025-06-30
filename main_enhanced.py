#!/usr/bin/env python3
"""
Enhanced File Writer - Multi-threaded Matrix Writer with Advanced Features

This is the enhanced main entry point that integrates:
- Multiprocessing for full CPU saturation
- GPU-based batch compression
- Fast file reader with index-first loading
- Thread migration scheduler
- Multi-disk RAID-style writes
"""

import asyncio
import threading
import time
import signal
import sys
import argparse
from typing import Optional, Dict, List
import os

# Import all the enhanced modules
from multiprocess_writer import MultiprocessLoadBalancedController, MultiprocessMatrixWriter
from gpu_compression import GPUCompressor, HybridCompressor
from fast_reader import FastMatrixReader, IndexedMatrixReader
from thread_migration import ThreadMigrationScheduler, AdaptiveMigrationScheduler
from multi_disk_writer import RAIDWriter, DiskShard
from logger import ThreadLogger
from dashboard_server import create_dashboard_server


class EnhancedFileWriterApp:
    """Enhanced application class that coordinates all advanced features."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.running = False
        
        # Initialize components based on configuration
        self._init_components()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _init_components(self):
        """Initialize all enhanced components."""
        print("Initializing enhanced file writer components...")
        
        # Thread logger
        self.thread_logger = ThreadLogger()
        
        # Multiprocessing controller
        if self.config.get("use_multiprocessing", True):
            print("Initializing multiprocessing controller...")
            self.controller = MultiprocessLoadBalancedController(
                num_processes=self.config.get("num_processes", 4)
            )
        else:
            from writer import LoadBalancedController
            self.controller = LoadBalancedController(
                num_threads=self.config.get("num_threads", 4)
            )
        
        # Thread migration scheduler
        if self.config.get("use_migration", True):
            print("Initializing thread migration scheduler...")
            self.migration_scheduler = AdaptiveMigrationScheduler(
                num_threads=self.config.get("num_threads", 4),
                migration_threshold=self.config.get("migration_threshold", 0.3)
            )
        else:
            self.migration_scheduler = None
        
        # GPU compression
        if self.config.get("use_gpu_compression", True):
            print("Initializing GPU compression...")
            self.gpu_compressor = HybridCompressor(
                gpu_threshold_mb=self.config.get("gpu_threshold_mb", 100)
            )
        else:
            self.gpu_compressor = None
        
        # Fast reader
        if self.config.get("use_fast_reader", True):
            print("Initializing fast matrix reader...")
            self.fast_reader = IndexedMatrixReader(
                data_dir=self.config.get("data_dir", "output"),
                cache_size=self.config.get("cache_size", 100)
            )
        else:
            self.fast_reader = None
        
        # RAID writer
        if self.config.get("use_raid", True):
            print("Initializing RAID writer...")
            self.raid_writer = RAIDWriter(
                raid_level=self.config.get("raid_level", 0),
                redundancy=self.config.get("raid_redundancy", True)
            )
            
            # Add disk shards
            self._setup_raid_shards()
        else:
            self.raid_writer = None
        
        # Dashboard server
        self.dashboard_server = create_dashboard_server(
            self.thread_logger,
            host=self.config.get("dashboard_host", "0.0.0.0"),
            port=self.config.get("dashboard_port", 8000)
        )
        
        # Dashboard thread
        self.dashboard_thread: Optional[threading.Thread] = None
    
    def _setup_raid_shards(self):
        """Setup RAID disk shards."""
        raid_config = self.config.get("raid_config", {})
        
        # Add default shards if none configured
        if not raid_config.get("shards"):
            base_path = self.config.get("data_dir", "output")
            for i in range(3):  # Default 3 shards
                shard_path = os.path.join(base_path, f"shard_{i}")
                self.raid_writer.add_disk_shard(
                    disk_id=f"shard_{i}",
                    path=shard_path,
                    capacity_gb=1000.0
                )
        else:
            # Add configured shards
            for shard_config in raid_config["shards"]:
                self.raid_writer.add_disk_shard(
                    disk_id=shard_config["id"],
                    path=shard_config["path"],
                    capacity_gb=shard_config.get("capacity_gb", 1000.0)
                )
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.shutdown()
    
    def start(self):
        """Start the enhanced file writer application."""
        print("Starting Enhanced File Writer Application...")
        print(f"Dashboard will be available at: http://{self.config.get('dashboard_host', '0.0.0.0')}:{self.config.get('dashboard_port', 8000)}")
        
        self.running = True
        
        # Connect logger to controller
        if hasattr(self.controller, 'set_logger'):
            self.controller.set_logger(self.thread_logger)
        
        # Start the controller
        print("Starting load-balanced controller...")
        self.controller.start()
        
        # Start migration scheduler
        if self.migration_scheduler:
            print("Thread migration scheduler is active")
        
        # Start dashboard server
        print("Starting dashboard server...")
        self.dashboard_thread = threading.Thread(
            target=self._run_dashboard_server,
            daemon=True
        )
        self.dashboard_thread.start()
        
        # Add example tasks
        self._add_enhanced_example_tasks()
        
        print("Enhanced application started successfully!")
        print("Press Ctrl+C to stop the application")
        
        # Keep the main thread alive
        try:
            while self.running:
                time.sleep(1)
                
                # Periodic status updates
                if time.time() % 30 < 1:  # Every 30 seconds
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
    
    def _add_enhanced_example_tasks(self):
        """Add enhanced example tasks to demonstrate all features."""
        print("Adding enhanced example tasks...")
        
        # Matrix creation tasks with varying complexity
        for i in range(5):
            size = 100 + (i * 100)  # Varying matrix sizes
            self.controller.add_task(
                self._create_matrix_with_compression,
                size=size,
                name=f"enhanced_matrix_{i}",
                use_gpu_compression=self.gpu_compressor is not None
            )
        
        # Add some delay
        time.sleep(2)
        
        # RAID writing tasks
        if self.raid_writer:
            for i in range(3):
                self.controller.add_task(
                    self._create_matrix_with_raid,
                    size=200 + (i * 50),
                    name=f"raid_matrix_{i}"
                )
        
        # Fast reading tasks
        if self.fast_reader:
            for i in range(2):
                self.controller.add_task(
                    self._test_fast_reading,
                    matrix_id=str(i)
                )
    
    def _create_matrix_with_compression(self, size: int, name: str, use_gpu_compression: bool = False):
        """Create matrix with optional GPU compression."""
        import numpy as np
        
        # Create matrix
        matrix = np.random.rand(size, size)
        
        # Write using appropriate method
        if hasattr(self.controller, 'matrix_writer'):
            matrix_id = self.controller.matrix_writer.write_matrix_parallel(matrix, name)
        else:
            from writer import MatrixWriter
            writer = MatrixWriter()
            matrix_id = writer.write_matrix(matrix, name)
        
        # Compress if GPU compression is available
        if use_gpu_compression and self.gpu_compressor:
            print(f"Compressing matrix {name} using hybrid compression...")
            compressed_files = self.gpu_compressor.compress_batch([matrix], "compressed_output")
            print(f"Compression completed: {len(compressed_files)} files created")
        
        return matrix_id
    
    def _create_matrix_with_raid(self, size: int, name: str):
        """Create matrix using RAID writing."""
        import numpy as np
        
        matrix = np.random.rand(size, size)
        
        if self.raid_writer:
            file_id = self.raid_writer.write_matrix_raid(matrix, name)
            print(f"Matrix {name} written to RAID array: {file_id}")
            return file_id
        else:
            return self._create_matrix_with_compression(size, name, False)
    
    def _test_fast_reading(self, matrix_id: str):
        """Test fast reading capabilities."""
        if self.fast_reader:
            try:
                # Test fast reading
                matrix = self.fast_reader.read_matrix_fast(matrix_id)
                print(f"Fast read successful for matrix {matrix_id}: {matrix.shape}")
                
                # Test slice reading
                if len(matrix.shape) >= 2:
                    slice_data = self.fast_reader.read_matrix_slice(
                        matrix_id, (slice(0, 10), slice(0, 10))
                    )
                    print(f"Slice read successful: {slice_data.shape}")
                
                return True
            except Exception as e:
                print(f"Fast reading test failed for matrix {matrix_id}: {e}")
                return False
    
    def _print_status_summary(self):
        """Print periodic status summary."""
        print("\n" + "="*50)
        print("ENHANCED FILE WRITER STATUS SUMMARY")
        print("="*50)
        
        # Controller status
        if hasattr(self.controller, 'get_system_stats'):
            stats = self.controller.get_system_stats()
            print(f"CPU Usage: {stats.get('total_cpu_usage', 0):.1f}%")
            print(f"Memory Usage: {stats.get('total_memory_usage', 0):.1f}%")
            print(f"Queue Size: {stats.get('queue_size', 0)}")
        
        # Migration scheduler status
        if self.migration_scheduler:
            migration_stats = self.migration_scheduler.get_migration_stats()
            print(f"Load Balance Score: {migration_stats.get('load_balance_score', 0):.3f}")
            print(f"Total Migrations: {migration_stats.get('performance_metrics', {}).get('total_migrations', 0)}")
        
        # RAID status
        if self.raid_writer:
            raid_stats = self.raid_writer.get_raid_stats()
            print(f"RAID Level: {raid_stats.get('raid_level', 0)}")
            print(f"Total Capacity: {raid_stats.get('total_capacity_gb', 0):.1f}GB")
            print(f"Usage: {raid_stats.get('usage_percent', 0):.1f}%")
        
        # Fast reader status
        if self.fast_reader:
            reader_stats = self.fast_reader.get_reader_stats()
            print(f"Total Matrices: {reader_stats.get('total_matrices', 0)}")
            print(f"Cache Size: {reader_stats.get('cache_stats', {}).get('cache_size', 0)}")
        
        print("="*50)
    
    def shutdown(self):
        """Shutdown the enhanced application gracefully."""
        print("Shutting down Enhanced File Writer Application...")
        self.running = False
        
        # Stop the controller
        if hasattr(self, 'controller'):
            print("Stopping load-balanced controller...")
            self.controller.stop()
        
        # Stop migration scheduler
        if self.migration_scheduler:
            print("Stopping thread migration scheduler...")
            self.migration_scheduler.shutdown()
        
        # Stop RAID writer
        if self.raid_writer:
            print("Stopping RAID writer...")
            self.raid_writer.shutdown()
        
        # Stop fast reader
        if self.fast_reader:
            print("Stopping fast reader...")
            self.fast_reader.shutdown()
        
        # Wait for dashboard thread
        if self.dashboard_thread and self.dashboard_thread.is_alive():
            print("Waiting for dashboard server to stop...")
        
        print("Enhanced application shutdown complete.")
        sys.exit(0)


def main():
    """Enhanced main entry point."""
    parser = argparse.ArgumentParser(description="Enhanced File Writer - Multi-threaded Matrix Writer with Advanced Features")
    
    # Basic configuration
    parser.add_argument("--threads", type=int, default=4, help="Number of worker threads (default: 4)")
    parser.add_argument("--processes", type=int, default=4, help="Number of worker processes for multiprocessing (default: 4)")
    parser.add_argument("--host", default="0.0.0.0", help="Dashboard server host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Dashboard server port (default: 8000)")
    
    # Advanced features
    parser.add_argument("--use-multiprocessing", action="store_true", help="Use multiprocessing instead of threading")
    parser.add_argument("--use-migration", action="store_true", help="Enable thread migration scheduler")
    parser.add_argument("--use-gpu-compression", action="store_true", help="Enable GPU compression")
    parser.add_argument("--use-fast-reader", action="store_true", help="Enable fast matrix reader")
    parser.add_argument("--use-raid", action="store_true", help="Enable RAID-style multi-disk writes")
    
    # Feature-specific options
    parser.add_argument("--migration-threshold", type=float, default=0.3, help="Migration threshold (default: 0.3)")
    parser.add_argument("--gpu-threshold-mb", type=int, default=100, help="GPU compression threshold in MB (default: 100)")
    parser.add_argument("--cache-size", type=int, default=100, help="Fast reader cache size (default: 100)")
    parser.add_argument("--raid-level", type=int, default=0, help="RAID level (0, 1, or 5, default: 0)")
    parser.add_argument("--raid-redundancy", action="store_true", help="Enable RAID redundancy")
    
    # Data directory
    parser.add_argument("--data-dir", default="output", help="Data directory (default: output)")
    
    args = parser.parse_args()
    
    # Build configuration
    config = {
        "num_threads": args.threads,
        "num_processes": args.processes,
        "dashboard_host": args.host,
        "dashboard_port": args.port,
        "use_multiprocessing": args.use_multiprocessing,
        "use_migration": args.use_migration,
        "use_gpu_compression": args.use_gpu_compression,
        "use_fast_reader": args.use_fast_reader,
        "use_raid": args.use_raid,
        "migration_threshold": args.migration_threshold,
        "gpu_threshold_mb": args.gpu_threshold_mb,
        "cache_size": args.cache_size,
        "raid_level": args.raid_level,
        "raid_redundancy": args.raid_redundancy,
        "data_dir": args.data_dir
    }
    
    # Create and start the enhanced application
    app = EnhancedFileWriterApp(config)
    
    try:
        app.start()
    except Exception as e:
        print(f"Enhanced application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 