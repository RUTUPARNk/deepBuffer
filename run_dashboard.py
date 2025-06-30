#!/usr/bin/env python3
"""
Startup script for Shared Memory Pipeline with Dashboard

This script starts the shared memory pipeline with real-time dashboard monitoring.
"""

import sys
import time
import threading
from main import SharedMemoryPipelineController
from logger import ProcessLogger
from dashboard_server import create_dashboard_server


def main():
    """Start the shared memory pipeline with dashboard."""
    print("🚀 Starting Shared Memory Pipeline with Dashboard")
    print("=" * 60)
    
    # Configuration
    config = {
        "num_producers": 2,
        "num_compressors": 2,
        "num_writers": 2,
        "max_buffers": 20,
        "buffer_size_mb": 100,
        "enable_buffer_reuse": True,
        "cleanup_interval": 30.0,
        "dashboard_host": "0.0.0.0",
        "dashboard_port": 8000,
        "output_dir": "output"
    }
    
    print("Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    print("\nStarting pipeline controller...")
    
    # Create and start the controller
    controller = SharedMemoryPipelineController(config)
    
    try:
        # Start the controller in a separate thread
        controller_thread = threading.Thread(target=controller.start, daemon=True)
        controller_thread.start()
        
        print(f"\n✅ Shared Memory Pipeline started!")
        print(f"🌐 Dashboard available at: http://localhost:{config['dashboard_port']}")
        print(f"📊 Real-time monitoring active")
        print(f"📁 Output directory: {config['output_dir']}")
        print("\nPress Ctrl+C to stop the application")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        controller.shutdown()
        print("✅ Shutdown complete")


if __name__ == "__main__":
    main() 