from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
import time
from typing import List, Dict, Optional, Any
import threading
from logger import ThreadLogger, ProcessLogger
import uvicorn
import psutil
import os
from transactional_checkpoint import CheckpointManager


class DashboardServer:
    """Real-time dashboard server for monitoring the shared memory pipeline."""
    
    def __init__(self, logger: ProcessLogger, host: str = "0.0.0.0", port: int = 8000, checkpoint_manager: Optional[CheckpointManager] = None):
        self.logger = logger
        self.host = host
        self.port = port
        self.checkpoint_manager = checkpoint_manager
        
        # FastAPI app
        self.app = FastAPI(title="Shared Memory Pipeline Dashboard")
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # WebSocket connections
        self.active_connections: List[WebSocket] = []
        
        # System state
        self.system_state = {
            "paused": False,
            "num_compressors": 2,
            "buffer_size_mb": 100,
            "max_buffers": 20
        }
        
        # Historical data for charts
        self.historical_data = {
            "compression_ratios": [],
            "throughput": [],
            "queue_sizes": [],
            "buffer_usage": [],
            "timestamps": []
        }
        
        # Checkpoint status
        self.checkpoint_status = {
            "last_checkpoint_time": None,
            "version": None,
            "interval": None,
            "saving": False
        }
        
        # Setup routes
        self._setup_routes()
        
        # Background task for sending updates
        self.update_task: Optional[asyncio.Task] = None
    
    def _setup_routes(self):
        """Setup FastAPI routes."""
        
        @self.app.get("/", response_class=HTMLResponse)
        async def get_dashboard():
            """Serve the dashboard HTML."""
            return self._get_dashboard_html()
        
        @self.app.get("/api/stats")
        async def get_stats():
            """Get current statistics."""
            return self._get_current_stats()
        
        @self.app.get("/api/pipeline-stats")
        async def get_pipeline_stats():
            """Get pipeline-specific statistics."""
            return self._get_pipeline_stats()
        
        @self.app.get("/api/process-stats")
        async def get_process_stats():
            """Get process statistics."""
            return self._get_process_stats()
        
        @self.app.get("/api/task-stats")
        async def get_task_stats():
            """Get task statistics."""
            return self._get_task_stats()
        
        @self.app.get("/api/system-state")
        async def get_system_state():
            """Get current system state."""
            return self.system_state
        
        @self.app.post("/api/control")
        async def control_system(command: Dict[str, Any]):
            """Send control commands to the system."""
            return await self._handle_control_command(command)
        
        @self.app.get("/api/historical-data")
        async def get_historical_data():
            """Get historical data for charts."""
            return self.historical_data
        
        @self.app.get("/api/checkpoint-info")
        async def get_checkpoint_info():
            """Get real-time checkpoint info for dashboard."""
            info = self.checkpoint_manager.get_last_checkpoint_info() if self.checkpoint_manager else None
            return {
                "last_checkpoint_time": info["timestamp"] if info else None,
                "version": info["version"] if info else None,
                "interval": self.checkpoint_status["interval"],
                "saving": self.checkpoint_status["saving"]
            }
        
        @self.app.get("/api/activity-heatmap")
        async def get_activity_heatmap():
            """Get thread and process activity heatmap data for visualization."""
            # Thread heatmap
            thread_stats = self.logger.get_all_thread_stats()
            process_stats = self.logger.get_all_process_stats()
            # For each thread/process, collect a time series of idle_percentage or cpu_usage
            thread_heatmap = []
            for tid, stats in thread_stats.items():
                thread_heatmap.append({
                    "thread_id": tid,
                    "idle_percentage": stats.get("idle_percentage", 0),
                    "cpu_usage": stats.get("avg_cpu_usage", 0),
                    "history": []  # Optionally, add more detailed history if available
                })
            process_heatmap = []
            for pid, stats in process_stats.items():
                process_heatmap.append({
                    "process_id": pid,
                    "idle_percentage": stats.get("idle_percentage", 0),
                    "cpu_usage": stats.get("avg_cpu_usage", 0),
                    "history": []  # Optionally, add more detailed history if available
                })
            return {
                "thread_heatmap": thread_heatmap,
                "process_heatmap": process_heatmap
            }
        
        @self.app.get("/api/throughput-history")
        async def get_throughput_history():
            """Get per-thread and per-process throughput history for advanced graphs."""
            thread_history = self.logger.get_thread_throughput_history()
            process_history = self.logger.get_process_throughput_history()
            return {
                "thread_throughput": thread_history,
                "process_throughput": process_history
            }
        
        @self.app.get("/api/buffer-usage-history")
        async def get_buffer_usage_history():
            """Get detailed buffer usage history for buffer usage charts."""
            # If self.buffer_manager exists, use it; else, return dummy data
            try:
                history = self.buffer_manager.get_full_usage_history(300)  # last 5 min
            except AttributeError:
                # Dummy data: 100 points, sine wave
                import math, time as t
                now = t.time()
                history = [(now - 100 + i, 10 + int(5*math.sin(i/10)), 10 + int(5*math.cos(i/10)), 20) for i in range(100)]
            return {"history": history}
        
        @self.app.websocket("/ws/status")
        async def websocket_status(websocket: WebSocket):
            """WebSocket endpoint for real-time status updates."""
            await websocket.accept()
            self.active_connections.append(websocket)
            
            try:
                while True:
                    await asyncio.sleep(1)
                    status_data = self._get_comprehensive_status()
                    # Add checkpoint info
                    checkpoint_info = self.checkpoint_manager.get_last_checkpoint_info() if self.checkpoint_manager else None
                    status_data["checkpoint_info"] = {
                        "last_checkpoint_time": checkpoint_info["timestamp"] if checkpoint_info else None,
                        "version": checkpoint_info["version"] if checkpoint_info else None,
                        "interval": self.checkpoint_status["interval"],
                        "saving": self.checkpoint_status["saving"]
                    }
                    await websocket.send_text(json.dumps(status_data))
            except WebSocketDisconnect:
                self.active_connections.remove(websocket)
    
    async def _handle_control_command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Handle system control commands."""
        command_type = command.get("type")
        
        try:
            if command_type == "pause":
                self.system_state["paused"] = True
                return {"status": "success", "message": "System paused"}
            
            elif command_type == "resume":
                self.system_state["paused"] = False
                return {"status": "success", "message": "System resumed"}
            
            elif command_type == "scale_compressors":
                new_count = command.get("count", 2)
                if 1 <= new_count <= 10:
                    self.system_state["num_compressors"] = new_count
                    return {"status": "success", "message": f"Scaled compressors to {new_count}"}
                else:
                    raise HTTPException(status_code=400, detail="Invalid compressor count")
            
            elif command_type == "adjust_buffer_size":
                new_size = command.get("size_mb", 100)
                if 10 <= new_size <= 1000:
                    self.system_state["buffer_size_mb"] = new_size
                    return {"status": "success", "message": f"Buffer size adjusted to {new_size}MB"}
                else:
                    raise HTTPException(status_code=400, detail="Invalid buffer size")
            
            elif command_type == "adjust_max_buffers":
                new_max = command.get("max_buffers", 20)
                if 5 <= new_max <= 100:
                    self.system_state["max_buffers"] = new_max
                    return {"status": "success", "message": f"Max buffers adjusted to {new_max}"}
                else:
                    raise HTTPException(status_code=400, detail="Invalid max buffers")
            
            else:
                raise HTTPException(status_code=400, detail="Unknown command type")
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    def _get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive system status for WebSocket updates."""
        try:
            # Get basic pipeline stats
            pipeline_stats = self.logger.get_pipeline_stats()
            process_stats = pipeline_stats.get("process_stats", {})
            
            # Calculate throughput metrics
            throughput_metrics = self._calculate_throughput_metrics(process_stats)
            
            # Calculate queue metrics
            queue_metrics = self._calculate_queue_metrics(process_stats)
            
            # Calculate buffer metrics
            buffer_metrics = self._calculate_buffer_metrics()
            
            # Calculate compression metrics
            compression_metrics = self._calculate_compression_metrics(process_stats)
            
            # Calculate disk I/O metrics
            disk_metrics = self._calculate_disk_metrics()
            
            # Calculate idle probabilities
            idle_metrics = self._calculate_idle_metrics(process_stats)
            
            # Get error logs
            error_logs = self._get_error_logs()
            
            # Update historical data
            self._update_historical_data(compression_metrics, throughput_metrics, queue_metrics, buffer_metrics)
            
            status_data = {
                "timestamp": time.time(),
                "system_state": self.system_state,
                "pipeline_stats": pipeline_stats,
                "process_stats": process_stats,
                "throughput_metrics": throughput_metrics,
                "queue_metrics": queue_metrics,
                "buffer_metrics": buffer_metrics,
                "compression_metrics": compression_metrics,
                "disk_metrics": disk_metrics,
                "idle_metrics": idle_metrics,
                "error_logs": error_logs,
                "historical_data": self.historical_data
            }
            
            return status_data
            
        except Exception as e:
            return {
                "timestamp": time.time(),
                "error": str(e),
                "system_state": self.system_state
            }
    
    def _calculate_throughput_metrics(self, process_stats: Dict) -> Dict[str, Any]:
        """Calculate throughput metrics for each process type."""
        producers = {k: v for k, v in process_stats.items() if "producer" in v.get("process_name", "").lower()}
        compressors = {k: v for k, v in process_stats.items() if "compressor" in v.get("process_name", "").lower()}
        writers = {k: v for k, v in process_stats.items() if "writer" in v.get("process_name", "").lower()}
        
        def calculate_avg_throughput(processes: Dict) -> float:
            if not processes:
                return 0.0
            total_activities = sum(p.get("total_activities", 0) for p in processes.values())
            total_duration = sum(p.get("total_duration", 0) for p in processes.values())
            return total_activities / total_duration if total_duration > 0 else 0.0
        
        return {
            "producer_throughput": calculate_avg_throughput(producers),
            "compressor_throughput": calculate_avg_throughput(compressors),
            "writer_throughput": calculate_avg_throughput(writers),
            "total_throughput": sum([
                calculate_avg_throughput(producers),
                calculate_avg_throughput(compressors),
                calculate_avg_throughput(writers)
            ])
        }
    
    def _calculate_queue_metrics(self, process_stats: Dict) -> Dict[str, Any]:
        """Calculate queue depth metrics."""
        total_queue_size = sum(p.get("current_queue_size", 0) for p in process_stats.values())
        avg_queue_size = total_queue_size / len(process_stats) if process_stats else 0
        
        return {
            "total_queue_size": total_queue_size,
            "avg_queue_size": avg_queue_size,
            "queue_utilization": min(100.0, (total_queue_size / 100) * 100)  # Assuming max 100 items
        }
    
    def _calculate_buffer_metrics(self) -> Dict[str, Any]:
        """Calculate buffer usage metrics."""
        # This would integrate with the actual buffer manager
        # For now, return placeholder data
        return {
            "total_buffers": self.system_state["max_buffers"],
            "active_buffers": 5,  # Placeholder
            "free_buffers": self.system_state["max_buffers"] - 5,
            "buffer_utilization": 25.0,  # Placeholder
            "buffer_size_mb": self.system_state["buffer_size_mb"]
        }
    
    def _calculate_compression_metrics(self, process_stats: Dict) -> Dict[str, Any]:
        """Calculate compression metrics."""
        compressors = {k: v for k, v in process_stats.items() if "compressor" in v.get("process_name", "").lower()}
        
        if not compressors:
            return {
                "avg_compression_ratio": 0.0,
                "total_compressed": 0,
                "compression_efficiency": 0.0
            }
        
        # Calculate average compression ratio (placeholder logic)
        avg_compression_ratio = 0.7  # Placeholder - would be calculated from actual data
        total_compressed = sum(p.get("total_activities", 0) for p in compressors.values())
        
        return {
            "avg_compression_ratio": avg_compression_ratio,
            "total_compressed": total_compressed,
            "compression_efficiency": (1 - avg_compression_ratio) * 100
        }
    
    def _calculate_disk_metrics(self) -> Dict[str, Any]:
        """Calculate disk I/O metrics."""
        try:
            # Get disk I/O statistics
            disk_io = psutil.disk_io_counters()
            if disk_io:
                return {
                    "read_bytes_per_sec": disk_io.read_bytes,
                    "write_bytes_per_sec": disk_io.write_bytes,
                    "read_count": disk_io.read_count,
                    "write_count": disk_io.write_count
                }
        except:
            pass
        
        return {
            "read_bytes_per_sec": 0,
            "write_bytes_per_sec": 0,
            "read_count": 0,
            "write_count": 0
        }
    
    def _calculate_idle_metrics(self, process_stats: Dict) -> Dict[str, Any]:
        """Calculate idle probability metrics for each process."""
        idle_metrics = {}
        
        for process_id, stats in process_stats.items():
            idle_percentage = stats.get("idle_percentage", 0)
            idle_metrics[process_id] = {
                "idle_percentage": idle_percentage,
                "idle_probability": min(1.0, idle_percentage / 100.0),
                "status": "idle" if idle_percentage > 50 else "active"
            }
        
        return idle_metrics
    
    def _get_error_logs(self) -> List[Dict[str, Any]]:
        """Get recent error logs."""
        # Placeholder - would integrate with actual error logging
        return [
            {
                "timestamp": time.time() - 60,
                "level": "WARNING",
                "message": "Queue depth approaching limit",
                "process_id": 1
            },
            {
                "timestamp": time.time() - 120,
                "level": "INFO",
                "message": "Buffer pool expanded",
                "process_id": 0
            }
        ]
    
    def _update_historical_data(self, compression_metrics: Dict, throughput_metrics: Dict, 
                               queue_metrics: Dict, buffer_metrics: Dict):
        """Update historical data for charts."""
        current_time = time.time()
        
        # Keep only last 100 data points
        max_history = 100
        
        self.historical_data["timestamps"].append(current_time)
        self.historical_data["compression_ratios"].append(compression_metrics.get("avg_compression_ratio", 0))
        self.historical_data["throughput"].append(throughput_metrics.get("total_throughput", 0))
        self.historical_data["queue_sizes"].append(queue_metrics.get("total_queue_size", 0))
        self.historical_data["buffer_usage"].append(buffer_metrics.get("buffer_utilization", 0))
        
        # Trim to max history
        if len(self.historical_data["timestamps"]) > max_history:
            self.historical_data["timestamps"] = self.historical_data["timestamps"][-max_history:]
            self.historical_data["compression_ratios"] = self.historical_data["compression_ratios"][-max_history:]
            self.historical_data["throughput"] = self.historical_data["throughput"][-max_history:]
            self.historical_data["queue_sizes"] = self.historical_data["queue_sizes"][-max_history:]
            self.historical_data["buffer_usage"] = self.historical_data["buffer_usage"][-max_history:]
    
    def _get_dashboard_html(self) -> str:
        """Get the enhanced dashboard HTML content."""
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shared Memory Pipeline Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .status-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
        }
        .status-healthy { background-color: #10b981; }
        .status-warning { background-color: #f59e0b; }
        .status-error { background-color: #ef4444; }
        .status-idle { background-color: #6b7280; }
        
        .metric-card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            border: 1px solid #e5e7eb;
        }
        
        .progress-bar {
            width: 100%;
            height: 8px;
            background-color: #e5e7eb;
            border-radius: 4px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6, #1d4ed8);
            transition: width 0.3s ease;
        }
        
        .chart-container {
            position: relative;
            height: 300px;
            margin: 20px 0;
        }
        
        .control-button {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            transition: background-color 0.2s;
        }
        
        .control-button:hover {
            background: #2563eb;
        }
        
        .control-button.danger {
            background: #ef4444;
        }
        
        .control-button.danger:hover {
            background: #dc2626;
        }
        
        .error-log {
            background: #fef2f2;
            border: 1px solid #fecaca;
            border-radius: 8px;
            padding: 12px;
            margin: 8px 0;
            font-family: monospace;
            font-size: 12px;
        }
        
        .error-log.warning {
            background: #fffbeb;
            border-color: #fed7aa;
        }
        
        .error-log.info {
            background: #eff6ff;
            border-color: #bfdbfe;
        }
    </style>
</head>
<body class="bg-gray-50 min-h-screen">
    <div class="container mx-auto px-4 py-8">
        <!-- Header -->
        <div class="text-center mb-8">
            <h1 class="text-4xl font-bold text-gray-900 mb-2">🚀 Shared Memory Pipeline Dashboard</h1>
            <p class="text-gray-600">Real-time monitoring and control system</p>
            <div class="mt-4">
                <span id="connectionStatus" class="px-4 py-2 rounded-full text-sm font-medium">
                    Connecting...
                </span>
            </div>
        </div>

        <!-- System Controls -->
        <div class="metric-card mb-6">
            <h2 class="text-xl font-semibold mb-4">🎛️ System Controls</h2>
            <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                    <button id="pauseBtn" class="control-button w-full">⏸️ Pause System</button>
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-2">Compressors</label>
                    <select id="compressorCount" class="w-full p-2 border border-gray-300 rounded-md">
                        <option value="1">1</option>
                        <option value="2" selected>2</option>
                        <option value="3">3</option>
                        <option value="4">4</option>
                        <option value="5">5</option>
                    </select>
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-2">Buffer Size (MB)</label>
                    <select id="bufferSize" class="w-full p-2 border border-gray-300 rounded-md">
                        <option value="50">50</option>
                        <option value="100" selected>100</option>
                        <option value="200">200</option>
                        <option value="500">500</option>
                    </select>
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-2">Max Buffers</label>
                    <select id="maxBuffers" class="w-full p-2 border border-gray-300 rounded-md">
                        <option value="10">10</option>
                        <option value="20" selected>20</option>
                        <option value="30">30</option>
                        <option value="50">50</option>
                    </select>
                </div>
            </div>
        </div>

        <!-- Process Overview -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
            <!-- Producer Status -->
            <div class="metric-card">
                <h3 class="text-lg font-semibold mb-4">📊 Producer Status</h3>
                <div id="producerStats">
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Idle Time</span>
                            <span id="producerIdle">-</span>
                        </div>
                        <div class="progress-bar">
                            <div id="producerIdleBar" class="progress-fill" style="width: 0%"></div>
                        </div>
                    </div>
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Tasks Processed</span>
                            <span id="producerTasks">-</span>
                        </div>
                    </div>
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Throughput</span>
                            <span id="producerThroughput">-</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Compressor Status -->
            <div class="metric-card">
                <h3 class="text-lg font-semibold mb-4">🗜️ Compressor Status</h3>
                <div id="compressorStats">
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Compression Ratio</span>
                            <span id="compressionRatio">-</span>
                        </div>
                        <div class="progress-bar">
                            <div id="compressionRatioBar" class="progress-fill" style="width: 0%"></div>
                        </div>
                    </div>
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Tasks in Queue</span>
                            <span id="compressorQueue">-</span>
                        </div>
                        <div class="progress-bar">
                            <div id="compressorQueueBar" class="progress-fill" style="width: 0%"></div>
                        </div>
                    </div>
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Throughput</span>
                            <span id="compressorThroughput">-</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Writer Status -->
            <div class="metric-card">
                <h3 class="text-lg font-semibold mb-4">💾 Writer Status</h3>
                <div id="writerStats">
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Write Speed</span>
                            <span id="writeSpeed">-</span>
                        </div>
                        <div class="progress-bar">
                            <div id="writeSpeedBar" class="progress-fill" style="width: 0%"></div>
                        </div>
                    </div>
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Buffer Recycling</span>
                            <span id="bufferRecycling">-</span>
                        </div>
                    </div>
                    <div class="mb-3">
                        <div class="flex justify-between text-sm">
                            <span>Throughput</span>
                            <span id="writerThroughput">-</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Shared Buffer Status -->
        <div class="metric-card mb-6">
            <h3 class="text-lg font-semibold mb-4">🔄 Shared Buffer Status</h3>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div>
                    <div class="flex justify-between text-sm mb-2">
                        <span>Active Buffers</span>
                        <span id="activeBuffers">-</span>
                    </div>
                    <div class="progress-bar">
                        <div id="activeBuffersBar" class="progress-fill" style="width: 0%"></div>
                    </div>
                </div>
                <div>
                    <div class="flex justify-between text-sm mb-2">
                        <span>Free Buffers</span>
                        <span id="freeBuffers">-</span>
                    </div>
                    <div class="progress-bar">
                        <div id="freeBuffersBar" class="progress-fill" style="width: 0%"></div>
                    </div>
                </div>
            </div>
            <div class="mt-4 text-sm text-gray-600">
                Buffer Size: <span id="bufferSizeDisplay">-</span> MB | 
                Total Buffers: <span id="totalBuffers">-</span>
            </div>
        </div>

        <!-- Thread/Process Activity Heatmap -->
        <div class="metric-card mb-6">
            <h3 class="text-lg font-semibold mb-4">🔥 Thread/Process Activity Heatmap</h3>
            <div id="activityHeatmap" class="overflow-x-auto">
                <!-- Heatmap will be rendered here -->
            </div>
        </div>

        <!-- Advanced Throughput Graphs -->
        <div class="metric-card mb-6">
            <h3 class="text-lg font-semibold mb-4">📈 Advanced Throughput Graphs</h3>
            <div class="flex gap-4 mb-2">
                <button id="showThreadThroughput" class="control-button">Thread Throughput</button>
                <button id="showProcessThroughput" class="control-button">Process Throughput</button>
            </div>
            <div class="chart-container" style="height:300px">
                <canvas id="advancedThroughputChart"></canvas>
            </div>
        </div>

        <!-- Buffer Usage Chart -->
        <div class="metric-card mb-6">
            <h3 class="text-lg font-semibold mb-4">🧮 Buffer Usage Over Time</h3>
            <div class="chart-container" style="height:300px">
                <canvas id="bufferUsageChart"></canvas>
        <!-- Charts -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
            <!-- Compression Ratio Chart -->
            <div class="metric-card">
                <h3 class="text-lg font-semibold mb-4">📈 Compression Ratio Trend</h3>
                <div class="chart-container">
                    <canvas id="compressionChart"></canvas>
                </div>
            </div>

            <!-- Throughput Chart -->
            <div class="metric-card">
                <h3 class="text-lg font-semibold mb-4">📊 System Throughput</h3>
                <div class="chart-container">
                    <canvas id="throughputChart"></canvas>
                </div>
            </div>
        </div>

        <!-- Error Logs -->
        <div class="metric-card">
            <h3 class="text-lg font-semibold mb-4">⚠️ Error Logs</h3>
            <div id="errorLogs" class="max-h-64 overflow-y-auto">
                <!-- Error logs will be populated here -->
            </div>
        </div>
    </div>

    <script>
        let ws = null;
        let reconnectAttempts = 0;
        const maxReconnectAttempts = 5;
        
        // Charts
        let compressionChart = null;
        let throughputChart = null;
        let advancedThroughputChart = null;
        let throughputMode = 'thread';
        
        function initializeCharts() {
            const compressionCtx = document.getElementById('compressionChart').getContext('2d');
            compressionChart = new Chart(compressionCtx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Compression Ratio',
                        data: [],
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true,
                            max: 1
                        }
                    }
                }
            });
            
            const throughputCtx = document.getElementById('throughputChart').getContext('2d');
            throughputChart = new Chart(throughputCtx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Total Throughput',
                        data: [],
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
        
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws/status`;
            
            ws = new WebSocket(wsUrl);
            
            ws.onopen = function() {
                console.log('WebSocket connected');
                document.getElementById('connectionStatus').textContent = 'Connected';
                document.getElementById('connectionStatus').className = 'px-4 py-2 rounded-full text-sm font-medium bg-green-100 text-green-800';
                reconnectAttempts = 0;
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            
            ws.onclose = function() {
                console.log('WebSocket disconnected');
                document.getElementById('connectionStatus').textContent = 'Disconnected';
                document.getElementById('connectionStatus').className = 'px-4 py-2 rounded-full text-sm font-medium bg-red-100 text-red-800';
                
                if (reconnectAttempts < maxReconnectAttempts) {
                    reconnectAttempts++;
                    setTimeout(connectWebSocket, 2000);
                }
            };
            
            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
            };
        }
        
        function updateDashboard(data) {
            // Update system state
            if (data.system_state) {
                updateSystemControls(data.system_state);
            }
            
            // Update process stats
            if (data.process_stats) {
                updateProcessStats(data.process_stats);
            }
            
            // Update buffer metrics
            if (data.buffer_metrics) {
                updateBufferMetrics(data.buffer_metrics);
            }
            
            // Update throughput metrics
            if (data.throughput_metrics) {
                updateThroughputMetrics(data.throughput_metrics);
            }
            
            // Update compression metrics
            if (data.compression_metrics) {
                updateCompressionMetrics(data.compression_metrics);
            }
            
            // Update queue metrics
            if (data.queue_metrics) {
                updateQueueMetrics(data.queue_metrics);
            }
            
            // Update error logs
            if (data.error_logs) {
                updateErrorLogs(data.error_logs);
            }
            
            // Update charts
            if (data.historical_data) {
                updateCharts(data.historical_data);
            }
        }
        
        function updateSystemControls(systemState) {
            document.getElementById('compressorCount').value = systemState.num_compressors;
            document.getElementById('bufferSize').value = systemState.buffer_size_mb;
            document.getElementById('maxBuffers').value = systemState.max_buffers;
            
            const pauseBtn = document.getElementById('pauseBtn');
            if (systemState.paused) {
                pauseBtn.textContent = '▶️ Resume System';
                pauseBtn.className = 'control-button w-full';
            } else {
                pauseBtn.textContent = '⏸️ Pause System';
                pauseBtn.className = 'control-button w-full';
            }
        }
        
        function updateProcessStats(processStats) {
            // Update producer stats
            const producers = Object.values(processStats).filter(p => 
                p.process_name && p.process_name.toLowerCase().includes('producer')
            );
            if (producers.length > 0) {
                const producer = producers[0];
                document.getElementById('producerIdle').textContent = producer.idle_percentage?.toFixed(1) + '%';
                document.getElementById('producerIdleBar').style.width = producer.idle_percentage + '%';
                document.getElementById('producerTasks').textContent = producer.total_activities || 0;
                document.getElementById('producerThroughput').textContent = (producer.total_activities / Math.max(producer.total_duration, 1)).toFixed(2) + '/s';
            }
            
            // Update compressor stats
            const compressors = Object.values(processStats).filter(p => 
                p.process_name && p.process_name.toLowerCase().includes('compressor')
            );
            if (compressors.length > 0) {
                const compressor = compressors[0];
                document.getElementById('compressorQueue').textContent = compressor.current_queue_size || 0;
                document.getElementById('compressorQueueBar').style.width = Math.min((compressor.current_queue_size / 10) * 100, 100) + '%';
                document.getElementById('compressorThroughput').textContent = (compressor.total_activities / Math.max(compressor.total_duration, 1)).toFixed(2) + '/s';
            }
            
            // Update writer stats
            const writers = Object.values(processStats).filter(p => 
                p.process_name && p.process_name.toLowerCase().includes('writer')
            );
            if (writers.length > 0) {
                const writer = writers[0];
                document.getElementById('writeSpeed').textContent = (writer.total_activities / Math.max(writer.total_duration, 1)).toFixed(2) + '/s';
                document.getElementById('writeSpeedBar').style.width = Math.min((writer.total_activities / 10) * 100, 100) + '%';
                document.getElementById('writerThroughput').textContent = (writer.total_activities / Math.max(writer.total_duration, 1)).toFixed(2) + '/s';
            }
        }
        
        function updateBufferMetrics(bufferMetrics) {
            document.getElementById('activeBuffers').textContent = bufferMetrics.active_buffers;
            document.getElementById('freeBuffers').textContent = bufferMetrics.free_buffers;
            document.getElementById('totalBuffers').textContent = bufferMetrics.total_buffers;
            document.getElementById('bufferSizeDisplay').textContent = bufferMetrics.buffer_size_mb;
            
            const activePercent = (bufferMetrics.active_buffers / bufferMetrics.total_buffers) * 100;
            const freePercent = (bufferMetrics.free_buffers / bufferMetrics.total_buffers) * 100;
            
            document.getElementById('activeBuffersBar').style.width = activePercent + '%';
            document.getElementById('freeBuffersBar').style.width = freePercent + '%';
        }
        
        function updateThroughputMetrics(throughputMetrics) {
            // Throughput metrics are updated in process stats
        }
        
        function updateCompressionMetrics(compressionMetrics) {
            document.getElementById('compressionRatio').textContent = (compressionMetrics.avg_compression_ratio * 100).toFixed(1) + '%';
            document.getElementById('compressionRatioBar').style.width = (compressionMetrics.avg_compression_ratio * 100) + '%';
        }
        
        function updateQueueMetrics(queueMetrics) {
            // Queue metrics are updated in process stats
        }
        
        function updateErrorLogs(errorLogs) {
            const errorLogsContainer = document.getElementById('errorLogs');
            errorLogsContainer.innerHTML = '';
            
            errorLogs.forEach(log => {
                const logElement = document.createElement('div');
                logElement.className = `error-log ${log.level.toLowerCase()}`;
                
                const timestamp = new Date(log.timestamp * 1000).toLocaleTimeString();
                logElement.innerHTML = `
                    <div class="flex justify-between">
                        <span class="font-medium">${log.level}</span>
                        <span class="text-gray-500">${timestamp}</span>
                    </div>
                    <div class="mt-1">${log.message}</div>
                    ${log.process_id !== undefined ? `<div class="text-xs text-gray-500 mt-1">Process ID: ${log.process_id}</div>` : ''}
                `;
                
                errorLogsContainer.appendChild(logElement);
            });
        }
        
        function updateCharts(historicalData) {
            if (historicalData.timestamps && historicalData.compression_ratios) {
                const labels = historicalData.timestamps.map(t => new Date(t * 1000).toLocaleTimeString());
                
                compressionChart.data.labels = labels;
                compressionChart.data.datasets[0].data = historicalData.compression_ratios;
                compressionChart.update('none');
            }
            
            if (historicalData.timestamps && historicalData.throughput) {
                const labels = historicalData.timestamps.map(t => new Date(t * 1000).toLocaleTimeString());
                
                throughputChart.data.labels = labels;
                throughputChart.data.datasets[0].data = historicalData.throughput;
                throughputChart.update('none');
            }
        }
        
        // Control event listeners
        document.getElementById('pauseBtn').addEventListener('click', async () => {
            const command = { type: 'pause' };
            try {
                const response = await fetch('/api/control', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(command)
                });
                const result = await response.json();
                console.log('Control result:', result);
            } catch (error) {
                console.error('Control error:', error);
            }
        });
        
        document.getElementById('compressorCount').addEventListener('change', async (e) => {
            const command = { type: 'scale_compressors', count: parseInt(e.target.value) };
            try {
                const response = await fetch('/api/control', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(command)
                });
                const result = await response.json();
                console.log('Control result:', result);
            } catch (error) {
                console.error('Control error:', error);
            }
        });
        
        document.getElementById('bufferSize').addEventListener('change', async (e) => {
            const command = { type: 'adjust_buffer_size', size_mb: parseInt(e.target.value) };
            try {
                const response = await fetch('/api/control', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(command)
                });
                const result = await response.json();
                console.log('Control result:', result);
            } catch (error) {
                console.error('Control error:', error);
            }
        });
        
        document.getElementById('maxBuffers').addEventListener('change', async (e) => {
            const command = { type: 'adjust_max_buffers', max_buffers: parseInt(e.target.value) };
            try {
                const response = await fetch('/api/control', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(command)
                });
                const result = await response.json();
                console.log('Control result:', result);
            } catch (error) {
                console.error('Control error:', error);
            }
        });
        
        // Advanced Throughput Graphs
        function setupAdvancedThroughputChart() {
            const ctx = document.getElementById('advancedThroughputChart').getContext('2d');
            advancedThroughputChart = new Chart(ctx, {
                type: 'line',
                data: { labels: [], datasets: [] },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } },
                    scales: { y: { beginAtZero: true } }
                }
            });
        }
        async function fetchThroughputHistory() {
            try {
                const response = await fetch('/api/throughput-history');
                if (!response.ok) return;
                const data = await response.json();
                updateAdvancedThroughputChart(data);
            } catch (e) {}
        }
        function updateAdvancedThroughputChart(data) {
            if (!advancedThroughputChart) return;
            let datasets = [];
            let labels = [];
            if (throughputMode === 'thread') {
                // Each thread is a dataset
                Object.entries(data.thread_throughput).forEach(([tid, history], idx) => {
                    const color = `hsl(${(idx*47)%360},70%,50%)`;
                    const points = history.map(([ts, val]) => ({x: new Date(ts*1000), y: val}));
                    datasets.push({
                        label: `Thread ${tid}`,
                        data: points,
                        borderColor: color,
                        backgroundColor: color + '22',
                        tension: 0.3,
                        pointRadius: 0
                    });
                });
            } else {
                // Each process is a dataset
                Object.entries(data.process_throughput).forEach(([pid, history], idx) => {
                    const color = `hsl(${(idx*67)%360},70%,50%)`;
                    const points = history.map(([ts, val]) => ({x: new Date(ts*1000), y: val}));
                    datasets.push({
                        label: `Process ${pid}`,
                        data: points,
                        borderColor: color,
                        backgroundColor: color + '22',
                        tension: 0.3,
                        pointRadius: 0
                    });
                });
            }
            // Set labels to time axis
            advancedThroughputChart.data.labels = [];
            advancedThroughputChart.data.datasets = datasets;
            advancedThroughputChart.options.scales.x = { type: 'time', time: { unit: 'second' } };
            advancedThroughputChart.update('none');
        }
        document.getElementById('showThreadThroughput').addEventListener('click', () => {
            throughputMode = 'thread';
            fetchThroughputHistory();
        });
        document.getElementById('showProcessThroughput').addEventListener('click', () => {
            throughputMode = 'process';
            fetchThroughputHistory();
        });
        setupAdvancedThroughputChart();
        setInterval(fetchThroughputHistory, 2000);
        fetchThroughputHistory();
        
        // Initialize
        initializeCharts();
        connectWebSocket();
        
        // Fallback polling
        setInterval(async () => {
            if (!ws || ws.readyState !== WebSocket.OPEN) {
                try {
                    const response = await fetch('/api/stats');
                    const data = await response.json();
                    updateDashboard(data);
                } catch (error) {
                    console.error('Failed to fetch stats:', error);
                }
            }
        }, 5000);

        // Heatmap rendering
        function renderHeatmap(data) {
            const container = document.getElementById('activityHeatmap');
            if (!container) return;
            let html = '';
            // Thread heatmap
            if (data.thread_heatmap && data.thread_heatmap.length > 0) {
                html += '<div class="mb-2 font-semibold">Threads</div>';
                html += '<table class="min-w-full text-xs mb-4"><thead><tr><th>ID</th><th>Idle %</th><th>CPU %</th></tr></thead><tbody>';
                data.thread_heatmap.forEach(row => {
                    const idle = row.idle_percentage || 0;
                    const cpu = row.cpu_usage || 0;
                    const color = idle > 70 ? '#10b981' : idle > 30 ? '#f59e0b' : '#ef4444';
                    html += `<tr style="background:${color}22"><td class="px-2">${row.thread_id}</td><td class="px-2">${idle.toFixed(1)}%</td><td class="px-2">${cpu.toFixed(1)}%</td></tr>`;
                });
                html += '</tbody></table>';
            }
            // Process heatmap
            if (data.process_heatmap && data.process_heatmap.length > 0) {
                html += '<div class="mb-2 font-semibold">Processes</div>';
                html += '<table class="min-w-full text-xs"><thead><tr><th>ID</th><th>Idle %</th><th>CPU %</th></tr></thead><tbody>';
                data.process_heatmap.forEach(row => {
                    const idle = row.idle_percentage || 0;
                    const cpu = row.cpu_usage || 0;
                    const color = idle > 70 ? '#10b981' : idle > 30 ? '#f59e0b' : '#ef4444';
                    html += `<tr style="background:${color}22"><td class="px-2">${row.process_id}</td><td class="px-2">${idle.toFixed(1)}%</td><td class="px-2">${cpu.toFixed(1)}%</td></tr>`;
                });
                html += '</tbody></table>';
            }
            container.innerHTML = html;
        }

        // Periodically fetch and update heatmap
        async function fetchHeatmap() {
            try {
                const response = await fetch('/api/activity-heatmap');
                if (response.ok) {
                    const data = await response.json();
                    renderHeatmap(data);
                }
            } catch (e) {
                // Ignore errors
            }
        }
        setInterval(fetchHeatmap, 2000);
        fetchHeatmap();
    </script>
</body>
</html>
        """
    
    def _get_current_stats(self) -> Dict:
        """Get current statistics for the dashboard."""
        return self._get_comprehensive_status()
    
    def _get_pipeline_stats(self) -> Dict:
        """Get pipeline-specific statistics."""
        try:
            return self.logger.get_pipeline_stats()
        except Exception as e:
            return {"error": str(e)}
    
    def _get_process_stats(self) -> Dict:
        """Get process statistics."""
        try:
            return self.logger.get_all_process_stats()
        except Exception as e:
            return {"error": str(e)}
    
    def _get_task_stats(self) -> Dict:
        """Get task statistics."""
        try:
            return self.logger.get_overall_stats()
        except Exception as e:
            return {"error": str(e)}
    
    def run(self):
        """Run the dashboard server."""
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info"
        )

    def notify_checkpoint_saving(self, saving: bool):
        """Set visual indicator for checkpoint saving."""
        self.checkpoint_status["saving"] = saving


def create_dashboard_server(logger: ProcessLogger, host: str = "0.0.0.0", port: int = 8000) -> DashboardServer:
    """Create a dashboard server instance."""
    return DashboardServer(logger, host, port) 