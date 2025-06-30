import time
import threading
import psutil
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict, deque
import statistics
import os
from datetime import datetime


@dataclass
class ThreadActivity:
    """Represents activity of a single thread."""
    thread_id: int
    task_id: Optional[str]
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    idle_time: float = 0.0
    status: str = "idle"


@dataclass
class ProcessActivity:
    """Represents activity of a single process."""
    process_id: int
    process_name: str
    task_id: Optional[str]
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    idle_time: float = 0.0
    status: str = "idle"
    buffer_usage: int = 0
    queue_size: int = 0
    throughput: float = 0.0
    compression_ratio: float = 0.0
    write_speed: float = 0.0


@dataclass
class TaskInfo:
    """Information about a task."""
    task_id: str
    task_type: str
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration: Optional[float] = None
    assigned_thread: Optional[int] = None
    assigned_process: Optional[int] = None
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemMetrics:
    """System-wide performance metrics."""
    timestamp: float
    total_cpu_usage: float
    total_memory_usage: float
    disk_read_bytes: int
    disk_write_bytes: int
    network_bytes_sent: int
    network_bytes_recv: int
    active_processes: int
    total_throughput: float
    avg_compression_ratio: float
    queue_depth: int
    buffer_utilization: float


class ThreadLogger:
    """Enhanced logger for tracking thread activities and idle times."""
    
    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        
        # Thread activity tracking
        self.thread_activities: Dict[int, ThreadActivity] = {}
        self.thread_history: Dict[int, deque] = defaultdict(lambda: deque(maxlen=max_history))
        
        # Task tracking
        self.tasks: Dict[str, TaskInfo] = {}
        self.task_history: deque = deque(maxlen=max_history)
        
        # Statistics
        self.stats = {
            "total_threads": 0,
            "active_threads": 0,
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "total_idle_time": 0.0,
            "total_cpu_time": 0.0
        }
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Start monitoring thread
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_threads, daemon=True)
        self.monitor_thread.start()
    
    def log_thread_start(self, thread_id: int, task_id: Optional[str] = None):
        """Log when a thread starts working on a task."""
        with self.lock:
            current_time = time.time()
            
            # End previous activity if exists
            if thread_id in self.thread_activities:
                self._end_thread_activity(thread_id, current_time)
            
            # Start new activity
            activity = ThreadActivity(
                thread_id=thread_id,
                task_id=task_id,
                start_time=current_time,
                status="active"
            )
            
            self.thread_activities[thread_id] = activity
            
            # Update task if provided
            if task_id and task_id in self.tasks:
                self.tasks[task_id].started_at = current_time
                self.tasks[task_id].assigned_thread = thread_id
                self.tasks[task_id].status = "running"
            
            self.stats["active_threads"] = max(self.stats["active_threads"], len(self.thread_activities))
    
    def log_thread_end(self, thread_id: int, task_id: Optional[str] = None):
        """Log when a thread finishes a task."""
        with self.lock:
            current_time = time.time()
            
            if thread_id in self.thread_activities:
                self._end_thread_activity(thread_id, current_time)
            
            # Update task if provided
            if task_id and task_id in self.tasks:
                self.tasks[task_id].completed_at = current_time
                if self.tasks[task_id].started_at:
                    self.tasks[task_id].duration = current_time - self.tasks[task_id].started_at
                self.tasks[task_id].status = "completed"
                self.stats["completed_tasks"] += 1
    
    def log_thread_idle(self, thread_id: int, idle_time: float):
        """Log idle time for a thread."""
        with self.lock:
            if thread_id in self.thread_activities:
                self.thread_activities[thread_id].idle_time += idle_time
                self.thread_activities[thread_id].status = "idle"
            
            self.stats["total_idle_time"] += idle_time
    
    def log_task_created(self, task_id: str, task_type: str, **metadata):
        """Log when a new task is created."""
        with self.lock:
            task = TaskInfo(
                task_id=task_id,
                task_type=task_type,
                created_at=time.time(),
                metadata=metadata
            )
            
            self.tasks[task_id] = task
            self.task_history.append(task)
            self.stats["total_tasks"] += 1
    
    def log_task_failed(self, task_id: str, error: str):
        """Log when a task fails."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = "failed"
                self.tasks[task_id].metadata["error"] = error
                self.stats["failed_tasks"] += 1
    
    def _end_thread_activity(self, thread_id: int, end_time: float):
        """End a thread activity and add to history."""
        activity = self.thread_activities[thread_id]
        activity.end_time = end_time
        activity.duration = end_time - activity.start_time
        
        # Get CPU and memory usage
        try:
            process = psutil.Process()
            activity.cpu_usage = process.cpu_percent()
            activity.memory_usage = process.memory_percent()
        except:
            pass
        
        # Add to history
        self.thread_history[thread_id].append(activity)
        
        # Update statistics
        self.stats["total_cpu_time"] += activity.duration
        
        # Remove from current activities
        del self.thread_activities[thread_id]
    
    def _monitor_threads(self):
        """Background thread for monitoring thread states."""
        while self.monitoring_active:
            try:
                current_time = time.time()
                
                # Update idle times for active threads
                with self.lock:
                    for thread_id, activity in self.thread_activities.items():
                        if activity.status == "idle":
                            idle_time = current_time - activity.start_time
                            activity.idle_time = idle_time
                
                time.sleep(1)  # Monitor every second
                
            except Exception as e:
                print(f"Error in thread monitoring: {e}")
                time.sleep(5)
    
    def get_thread_stats(self, thread_id: int) -> Dict:
        """Get statistics for a specific thread."""
        with self.lock:
            if thread_id not in self.thread_history:
                return {}
            
            activities = list(self.thread_history[thread_id])
            if not activities:
                return {}
            
            total_duration = sum(a.duration or 0 for a in activities)
            total_idle_time = sum(a.idle_time for a in activities)
            avg_cpu_usage = sum(a.cpu_usage for a in activities) / len(activities)
            avg_memory_usage = sum(a.memory_usage for a in activities) / len(activities)
            
            return {
                "thread_id": thread_id,
                "total_activities": len(activities),
                "total_duration": total_duration,
                "total_idle_time": total_idle_time,
                "idle_percentage": (total_idle_time / total_duration * 100) if total_duration > 0 else 0,
                "avg_cpu_usage": avg_cpu_usage,
                "avg_memory_usage": avg_memory_usage,
                "current_status": self.thread_activities.get(thread_id, {}).get("status", "unknown")
            }
    
    def get_all_thread_stats(self) -> Dict[int, Dict]:
        """Get statistics for all threads."""
        with self.lock:
            all_thread_ids = set(self.thread_history.keys()) | set(self.thread_activities.keys())
            return {thread_id: self.get_thread_stats(thread_id) for thread_id in all_thread_ids}
    
    def get_task_stats(self, task_id: str) -> Dict:
        """Get statistics for a specific task."""
        with self.lock:
            if task_id not in self.tasks:
                return {}
            
            task = self.tasks[task_id]
            return {
                "task_id": task_id,
                "task_type": task.task_type,
                "status": task.status,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "duration": task.duration,
                "assigned_thread": task.assigned_thread,
                "metadata": task.metadata
            }
    
    def get_overall_stats(self) -> Dict:
        """Get overall system statistics."""
        with self.lock:
            return {
                **self.stats,
                "current_active_threads": len(self.thread_activities),
                "total_threads_tracked": len(set(self.thread_history.keys()) | set(self.thread_activities.keys())),
                "pending_tasks": sum(1 for task in self.tasks.values() if task.status == "pending"),
                "running_tasks": sum(1 for task in self.tasks.values() if task.status == "running"),
                "completed_tasks": sum(1 for task in self.tasks.values() if task.status == "completed"),
                "failed_tasks": sum(1 for task in self.tasks.values() if task.status == "failed")
            }
    
    def export_logs(self, filename: str):
        """Export logs to a JSON file."""
        with self.lock:
            export_data = {
                "export_timestamp": time.time(),
                "stats": self.stats,
                "tasks": {
                    task_id: {
                        "task_type": task.task_type,
                        "created_at": task.created_at,
                        "started_at": task.started_at,
                        "completed_at": task.completed_at,
                        "duration": task.duration,
                        "assigned_thread": task.assigned_thread,
                        "status": task.status,
                        "metadata": task.metadata
                    }
                    for task_id, task in self.tasks.items()
                },
                "thread_activities": {
                    thread_id: [
                        {
                            "task_id": activity.task_id,
                            "start_time": activity.start_time,
                            "end_time": activity.end_time,
                            "duration": activity.duration,
                            "cpu_usage": activity.cpu_usage,
                            "memory_usage": activity.memory_usage,
                            "idle_time": activity.idle_time,
                            "status": activity.status
                        }
                        for activity in activities
                    ]
                    for thread_id, activities in self.thread_history.items()
                }
            }
            
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2)
    
    def shutdown(self):
        """Shutdown the logger."""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)


class ProcessLogger(ThreadLogger):
    """Enhanced logger for tracking process activities in the shared memory pipeline."""
    
    def __init__(self, max_history: int = 1000):
        super().__init__(max_history)
        
        # Process activity tracking
        self.process_activities: Dict[int, ProcessActivity] = {}
        self.process_history: Dict[int, deque] = defaultdict(lambda: deque(maxlen=max_history))
        
        # Buffer tracking
        self.buffer_usage: Dict[int, int] = defaultdict(int)
        self.queue_sizes: Dict[int, int] = defaultdict(int)
        
        # Performance metrics
        self.throughput_metrics: Dict[int, float] = defaultdict(float)
        self.compression_metrics: Dict[int, float] = defaultdict(float)
        self.write_speed_metrics: Dict[int, float] = defaultdict(float)
        
        # System metrics history
        self.system_metrics_history: deque = deque(maxlen=max_history)
        
        # Error tracking
        self.error_logs: deque = deque(maxlen=100)
        
        # WebSocket update data
        self.last_websocket_update = time.time()
        self.websocket_update_interval = 1.0  # 1 second
    
    def log_process_start(self, process_id: int, process_name: str, task_id: Optional[str] = None):
        """Log when a process starts working on a task."""
        with self.lock:
            current_time = time.time()
            
            # End previous activity if exists
            if process_id in self.process_activities:
                self._end_process_activity(process_id, current_time)
            
            # Start new activity
            activity = ProcessActivity(
                process_id=process_id,
                process_name=process_name,
                task_id=task_id,
                start_time=current_time,
                status="active",
                buffer_usage=self.buffer_usage[process_id],
                queue_size=self.queue_sizes[process_id],
                throughput=self.throughput_metrics[process_id],
                compression_ratio=self.compression_metrics[process_id],
                write_speed=self.write_speed_metrics[process_id]
            )
            
            self.process_activities[process_id] = activity
            
            # Update task if provided
            if task_id and task_id in self.tasks:
                self.tasks[task_id].started_at = current_time
                self.tasks[task_id].assigned_process = process_id
                self.tasks[task_id].status = "running"
    
    def log_process_end(self, process_id: int, task_id: Optional[str] = None):
        """Log when a process finishes a task."""
        with self.lock:
            current_time = time.time()
            
            if process_id in self.process_activities:
                self._end_process_activity(process_id, current_time)
            
            # Update task if provided
            if task_id and task_id in self.tasks:
                self.tasks[task_id].completed_at = current_time
                if self.tasks[task_id].started_at:
                    self.tasks[task_id].duration = current_time - self.tasks[task_id].started_at
                self.tasks[task_id].status = "completed"
                self.stats["completed_tasks"] += 1
    
    def log_process_idle(self, process_id: int, idle_time: float):
        """Log idle time for a process."""
        with self.lock:
            if process_id in self.process_activities:
                self.process_activities[process_id].idle_time += idle_time
                self.process_activities[process_id].status = "idle"
            
            self.stats["total_idle_time"] += idle_time
    
    def log_buffer_usage(self, process_id: int, buffer_count: int):
        """Log buffer usage for a process."""
        with self.lock:
            self.buffer_usage[process_id] = buffer_count
            if process_id in self.process_activities:
                self.process_activities[process_id].buffer_usage = buffer_count
    
    def log_queue_size(self, process_id: int, queue_size: int):
        """Log queue size for a process."""
        with self.lock:
            self.queue_sizes[process_id] = queue_size
            if process_id in self.process_activities:
                self.process_activities[process_id].queue_size = queue_size
    
    def log_throughput(self, process_id: int, throughput: float):
        """Log throughput for a process."""
        with self.lock:
            self.throughput_metrics[process_id] = throughput
            if process_id in self.process_activities:
                self.process_activities[process_id].throughput = throughput
    
    def log_compression_ratio(self, process_id: int, compression_ratio: float):
        """Log compression ratio for a process."""
        with self.lock:
            self.compression_metrics[process_id] = compression_ratio
            if process_id in self.process_activities:
                self.process_activities[process_id].compression_ratio = compression_ratio
    
    def log_write_speed(self, process_id: int, write_speed: float):
        """Log write speed for a process."""
        with self.lock:
            self.write_speed_metrics[process_id] = write_speed
            if process_id in self.process_activities:
                self.process_activities[process_id].write_speed = write_speed
    
    def log_error(self, level: str, message: str, process_id: Optional[int] = None):
        """Log an error or warning message."""
        with self.lock:
            error_log = {
                "timestamp": time.time(),
                "level": level.upper(),
                "message": message,
                "process_id": process_id
            }
            self.error_logs.append(error_log)
    
    def _end_process_activity(self, process_id: int, end_time: float):
        """End a process activity and add to history."""
        activity = self.process_activities[process_id]
        activity.end_time = end_time
        activity.duration = end_time - activity.start_time
        
        # Get CPU and memory usage
        try:
            process = psutil.Process()
            activity.cpu_usage = process.cpu_percent()
            activity.memory_usage = process.memory_percent()
        except:
            pass
        
        # Add to history
        self.process_history[process_id].append(activity)
        
        # Update statistics
        self.stats["total_cpu_time"] += activity.duration
        
        # Remove from current activities
        del self.process_activities[process_id]
    
    def collect_system_metrics(self):
        """Collect system-wide performance metrics."""
        try:
            # CPU and memory usage
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            disk_read = disk_io.read_bytes if disk_io else 0
            disk_write = disk_io.write_bytes if disk_io else 0
            
            # Network I/O
            network_io = psutil.net_io_counters()
            net_sent = network_io.bytes_sent if network_io else 0
            net_recv = network_io.bytes_recv if network_io else 0
            
            # Process metrics
            active_processes = len(self.process_activities)
            total_throughput = sum(self.throughput_metrics.values())
            avg_compression_ratio = sum(self.compression_metrics.values()) / max(len(self.compression_metrics), 1)
            queue_depth = sum(self.queue_sizes.values())
            buffer_utilization = sum(self.buffer_usage.values()) / max(sum(self.buffer_usage.values()) + 1, 1) * 100
            
            metrics = SystemMetrics(
                timestamp=time.time(),
                total_cpu_usage=cpu_percent,
                total_memory_usage=memory.percent,
                disk_read_bytes=disk_read,
                disk_write_bytes=disk_write,
                network_bytes_sent=net_sent,
                network_bytes_recv=net_recv,
                active_processes=active_processes,
                total_throughput=total_throughput,
                avg_compression_ratio=avg_compression_ratio,
                queue_depth=queue_depth,
                buffer_utilization=buffer_utilization
            )
            
            self.system_metrics_history.append(metrics)
            return metrics
            
        except Exception as e:
            self.log_error("ERROR", f"Failed to collect system metrics: {e}")
            return None
    
    def get_process_stats(self, process_id: int) -> Dict:
        """Get statistics for a specific process."""
        with self.lock:
            if process_id not in self.process_history:
                return {}
            
            activities = list(self.process_history[process_id])
            if not activities:
                return {}
            
            total_duration = sum(a.duration or 0 for a in activities)
            total_idle_time = sum(a.idle_time for a in activities)
            avg_cpu_usage = sum(a.cpu_usage for a in activities) / len(activities)
            avg_memory_usage = sum(a.memory_usage for a in activities) / len(activities)
            avg_buffer_usage = sum(a.buffer_usage for a in activities) / len(activities)
            avg_queue_size = sum(a.queue_size for a in activities) / len(activities)
            avg_throughput = sum(a.throughput for a in activities) / len(activities)
            avg_compression_ratio = sum(a.compression_ratio for a in activities) / len(activities)
            avg_write_speed = sum(a.write_speed for a in activities) / len(activities)
            
            return {
                "process_id": process_id,
                "process_name": activities[0].process_name if activities else "unknown",
                "total_activities": len(activities),
                "total_duration": total_duration,
                "total_idle_time": total_idle_time,
                "idle_percentage": (total_idle_time / total_duration * 100) if total_duration > 0 else 0,
                "avg_cpu_usage": avg_cpu_usage,
                "avg_memory_usage": avg_memory_usage,
                "avg_buffer_usage": avg_buffer_usage,
                "avg_queue_size": avg_queue_size,
                "avg_throughput": avg_throughput,
                "avg_compression_ratio": avg_compression_ratio,
                "avg_write_speed": avg_write_speed,
                "current_status": self.process_activities.get(process_id, {}).get("status", "unknown"),
                "current_buffer_usage": self.buffer_usage.get(process_id, 0),
                "current_queue_size": self.queue_sizes.get(process_id, 0),
                "current_throughput": self.throughput_metrics.get(process_id, 0),
                "current_compression_ratio": self.compression_metrics.get(process_id, 0),
                "current_write_speed": self.write_speed_metrics.get(process_id, 0)
            }
    
    def get_all_process_stats(self) -> Dict[int, Dict]:
        """Get statistics for all processes."""
        with self.lock:
            all_process_ids = set(self.process_history.keys()) | set(self.process_activities.keys())
            return {process_id: self.get_process_stats(process_id) for process_id in all_process_ids}
    
    def get_pipeline_stats(self) -> Dict:
        """Get comprehensive pipeline statistics."""
        with self.lock:
            process_stats = self.get_all_process_stats()
            
            # Calculate pipeline-wide metrics
            total_processes = len(process_stats)
            active_processes = sum(1 for stats in process_stats.values() if stats.get("current_status") == "active")
            total_buffer_usage = sum(stats.get("current_buffer_usage", 0) for stats in process_stats.values())
            total_queue_size = sum(stats.get("current_queue_size", 0) for stats in process_stats.values())
            total_throughput = sum(stats.get("current_throughput", 0) for stats in process_stats.values())
            avg_compression_ratio = sum(stats.get("current_compression_ratio", 0) for stats in process_stats.values()) / max(total_processes, 1)
            
            return {
                "total_processes": total_processes,
                "active_processes": active_processes,
                "idle_processes": total_processes - active_processes,
                "total_buffer_usage": total_buffer_usage,
                "total_queue_size": total_queue_size,
                "total_throughput": total_throughput,
                "avg_compression_ratio": avg_compression_ratio,
                "process_stats": process_stats,
                "overall_stats": self.get_overall_stats()
            }
    
    def get_websocket_update_data(self) -> Dict:
        """Get data for WebSocket updates."""
        current_time = time.time()
        
        # Only update if enough time has passed
        if current_time - self.last_websocket_update < self.websocket_update_interval:
            return {}
        
        self.last_websocket_update = current_time
        
        # Collect system metrics
        system_metrics = self.collect_system_metrics()
        
        # Get comprehensive stats
        pipeline_stats = self.get_pipeline_stats()
        
        # Get error logs
        error_logs = list(self.error_logs)[-10:]  # Last 10 errors
        
        return {
            "timestamp": current_time,
            "pipeline_stats": pipeline_stats,
            "system_metrics": system_metrics.__dict__ if system_metrics else {},
            "error_logs": error_logs,
            "update_interval": self.websocket_update_interval
        }
    
    def export_process_logs(self, filename: str):
        """Export process logs to a JSON file."""
        with self.lock:
            export_data = {
                "export_timestamp": time.time(),
                "stats": self.stats,
                "tasks": {
                    task_id: {
                        "task_type": task.task_type,
                        "created_at": task.created_at,
                        "started_at": task.started_at,
                        "completed_at": task.completed_at,
                        "duration": task.duration,
                        "assigned_thread": task.assigned_thread,
                        "assigned_process": task.assigned_process,
                        "status": task.status,
                        "metadata": task.metadata
                    }
                    for task_id, task in self.tasks.items()
                },
                "process_activities": {
                    process_id: [
                        {
                            "process_name": activity.process_name,
                            "task_id": activity.task_id,
                            "start_time": activity.start_time,
                            "end_time": activity.end_time,
                            "duration": activity.duration,
                            "cpu_usage": activity.cpu_usage,
                            "memory_usage": activity.memory_usage,
                            "idle_time": activity.idle_time,
                            "status": activity.status,
                            "buffer_usage": activity.buffer_usage,
                            "queue_size": activity.queue_size,
                            "throughput": activity.throughput,
                            "compression_ratio": activity.compression_ratio,
                            "write_speed": activity.write_speed
                        }
                        for activity in activities
                    ]
                    for process_id, activities in self.process_history.items()
                },
                "current_buffer_usage": dict(self.buffer_usage),
                "current_queue_sizes": dict(self.queue_sizes),
                "current_throughput": dict(self.throughput_metrics),
                "current_compression_ratios": dict(self.compression_metrics),
                "current_write_speeds": dict(self.write_speed_metrics),
                "error_logs": list(self.error_logs),
                "system_metrics": [m.__dict__ for m in self.system_metrics_history]
            }
            
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2) 