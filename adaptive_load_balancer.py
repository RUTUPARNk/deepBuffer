import threading
import time
from typing import Dict, List, Optional
from logger import ProcessLogger

class LoadBalancer:
    """
    Adaptive load balancer for dynamic task assignment in the shared memory pipeline.
    Monitors process idle times and reassigns tasks to maximize throughput.
    Supports live scaling and real-time dashboard integration.
    """
    def __init__(self, logger: ProcessLogger, window_sec: int = 15):
        self.logger = logger
        self.window_sec = window_sec
        self.idle_history: Dict[str, List[float]] = {
            'producer': [],
            'compressor': [],
            'writer': []
        }
        self.lock = threading.Lock()
        self.running = False
        self.scaling_callback: Optional[callable] = None
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)

    def start(self):
        self.running = True
        self.monitor_thread.start()

    def stop(self):
        self.running = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2)

    def set_scaling_callback(self, callback):
        """Set a callback for live scaling (add/remove compressors)."""
        self.scaling_callback = callback

    def record_idle(self, process_type: str, idle_percentage: float):
        with self.lock:
            if process_type in self.idle_history:
                self.idle_history[process_type].append((time.time(), idle_percentage))
                # Keep only last window_sec
                cutoff = time.time() - self.window_sec
                self.idle_history[process_type] = [x for x in self.idle_history[process_type] if x[0] >= cutoff]

    def get_moving_average(self, process_type: str) -> float:
        with self.lock:
            values = [v for t, v in self.idle_history[process_type] if t >= time.time() - self.window_sec]
            return float(sum(values)) / len(values) if values else 0.0

    def _monitor_loop(self):
        while self.running:
            stats = self.logger.get_pipeline_stats()
            process_stats = stats.get('process_stats', {})
            # Record idle percentages
            for p in process_stats.values():
                ptype = 'unknown'
                if 'producer' in p.get('process_name', ''):
                    ptype = 'producer'
                elif 'compressor' in p.get('process_name', ''):
                    ptype = 'compressor'
                elif 'writer' in p.get('process_name', ''):
                    ptype = 'writer'
                idle = p.get('idle_percentage', 0.0)
                self.record_idle(ptype, idle)
            # Make load balancing decisions
            self._balance_load(process_stats)
            time.sleep(1)

    def _balance_load(self, process_stats: Dict):
        # Example: scale compressors if average idle < 20% for 10s
        avg_compressor_idle = self.get_moving_average('compressor')
        decision = None
        if avg_compressor_idle < 20.0 and self.scaling_callback:
            self.scaling_callback('add_compressor')
            decision = 'add_compressor'
        elif avg_compressor_idle > 80.0 and self.scaling_callback:
            self.scaling_callback('remove_compressor')
            decision = 'remove_compressor'
        # Push metrics and decisions to logger
        self.logger.log_error('INFO', f'LoadBalancer decision: {decision}, avg_compressor_idle={avg_compressor_idle:.1f}')
        self.logger.log_throughput(-2, avg_compressor_idle)  # -2: load balancer metric

    def assign_task(self, task_type: str, candidates: List[int], process_stats: Dict[int, Dict]) -> Optional[int]:
        """
        Assign a task to the least busy candidate process/thread.
        """
        min_idle = float('inf')
        selected = None
        for pid in candidates:
            idle = process_stats.get(pid, {}).get('idle_percentage', 100.0)
            if idle < min_idle:
                min_idle = idle
                selected = pid
        return selected

class AdaptiveLoadBalancer(LoadBalancer):
    def __init__(self, logger: ProcessLogger, tensor_manager: 'TensorBufferManager', window_sec: int = 15):
        super().__init__(logger, window_sec)
        self.tensor_manager = tensor_manager

    def assign_task(self, task_type: str, candidates: List[int], process_stats: Dict[int, Dict], batch_size: int = 1) -> Optional[int]:
        """
        Assign a tensor batch to the least busy candidate, considering buffer pool state and batch size.
        Only assign if enough buffers are available.
        """
        buffer_state = self.tensor_manager.get_buffer_state()
        if buffer_state['available'] < batch_size:
            return None  # Not enough buffers, wait
        min_idle = float('inf')
        selected = None
        for pid in candidates:
            idle = process_stats.get(pid, {}).get('idle_percentage', 100.0)
            if idle < min_idle:
                min_idle = idle
                selected = pid
        return selected 