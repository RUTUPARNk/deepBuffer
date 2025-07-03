import threading
import time
from typing import Dict, List, Tuple, Optional
import numpy as np
import os
from logger import ProcessLogger

class Benchmarking:
    """
    Monitors and benchmarks system performance in real-time for the pipeline.
    Tracks throughput, buffer usage, queue depths, compression ratio, and trends.
    Pushes rolling window stats to logger for dashboard updates.
    """
    def __init__(self, logger: ProcessLogger, window_sec: int = 10):
        self.logger = logger
        self.window_sec = window_sec
        self.metrics_history: Dict[str, List[Tuple[float, float]]] = {
            'producer_rate': [],
            'compressor_rate': [],
            'writer_rate': [],
            'buffer_usage': [],
            'queue_depth': [],
            'compression_ratio': [],
        }
        self.lock = threading.Lock()
        self.running = False
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)

    def start(self):
        self.running = True
        self.monitor_thread.start()

    def stop(self):
        self.running = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2)

    def record_metric(self, name: str, value: float):
        with self.lock:
            self.metrics_history[name].append((time.time(), value))
            # Keep only last window_sec
            cutoff = time.time() - self.window_sec
            self.metrics_history[name] = [x for x in self.metrics_history[name] if x[0] >= cutoff]

    def get_rolling_average(self, name: str) -> float:
        with self.lock:
            values = [v for t, v in self.metrics_history[name] if t >= time.time() - self.window_sec]
            return float(np.mean(values)) if values else 0.0

    def _monitor_loop(self):
        while self.running:
            # Example: Pull stats from logger and record
            stats = self.logger.get_pipeline_stats()
            process_stats = stats.get('process_stats', {})
            # Aggregate rates
            prod_rate = sum(p.get('current_throughput', 0) for p in process_stats.values() if 'producer' in p.get('process_name', ''))
            comp_rate = sum(p.get('current_throughput', 0) for p in process_stats.values() if 'compressor' in p.get('process_name', ''))
            writ_rate = sum(p.get('current_throughput', 0) for p in process_stats.values() if 'writer' in p.get('process_name', ''))
            queue_depth = sum(p.get('current_queue_size', 0) for p in process_stats.values())
            comp_ratio = np.mean([p.get('current_compression_ratio', 0) for p in process_stats.values() if 'compressor' in p.get('process_name', '')])
            # Buffer usage (simulate or pull from buffer manager if available)
            buffer_usage = stats.get('total_buffer_usage', 0)
            # Record
            self.record_metric('producer_rate', prod_rate)
            self.record_metric('compressor_rate', comp_rate)
            self.record_metric('writer_rate', writ_rate)
            self.record_metric('queue_depth', queue_depth)
            self.record_metric('compression_ratio', comp_ratio)
            self.record_metric('buffer_usage', buffer_usage)
            # Push to logger for dashboard
            self.logger.log_throughput(-1, self.get_rolling_average('writer_rate'))  # -1: system-wide
            time.sleep(1)

    def get_metrics(self) -> Dict[str, float]:
        """Get current rolling averages for all metrics."""
        return {k: self.get_rolling_average(k) for k in self.metrics_history}

    def disk_write_benchmark(self, path: str = '/tmp', size_mb: int = 100) -> float:
        """
        Benchmark raw disk write speed (MB/s).
        """
        arr = np.random.bytes(size_mb * 1024 * 1024)
        filename = os.path.join(path, f'benchmark_{int(time.time())}.bin')
        start = time.time()
        with open(filename, 'wb') as f:
            f.write(arr)
        elapsed = time.time() - start
        os.remove(filename)
        return size_mb / elapsed if elapsed > 0 else 0.0 