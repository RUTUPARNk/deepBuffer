import os
import multiprocessing as mp
import threading
import queue
import time
import numpy as np
import zstandard as zstd
from typing import Dict, List, Optional, Callable, Any, Tuple
import mmap
import json
import psutil
from concurrent.futures import ProcessPoolExecutor, as_completed
import jax
import jax.numpy as jnp
from multiprocessing import Manager, Lock, Value, Array
import pickle


class MultiprocessMatrixWriter:
    """Multiprocessing version of MatrixWriter that bypasses the GIL."""
    
    def __init__(self, output_dir: str = "output", num_processes: int = None, chunk_size: int = 1024):
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.num_processes = num_processes or mp.cpu_count()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Shared memory manager for inter-process communication
        self.manager = Manager()
        self.shared_index = self.manager.dict()
        self.index_lock = Lock()
        
        # Process pool for parallel operations
        self.process_pool = ProcessPoolExecutor(max_workers=self.num_processes)
        
        # Load or create index
        self._load_index()
    
    def _load_index(self):
        """Load or create the matrix index file."""
        index_file = os.path.join(self.output_dir, "matrix_index.json")
        if os.path.exists(index_file):
            with open(index_file, 'r') as f:
                index_data = json.load(f)
                self.shared_index.update(index_data)
        else:
            self.shared_index.update({"matrices": {}, "next_id": 0})
            self._save_index()
    
    def _save_index(self):
        """Save the matrix index to file."""
        with self.index_lock:
            index_file = os.path.join(self.output_dir, "matrix_index.json")
            with open(index_file, 'w') as f:
                json.dump(dict(self.shared_index), f, indent=2)
    
    def write_matrix_parallel(self, matrix: np.ndarray, name: str, 
                            transformations: Optional[List[Callable]] = None) -> str:
        """Write a matrix using parallel processing."""
        # Get next matrix ID
        with self.index_lock:
            matrix_id = str(self.shared_index["next_id"])
            self.shared_index["next_id"] += 1
        
        # Submit parallel processing task
        future = self.process_pool.submit(
            self._write_matrix_worker,
            matrix_id, matrix, name, transformations, self.output_dir, self.chunk_size
        )
        
        # Wait for completion
        result = future.result()
        
        # Update shared index
        with self.index_lock:
            self.shared_index["matrices"][matrix_id] = result
            self._save_index()
        
        return matrix_id
    
    @staticmethod
    def _write_matrix_worker(matrix_id: str, matrix: np.ndarray, name: str,
                           transformations: Optional[List[Callable]], 
                           output_dir: str, chunk_size: int) -> Dict:
        """Worker function for parallel matrix writing."""
        # Apply transformations if provided
        if transformations:
            matrix = MultiprocessMatrixWriter._apply_transformations_worker(matrix, transformations)
        
        # Create memory-mapped file
        filename = os.path.join(output_dir, f"matrix_{matrix_id}.mmap")
        shape = matrix.shape
        dtype = matrix.dtype
        
        # Write matrix to memory-mapped file
        with open(filename, 'wb') as f:
            header = {
                "shape": shape,
                "dtype": str(dtype),
                "chunk_size": chunk_size
            }
            header_bytes = json.dumps(header).encode('utf-8')
            f.write(len(header_bytes).to_bytes(8, 'big'))
            f.write(header_bytes)
            matrix.tofile(f)
        
        return {
            "name": name,
            "filename": filename,
            "shape": shape,
            "dtype": str(dtype),
            "created_at": time.time()
        }
    
    @staticmethod
    def _apply_transformations_worker(matrix: np.ndarray, 
                                    transformations: List[Callable]) -> np.ndarray:
        """Apply JAX transformations in worker process."""
        jax_matrix = jnp.array(matrix)
        for transform in transformations:
            jax_matrix = transform(jax_matrix)
        return np.array(jax_matrix)
    
    def read_matrix_parallel(self, matrix_id: str) -> np.ndarray:
        """Read a matrix using parallel processing."""
        if matrix_id not in self.shared_index["matrices"]:
            raise ValueError(f"Matrix ID {matrix_id} not found")
        
        matrix_info = self.shared_index["matrices"][matrix_id]
        filename = matrix_info["filename"]
        
        # Submit parallel reading task
        future = self.process_pool.submit(
            self._read_matrix_worker, filename
        )
        
        return future.result()
    
    @staticmethod
    def _read_matrix_worker(filename: str) -> np.ndarray:
        """Worker function for parallel matrix reading."""
        with open(filename, 'rb') as f:
            header_size = int.from_bytes(f.read(8), 'big')
            header_bytes = f.read(header_size)
            header = json.loads(header_bytes.decode('utf-8'))
            
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                mm.seek(8 + header_size)
                matrix = np.frombuffer(mm, dtype=header["dtype"])
                matrix = matrix.reshape(header["shape"])
                return matrix
    
    def compress_matrix_parallel(self, matrix_id: str) -> str:
        """Compress a matrix using parallel processing."""
        if matrix_id not in self.shared_index["matrices"]:
            raise ValueError(f"Matrix ID {matrix_id} not found")
        
        matrix_info = self.shared_index["matrices"][matrix_id]
        filename = matrix_info["filename"]
        compressed_filename = os.path.join(self.output_dir, f"matrix_{matrix_id}_compressed.zst")
        
        # Submit parallel compression task
        future = self.process_pool.submit(
            self._compress_matrix_worker, filename, compressed_filename
        )
        
        result = future.result()
        
        # Update index
        with self.index_lock:
            self.shared_index["matrices"][matrix_id]["compressed_file"] = compressed_filename
            self._save_index()
        
        return compressed_filename
    
    @staticmethod
    def _compress_matrix_worker(filename: str, compressed_filename: str) -> str:
        """Worker function for parallel matrix compression."""
        # Read matrix
        with open(filename, 'rb') as f:
            header_size = int.from_bytes(f.read(8), 'big')
            f.read(header_size)  # Skip header
            matrix_data = f.read()
        
        # Compress
        compressor = zstd.ZstdCompressor(level=3)
        compressed_data = compressor.compress(matrix_data)
        
        # Write compressed data
        with open(compressed_filename, 'wb') as f:
            f.write(compressed_data)
        
        return compressed_filename
    
    def batch_write_matrices(self, matrices: List[Tuple[np.ndarray, str, Optional[List[Callable]]]]) -> List[str]:
        """Write multiple matrices in parallel."""
        futures = []
        
        for matrix, name, transformations in matrices:
            with self.index_lock:
                matrix_id = str(self.shared_index["next_id"])
                self.shared_index["next_id"] += 1
            
            future = self.process_pool.submit(
                self._write_matrix_worker,
                matrix_id, matrix, name, transformations, self.output_dir, self.chunk_size
            )
            futures.append((matrix_id, future))
        
        # Collect results
        results = []
        for matrix_id, future in futures:
            result = future.result()
            with self.index_lock:
                self.shared_index["matrices"][matrix_id] = result
            results.append(matrix_id)
        
        self._save_index()
        return results
    
    def get_system_stats(self) -> Dict:
        """Get system statistics for monitoring."""
        cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
        memory = psutil.virtual_memory()
        
        return {
            "cpu_usage_per_core": cpu_percent,
            "cpu_usage_total": sum(cpu_percent) / len(cpu_percent),
            "memory_usage_percent": memory.percent,
            "memory_available_gb": memory.available / (1024**3),
            "num_processes": self.num_processes,
            "active_processes": len(self.process_pool._processes)
        }
    
    def shutdown(self):
        """Shutdown the process pool."""
        self.process_pool.shutdown(wait=True)


class MultiprocessLoadBalancedController:
    """Multiprocessing version of LoadBalancedController for full CPU saturation."""
    
    def __init__(self, num_processes: int = None, max_queue_size: int = 100):
        self.num_processes = num_processes or mp.cpu_count()
        self.max_queue_size = max_queue_size
        
        # Shared task queue
        self.manager = Manager()
        self.task_queue = self.manager.Queue(maxsize=max_queue_size)
        self.result_queue = self.manager.Queue()
        
        # Shared state
        self.shared_status = self.manager.dict()
        self.status_lock = Lock()
        self.running = Value('b', False)
        
        # Process pool
        self.process_pool = ProcessPoolExecutor(max_workers=self.num_processes)
        self.matrix_writer = MultiprocessMatrixWriter(num_processes=self.num_processes)
        
        # Initialize status
        self._init_status()
    
    def _init_status(self):
        """Initialize shared status for all processes."""
        for i in range(self.num_processes):
            self.shared_status[i] = {
                "idle": True,
                "current_task": None,
                "tasks_completed": 0,
                "cpu_usage": 0.0,
                "memory_usage": 0.0
            }
    
    def start(self):
        """Start the multiprocessing controller."""
        self.running.value = True
        
        # Start worker processes
        self.workers = []
        for i in range(self.num_processes):
            worker = mp.Process(
                target=self._worker_process,
                args=(i, self.task_queue, self.result_queue, self.shared_status, self.status_lock)
            )
            worker.start()
            self.workers.append(worker)
        
        print(f"Started {self.num_processes} worker processes")
    
    def stop(self):
        """Stop the multiprocessing controller."""
        self.running.value = False
        
        # Wait for all workers to finish
        for worker in self.workers:
            worker.join()
        
        self.process_pool.shutdown(wait=True)
        self.matrix_writer.shutdown()
        print("All worker processes stopped")
    
    def add_task(self, task_func: Callable, *args, **kwargs) -> bool:
        """Add a task to the queue."""
        try:
            # Serialize task for inter-process communication
            task_data = {
                "func_name": task_func.__name__,
                "args": args,
                "kwargs": kwargs,
                "timestamp": time.time()
            }
            self.task_queue.put(task_data, timeout=1)
            return True
        except queue.Full:
            return False
    
    @staticmethod
    def _worker_process(process_id: int, task_queue, result_queue, shared_status, status_lock):
        """Worker process function."""
        print(f"Worker process {process_id} started")
        
        while True:
            try:
                # Get task from queue
                task_data = task_queue.get(timeout=1)
                
                # Update status
                with status_lock:
                    shared_status[process_id]["idle"] = False
                    shared_status[process_id]["current_task"] = task_data["func_name"]
                
                # Execute task
                start_time = time.time()
                try:
                    # Import and execute task function
                    if task_data["func_name"] == "create_random_matrix":
                        result = MultiprocessLoadBalancedController._create_random_matrix_worker(
                            *task_data["args"], **task_data["kwargs"]
                        )
                    elif task_data["func_name"] == "apply_matrix_transformations":
                        result = MultiprocessLoadBalancedController._apply_transformations_worker(
                            *task_data["args"], **task_data["kwargs"]
                        )
                    else:
                        result = None
                    
                    # Update completion count
                    with status_lock:
                        shared_status[process_id]["tasks_completed"] += 1
                        shared_status[process_id]["cpu_usage"] = psutil.cpu_percent()
                        shared_status[process_id]["memory_usage"] = psutil.virtual_memory().percent
                    
                except Exception as e:
                    print(f"Error in worker process {process_id}: {e}")
                    result = None
                
                # Update status
                with status_lock:
                    shared_status[process_id]["idle"] = True
                    shared_status[process_id]["current_task"] = None
                
                # Put result in result queue
                result_queue.put({
                    "process_id": process_id,
                    "result": result,
                    "duration": time.time() - start_time
                })
                
            except queue.Empty:
                # No tasks available
                with status_lock:
                    shared_status[process_id]["idle"] = True
                    shared_status[process_id]["current_task"] = None
                continue
    
    @staticmethod
    def _create_random_matrix_worker(size: int, name: str) -> str:
        """Worker function for creating random matrices."""
        matrix = np.random.rand(size, size)
        writer = MultiprocessMatrixWriter()
        return writer.write_matrix_parallel(matrix, name)
    
    @staticmethod
    def _apply_transformations_worker(matrix_id: str, transformations: List[Callable]) -> str:
        """Worker function for applying transformations."""
        writer = MultiprocessMatrixWriter()
        matrix = writer.read_matrix_parallel(matrix_id)
        return writer.write_matrix_parallel(matrix, f"transformed_{matrix_id}", transformations)
    
    def get_process_status(self) -> Dict[int, Dict]:
        """Get current status of all processes."""
        with self.status_lock:
            return dict(self.shared_status)
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        return self.task_queue.qsize()
    
    def get_system_stats(self) -> Dict:
        """Get comprehensive system statistics."""
        return {
            "matrix_writer_stats": self.matrix_writer.get_system_stats(),
            "process_status": self.get_process_status(),
            "queue_size": self.get_queue_size(),
            "total_cpu_usage": psutil.cpu_percent(interval=1),
            "total_memory_usage": psutil.virtual_memory().percent
        } 