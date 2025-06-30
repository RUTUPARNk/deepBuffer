import os
import threading
import queue
import time
import numpy as np
import zstandard as zstd
from typing import Dict, List, Optional, Callable, Any
import mmap
import tempfile
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import jax
import jax.numpy as jnp


class MatrixWriter:
    """Handles memory-mapped file writing with JAX transformations, compression, and indexing."""
    
    def __init__(self, output_dir: str = "output", chunk_size: int = 1024):
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.compressor = zstd.ZstdCompressor(level=3)
        self.decompressor = zstd.ZstdDecompressor()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Index file to track matrix metadata
        self.index_file = os.path.join(output_dir, "matrix_index.json")
        self.index_lock = threading.Lock()
        self._load_index()
    
    def _load_index(self):
        """Load or create the matrix index file."""
        if os.path.exists(self.index_file):
            with open(self.index_file, 'r') as f:
                self.matrix_index = json.load(f)
        else:
            self.matrix_index = {"matrices": {}, "next_id": 0}
            self._save_index()
    
    def _save_index(self):
        """Save the matrix index to file."""
        with self.index_lock:
            with open(self.index_file, 'w') as f:
                json.dump(self.matrix_index, f, indent=2)
    
    def write_matrix(self, matrix: np.ndarray, name: str, 
                    transformations: Optional[List[Callable]] = None) -> str:
        """Write a matrix to memory-mapped file with optional JAX transformations."""
        matrix_id = str(self.matrix_index["next_id"])
        self.matrix_index["next_id"] += 1
        
        # Apply transformations if provided
        if transformations:
            matrix = self._apply_transformations(matrix, transformations)
        
        # Create memory-mapped file
        filename = os.path.join(self.output_dir, f"matrix_{matrix_id}.mmap")
        shape = matrix.shape
        dtype = matrix.dtype
        
        # Write matrix to memory-mapped file
        with open(filename, 'wb') as f:
            # Write header with shape and dtype info
            header = {
                "shape": shape,
                "dtype": str(dtype),
                "chunk_size": self.chunk_size
            }
            header_bytes = json.dumps(header).encode('utf-8')
            f.write(len(header_bytes).to_bytes(8, 'big'))
            f.write(header_bytes)
            
            # Write matrix data
            matrix.tofile(f)
        
        # Update index
        self.matrix_index["matrices"][matrix_id] = {
            "name": name,
            "filename": filename,
            "shape": shape,
            "dtype": str(dtype),
            "created_at": time.time()
        }
        self._save_index()
        
        return matrix_id
    
    def _apply_transformations(self, matrix: np.ndarray, 
                             transformations: List[Callable]) -> np.ndarray:
        """Apply JAX transformations to the matrix."""
        # Convert to JAX array
        jax_matrix = jnp.array(matrix)
        
        # Apply each transformation
        for transform in transformations:
            jax_matrix = transform(jax_matrix)
        
        # Convert back to numpy
        return np.array(jax_matrix)
    
    def read_matrix(self, matrix_id: str) -> np.ndarray:
        """Read a matrix from memory-mapped file."""
        if matrix_id not in self.matrix_index["matrices"]:
            raise ValueError(f"Matrix ID {matrix_id} not found")
        
        matrix_info = self.matrix_index["matrices"][matrix_id]
        filename = matrix_info["filename"]
        
        with open(filename, 'rb') as f:
            # Read header
            header_size = int.from_bytes(f.read(8), 'big')
            header_bytes = f.read(header_size)
            header = json.loads(header_bytes.decode('utf-8'))
            
            # Memory map the file
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Skip header
                mm.seek(8 + header_size)
                
                # Read matrix data
                matrix = np.frombuffer(mm, dtype=header["dtype"])
                matrix = matrix.reshape(header["shape"])
                
                return matrix
    
    def compress_matrix(self, matrix_id: str) -> str:
        """Compress a matrix using zstandard."""
        matrix = self.read_matrix(matrix_id)
        
        # Compress the matrix
        compressed_data = self.compressor.compress(matrix.tobytes())
        
        # Save compressed data
        compressed_filename = os.path.join(self.output_dir, f"matrix_{matrix_id}_compressed.zst")
        with open(compressed_filename, 'wb') as f:
            f.write(compressed_data)
        
        # Update index
        self.matrix_index["matrices"][matrix_id]["compressed_file"] = compressed_filename
        self._save_index()
        
        return compressed_filename
    
    def decompress_matrix(self, matrix_id: str) -> np.ndarray:
        """Decompress a matrix from zstandard format."""
        if matrix_id not in self.matrix_index["matrices"]:
            raise ValueError(f"Matrix ID {matrix_id} not found")
        
        matrix_info = self.matrix_index["matrices"][matrix_id]
        if "compressed_file" not in matrix_info:
            raise ValueError(f"Matrix {matrix_id} is not compressed")
        
        compressed_filename = matrix_info["compressed_file"]
        
        with open(compressed_filename, 'rb') as f:
            compressed_data = f.read()
        
        # Decompress
        decompressed_data = self.decompressor.decompress(compressed_data)
        
        # Convert back to matrix
        matrix = np.frombuffer(decompressed_data, dtype=matrix_info["dtype"])
        matrix = matrix.reshape(matrix_info["shape"])
        
        return matrix


class LoadBalancedController:
    """Manages dynamic task queue, threads, and load balancing."""
    
    def __init__(self, num_threads: int = 4, max_queue_size: int = 100):
        self.num_threads = num_threads
        self.task_queue = queue.Queue(maxsize=max_queue_size)
        self.threads: List[threading.Thread] = []
        self.running = False
        self.thread_logger = None  # Will be set externally
        self.matrix_writer = MatrixWriter()
        
        # Thread status tracking
        self.thread_status = {}
        self.status_lock = threading.Lock()
    
    def set_logger(self, logger):
        """Set the thread logger for tracking task execution."""
        self.thread_logger = logger
    
    def start(self):
        """Start the load-balanced controller and worker threads."""
        self.running = True
        
        # Create and start worker threads
        for i in range(self.num_threads):
            thread = threading.Thread(target=self._worker, args=(i,), daemon=True)
            thread.start()
            self.threads.append(thread)
            
            # Initialize thread status
            with self.status_lock:
                self.thread_status[i] = {
                    "idle": True,
                    "current_task": None,
                    "tasks_completed": 0
                }
        
        print(f"Started {self.num_threads} worker threads")
    
    def stop(self):
        """Stop the load-balanced controller."""
        self.running = False
        
        # Wait for all threads to finish
        for thread in self.threads:
            thread.join()
        
        print("All worker threads stopped")
    
    def add_task(self, task_func: Callable, *args, **kwargs) -> bool:
        """Add a task to the queue."""
        try:
            self.task_queue.put((task_func, args, kwargs), timeout=1)
            return True
        except queue.Full:
            return False
    
    def _worker(self, thread_id: int):
        """Worker function that processes tasks from the queue."""
        print(f"Worker thread {thread_id} started")
        
        while self.running:
            try:
                # Get task from queue with timeout
                task_func, args, kwargs = self.task_queue.get(timeout=1)
                
                # Update thread status
                with self.status_lock:
                    self.thread_status[thread_id]["idle"] = False
                    self.thread_status[thread_id]["current_task"] = str(task_func.__name__)
                
                # Log task start
                if self.thread_logger:
                    self.thread_logger.log_task_start(thread_id, str(task_func.__name__))
                
                # Execute task
                try:
                    result = task_func(*args, **kwargs)
                    
                    # Update completion count
                    with self.status_lock:
                        self.thread_status[thread_id]["tasks_completed"] += 1
                    
                except Exception as e:
                    print(f"Error in worker {thread_id}: {e}")
                    result = None
                
                # Log task end
                if self.thread_logger:
                    self.thread_logger.log_task_end(thread_id)
                
                # Update thread status
                with self.status_lock:
                    self.thread_status[thread_id]["idle"] = True
                    self.thread_status[thread_id]["current_task"] = None
                
                # Mark task as done
                self.task_queue.task_done()
                
            except queue.Empty:
                # No tasks available, thread is idle
                with self.status_lock:
                    self.thread_status[thread_id]["idle"] = True
                    self.thread_status[thread_id]["current_task"] = None
                continue
    
    def get_thread_status(self) -> Dict[int, Dict]:
        """Get current status of all threads."""
        with self.status_lock:
            return self.thread_status.copy()
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        return self.task_queue.qsize()
    
    def wait_for_completion(self):
        """Wait for all tasks in the queue to be completed."""
        self.task_queue.join()


def worker(task_func: Callable, *args, **kwargs) -> Any:
    """Generic worker function for task execution."""
    return task_func(*args, **kwargs)


# Example task functions
def create_random_matrix(size: int, name: str) -> str:
    """Create and write a random matrix."""
    matrix = np.random.rand(size, size)
    writer = MatrixWriter()
    return writer.write_matrix(matrix, name)


def apply_matrix_transformations(matrix_id: str, transformations: List[Callable]) -> str:
    """Apply transformations to a matrix."""
    writer = MatrixWriter()
    matrix = writer.read_matrix(matrix_id)
    return writer.write_matrix(matrix, f"transformed_{matrix_id}", transformations)


def compress_matrix_task(matrix_id: str) -> str:
    """Compress a matrix."""
    writer = MatrixWriter()
    return writer.compress_matrix(matrix_id) 