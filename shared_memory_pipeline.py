import os
import time
import multiprocessing as mp
from multiprocessing import shared_memory, Queue, Event, Lock
import numpy as np
import zstandard as zstd
import json
import threading
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import psutil
import pickle


@dataclass
class BufferMetadata:
    """Metadata for shared memory buffers."""
    buffer_id: str
    shape: Tuple[int, ...]
    dtype: str
    size_bytes: int
    created_at: float
    process_id: int
    task_id: str


@dataclass
class CompressionMetadata:
    """Metadata for compressed data."""
    original_buffer_id: str
    compressed_size: int
    compression_ratio: float
    compression_time: float
    algorithm: str


class SharedMemoryManager:
    """Manages shared memory blocks for inter-process communication."""
    
    def __init__(self, max_buffers: int = 10, buffer_size_mb: int = 100):
        self.max_buffers = max_buffers
        self.buffer_size = buffer_size_mb * 1024 * 1024  # Convert to bytes
        
        # Shared memory blocks
        self.shared_buffers: Dict[str, shared_memory.SharedMemory] = {}
        self.buffer_metadata: Dict[str, BufferMetadata] = {}
        
        # Buffer management
        self.available_buffers = Queue(maxsize=max_buffers)
        self.buffer_lock = Lock()
        
        # Initialize shared memory blocks
        self._initialize_buffers()
    
    def _initialize_buffers(self):
        """Initialize shared memory buffers."""
        print(f"Initializing {self.max_buffers} shared memory buffers of {self.buffer_size / (1024*1024):.1f}MB each")
        
        for i in range(self.max_buffers):
            buffer_id = f"buffer_{i}"
            
            # Create shared memory block
            shm = shared_memory.SharedMemory(create=True, size=self.buffer_size, name=buffer_id)
            self.shared_buffers[buffer_id] = shm
            
            # Add to available queue
            self.available_buffers.put(buffer_id)
            
            print(f"Created shared memory buffer: {buffer_id} ({self.buffer_size / (1024*1024):.1f}MB)")
    
    def get_available_buffer(self, timeout: float = 5.0) -> Optional[str]:
        """Get an available buffer ID."""
        try:
            return self.available_buffers.get(timeout=timeout)
        except:
            return None
    
    def return_buffer(self, buffer_id: str):
        """Return a buffer to the available pool."""
        if buffer_id in self.shared_buffers:
            self.available_buffers.put(buffer_id)
    
    def create_numpy_array(self, buffer_id: str, shape: Tuple[int, ...], dtype: str) -> np.ndarray:
        """Create a NumPy array from shared memory."""
        if buffer_id not in self.shared_buffers:
            raise ValueError(f"Buffer {buffer_id} not found")
        
        shm = self.shared_buffers[buffer_id]
        return np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    
    def get_buffer_info(self) -> Dict:
        """Get information about all buffers."""
        with self.buffer_lock:
            return {
                "total_buffers": len(self.shared_buffers),
                "available_buffers": self.available_buffers.qsize(),
                "buffer_size_mb": self.buffer_size / (1024 * 1024),
                "buffer_ids": list(self.shared_buffers.keys()),
                "metadata_count": len(self.buffer_metadata)
            }
    
    def cleanup(self):
        """Clean up shared memory resources."""
        print("Cleaning up shared memory resources...")
        
        for buffer_id, shm in self.shared_buffers.items():
            try:
                shm.close()
                shm.unlink()
                print(f"Cleaned up buffer: {buffer_id}")
            except Exception as e:
                print(f"Error cleaning up buffer {buffer_id}: {e}")


class ProducerProcess:
    """Process that generates or reads matrices and puts them into shared buffers."""
    
    def __init__(self, shared_memory_manager: SharedMemoryManager, 
                 output_queue: Queue, task_queue: Queue, 
                 buffer_return_queue: Queue, process_id: int):
        self.shm_manager = shared_memory_manager
        self.output_queue = output_queue
        self.task_queue = task_queue
        self.buffer_return_queue = buffer_return_queue
        self.process_id = process_id
        self.running = True
        
        # Statistics
        self.stats = {
            "matrices_produced": 0,
            "total_bytes_produced": 0,
            "avg_production_time": 0.0,
            "idle_time": 0.0,
            "last_activity": time.time()
        }
    
    def run(self):
        """Main producer process loop."""
        print(f"Producer process {self.process_id} started")
        
        while self.running:
            try:
                # Check for tasks
                if not self.task_queue.empty():
                    task = self.task_queue.get_nowait()
                    self._process_task(task)
                else:
                    # No tasks, check for returned buffers
                    self._check_returned_buffers()
                    time.sleep(0.1)  # Small delay to prevent busy waiting
                    
            except Exception as e:
                print(f"Error in producer process {self.process_id}: {e}")
                time.sleep(1)
        
        print(f"Producer process {self.process_id} stopped")
    
    def _process_task(self, task: Dict):
        """Process a production task."""
        start_time = time.time()
        
        try:
            # Get available buffer
            buffer_id = self.shm_manager.get_available_buffer()
            if not buffer_id:
                print(f"Producer {self.process_id}: No available buffers")
                return
            
            # Generate matrix
            matrix = self._generate_matrix(task)
            
            # Create NumPy array in shared memory
            array = self.shm_manager.create_numpy_array(
                buffer_id, matrix.shape, str(matrix.dtype)
            )
            
            # Copy matrix data to shared memory
            np.copyto(array, matrix)
            
            # Create metadata
            metadata = BufferMetadata(
                buffer_id=buffer_id,
                shape=matrix.shape,
                dtype=str(matrix.dtype),
                size_bytes=matrix.nbytes,
                created_at=time.time(),
                process_id=self.process_id,
                task_id=task.get("task_id", f"task_{int(time.time())}")
            )
            
            # Send to compressor
            self.output_queue.put({
                "buffer_id": buffer_id,
                "metadata": metadata,
                "task_info": task
            })
            
            # Update statistics
            production_time = time.time() - start_time
            self.stats["matrices_produced"] += 1
            self.stats["total_bytes_produced"] += matrix.nbytes
            self.stats["avg_production_time"] = (
                (self.stats["avg_production_time"] * (self.stats["matrices_produced"] - 1) + production_time) /
                self.stats["matrices_produced"]
            )
            self.stats["last_activity"] = time.time()
            
            print(f"Producer {self.process_id}: Produced matrix {matrix.shape} in {production_time:.3f}s")
            
        except Exception as e:
            print(f"Error processing task in producer {self.process_id}: {e}")
            # Return buffer if we got one
            if 'buffer_id' in locals():
                self.shm_manager.return_buffer(buffer_id)
    
    def _generate_matrix(self, task: Dict) -> np.ndarray:
        """Generate a matrix based on task parameters."""
        size = task.get("size", 100)
        matrix_type = task.get("type", "random")
        
        if matrix_type == "random":
            return np.random.rand(size, size).astype(np.float32)
        elif matrix_type == "identity":
            return np.eye(size, dtype=np.float32)
        elif matrix_type == "zeros":
            return np.zeros((size, size), dtype=np.float32)
        else:
            return np.random.rand(size, size).astype(np.float32)
    
    def _check_returned_buffers(self):
        """Check for buffers returned from writer."""
        try:
            while not self.buffer_return_queue.empty():
                buffer_id = self.buffer_return_queue.get_nowait()
                self.shm_manager.return_buffer(buffer_id)
                print(f"Producer {self.process_id}: Buffer {buffer_id} returned for reuse")
        except:
            pass
    
    def get_stats(self) -> Dict:
        """Get producer statistics."""
        current_time = time.time()
        idle_time = current_time - self.stats["last_activity"]
        
        return {
            **self.stats,
            "current_idle_time": idle_time,
            "idle_probability": min(1.0, idle_time / 10.0)  # Idle if no activity for 10s
        }
    
    def stop(self):
        """Stop the producer process."""
        self.running = False


class CompressorProcess:
    """Process that compresses matrices from shared buffers."""
    
    def __init__(self, shared_memory_manager: SharedMemoryManager,
                 input_queue: Queue, output_queue: Queue, process_id: int):
        self.shm_manager = shared_memory_manager
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.process_id = process_id
        self.running = True
        
        # Compression settings
        self.compressor = zstd.ZstdCompressor(level=3)
        
        # Statistics
        self.stats = {
            "matrices_compressed": 0,
            "total_compression_time": 0.0,
            "avg_compression_ratio": 0.0,
            "idle_time": 0.0,
            "last_activity": time.time()
        }
    
    def run(self):
        """Main compressor process loop."""
        print(f"Compressor process {self.process_id} started")
        
        while self.running:
            try:
                # Get matrix from producer
                if not self.input_queue.empty():
                    data = self.input_queue.get(timeout=1.0)
                    self._compress_matrix(data)
                else:
                    time.sleep(0.1)
                    
            except Exception as e:
                if self.running:  # Only log if not shutting down
                    print(f"Error in compressor process {self.process_id}: {e}")
                time.sleep(1)
        
        print(f"Compressor process {self.process_id} stopped")
    
    def _compress_matrix(self, data: Dict):
        """Compress a matrix from shared memory."""
        start_time = time.time()
        
        try:
            buffer_id = data["buffer_id"]
            metadata = data["metadata"]
            
            # Get NumPy array from shared memory
            array = self.shm_manager.create_numpy_array(
                buffer_id, metadata.shape, metadata.dtype
            )
            
            # Compress the data
            compressed_data = self.compressor.compress(array.tobytes())
            
            # Calculate compression statistics
            compression_time = time.time() - start_time
            compression_ratio = len(compressed_data) / metadata.size_bytes
            
            # Create compression metadata
            comp_metadata = CompressionMetadata(
                original_buffer_id=buffer_id,
                compressed_size=len(compressed_data),
                compression_ratio=compression_ratio,
                compression_time=compression_time,
                algorithm="zstd"
            )
            
            # Send to writer
            self.output_queue.put({
                "compressed_data": compressed_data,
                "original_metadata": metadata,
                "compression_metadata": comp_metadata,
                "task_info": data.get("task_info", {})
            })
            
            # Update statistics
            self.stats["matrices_compressed"] += 1
            self.stats["total_compression_time"] += compression_time
            self.stats["avg_compression_ratio"] = (
                (self.stats["avg_compression_ratio"] * (self.stats["matrices_compressed"] - 1) + compression_ratio) /
                self.stats["matrices_compressed"]
            )
            self.stats["last_activity"] = time.time()
            
            print(f"Compressor {self.process_id}: Compressed {metadata.shape} "
                  f"({compression_ratio:.2f} ratio) in {compression_time:.3f}s")
            
        except Exception as e:
            print(f"Error compressing matrix in compressor {self.process_id}: {e}")
    
    def get_stats(self) -> Dict:
        """Get compressor statistics."""
        current_time = time.time()
        idle_time = current_time - self.stats["last_activity"]
        
        return {
            **self.stats,
            "current_idle_time": idle_time,
            "idle_probability": min(1.0, idle_time / 10.0),
            "avg_compression_time": (
                self.stats["total_compression_time"] / self.stats["matrices_compressed"]
                if self.stats["matrices_compressed"] > 0 else 0.0
            )
        }
    
    def stop(self):
        """Stop the compressor process."""
        self.running = False


class WriterProcess:
    """Process that writes compressed data to disk."""
    
    def __init__(self, shared_memory_manager: SharedMemoryManager,
                 input_queue: Queue, buffer_return_queue: Queue, 
                 output_dir: str, process_id: int):
        self.shm_manager = shared_memory_manager
        self.input_queue = input_queue
        self.buffer_return_queue = buffer_return_queue
        self.output_dir = output_dir
        self.process_id = process_id
        self.running = True
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Statistics
        self.stats = {
            "files_written": 0,
            "total_bytes_written": 0,
            "avg_write_time": 0.0,
            "idle_time": 0.0,
            "last_activity": time.time()
        }
    
    def run(self):
        """Main writer process loop."""
        print(f"Writer process {self.process_id} started")
        
        while self.running:
            try:
                # Get compressed data from compressor
                if not self.input_queue.empty():
                    data = self.input_queue.get(timeout=1.0)
                    self._write_compressed_data(data)
                else:
                    time.sleep(0.1)
                    
            except Exception as e:
                if self.running:  # Only log if not shutting down
                    print(f"Error in writer process {self.process_id}: {e}")
                time.sleep(1)
        
        print(f"Writer process {self.process_id} stopped")
    
    def _write_compressed_data(self, data: Dict):
        """Write compressed data to disk."""
        start_time = time.time()
        
        try:
            compressed_data = data["compressed_data"]
            original_metadata = data["original_metadata"]
            compression_metadata = data["compression_metadata"]
            task_info = data.get("task_info", {})
            
            # Generate filename
            timestamp = int(time.time() * 1000)
            filename = f"matrix_{original_metadata.task_id}_{timestamp}.zst"
            filepath = os.path.join(self.output_dir, filename)
            
            # Write compressed data
            with open(filepath, 'wb') as f:
                # Write header
                header = {
                    "original_metadata": {
                        "shape": original_metadata.shape,
                        "dtype": original_metadata.dtype,
                        "size_bytes": original_metadata.size_bytes,
                        "task_id": original_metadata.task_id
                    },
                    "compression_metadata": {
                        "compressed_size": compression_metadata.compressed_size,
                        "compression_ratio": compression_metadata.compression_ratio,
                        "compression_time": compression_metadata.compression_time,
                        "algorithm": compression_metadata.algorithm
                    },
                    "task_info": task_info,
                    "created_at": time.time()
                }
                
                header_bytes = json.dumps(header).encode('utf-8')
                f.write(len(header_bytes).to_bytes(8, 'big'))
                f.write(header_bytes)
                
                # Write compressed data
                f.write(compressed_data)
            
            # Return buffer for reuse
            self.buffer_return_queue.put(original_metadata.buffer_id)
            
            # Update statistics
            write_time = time.time() - start_time
            self.stats["files_written"] += 1
            self.stats["total_bytes_written"] += len(compressed_data)
            self.stats["avg_write_time"] = (
                (self.stats["avg_write_time"] * (self.stats["files_written"] - 1) + write_time) /
                self.stats["files_written"]
            )
            self.stats["last_activity"] = time.time()
            
            print(f"Writer {self.process_id}: Wrote {filename} "
                  f"({len(compressed_data)} bytes) in {write_time:.3f}s")
            
        except Exception as e:
            print(f"Error writing compressed data in writer {self.process_id}: {e}")
    
    def get_stats(self) -> Dict:
        """Get writer statistics."""
        current_time = time.time()
        idle_time = current_time - self.stats["last_activity"]
        
        return {
            **self.stats,
            "current_idle_time": idle_time,
            "idle_probability": min(1.0, idle_time / 10.0)
        }
    
    def stop(self):
        """Stop the writer process."""
        self.running = False


class SharedMemoryPipeline:
    """Main pipeline coordinator that manages all processes."""
    
    def __init__(self, num_producers: int = 2, num_compressors: int = 2, 
                 num_writers: int = 2, max_buffers: int = 10, 
                 buffer_size_mb: int = 100, output_dir: str = "output"):
        self.num_producers = num_producers
        self.num_compressors = num_compressors
        self.num_writers = num_writers
        self.output_dir = output_dir
        
        # Shared memory manager
        self.shm_manager = SharedMemoryManager(max_buffers, buffer_size_mb)
        
        # Queues for inter-process communication
        self.producer_compressor_queue = Queue(maxsize=50)
        self.compressor_writer_queue = Queue(maxsize=50)
        self.buffer_return_queue = Queue(maxsize=50)
        self.task_queue = Queue()
        
        # Process lists
        self.producer_processes: List[ProducerProcess] = []
        self.compressor_processes: List[CompressorProcess] = []
        self.writer_processes: List[WriterProcess] = []
        self.processes: List[mp.Process] = []
        
        # Control
        self.running = False
        self.shutdown_event = Event()
    
    def start(self):
        """Start the shared memory pipeline."""
        print("Starting Shared Memory Pipeline...")
        print(f"Configuration: {self.num_producers} producers, {self.num_compressors} compressors, {self.num_writers} writers")
        
        self.running = True
        
        # Start producer processes
        for i in range(self.num_producers):
            producer = ProducerProcess(
                self.shm_manager, self.producer_compressor_queue, 
                self.task_queue, self.buffer_return_queue, i
            )
            process = mp.Process(target=producer.run, name=f"Producer-{i}")
            process.start()
            self.producer_processes.append(producer)
            self.processes.append(process)
        
        # Start compressor processes
        for i in range(self.num_compressors):
            compressor = CompressorProcess(
                self.shm_manager, self.producer_compressor_queue, 
                self.compressor_writer_queue, i
            )
            process = mp.Process(target=compressor.run, name=f"Compressor-{i}")
            process.start()
            self.compressor_processes.append(compressor)
            self.processes.append(process)
        
        # Start writer processes
        for i in range(self.num_writers):
            writer = WriterProcess(
                self.shm_manager, self.compressor_writer_queue, 
                self.buffer_return_queue, self.output_dir, i
            )
            process = mp.Process(target=writer.run, name=f"Writer-{i}")
            process.start()
            self.writer_processes.append(writer)
            self.processes.append(process)
        
        print(f"Started {len(self.processes)} processes")
    
    def add_task(self, task: Dict):
        """Add a task to the producer queue."""
        self.task_queue.put(task)
    
    def get_pipeline_stats(self) -> Dict:
        """Get comprehensive pipeline statistics."""
        producer_stats = [p.get_stats() for p in self.producer_processes]
        compressor_stats = [c.get_stats() for c in self.compressor_processes]
        writer_stats = [w.get_stats() for w in self.writer_processes]
        
        return {
            "pipeline_status": "running" if self.running else "stopped",
            "buffer_info": self.shm_manager.get_buffer_info(),
            "queue_sizes": {
                "task_queue": self.task_queue.qsize(),
                "producer_compressor_queue": self.producer_compressor_queue.qsize(),
                "compressor_writer_queue": self.compressor_writer_queue.qsize(),
                "buffer_return_queue": self.buffer_return_queue.qsize()
            },
            "producer_stats": producer_stats,
            "compressor_stats": compressor_stats,
            "writer_stats": writer_stats,
            "total_processes": len(self.processes),
            "active_processes": sum(1 for p in self.processes if p.is_alive())
        }
    
    def stop(self):
        """Stop the pipeline gracefully."""
        print("Stopping Shared Memory Pipeline...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop all processes
        for process in self.processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
        
        # Cleanup shared memory
        self.shm_manager.cleanup()
        
        print("Shared Memory Pipeline stopped") 