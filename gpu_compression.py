import numpy as np
import time
from typing import List, Optional, Tuple, Dict
import os
import json
import mmap
from concurrent.futures import ThreadPoolExecutor
import threading


class GPUCompressor:
    """GPU-based compression using CUDA for high-performance batch compression."""
    
    def __init__(self, use_gpu: bool = True, compression_level: int = 3):
        self.use_gpu = use_gpu
        self.compression_level = compression_level
        self.gpu_available = False
        
        # Try to initialize GPU
        if self.use_gpu:
            self.gpu_available = self._init_gpu()
        
        if not self.gpu_available:
            print("GPU not available, falling back to CPU compression")
    
    def _init_gpu(self) -> bool:
        """Initialize GPU for compression operations."""
        try:
            import cupy as cp
            self.cp = cp
            # Test GPU availability
            test_array = cp.array([1, 2, 3, 4, 5])
            result = cp.sum(test_array)
            print(f"GPU initialized successfully: {cp.cuda.Device(0).name}")
            return True
        except ImportError:
            print("CuPy not available, GPU compression disabled")
            return False
        except Exception as e:
            print(f"GPU initialization failed: {e}")
            return False
    
    def compress_batch_gpu(self, matrices: List[np.ndarray], 
                          output_dir: str = "output") -> List[str]:
        """Compress multiple matrices using GPU acceleration."""
        if not self.gpu_available:
            return self._compress_batch_cpu(matrices, output_dir)
        
        print(f"Compressing {len(matrices)} matrices using GPU...")
        start_time = time.time()
        
        # Transfer matrices to GPU
        gpu_matrices = []
        for matrix in matrices:
            gpu_matrix = self.cp.asarray(matrix)
            gpu_matrices.append(gpu_matrix)
        
        # Perform GPU-based compression
        compressed_files = []
        for i, gpu_matrix in enumerate(gpu_matrices):
            compressed_file = self._compress_single_gpu(gpu_matrix, f"matrix_{i}", output_dir)
            compressed_files.append(compressed_file)
        
        compression_time = time.time() - start_time
        print(f"GPU compression completed in {compression_time:.2f} seconds")
        
        return compressed_files
    
    def _compress_single_gpu(self, gpu_matrix: 'cp.ndarray', name: str, output_dir: str) -> str:
        """Compress a single matrix using GPU."""
        # Convert to bytes on GPU
        gpu_bytes = gpu_matrix.tobytes()
        
        # Apply GPU-based compression algorithm
        compressed_data = self._gpu_compression_algorithm(gpu_bytes)
        
        # Transfer back to CPU
        compressed_cpu = self.cp.asnumpy(compressed_data)
        
        # Save compressed data
        filename = os.path.join(output_dir, f"{name}_gpu_compressed.bin")
        with open(filename, 'wb') as f:
            # Write header
            header = {
                "shape": gpu_matrix.shape,
                "dtype": str(gpu_matrix.dtype),
                "compression_type": "gpu",
                "original_size": len(gpu_bytes),
                "compressed_size": len(compressed_cpu)
            }
            header_bytes = json.dumps(header).encode('utf-8')
            f.write(len(header_bytes).to_bytes(8, 'big'))
            f.write(header_bytes)
            
            # Write compressed data
            f.write(compressed_cpu)
        
        return filename
    
    def _gpu_compression_algorithm(self, gpu_bytes: 'cp.ndarray') -> 'cp.ndarray':
        """GPU-based compression algorithm using CUDA kernels."""
        # This is a simplified GPU compression algorithm
        # In practice, you would implement more sophisticated algorithms
        
        # Reshape bytes to 32-bit integers for processing
        gpu_ints = gpu_bytes.view(self.cp.uint32)
        
        # Apply run-length encoding on GPU
        compressed = self._gpu_run_length_encode(gpu_ints)
        
        return compressed
    
    def _gpu_run_length_encode(self, data: 'cp.ndarray') -> 'cp.ndarray':
        """GPU-based run-length encoding."""
        # Simplified RLE implementation
        # In practice, you would use CUDA kernels for this
        
        # Find runs of identical values
        diff = self.cp.diff(data)
        run_starts = self.cp.concatenate(([True], diff != 0))
        run_values = data[run_starts]
        run_lengths = self.cp.diff(self.cp.concatenate((self.cp.where(run_starts)[0], [len(data)])))
        
        # Pack into compressed format
        compressed = self.cp.empty(len(run_values) * 2, dtype=self.cp.uint32)
        compressed[0::2] = run_values
        compressed[1::2] = run_lengths
        
        return compressed
    
    def _compress_batch_cpu(self, matrices: List[np.ndarray], output_dir: str) -> List[str]:
        """Fallback CPU compression when GPU is not available."""
        import zstandard as zstd
        
        print(f"Compressing {len(matrices)} matrices using CPU...")
        start_time = time.time()
        
        compressor = zstd.ZstdCompressor(level=self.compression_level)
        compressed_files = []
        
        for i, matrix in enumerate(matrices):
            # Convert matrix to bytes
            matrix_bytes = matrix.tobytes()
            
            # Compress
            compressed_data = compressor.compress(matrix_bytes)
            
            # Save compressed data
            filename = os.path.join(output_dir, f"matrix_{i}_cpu_compressed.zst")
            with open(filename, 'wb') as f:
                # Write header
                header = {
                    "shape": matrix.shape,
                    "dtype": str(matrix.dtype),
                    "compression_type": "cpu_zstd",
                    "original_size": len(matrix_bytes),
                    "compressed_size": len(compressed_data)
                }
                header_bytes = json.dumps(header).encode('utf-8')
                f.write(len(header_bytes).to_bytes(8, 'big'))
                f.write(header_bytes)
                
                # Write compressed data
                f.write(compressed_data)
            
            compressed_files.append(filename)
        
        compression_time = time.time() - start_time
        print(f"CPU compression completed in {compression_time:.2f} seconds")
        
        return compressed_files
    
    def decompress_gpu(self, filename: str) -> np.ndarray:
        """Decompress a GPU-compressed file."""
        with open(filename, 'rb') as f:
            # Read header
            header_size = int.from_bytes(f.read(8), 'big')
            header_bytes = f.read(header_size)
            header = json.loads(header_bytes.decode('utf-8'))
            
            # Read compressed data
            compressed_data = f.read()
        
        if header["compression_type"] == "gpu":
            return self._decompress_single_gpu(compressed_data, header)
        else:
            return self._decompress_cpu(compressed_data, header)
    
    def _decompress_single_gpu(self, compressed_data: bytes, header: Dict) -> np.ndarray:
        """Decompress GPU-compressed data."""
        if not self.gpu_available:
            raise RuntimeError("GPU not available for decompression")
        
        # Transfer to GPU
        gpu_compressed = self.cp.frombuffer(compressed_data, dtype=self.cp.uint32)
        
        # Decompress on GPU
        gpu_decompressed = self._gpu_decompression_algorithm(gpu_compressed, header)
        
        # Transfer back to CPU
        return self.cp.asnumpy(gpu_decompressed)
    
    def _gpu_decompression_algorithm(self, gpu_compressed: 'cp.ndarray', header: Dict) -> 'cp.ndarray':
        """GPU-based decompression algorithm."""
        # Reconstruct original data from RLE format
        run_values = gpu_compressed[0::2]
        run_lengths = gpu_compressed[1::2]
        
        # Expand runs
        total_length = int(self.cp.sum(run_lengths))
        decompressed = self.cp.empty(total_length, dtype=self.cp.uint32)
        
        current_pos = 0
        for value, length in zip(run_values, run_lengths):
            decompressed[current_pos:current_pos + length] = value
            current_pos += length
        
        # Reshape to original shape
        original_shape = tuple(header["shape"])
        decompressed = decompressed.reshape(original_shape)
        
        return decompressed
    
    def _decompress_cpu(self, compressed_data: bytes, header: Dict) -> np.ndarray:
        """Decompress CPU-compressed data."""
        import zstandard as zstd
        
        decompressor = zstd.ZstdDecompressor()
        decompressed_bytes = decompressor.decompress(compressed_data)
        
        # Convert back to matrix
        dtype = np.dtype(header["dtype"])
        matrix = np.frombuffer(decompressed_bytes, dtype=dtype)
        matrix = matrix.reshape(header["shape"])
        
        return matrix
    
    def benchmark_compression(self, matrices: List[np.ndarray]) -> Dict:
        """Benchmark GPU vs CPU compression performance."""
        print("Running compression benchmark...")
        
        # Test GPU compression
        gpu_start = time.time()
        gpu_files = self.compress_batch_gpu(matrices, "benchmark_gpu")
        gpu_time = time.time() - gpu_start
        
        # Test CPU compression
        cpu_start = time.time()
        cpu_files = self._compress_batch_cpu(matrices, "benchmark_cpu")
        cpu_time = time.time() - cpu_start
        
        # Calculate compression ratios
        total_original_size = sum(matrix.nbytes for matrix in matrices)
        gpu_compressed_size = sum(os.path.getsize(f) for f in gpu_files)
        cpu_compressed_size = sum(os.path.getsize(f) for f in cpu_files)
        
        results = {
            "gpu_time": gpu_time,
            "cpu_time": cpu_time,
            "gpu_speedup": cpu_time / gpu_time if gpu_time > 0 else float('inf'),
            "gpu_compression_ratio": total_original_size / gpu_compressed_size,
            "cpu_compression_ratio": total_original_size / cpu_compressed_size,
            "gpu_available": self.gpu_available
        }
        
        print(f"Benchmark Results:")
        print(f"  GPU Time: {gpu_time:.2f}s")
        print(f"  CPU Time: {cpu_time:.2f}s")
        print(f"  GPU Speedup: {results['gpu_speedup']:.2f}x")
        print(f"  GPU Compression Ratio: {results['gpu_compression_ratio']:.2f}")
        print(f"  CPU Compression Ratio: {results['cpu_compression_ratio']:.2f}")
        
        return results


class HybridCompressor:
    """Hybrid compressor that automatically chooses GPU or CPU based on data size."""
    
    def __init__(self, gpu_threshold_mb: int = 100):
        self.gpu_threshold_mb = gpu_threshold_mb
        self.gpu_compressor = GPUCompressor()
        self.cpu_compressor = GPUCompressor(use_gpu=False)
    
    def compress_batch(self, matrices: List[np.ndarray], output_dir: str = "output") -> List[str]:
        """Compress matrices using hybrid approach."""
        total_size_mb = sum(matrix.nbytes for matrix in matrices) / (1024 * 1024)
        
        if total_size_mb > self.gpu_threshold_mb and self.gpu_compressor.gpu_available:
            print(f"Using GPU compression for {total_size_mb:.1f}MB of data")
            return self.gpu_compressor.compress_batch_gpu(matrices, output_dir)
        else:
            print(f"Using CPU compression for {total_size_mb:.1f}MB of data")
            return self.cpu_compressor._compress_batch_cpu(matrices, output_dir)
    
    def get_compression_stats(self) -> Dict:
        """Get compression statistics and recommendations."""
        return {
            "gpu_available": self.gpu_compressor.gpu_available,
            "gpu_threshold_mb": self.gpu_threshold_mb,
            "recommended_approach": "GPU" if self.gpu_compressor.gpu_available else "CPU"
        } 