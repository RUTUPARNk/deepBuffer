import os
import json
import mmap
import numpy as np
import time
from typing import Dict, List, Optional, Tuple, Union
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import hashlib


class FastMatrixReader:
    """Fast file reader with index-first loading for efficient random access."""
    
    def __init__(self, data_dir: str = "output", cache_size: int = 100):
        self.data_dir = data_dir
        self.cache_size = cache_size
        
        # Memory cache for frequently accessed matrices
        self.cache: Dict[str, np.ndarray] = {}
        self.cache_timestamps: Dict[str, float] = {}
        self.cache_lock = threading.Lock()
        
        # Index cache for fast lookups
        self.index_cache: Dict[str, Dict] = {}
        self.index_file = os.path.join(data_dir, "matrix_index.json")
        
        # Thread pool for parallel reading
        self.reader_pool = ThreadPoolExecutor(max_workers=4)
        
        # Load index
        self._load_index()
    
    def _load_index(self):
        """Load the matrix index into memory for fast access."""
        if os.path.exists(self.index_file):
            with open(self.index_file, 'r') as f:
                self.index_cache = json.load(f)
            print(f"Loaded index with {len(self.index_cache.get('matrices', {}))} matrices")
        else:
            self.index_cache = {"matrices": {}, "next_id": 0}
            print("No index found, creating new one")
    
    def _update_cache(self, matrix_id: str, matrix: np.ndarray):
        """Update the memory cache with LRU eviction."""
        with self.cache_lock:
            current_time = time.time()
            
            # Add new matrix to cache
            self.cache[matrix_id] = matrix
            self.cache_timestamps[matrix_id] = current_time
            
            # Evict oldest entries if cache is full
            if len(self.cache) > self.cache_size:
                oldest_id = min(self.cache_timestamps.keys(), 
                              key=lambda k: self.cache_timestamps[k])
                del self.cache[oldest_id]
                del self.cache_timestamps[oldest_id]
    
    def read_matrix_fast(self, matrix_id: str, use_cache: bool = True) -> np.ndarray:
        """Read a matrix with fast access using cache and memory mapping."""
        # Check cache first
        if use_cache:
            with self.cache_lock:
                if matrix_id in self.cache:
                    # Update timestamp for LRU
                    self.cache_timestamps[matrix_id] = time.time()
                    return self.cache[matrix_id].copy()
        
        # Check if matrix exists in index
        if matrix_id not in self.index_cache.get("matrices", {}):
            raise ValueError(f"Matrix ID {matrix_id} not found in index")
        
        matrix_info = self.index_cache["matrices"][matrix_id]
        filename = matrix_info["filename"]
        
        # Read matrix using memory mapping
        matrix = self._read_matrix_mmap(filename, matrix_info)
        
        # Cache the result
        if use_cache:
            self._update_cache(matrix_id, matrix)
        
        return matrix
    
    def _read_matrix_mmap(self, filename: str, matrix_info: Dict) -> np.ndarray:
        """Read matrix using memory mapping for fast access."""
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Matrix file not found: {filename}")
        
        with open(filename, 'rb') as f:
            # Read header
            header_size = int.from_bytes(f.read(8), 'big')
            header_bytes = f.read(header_size)
            header = json.loads(header_bytes.decode('utf-8'))
            
            # Memory map the file for fast access
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Skip header
                mm.seek(8 + header_size)
                
                # Read matrix data
                matrix = np.frombuffer(mm, dtype=header["dtype"])
                matrix = matrix.reshape(header["shape"])
                
                return matrix
    
    def read_matrices_batch(self, matrix_ids: List[str], 
                          parallel: bool = True) -> Dict[str, np.ndarray]:
        """Read multiple matrices in parallel for batch operations."""
        if not parallel:
            return {matrix_id: self.read_matrix_fast(matrix_id) 
                   for matrix_id in matrix_ids}
        
        # Submit parallel reading tasks
        futures = {}
        for matrix_id in matrix_ids:
            future = self.reader_pool.submit(self.read_matrix_fast, matrix_id)
            futures[future] = matrix_id
        
        # Collect results
        results = {}
        for future in as_completed(futures):
            matrix_id = futures[future]
            try:
                matrix = future.result()
                results[matrix_id] = matrix
            except Exception as e:
                print(f"Error reading matrix {matrix_id}: {e}")
        
        return results
    
    def read_matrix_slice(self, matrix_id: str, 
                         slice_indices: Tuple[slice, ...]) -> np.ndarray:
        """Read a slice of a matrix without loading the entire matrix."""
        if matrix_id not in self.index_cache.get("matrices", {}):
            raise ValueError(f"Matrix ID {matrix_id} not found in index")
        
        matrix_info = self.index_cache["matrices"][matrix_id]
        filename = matrix_info["filename"]
        
        return self._read_matrix_slice_mmap(filename, matrix_info, slice_indices)
    
    def _read_matrix_slice_mmap(self, filename: str, matrix_info: Dict, 
                               slice_indices: Tuple[slice, ...]) -> np.ndarray:
        """Read a slice of matrix using memory mapping."""
        with open(filename, 'rb') as f:
            # Read header
            header_size = int.from_bytes(f.read(8), 'big')
            header_bytes = f.read(header_size)
            header = json.loads(header_bytes.decode('utf-8'))
            
            # Calculate slice offsets
            shape = tuple(header["shape"])
            dtype = np.dtype(header["dtype"])
            item_size = dtype.itemsize
            
            # Calculate start and end positions for the slice
            start_pos = self._calculate_slice_offset(shape, slice_indices, item_size)
            end_pos = self._calculate_slice_end(shape, slice_indices, item_size)
            
            # Memory map and read slice
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                mm.seek(8 + header_size + start_pos)
                slice_data = mm.read(end_pos - start_pos)
                
                # Convert to numpy array
                slice_array = np.frombuffer(slice_data, dtype=dtype)
                
                # Reshape to slice shape
                slice_shape = self._calculate_slice_shape(shape, slice_indices)
                return slice_array.reshape(slice_shape)
    
    def _calculate_slice_offset(self, shape: Tuple[int, ...], 
                               slice_indices: Tuple[slice, ...], 
                               item_size: int) -> int:
        """Calculate the byte offset for the start of a slice."""
        offset = 0
        stride = 1
        
        for i in range(len(shape) - 1, -1, -1):
            if i < len(slice_indices):
                start = slice_indices[i].start or 0
                offset += start * stride
            stride *= shape[i]
        
        return offset * item_size
    
    def _calculate_slice_end(self, shape: Tuple[int, ...], 
                           slice_indices: Tuple[slice, ...], 
                           item_size: int) -> int:
        """Calculate the byte offset for the end of a slice."""
        offset = 0
        stride = 1
        
        for i in range(len(shape) - 1, -1, -1):
            if i < len(slice_indices):
                stop = slice_indices[i].stop or shape[i]
                offset += stop * stride
            else:
                offset += shape[i] * stride
            stride *= shape[i]
        
        return offset * item_size
    
    def _calculate_slice_shape(self, shape: Tuple[int, ...], 
                             slice_indices: Tuple[slice, ...]) -> Tuple[int, ...]:
        """Calculate the shape of a slice."""
        slice_shape = []
        for i, dim_size in enumerate(shape):
            if i < len(slice_indices):
                slice_obj = slice_indices[i]
                start = slice_obj.start or 0
                stop = slice_obj.stop or dim_size
                step = slice_obj.step or 1
                slice_shape.append((stop - start + step - 1) // step)
            else:
                slice_shape.append(dim_size)
        return tuple(slice_shape)
    
    def search_matrices(self, criteria: Dict) -> List[str]:
        """Search for matrices based on criteria."""
        matching_ids = []
        
        for matrix_id, matrix_info in self.index_cache.get("matrices", {}).items():
            if self._matches_criteria(matrix_info, criteria):
                matching_ids.append(matrix_id)
        
        return matching_ids
    
    def _matches_criteria(self, matrix_info: Dict, criteria: Dict) -> bool:
        """Check if a matrix matches the search criteria."""
        for key, value in criteria.items():
            if key not in matrix_info:
                return False
            
            if isinstance(value, (list, tuple)):
                if matrix_info[key] not in value:
                    return False
            elif isinstance(value, dict):
                if not isinstance(matrix_info[key], dict):
                    return False
                for sub_key, sub_value in value.items():
                    if sub_key not in matrix_info[key] or matrix_info[key][sub_key] != sub_value:
                        return False
            else:
                if matrix_info[key] != value:
                    return False
        
        return True
    
    def get_matrix_info(self, matrix_id: str) -> Dict:
        """Get information about a matrix without loading it."""
        if matrix_id not in self.index_cache.get("matrices", {}):
            raise ValueError(f"Matrix ID {matrix_id} not found in index")
        
        return self.index_cache["matrices"][matrix_id].copy()
    
    def list_matrices(self, limit: Optional[int] = None) -> List[Dict]:
        """List all matrices with their metadata."""
        matrices = list(self.index_cache.get("matrices", {}).values())
        
        if limit:
            matrices = matrices[:limit]
        
        return matrices
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        with self.cache_lock:
            return {
                "cache_size": len(self.cache),
                "max_cache_size": self.cache_size,
                "cached_matrices": list(self.cache.keys()),
                "cache_hit_ratio": self._calculate_cache_hit_ratio()
            }
    
    def _calculate_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio (simplified implementation)."""
        # This would need to track actual hits/misses in a real implementation
        return len(self.cache) / self.cache_size if self.cache_size > 0 else 0.0
    
    def clear_cache(self):
        """Clear the memory cache."""
        with self.cache_lock:
            self.cache.clear()
            self.cache_timestamps.clear()
    
    def preload_matrices(self, matrix_ids: List[str]):
        """Preload matrices into cache for faster subsequent access."""
        print(f"Preloading {len(matrix_ids)} matrices into cache...")
        
        for matrix_id in matrix_ids:
            try:
                self.read_matrix_fast(matrix_id, use_cache=True)
            except Exception as e:
                print(f"Failed to preload matrix {matrix_id}: {e}")
    
    def get_reader_stats(self) -> Dict:
        """Get comprehensive reader statistics."""
        return {
            "total_matrices": len(self.index_cache.get("matrices", {})),
            "cache_stats": self.get_cache_stats(),
            "index_size_mb": os.path.getsize(self.index_file) / (1024 * 1024) if os.path.exists(self.index_file) else 0,
            "data_dir_size_mb": self._get_directory_size(self.data_dir) / (1024 * 1024)
        }
    
    def _get_directory_size(self, directory: str) -> int:
        """Calculate the total size of a directory."""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size
    
    def shutdown(self):
        """Shutdown the reader and cleanup resources."""
        self.reader_pool.shutdown(wait=True)
        self.clear_cache()


class IndexedMatrixReader(FastMatrixReader):
    """Enhanced reader with advanced indexing capabilities."""
    
    def __init__(self, data_dir: str = "output", cache_size: int = 100):
        super().__init__(data_dir, cache_size)
        
        # Advanced index structures
        self.shape_index: Dict[Tuple[int, ...], List[str]] = {}
        self.size_index: Dict[int, List[str]] = {}
        self.name_index: Dict[str, List[str]] = {}
        
        # Build advanced indices
        self._build_advanced_indices()
    
    def _build_advanced_indices(self):
        """Build advanced indices for fast searching."""
        for matrix_id, matrix_info in self.index_cache.get("matrices", {}).items():
            # Index by shape
            shape = tuple(matrix_info["shape"])
            if shape not in self.shape_index:
                self.shape_index[shape] = []
            self.shape_index[shape].append(matrix_id)
            
            # Index by total size
            total_size = np.prod(shape)
            if total_size not in self.size_index:
                self.size_index[total_size] = []
            self.size_index[total_size].append(matrix_id)
            
            # Index by name
            name = matrix_info.get("name", "")
            if name not in self.name_index:
                self.name_index[name] = []
            self.name_index[name].append(matrix_id)
    
    def find_by_shape(self, shape: Tuple[int, ...]) -> List[str]:
        """Find matrices by shape."""
        return self.shape_index.get(shape, [])
    
    def find_by_size_range(self, min_size: int, max_size: int) -> List[str]:
        """Find matrices within a size range."""
        matching_ids = []
        for size, matrix_ids in self.size_index.items():
            if min_size <= size <= max_size:
                matching_ids.extend(matrix_ids)
        return matching_ids
    
    def find_by_name_pattern(self, pattern: str) -> List[str]:
        """Find matrices by name pattern."""
        import re
        matching_ids = []
        regex = re.compile(pattern, re.IGNORECASE)
        
        for name, matrix_ids in self.name_index.items():
            if regex.search(name):
                matching_ids.extend(matrix_ids)
        
        return matching_ids
    
    def get_index_stats(self) -> Dict:
        """Get advanced index statistics."""
        return {
            "unique_shapes": len(self.shape_index),
            "unique_sizes": len(self.size_index),
            "unique_names": len(self.name_index),
            "shape_distribution": {str(shape): len(ids) for shape, ids in self.shape_index.items()},
            "size_distribution": {str(size): len(ids) for size, ids in self.size_index.items()}
        } 