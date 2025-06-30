import os
import threading
import hashlib
import json
import time
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import math


class DiskShard:
    """Represents a single disk shard in the multi-disk system."""
    
    def __init__(self, disk_id: str, path: str, capacity_gb: float = 1000.0):
        self.disk_id = disk_id
        self.path = path
        self.capacity_gb = capacity_gb
        self.used_gb = 0.0
        
        # Ensure directory exists
        os.makedirs(path, exist_ok=True)
        
        # Calculate available space
        self._update_usage()
    
    def _update_usage(self):
        """Update disk usage statistics."""
        try:
            total, used, free = shutil.disk_usage(self.path)
            self.used_gb = used / (1024**3)  # Convert to GB
        except Exception as e:
            print(f"Error updating disk usage for {self.disk_id}: {e}")
    
    def get_available_gb(self) -> float:
        """Get available space in GB."""
        self._update_usage()
        return self.capacity_gb - self.used_gb
    
    def get_usage_percent(self) -> float:
        """Get disk usage percentage."""
        return (self.used_gb / self.capacity_gb) * 100
    
    def can_write(self, size_gb: float) -> bool:
        """Check if disk can accommodate the write."""
        return self.get_available_gb() >= size_gb
    
    def write_file(self, filename: str, data: bytes) -> bool:
        """Write data to disk."""
        try:
            filepath = os.path.join(self.path, filename)
            with open(filepath, 'wb') as f:
                f.write(data)
            
            # Update usage
            self._update_usage()
            return True
        except Exception as e:
            print(f"Error writing to disk {self.disk_id}: {e}")
            return False
    
    def read_file(self, filename: str) -> Optional[bytes]:
        """Read data from disk."""
        try:
            filepath = os.path.join(self.path, filename)
            with open(filepath, 'rb') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading from disk {self.disk_id}: {e}")
            return None
    
    def delete_file(self, filename: str) -> bool:
        """Delete file from disk."""
        try:
            filepath = os.path.join(self.path, filename)
            if os.path.exists(filepath):
                os.remove(filepath)
                self._update_usage()
                return True
            return False
        except Exception as e:
            print(f"Error deleting from disk {self.disk_id}: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Get disk statistics."""
        return {
            "disk_id": self.disk_id,
            "path": self.path,
            "capacity_gb": self.capacity_gb,
            "used_gb": self.used_gb,
            "available_gb": self.get_available_gb(),
            "usage_percent": self.get_usage_percent()
        }


class RAIDWriter:
    """RAID-style multi-disk writer for ultra-scale data writes."""
    
    def __init__(self, raid_level: int = 0, redundancy: bool = True):
        self.raid_level = raid_level
        self.redundancy = redundancy
        self.shards: List[DiskShard] = []
        self.shard_lock = threading.Lock()
        
        # RAID configuration
        self.stripe_size = 64 * 1024  # 64KB stripes
        self.parity_shards = 1 if redundancy else 0
        
        # Thread pool for parallel writes
        self.write_pool = ThreadPoolExecutor(max_workers=8)
        
        # Global index for tracking file locations
        self.global_index: Dict[str, Dict] = {}
        self.index_lock = threading.Lock()
        
        # Performance metrics
        self.write_metrics = {
            "total_writes": 0,
            "total_bytes_written": 0,
            "parallel_writes": 0,
            "failed_writes": 0
        }
    
    def add_disk_shard(self, disk_id: str, path: str, capacity_gb: float = 1000.0):
        """Add a disk shard to the RAID array."""
        with self.shard_lock:
            shard = DiskShard(disk_id, path, capacity_gb)
            self.shards.append(shard)
            print(f"Added disk shard {disk_id} at {path} ({capacity_gb}GB)")
    
    def get_raid_stats(self) -> Dict:
        """Get RAID array statistics."""
        with self.shard_lock:
            total_capacity = sum(shard.capacity_gb for shard in self.shards)
            total_used = sum(shard.used_gb for shard in self.shards)
            total_available = sum(shard.get_available_gb() for shard in self.shards)
            
            return {
                "raid_level": self.raid_level,
                "num_shards": len(self.shards),
                "total_capacity_gb": total_capacity,
                "total_used_gb": total_used,
                "total_available_gb": total_available,
                "usage_percent": (total_used / total_capacity * 100) if total_capacity > 0 else 0,
                "effective_capacity_gb": self._calculate_effective_capacity(),
                "shard_stats": [shard.get_stats() for shard in self.shards],
                "write_metrics": self.write_metrics.copy()
            }
    
    def _calculate_effective_capacity(self) -> float:
        """Calculate effective capacity considering RAID level and redundancy."""
        if not self.shards:
            return 0.0
        
        total_capacity = sum(shard.capacity_gb for shard in self.shards)
        
        if self.raid_level == 0:
            # RAID 0: Striping, no redundancy
            return total_capacity
        elif self.raid_level == 1:
            # RAID 1: Mirroring
            return total_capacity / 2
        elif self.raid_level == 5:
            # RAID 5: Distributed parity
            return total_capacity * (len(self.shards) - 1) / len(self.shards)
        else:
            # Default to RAID 0
            return total_capacity
    
    def write_matrix_raid(self, matrix: np.ndarray, name: str) -> str:
        """Write matrix using RAID-style distribution."""
        if not self.shards:
            raise ValueError("No disk shards available")
        
        # Generate unique file ID
        file_id = self._generate_file_id(matrix, name)
        
        # Convert matrix to bytes
        matrix_bytes = matrix.tobytes()
        matrix_size = len(matrix_bytes)
        
        # Create metadata
        metadata = {
            "name": name,
            "shape": matrix.shape,
            "dtype": str(matrix.dtype),
            "size_bytes": matrix_size,
            "created_at": time.time(),
            "raid_level": self.raid_level
        }
        
        # Distribute data across shards
        if self.raid_level == 0:
            success = self._write_raid0(file_id, matrix_bytes, metadata)
        elif self.raid_level == 1:
            success = self._write_raid1(file_id, matrix_bytes, metadata)
        elif self.raid_level == 5:
            success = self._write_raid5(file_id, matrix_bytes, metadata)
        else:
            success = self._write_raid0(file_id, matrix_bytes, metadata)
        
        if success:
            # Update global index
            with self.index_lock:
                self.global_index[file_id] = {
                    "metadata": metadata,
                    "shard_locations": self._get_shard_locations(file_id),
                    "raid_level": self.raid_level
                }
            
            # Update metrics
            self.write_metrics["total_writes"] += 1
            self.write_metrics["total_bytes_written"] += matrix_size
            
            return file_id
        else:
            raise RuntimeError(f"Failed to write matrix {name} to RAID array")
    
    def _write_raid0(self, file_id: str, data: bytes, metadata: Dict) -> bool:
        """Write data using RAID 0 (striping)."""
        # Split data into stripes
        stripes = self._split_into_stripes(data)
        
        # Write stripes to different shards
        futures = []
        for i, stripe in enumerate(stripes):
            shard_index = i % len(self.shards)
            shard = self.shards[shard_index]
            
            # Create stripe metadata
            stripe_metadata = {
                **metadata,
                "stripe_index": i,
                "total_stripes": len(stripes),
                "shard_index": shard_index
            }
            
            # Write stripe
            future = self.write_pool.submit(
                self._write_stripe, shard, file_id, i, stripe, stripe_metadata
            )
            futures.append(future)
        
        # Wait for all writes to complete
        success_count = sum(1 for future in as_completed(futures) if future.result())
        return success_count == len(stripes)
    
    def _write_raid1(self, file_id: str, data: bytes, metadata: Dict) -> bool:
        """Write data using RAID 1 (mirroring)."""
        # Write complete data to multiple shards
        futures = []
        for i, shard in enumerate(self.shards):
            future = self.write_pool.submit(
                self._write_complete_file, shard, file_id, data, metadata
            )
            futures.append(future)
        
        # At least half of the writes must succeed
        success_count = sum(1 for future in as_completed(futures) if future.result())
        return success_count >= len(self.shards) // 2
    
    def _write_raid5(self, file_id: str, data: bytes, metadata: Dict) -> bool:
        """Write data using RAID 5 (distributed parity)."""
        # Split data into stripes
        stripes = self._split_into_stripes(data)
        
        # Calculate parity for each stripe group
        stripe_groups = self._group_stripes_for_raid5(stripes)
        
        futures = []
        for group_index, stripe_group in enumerate(stripe_groups):
            # Calculate parity
            parity = self._calculate_parity(stripe_group)
            
            # Write data stripes
            for stripe_index, stripe in enumerate(stripe_group):
                shard_index = (group_index * len(stripe_group) + stripe_index) % len(self.shards)
                shard = self.shards[shard_index]
                
                stripe_metadata = {
                    **metadata,
                    "stripe_index": group_index * len(stripe_group) + stripe_index,
                    "group_index": group_index,
                    "shard_index": shard_index
                }
                
                future = self.write_pool.submit(
                    self._write_stripe, shard, file_id, 
                    group_index * len(stripe_group) + stripe_index, stripe, stripe_metadata
                )
                futures.append(future)
            
            # Write parity stripe
            parity_shard_index = (group_index * len(stripe_group) + len(stripe_group)) % len(self.shards)
            parity_shard = self.shards[parity_shard_index]
            
            parity_metadata = {
                **metadata,
                "stripe_index": group_index * len(stripe_group) + len(stripe_group),
                "group_index": group_index,
                "shard_index": parity_shard_index,
                "is_parity": True
            }
            
            future = self.write_pool.submit(
                self._write_stripe, parity_shard, file_id,
                group_index * len(stripe_group) + len(stripe_group), parity, parity_metadata
            )
            futures.append(future)
        
        # Wait for all writes to complete
        success_count = sum(1 for future in as_completed(futures) if future.result())
        return success_count == len(futures)
    
    def _split_into_stripes(self, data: bytes) -> List[bytes]:
        """Split data into stripes."""
        stripes = []
        for i in range(0, len(data), self.stripe_size):
            stripe = data[i:i + self.stripe_size]
            stripes.append(stripe)
        return stripes
    
    def _group_stripes_for_raid5(self, stripes: List[bytes]) -> List[List[bytes]]:
        """Group stripes for RAID 5 parity calculation."""
        group_size = len(self.shards) - 1  # One shard reserved for parity
        groups = []
        
        for i in range(0, len(stripes), group_size):
            group = stripes[i:i + group_size]
            # Pad last group if necessary
            while len(group) < group_size:
                group.append(b'\x00' * self.stripe_size)
            groups.append(group)
        
        return groups
    
    def _calculate_parity(self, stripes: List[bytes]) -> bytes:
        """Calculate parity for a group of stripes."""
        if not stripes:
            return b''
        
        # XOR all stripes to calculate parity
        parity = bytearray(stripes[0])
        for stripe in stripes[1:]:
            for i in range(len(parity)):
                parity[i] ^= stripe[i] if i < len(stripe) else 0
        
        return bytes(parity)
    
    def _write_stripe(self, shard: DiskShard, file_id: str, stripe_index: int, 
                     stripe_data: bytes, metadata: Dict) -> bool:
        """Write a single stripe to a shard."""
        filename = f"{file_id}_stripe_{stripe_index}.dat"
        
        # Combine metadata and data
        metadata_bytes = json.dumps(metadata).encode('utf-8')
        combined_data = len(metadata_bytes).to_bytes(8, 'big') + metadata_bytes + stripe_data
        
        return shard.write_file(filename, combined_data)
    
    def _write_complete_file(self, shard: DiskShard, file_id: str, 
                           data: bytes, metadata: Dict) -> bool:
        """Write complete file to a shard."""
        filename = f"{file_id}_complete.dat"
        
        # Combine metadata and data
        metadata_bytes = json.dumps(metadata).encode('utf-8')
        combined_data = len(metadata_bytes).to_bytes(8, 'big') + metadata_bytes + data
        
        return shard.write_file(filename, combined_data)
    
    def _generate_file_id(self, matrix: np.ndarray, name: str) -> str:
        """Generate unique file ID based on matrix content and name."""
        content_hash = hashlib.md5(matrix.tobytes()).hexdigest()
        name_hash = hashlib.md5(name.encode()).hexdigest()
        timestamp = str(int(time.time() * 1000))
        
        return f"{content_hash[:8]}_{name_hash[:8]}_{timestamp}"
    
    def _get_shard_locations(self, file_id: str) -> List[str]:
        """Get shard locations for a file."""
        return [shard.disk_id for shard in self.shards]
    
    def read_matrix_raid(self, file_id: str) -> np.ndarray:
        """Read matrix from RAID array."""
        if file_id not in self.global_index:
            raise ValueError(f"File ID {file_id} not found in index")
        
        file_info = self.global_index[file_id]
        metadata = file_info["metadata"]
        raid_level = file_info["raid_level"]
        
        if raid_level == 0:
            return self._read_raid0(file_id, metadata)
        elif raid_level == 1:
            return self._read_raid1(file_id, metadata)
        elif raid_level == 5:
            return self._read_raid5(file_id, metadata)
        else:
            return self._read_raid0(file_id, metadata)
    
    def _read_raid0(self, file_id: str, metadata: Dict) -> np.ndarray:
        """Read data using RAID 0 (striping)."""
        # Read stripes from different shards
        stripes = []
        total_stripes = math.ceil(metadata["size_bytes"] / self.stripe_size)
        
        for i in range(total_stripes):
            shard_index = i % len(self.shards)
            shard = self.shards[shard_index]
            
            stripe_data = self._read_stripe(shard, file_id, i)
            if stripe_data is not None:
                stripes.append(stripe_data)
            else:
                raise RuntimeError(f"Failed to read stripe {i} from shard {shard_index}")
        
        # Combine stripes
        combined_data = b''.join(stripes)
        
        # Convert back to matrix
        matrix = np.frombuffer(combined_data, dtype=metadata["dtype"])
        matrix = matrix.reshape(metadata["shape"])
        
        return matrix
    
    def _read_raid1(self, file_id: str, metadata: Dict) -> np.ndarray:
        """Read data using RAID 1 (mirroring)."""
        # Try to read from any available shard
        for shard in self.shards:
            data = self._read_complete_file(shard, file_id)
            if data is not None:
                matrix = np.frombuffer(data, dtype=metadata["dtype"])
                matrix = matrix.reshape(metadata["shape"])
                return matrix
        
        raise RuntimeError(f"Failed to read file {file_id} from any shard")
    
    def _read_raid5(self, file_id: str, metadata: Dict) -> np.ndarray:
        """Read data using RAID 5 (distributed parity)."""
        # This is a simplified RAID 5 read implementation
        # In practice, you would need to handle parity reconstruction
        return self._read_raid0(file_id, metadata)
    
    def _read_stripe(self, shard: DiskShard, file_id: str, stripe_index: int) -> Optional[bytes]:
        """Read a single stripe from a shard."""
        filename = f"{file_id}_stripe_{stripe_index}.dat"
        data = shard.read_file(filename)
        
        if data is None:
            return None
        
        # Extract stripe data (skip metadata)
        metadata_size = int.from_bytes(data[:8], 'big')
        stripe_data = data[8 + metadata_size:]
        
        return stripe_data
    
    def _read_complete_file(self, shard: DiskShard, file_id: str) -> Optional[bytes]:
        """Read complete file from a shard."""
        filename = f"{file_id}_complete.dat"
        data = shard.read_file(filename)
        
        if data is None:
            return None
        
        # Extract file data (skip metadata)
        metadata_size = int.from_bytes(data[:8], 'big')
        file_data = data[8 + metadata_size:]
        
        return file_data
    
    def get_optimal_shard_for_write(self, size_gb: float) -> Optional[DiskShard]:
        """Find the optimal shard for writing based on available space."""
        with self.shard_lock:
            available_shards = [shard for shard in self.shards if shard.can_write(size_gb)]
            
            if not available_shards:
                return None
            
            # Choose shard with most available space
            return max(available_shards, key=lambda s: s.get_available_gb())
    
    def rebalance_data(self) -> bool:
        """Rebalance data across shards for optimal distribution."""
        print("Starting data rebalancing...")
        
        # This is a simplified rebalancing implementation
        # In practice, you would move data between shards to balance usage
        
        shard_stats = [shard.get_stats() for shard in self.shards]
        avg_usage = sum(stat["usage_percent"] for stat in shard_stats) / len(shard_stats)
        
        print(f"Average shard usage: {avg_usage:.1f}%")
        
        # Check for imbalance
        max_usage = max(stat["usage_percent"] for stat in shard_stats)
        min_usage = min(stat["usage_percent"] for stat in shard_stats)
        
        if max_usage - min_usage > 20:  # More than 20% difference
            print("Significant imbalance detected, rebalancing recommended")
            return True
        else:
            print("Shards are well balanced")
            return False
    
    def shutdown(self):
        """Shutdown the RAID writer."""
        self.write_pool.shutdown(wait=True)
        print("RAID writer shutdown complete") 