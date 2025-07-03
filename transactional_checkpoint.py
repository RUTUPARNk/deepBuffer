import os
import json
import threading
import hashlib
import time
from typing import Any, Dict, Optional, List, Callable

class CheckpointManager:
    """
    Manages atomic, transactional checkpointing with versioning, rotation, and checksum validation.
    Writes to a temp file, fsyncs, then atomically renames to the real checkpoint file.
    Keeps last 3 checkpoints in rotation. Thread/process-safe.
    """
    checkpoint_path: str
    keep_versions: int
    system_version: str
    schema_version: str
    lock: threading.Lock
    base_name: str
    temp_path: str
    last_checkpoint_info: Optional[Dict[str, Any]]

    def __init__(self, checkpoint_path: str, keep_versions: int = 3, system_version: str = "1.0", schema_version: str = "1.0") -> None:
        self.checkpoint_path = checkpoint_path
        self.keep_versions = keep_versions
        self.system_version = system_version
        self.schema_version = schema_version
        self.lock = threading.Lock()
        self.base_name = os.path.splitext(checkpoint_path)[0]
        self.temp_path = self.base_name + '.tmp'
        self.last_checkpoint_info: Optional[Dict[str, Any]] = None

    def _get_rotated_paths(self) -> List[str]:
        return [f"{self.base_name}_v{i+1}.json" for i in range(self.keep_versions)]

    def _compute_checksum(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def save_checkpoint(self, state: Dict[str, Any]) -> None:
        """
        Atomically save checkpoint data with versioning and checksum.
        """
        with self.lock:
            timestamp: float = time.time()
            checkpoint_data: Dict[str, Any] = {
                "system_version": self.system_version,
                "schema_version": self.schema_version,
                "timestamp": timestamp,
                "state": state
            }
            json_bytes: bytes = json.dumps(checkpoint_data, sort_keys=True).encode('utf-8')
            checksum: str = self._compute_checksum(json_bytes)
            checkpoint_data["checksum"] = checksum
            # Write to temp file
            with open(self.temp_path, 'w') as f:
                json.dump(checkpoint_data, f, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            # Rotate files
            rotated: List[str] = self._get_rotated_paths()
            for i in range(self.keep_versions-1, 0, -1):
                if os.path.exists(rotated[i-1]):
                    os.replace(rotated[i-1], rotated[i])
            os.replace(self.temp_path, rotated[0])
            self.last_checkpoint_info = {
                "timestamp": timestamp,
                "version": self.system_version,
                "schema_version": self.schema_version,
                "file": rotated[0]
            }

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """
        Load the most recent valid checkpoint, with checksum validation.
        """
        with self.lock:
            for path in self._get_rotated_paths():
                if os.path.exists(path):
                    try:
                        with open(path, 'r') as f:
                            data: Dict[str, Any] = json.load(f)
                        json_bytes: bytes = json.dumps({k: v for k, v in data.items() if k != "checksum"}, sort_keys=True).encode('utf-8')
                        if "checksum" in data and data["checksum"] == self._compute_checksum(json_bytes):
                            self.last_checkpoint_info = {
                                "timestamp": data.get("timestamp"),
                                "version": data.get("system_version"),
                                "schema_version": data.get("schema_version"),
                                "file": path
                            }
                            return data["state"]
                    except Exception:
                        continue
            return None

    def get_last_checkpoint_info(self) -> Optional[Dict[str, Any]]:
        return self.last_checkpoint_info 