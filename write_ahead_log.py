import os
import struct
import threading
import time
from typing import Optional, Dict, Any, List, Tuple
import hashlib
import json

# WAL entry types
OP_WRITE_INTENT = 1
OP_COMMIT = 2
OP_ROLLBACK = 3

# WAL entry format: op(1) | ts(8) | offset(8) | length(8) | metalen(4) | meta | checksum(16)
WAL_HEADER_FMT = '>B d q q I'
WAL_HEADER_SIZE = struct.calcsize(WAL_HEADER_FMT)
CHECKSUM_SIZE = 16

class WriteAheadLog:
    """
    Binary, per-disk/per-file Write-Ahead Log (WAL) for crash-safe, atomic writes.
    Each WAL entry: op type, timestamp, offset, length, metadata, checksum.
    Thread/process-safe.
    """
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.lock = threading.Lock()
        # Ensure WAL file exists
        open(self.wal_path, 'ab').close()

    def log_write_intent(self, offset: int, length: int, metadata: Dict[str, Any]) -> None:
        self._append_entry(OP_WRITE_INTENT, offset, length, metadata)

    def log_commit(self, offset: int, length: int, metadata: Dict[str, Any]) -> None:
        self._append_entry(OP_COMMIT, offset, length, metadata)

    def log_rollback(self, offset: int, length: int, metadata: Dict[str, Any]) -> None:
        self._append_entry(OP_ROLLBACK, offset, length, metadata)

    def _append_entry(self, op: int, offset: int, length: int, metadata: Dict[str, Any]) -> None:
        ts = time.time()
        meta_bytes = json.dumps(metadata).encode('utf-8')
        meta_len = len(meta_bytes)
        header = struct.pack(WAL_HEADER_FMT, op, ts, offset, length, meta_len)
        # Compute checksum (MD5 of header+meta)
        checksum = hashlib.md5(header + meta_bytes).digest()
        entry = header + meta_bytes + checksum
        with self.lock, open(self.wal_path, 'ab') as f:
            f.write(entry)
            f.flush()
            os.fsync(f.fileno())

    def recover_uncommitted(self) -> List[Tuple[int, int, Dict[str, Any]]]:
        """
        Returns a list of (offset, length, metadata) for write-intents not followed by commit.
        """
        with self.lock, open(self.wal_path, 'rb') as f:
            entries = []
            committed = set()
            while True:
                header_bytes = f.read(WAL_HEADER_SIZE)
                if not header_bytes or len(header_bytes) < WAL_HEADER_SIZE:
                    break
                op, ts, offset, length, meta_len = struct.unpack(WAL_HEADER_FMT, header_bytes)
                meta_bytes = f.read(meta_len)
                checksum = f.read(CHECKSUM_SIZE)
                # Validate checksum
                if hashlib.md5(header_bytes + meta_bytes).digest() != checksum:
                    continue  # Corrupt entry, skip
                meta = json.loads(meta_bytes.decode('utf-8'))
                key = (offset, length, json.dumps(meta, sort_keys=True))
                if op == OP_WRITE_INTENT:
                    entries.append((offset, length, meta))
                elif op == OP_COMMIT:
                    committed.add(key)
            # Return intents not committed
            uncommitted = [e for e in entries if (e[0], e[1], json.dumps(e[2], sort_keys=True)) not in committed]
            return uncommitted

    def clear(self):
        """Clear the WAL file (after successful recovery)."""
        with self.lock:
            open(self.wal_path, 'wb').close() 