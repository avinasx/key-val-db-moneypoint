"""
Storage Engine for Key/Value Database
Uses LSM-tree inspired approach with memtable, WAL, and SSTables
"""

import os
import json
import time
import threading
from typing import Any, Optional, List, Tuple
from collections import OrderedDict


class MemTable:
    """In-memory sorted table for fast writes"""
    
    def __init__(self, max_size: int = 1000):
        self.data = OrderedDict()
        self.max_size = max_size
        self.lock = threading.RLock()
    
    def put(self, key: str, value: Any) -> None:
        """Insert or update a key-value pair"""
        with self.lock:
            self.data[key] = value
            self._sort()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for a key"""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key: str) -> None:
        """Mark key as deleted (tombstone)"""
        with self.lock:
            self.data[key] = None
            self._sort()
    
    def get_range(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in range [start_key, end_key]"""
        with self.lock:
            result = []
            for key, value in self.data.items():
                if start_key <= key <= end_key and value is not None:
                    result.append((key, value))
            return result
    
    def is_full(self) -> bool:
        """Check if memtable is full"""
        with self.lock:
            return len(self.data) >= self.max_size
    
    def get_all(self) -> List[Tuple[str, Any]]:
        """Get all key-value pairs"""
        with self.lock:
            return list(self.data.items())
    
    def clear(self) -> None:
        """Clear the memtable"""
        with self.lock:
            self.data.clear()
    
    def _sort(self) -> None:
        """Keep data sorted by keys"""
        with self.lock:
            self.data = OrderedDict(sorted(self.data.items()))


class WAL:
    """Write-Ahead Log for crash recovery"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.lock = threading.Lock()
        self._ensure_file_exists()
    
    def _ensure_file_exists(self) -> None:
        """Create WAL file if it doesn't exist"""
        os.makedirs(os.path.dirname(self.file_path) or '.', exist_ok=True)
        if not os.path.exists(self.file_path):
            open(self.file_path, 'a').close()
    
    def append(self, operation: str, key: str, value: Any = None) -> None:
        """Append an operation to the WAL"""
        with self.lock:
            entry = {
                'timestamp': time.time(),
                'operation': operation,
                'key': key,
                'value': value
            }
            with open(self.file_path, 'a') as f:
                f.write(json.dumps(entry) + '\n')
                f.flush()
                os.fsync(f.fileno())
    
    def replay(self) -> List[dict]:
        """Replay all operations from WAL"""
        operations = []
        with self.lock:
            try:
                with open(self.file_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            operations.append(json.loads(line))
            except (FileNotFoundError, json.JSONDecodeError):
                pass
        return operations
    
    def clear(self) -> None:
        """Clear the WAL"""
        with self.lock:
            with open(self.file_path, 'w') as f:
                f.truncate()


class SSTable:
    """Sorted String Table - immutable disk-based storage"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.index = {}  # key -> file offset
        self._build_index()
    
    def _build_index(self) -> None:
        """Build in-memory index from file"""
        if not os.path.exists(self.file_path):
            return
        
        with open(self.file_path, 'r') as f:
            offset = 0
            while True:
                line = f.readline()
                if not line:
                    break
                if line.strip():
                    try:
                        entry = json.loads(line)
                        self.index[entry['key']] = offset
                    except json.JSONDecodeError:
                        pass
                offset = f.tell()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for a key"""
        if key not in self.index:
            return None
        
        with open(self.file_path, 'r') as f:
            f.seek(self.index[key])
            line = f.readline()
            if line:
                entry = json.loads(line)
                return entry.get('value')
        return None
    
    def get_range(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in range"""
        result = []
        for key in sorted(self.index.keys()):
            if start_key <= key <= end_key:
                value = self.get(key)
                if value is not None:
                    result.append((key, value))
        return result
    
    @staticmethod
    def write(file_path: str, data: List[Tuple[str, Any]]) -> 'SSTable':
        """Write sorted data to a new SSTable file"""
        os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
        
        with open(file_path, 'w') as f:
            for key, value in sorted(data):
                entry = {'key': key, 'value': value}
                f.write(json.dumps(entry) + '\n')
            f.flush()
            os.fsync(f.fileno())
        
        return SSTable(file_path)


class StorageEngine:
    """Main storage engine coordinating memtable, WAL, and SSTables"""
    
    def __init__(self, data_dir: str = './data', memtable_size: int = 1000):
        self.data_dir = data_dir
        self.memtable_size = memtable_size
        self.memtable = MemTable(max_size=memtable_size)
        self.wal = WAL(os.path.join(data_dir, 'wal.log'))
        self.sstables = []
        self.lock = threading.RLock()
        
        os.makedirs(data_dir, exist_ok=True)
        self._load_sstables()
        self._recover_from_wal()
    
    def _load_sstables(self) -> None:
        """Load existing SSTables from disk"""
        if not os.path.exists(self.data_dir):
            return
        
        sstable_files = sorted([
            f for f in os.listdir(self.data_dir)
            if f.startswith('sstable_') and f.endswith('.dat')
        ])
        
        for filename in sstable_files:
            file_path = os.path.join(self.data_dir, filename)
            self.sstables.append(SSTable(file_path))
    
    def _recover_from_wal(self) -> None:
        """Recover state from WAL after crash"""
        with self.lock:
            operations = self.wal.replay()
            for op in operations:
                if op['operation'] == 'put':
                    self.memtable.put(op['key'], op['value'])
                elif op['operation'] == 'delete':
                    self.memtable.delete(op['key'])
    
    def _flush_memtable(self) -> None:
        """Flush memtable to disk as SSTable"""
        with self.lock:
            if len(self.memtable.data) == 0:
                return
            
            timestamp = int(time.time() * 1000000)
            sstable_path = os.path.join(
                self.data_dir, f'sstable_{timestamp}.dat'
            )
            
            data = self.memtable.get_all()
            sstable = SSTable.write(sstable_path, data)
            self.sstables.append(sstable)
            
            self.memtable.clear()
            self.wal.clear()
            
            # Trigger compaction if too many SSTables (already holding lock)
            if len(self.sstables) > 10:
                self._compact_sstables_internal()
    
    def _compact_sstables_internal(self) -> None:
        """Merge and compact SSTables (internal, assumes lock is held)"""
        if len(self.sstables) < 2:
            return
        
        # Merge all data
        all_data = {}
        for sstable in self.sstables:
            for key in sstable.index.keys():
                value = sstable.get(key)
                all_data[key] = value
        
        # Create new compacted SSTable
        timestamp = int(time.time() * 1000000)
        compacted_path = os.path.join(
            self.data_dir, f'sstable_{timestamp}_compacted.dat'
        )
        
        new_sstable = SSTable.write(
            compacted_path,
            [(k, v) for k, v in all_data.items() if v is not None]
        )
        
        # Remove old SSTables
        for sstable in self.sstables:
            try:
                os.remove(sstable.file_path)
            except OSError:
                pass
        
        self.sstables = [new_sstable]
    
    def _compact_sstables(self) -> None:
        """Merge and compact SSTables (public, acquires lock)"""
        with self.lock:
            self._compact_sstables_internal()
    
    def put(self, key: str, value: Any) -> None:
        """Insert or update a key-value pair"""
        with self.lock:
            self.wal.append('put', key, value)
            self.memtable.put(key, value)
            
            if self.memtable.is_full():
                self._flush_memtable()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for a key"""
        with self.lock:
            # Check memtable first
            value = self.memtable.get(key)
            if value is not None:
                return value
            
            # Check SSTables (newest first)
            for sstable in reversed(self.sstables):
                value = sstable.get(key)
                if value is not None:
                    return value
            
            return None
    
    def get_range(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in range [start_key, end_key]"""
        with self.lock:
            result_dict = {}
            
            # Get from SSTables (oldest first)
            for sstable in self.sstables:
                for key, value in sstable.get_range(start_key, end_key):
                    result_dict[key] = value
            
            # Override with memtable data
            for key, value in self.memtable.get_range(start_key, end_key):
                result_dict[key] = value
            
            return sorted(result_dict.items())
    
    def delete(self, key: str) -> None:
        """Delete a key"""
        with self.lock:
            self.wal.append('delete', key)
            self.memtable.delete(key)
            
            if self.memtable.is_full():
                self._flush_memtable()
    
    def batch_put(self, keys: List[str], values: List[Any]) -> None:
        """Batch insert/update multiple key-value pairs"""
        if len(keys) != len(values):
            raise ValueError("Keys and values must have the same length")
        
        with self.lock:
            for key, value in zip(keys, values):
                self.wal.append('put', key, value)
                self.memtable.put(key, value)
            
            if self.memtable.is_full():
                self._flush_memtable()
    
    def close(self) -> None:
        """Close the storage engine, flushing any remaining data"""
        with self.lock:
            self._flush_memtable()
