"""
Key-Value Database Implementation
A network-available persistent Key/Value database with low-latency operations.
"""

import json
import os
import threading
from typing import Any, Dict, List, Optional, Tuple


class KeyValueDB:
    """
    A persistent Key-Value database with low-latency operations.
    
    Features:
    - Thread-safe operations using locks
    - In-memory cache for fast reads
    - Write-ahead logging for durability
    - Periodic snapshots for recovery
    """
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the Key-Value database.
        
        Args:
            data_dir: Directory to store database files
        """
        self.data_dir = data_dir
        self.db_file = os.path.join(data_dir, "kvdb.json")
        self.wal_file = os.path.join(data_dir, "wal.log")
        
        # In-memory cache for low-latency reads
        self.cache: Dict[str, Any] = {}
        
        # Lock for thread-safe operations
        self.lock = threading.RLock()
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Load existing data
        self._load_data()
    
    def _load_data(self):
        """Load data from disk into memory cache."""
        try:
            if os.path.exists(self.db_file):
                with open(self.db_file, 'r') as f:
                    self.cache = json.load(f)
            
            # Replay WAL if exists
            if os.path.exists(self.wal_file):
                self._replay_wal()
                # Clear WAL after replay
                open(self.wal_file, 'w').close()
        except Exception as e:
            print(f"Error loading data: {e}")
            self.cache = {}
    
    def _replay_wal(self):
        """Replay write-ahead log to recover uncommitted operations."""
        try:
            with open(self.wal_file, 'r') as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line)
                        op = entry.get('op')
                        
                        if op == 'put':
                            self.cache[entry['key']] = entry['value']
                        elif op == 'delete':
                            self.cache.pop(entry['key'], None)
                        elif op == 'batch_put':
                            for k, v in zip(entry['keys'], entry['values']):
                                self.cache[k] = v
        except Exception as e:
            print(f"Error replaying WAL: {e}")
    
    def _write_wal(self, operation: Dict):
        """Write operation to write-ahead log."""
        try:
            with open(self.wal_file, 'a') as f:
                f.write(json.dumps(operation) + '\n')
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            print(f"Error writing to WAL: {e}")
    
    def _persist(self):
        """Persist the entire cache to disk."""
        try:
            temp_file = self.db_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(self.cache, f)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            os.replace(temp_file, self.db_file)
            
            # Clear WAL after successful snapshot
            open(self.wal_file, 'w').close()
        except Exception as e:
            print(f"Error persisting data: {e}")
    
    def put(self, key: str, value: Any) -> bool:
        """
        Store a key-value pair.
        
        Args:
            key: The key to store
            value: The value to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                # Write to WAL first
                self._write_wal({'op': 'put', 'key': key, 'value': value})
                
                # Update cache
                self.cache[key] = value
                
                # Persist periodically (every 100 operations for performance)
                if len(self.cache) % 100 == 0:
                    self._persist()
                
                return True
        except Exception as e:
            print(f"Error in put: {e}")
            return False
    
    def read(self, key: str) -> Optional[Any]:
        """
        Read a value by key.
        
        Args:
            key: The key to read
            
        Returns:
            The value if found, None otherwise
        """
        try:
            with self.lock:
                return self.cache.get(key)
        except Exception as e:
            print(f"Error in read: {e}")
            return None
    
    def read_key_range(self, start_key: str, end_key: str) -> Dict[str, Any]:
        """
        Read all key-value pairs in a range (inclusive).
        
        Args:
            start_key: Start of the range
            end_key: End of the range
            
        Returns:
            Dictionary of key-value pairs in the range
        """
        try:
            with self.lock:
                result = {}
                for key, value in self.cache.items():
                    if start_key <= key <= end_key:
                        result[key] = value
                return result
        except Exception as e:
            print(f"Error in read_key_range: {e}")
            return {}
    
    def batch_put(self, keys: List[str], values: List[Any]) -> bool:
        """
        Store multiple key-value pairs atomically.
        
        Args:
            keys: List of keys
            values: List of values
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if len(keys) != len(values):
                raise ValueError("Keys and values must have the same length")
            
            with self.lock:
                # Write to WAL first
                self._write_wal({'op': 'batch_put', 'keys': keys, 'values': values})
                
                # Update cache
                for key, value in zip(keys, values):
                    self.cache[key] = value
                
                # Persist periodically
                if len(self.cache) % 100 == 0:
                    self._persist()
                
                return True
        except Exception as e:
            print(f"Error in batch_put: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                if key in self.cache:
                    # Write to WAL first
                    self._write_wal({'op': 'delete', 'key': key})
                    
                    # Remove from cache
                    del self.cache[key]
                    
                    # Persist periodically
                    if len(self.cache) % 100 == 0:
                        self._persist()
                    
                    return True
                return False
        except Exception as e:
            print(f"Error in delete: {e}")
            return False
    
    def close(self):
        """Close the database and persist all data."""
        with self.lock:
            self._persist()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
