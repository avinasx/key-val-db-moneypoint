# Key/Value Database - MoneyPoint

A high-performance, network-available persistent Key/Value database system implemented in Python using only standard libraries.

## Features

- **Network-Available**: TCP-based server for remote access
- **Persistent Storage**: LSM-tree inspired storage engine with crash recovery
- **High Performance**: In-memory memtable for low-latency reads/writes
- **Crash Friendliness**: Write-Ahead Log (WAL) for durability and fast recovery
- **Range Queries**: Efficient range scans over sorted keys
- **Batch Operations**: Optimized batch write support
- **Concurrent Access**: Thread-safe operations
- **Automatic Compaction**: Background compaction to optimize storage

## Architecture

The database uses an LSM-tree (Log-Structured Merge-tree) inspired approach:

1. **MemTable**: In-memory sorted table for fast writes
2. **Write-Ahead Log (WAL)**: Ensures durability and crash recovery
3. **SSTables**: Immutable sorted string tables on disk
4. **Compaction**: Periodic merging of SSTables to optimize reads

## Installation

No external dependencies required! Uses only Python standard library.

```bash
# Clone the repository
git clone https://github.com/avinasx/key-val-db-moneypoint.git
cd key-val-db-moneypoint

# Verify Python 3.7+ is installed
python3 --version
```

## Usage

### Starting the Server

```bash
# Start server with default settings (host: 0.0.0.0, port: 9999)
python3 server.py

# Start server with custom settings
python3 server.py --host 127.0.0.1 --port 8080 --data-dir ./mydata --memtable-size 5000
```

Server options:
- `--host`: Server host address (default: 0.0.0.0)
- `--port`: Server port (default: 9999)
- `--data-dir`: Data directory for persistence (default: ./data)
- `--memtable-size`: Number of entries before memtable flush (default: 1000)

### Using the Interactive Client

```bash
# Connect to server
python3 client.py

# Connect to custom host/port
python3 client.py --host 127.0.0.1 --port 8080
```

Client commands:
```
put <key> <value>          - Store a key-value pair
get <key>                  - Retrieve value for a key
range <start> <end>        - Get all keys in range [start, end]
batch <k1> <v1> <k2> <v2>  - Batch store key-value pairs
delete <key>               - Delete a key
ping                       - Check server connection
quit/exit                  - Exit the client
```

### Using the Client Library

```python
from client import KVClient

# Using context manager (recommended)
with KVClient(host='localhost', port=9999) as client:
    # Put operation
    client.put('user:1000', 'John Doe')
    
    # Get operation
    value = client.get('user:1000')
    print(f"Value: {value}")
    
    # Batch put
    keys = ['user:1001', 'user:1002', 'user:1003']
    values = ['Alice', 'Bob', 'Charlie']
    client.batch_put(keys, values)
    
    # Range query
    results = client.get_range('user:1000', 'user:1003')
    for key, value in results:
        print(f"{key}: {value}")
    
    # Delete operation
    client.delete('user:1000')
```

### Using the Storage Engine Directly

```python
from storage_engine import StorageEngine

# Create storage engine
engine = StorageEngine(data_dir='./data', memtable_size=1000)

# Put operation
engine.put('key1', 'value1')

# Get operation
value = engine.get('key1')

# Batch put
keys = ['key2', 'key3', 'key4']
values = ['value2', 'value3', 'value4']
engine.batch_put(keys, values)

# Range query
results = engine.get_range('key1', 'key3')

# Delete operation
engine.delete('key1')

# Close engine (flushes remaining data)
engine.close()
```

## API Reference

### Server Protocol

The server uses JSON over TCP with newline-delimited messages.

#### Put Operation
```json
{"command": "put", "key": "mykey", "value": "myvalue"}
// Response: {"status": "ok", "message": "Key-value pair stored"}
```

#### Get Operation
```json
{"command": "get", "key": "mykey"}
// Response: {"status": "ok", "value": "myvalue"}
```

#### Range Query
```json
{"command": "get_range", "start_key": "a", "end_key": "z"}
// Response: {"status": "ok", "data": [["key1", "value1"], ["key2", "value2"]]}
```

#### Batch Put
```json
{"command": "batch_put", "keys": ["k1", "k2"], "values": ["v1", "v2"]}
// Response: {"status": "ok", "message": "2 key-value pairs stored"}
```

#### Delete Operation
```json
{"command": "delete", "key": "mykey"}
// Response: {"status": "ok", "message": "Key deleted"}
```

## Performance Characteristics

- **Write Latency**: O(log n) for memtable insertion
- **Read Latency**: O(1) for memtable hits, O(m) for m SSTables
- **Range Query**: O(k) where k is the number of keys in range
- **Space**: Handles datasets much larger than RAM
- **Durability**: All writes are logged to WAL before acknowledgment
- **Recovery**: Fast recovery through WAL replay

## Design Trade-offs

1. **LSM-tree approach**: Optimized for write-heavy workloads
2. **No compression**: Simplicity over storage efficiency
3. **Single-threaded compaction**: Predictable performance
4. **Synchronous WAL**: Durability over throughput
5. **In-memory index**: Fast lookups at cost of memory

## Testing

Run the test suite:

```bash
python3 test_kvdb.py
```

Test coverage includes:
- MemTable operations
- Write-Ahead Log
- SSTable operations
- Storage engine
- Server-client integration
- Concurrent access
- Large datasets
- Crash recovery

## Examples

### Example 1: Simple Key-Value Store

```python
from client import KVClient

with KVClient() as client:
    # Store user data
    client.put('user:alice', 'Alice Smith')
    client.put('user:bob', 'Bob Jones')
    
    # Retrieve data
    print(client.get('user:alice'))  # Alice Smith
```

### Example 2: Session Store

```python
from client import KVClient
import json

with KVClient() as client:
    # Store session data
    session_data = {
        'user_id': 1234,
        'login_time': '2024-01-01T10:00:00',
        'expires': '2024-01-01T12:00:00'
    }
    client.put('session:abc123', json.dumps(session_data))
    
    # Retrieve session
    data = json.loads(client.get('session:abc123'))
    print(data['user_id'])  # 1234
```

### Example 3: Time-Series Data

```python
from client import KVClient

with KVClient() as client:
    # Store time-series data with timestamp keys
    client.put('metrics:2024-01-01T10:00:00', '{"cpu": 45, "mem": 60}')
    client.put('metrics:2024-01-01T10:01:00', '{"cpu": 50, "mem": 62}')
    client.put('metrics:2024-01-01T10:02:00', '{"cpu": 48, "mem": 61}')
    
    # Query range
    results = client.get_range(
        'metrics:2024-01-01T10:00:00',
        'metrics:2024-01-01T10:02:00'
    )
    for timestamp, metrics in results:
        print(f"{timestamp}: {metrics}")
```

### Example 4: Batch Loading

```python
from client import KVClient

with KVClient() as client:
    # Prepare batch data
    keys = [f'product:{i}' for i in range(1000)]
    values = [f'Product {i}' for i in range(1000)]
    
    # Batch insert
    client.batch_put(keys, values)
    print("Loaded 1000 products")
```

## Project Structure

```
key-val-db-moneypoint/
├── storage_engine.py    # Core storage engine (MemTable, WAL, SSTable)
├── server.py           # TCP server implementation
├── client.py           # Client library and CLI
├── test_kvdb.py        # Unit and integration tests
├── README.md           # This file
└── data/               # Default data directory (created at runtime)
    ├── wal.log         # Write-ahead log
    └── sstable_*.dat   # SSTable files
```

## Future Enhancements

- **Replication**: Multi-node replication for high availability
- **Consensus**: Raft-based consensus for distributed writes
- **Sharding**: Horizontal partitioning for scalability
- **Compression**: LZ4/Snappy compression for storage efficiency
- **Bloom Filters**: Reduce disk reads for non-existent keys
- **Metrics**: Prometheus-style metrics endpoint
- **TLS Support**: Encrypted network communication

## References

- [Bigtable: A Distributed Storage System](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)
- [Bitcask: A Log-Structured Hash Table](https://riak.com/assets/bitcask-intro.pdf)
- [The Log-Structured Merge-Tree](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Raft Consensus Algorithm](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf)

## License

MIT License

## Author

MoneyPoint Assessment Project
