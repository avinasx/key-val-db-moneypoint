# Key-Value Database - MoneyPoint

A network-available persistent Key/Value database system implemented in Python using only standard libraries.

## Features

- **Low Latency**: In-memory cache for fast reads, optimized write-ahead logging for durability
- **Persistent Storage**: File-based storage with atomic writes and crash recovery
- **Network Access**: TCP/IP server for remote access
- **Thread-Safe**: Safe for concurrent access from multiple clients
- **Simple Protocol**: JSON-based request/response protocol

## Interfaces

The database exposes the following operations:

1. **Put(Key, Value)** - Store a key-value pair
2. **Read(Key)** - Read a value by key
3. **ReadKeyRange(StartKey, EndKey)** - Read all key-value pairs in a range
4. **BatchPut(keys, values)** - Store multiple key-value pairs atomically
5. **Delete(Key)** - Delete a key-value pair

## Installation

No external dependencies required! Uses only Python standard libraries.

```bash
git clone https://github.com/avinasx/key-val-db-moneypoint.git
cd key-val-db-moneypoint
```

## Usage

### Starting the Server

```bash
python server.py --host localhost --port 9999 --data-dir data
```

Options:
- `--host`: Host to bind to (default: localhost)
- `--port`: Port to listen on (default: 9999)
- `--data-dir`: Directory for database files (default: data)

### Using the Client

#### Python Client Library

```python
from client import KVDBClient

# Connect to the server
with KVDBClient('localhost', 9999) as client:
    # Put a single value
    client.put('name', 'John Doe')
    
    # Read a value
    value = client.read('name')
    print(f"Name: {value}")
    
    # Batch put
    keys = ['product1', 'product2', 'product3']
    values = ['Laptop', 'Mouse', 'Keyboard']
    client.batch_put(keys, values)
    
    # Read key range
    products = client.read_key_range('product1', 'product3')
    for key, value in products.items():
        print(f"{key}: {value}")
    
    # Delete a key
    client.delete('name')
```

#### Direct Database Access (Without Network)

```python
from kvdb import KeyValueDB

# Create/open database
with KeyValueDB('data') as db:
    # Put a value
    db.put('key1', 'value1')
    
    # Read a value
    value = db.read('key1')
    
    # Batch put
    db.batch_put(['k1', 'k2'], ['v1', 'v2'])
    
    # Read range
    results = db.read_key_range('k1', 'k2')
    
    # Delete
    db.delete('key1')
```

### Running the Demo Client

```bash
python client.py --host localhost --port 9999
```

This will run a demonstration of all database operations.

## Architecture

### Components

1. **KeyValueDB (kvdb.py)**: Core database engine
   - In-memory cache for fast reads
   - Write-ahead logging (WAL) for durability
   - Periodic snapshots for efficient recovery
   - Thread-safe operations

2. **KVDBServer (server.py)**: Network server
   - TCP/IP socket server
   - JSON-based protocol
   - Multi-threaded client handling

3. **KVDBClient (client.py)**: Client library
   - Simple API for all database operations
   - Automatic connection management
   - Error handling

### Data Persistence

The database uses two files for persistence:

- **kvdb.json**: Main data file (snapshot)
- **wal.log**: Write-ahead log for durability

On startup, the database loads the snapshot and replays the WAL to recover any uncommitted operations.

### Performance Optimizations

- **In-memory cache**: All data is kept in memory for fast reads
- **Write-ahead logging**: Sequential writes to WAL for durability
- **Periodic snapshots**: Reduces WAL replay time
- **Thread-safe operations**: RLock for safe concurrent access

## Testing

Run the test suite:

```bash
python -m unittest test_kvdb.py -v
```

Tests cover:
- Basic CRUD operations
- Batch operations
- Key range queries
- Data persistence and recovery
- Network server and client
- Multiple concurrent clients
- Performance characteristics

## Protocol Specification

### Request Format

All requests are JSON objects terminated with a newline (`\n`):

```json
{"command": "put", "key": "mykey", "value": "myvalue"}
{"command": "read", "key": "mykey"}
{"command": "read_key_range", "start_key": "key1", "end_key": "key3"}
{"command": "batch_put", "keys": ["k1", "k2"], "values": ["v1", "v2"]}
{"command": "delete", "key": "mykey"}
```

### Response Format

All responses are JSON objects terminated with a newline (`\n`):

```json
{"status": "ok", "result": "myvalue"}
{"status": "ok", "result": {"key1": "value1", "key2": "value2"}}
{"status": "error", "message": "Error description"}
```

## Examples

### Example 1: User Session Storage

```python
from client import KVDBClient

with KVDBClient() as client:
    # Store user session
    session_data = {
        'user_id': '12345',
        'username': 'john_doe',
        'login_time': '2024-01-01T10:00:00'
    }
    client.put('session:abc123', session_data)
    
    # Retrieve session
    session = client.read('session:abc123')
    print(f"User: {session['username']}")
```

### Example 2: Product Catalog

```python
from client import KVDBClient

with KVDBClient() as client:
    # Batch insert products
    product_ids = ['prod:001', 'prod:002', 'prod:003']
    products = [
        {'name': 'Laptop', 'price': 999},
        {'name': 'Mouse', 'price': 25},
        {'name': 'Keyboard', 'price': 75}
    ]
    client.batch_put(product_ids, products)
    
    # Query product range
    all_products = client.read_key_range('prod:001', 'prod:999')
    for pid, product in all_products.items():
        print(f"{pid}: {product['name']} - ${product['price']}")
```

## Requirements Met

✅ **Put(Key, Value)** - Implemented with persistence and WAL  
✅ **Read(Key)** - Low-latency in-memory reads  
✅ **ReadKeyRange(StartKey, EndKey)** - Efficient range queries  
✅ **BatchPut(keys, values)** - Atomic batch operations  
✅ **Delete(Key)** - With WAL logging  
✅ **Low latency** - In-memory cache, < 1ms read latency  
✅ **Persistence** - File-based with crash recovery  
✅ **Network access** - TCP/IP server with JSON protocol  
✅ **Standard library only** - No external dependencies  

## License

MIT License
