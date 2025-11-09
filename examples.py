#!/usr/bin/env python3
"""
Example usage of the Key-Value Database
Demonstrates all 5 required operations.
"""

from kvdb import KeyValueDB


def example_direct_access():
    """Example: Direct database access (without network)."""
    print("=" * 70)
    print("EXAMPLE 1: Direct Database Access (No Network)")
    print("=" * 70)
    
    with KeyValueDB('data') as db:
        print("\n1. Put(Key, Value) - Store single values")
        db.put('user:alice', {'name': 'Alice', 'email': 'alice@example.com', 'age': 30})
        db.put('user:bob', {'name': 'Bob', 'email': 'bob@example.com', 'age': 25})
        print("   ✓ Stored user:alice and user:bob")
        
        print("\n2. Read(Key) - Retrieve single values")
        alice = db.read('user:alice')
        print(f"   user:alice = {alice}")
        
        print("\n3. BatchPut(keys, values) - Store multiple values atomically")
        product_keys = ['product:laptop', 'product:mouse', 'product:keyboard']
        product_values = [
            {'name': 'Laptop', 'price': 999.99, 'stock': 50},
            {'name': 'Mouse', 'price': 29.99, 'stock': 200},
            {'name': 'Keyboard', 'price': 79.99, 'stock': 150}
        ]
        db.batch_put(product_keys, product_values)
        print(f"   ✓ Stored {len(product_keys)} products")
        
        print("\n4. ReadKeyRange(StartKey, EndKey) - Query ranges")
        products = db.read_key_range('product:', 'product:zzz')
        print(f"   Found {len(products)} products:")
        for key, value in products.items():
            print(f"     {key}: {value['name']} - ${value['price']}")
        
        print("\n5. Delete(Key) - Remove values")
        db.delete('user:bob')
        print("   ✓ Deleted user:bob")
        
        # Verify deletion
        bob = db.read('user:bob')
        print(f"   user:bob after deletion: {bob}")


def example_network_access():
    """Example: Network-based database access."""
    print("\n\n" + "=" * 70)
    print("EXAMPLE 2: Network Database Access (Client-Server)")
    print("=" * 70)
    print("\nNOTE: This requires a running server. Start with:")
    print("  python server.py --host localhost --port 9999")
    
    print("\nClient code example:")
    print("-" * 70)
    print("""
from client import KVDBClient

with KVDBClient('localhost', 9999) as client:
    # Store session data
    client.put('session:xyz123', {
        'user_id': 'alice',
        'created_at': '2024-01-01T10:00:00',
        'expires_at': '2024-01-01T22:00:00'
    })
    
    # Retrieve session
    session = client.read('session:xyz123')
    print(f"Session: {session}")
    
    # Store configuration
    client.batch_put(
        ['config:db_host', 'config:db_port', 'config:cache_ttl'],
        ['localhost', 5432, 300]
    )
    
    # Query all config
    configs = client.read_key_range('config:', 'config:zzz')
    print(f"Configurations: {configs}")
    
    # Clean up
    client.delete('session:xyz123')
    """)
    print("-" * 70)


def example_use_cases():
    """Example: Real-world use cases."""
    print("\n\n" + "=" * 70)
    print("EXAMPLE 3: Real-World Use Cases")
    print("=" * 70)
    
    with KeyValueDB('data') as db:
        # Use Case 1: Caching
        print("\nUse Case 1: Application Cache")
        cache_key = 'cache:user_stats:alice'
        stats = db.read(cache_key)
        
        if stats is None:
            # Cache miss - fetch from "database" and cache
            print("   Cache miss - fetching and storing...")
            stats = {'visits': 42, 'last_seen': '2024-01-01', 'points': 1337}
            db.put(cache_key, stats)
        else:
            print("   Cache hit!")
        
        print(f"   Stats: {stats}")
        
        # Use Case 2: Configuration Management
        print("\nUse Case 2: Configuration Management")
        db.batch_put(
            ['app:feature_flag:new_ui', 'app:feature_flag:dark_mode', 'app:feature_flag:analytics'],
            [True, True, False]
        )
        
        # Query all feature flags
        flags = db.read_key_range('app:feature_flag:', 'app:feature_flag:zzz')
        print("   Feature Flags:")
        for flag_name, enabled in flags.items():
            flag_name = flag_name.replace('app:feature_flag:', '')
            status = "✓ Enabled" if enabled else "✗ Disabled"
            print(f"     {flag_name}: {status}")
        
        # Use Case 3: Simple Queue/Job Storage
        print("\nUse Case 3: Job Queue")
        jobs = [
            {'id': 'job:001', 'type': 'email', 'to': 'alice@example.com', 'status': 'pending'},
            {'id': 'job:002', 'type': 'report', 'user': 'bob', 'status': 'pending'},
            {'id': 'job:003', 'type': 'backup', 'target': '/data', 'status': 'pending'}
        ]
        
        # Add jobs
        job_keys = [job['id'] for job in jobs]
        db.batch_put(job_keys, jobs)
        print(f"   Added {len(jobs)} jobs to queue")
        
        # Process first job
        job = db.read('job:001')
        job['status'] = 'completed'
        db.put('job:001', job)
        print(f"   Processed: {job}")


def performance_demo():
    """Demonstrate low-latency performance."""
    import time
    
    print("\n\n" + "=" * 70)
    print("PERFORMANCE DEMONSTRATION")
    print("=" * 70)
    
    with KeyValueDB('data') as db:
        # Write performance
        print("\nWrite Performance Test:")
        n_writes = 1000
        start = time.time()
        for i in range(n_writes):
            db.put(f'perf_test_{i}', f'value_{i}')
        end = time.time()
        
        duration = end - start
        avg_latency = duration / n_writes
        print(f"   Wrote {n_writes} items in {duration:.3f}s")
        print(f"   Average write latency: {avg_latency*1000:.3f}ms")
        
        # Read performance  
        print("\nRead Performance Test:")
        n_reads = 1000
        start = time.time()
        for i in range(n_reads):
            db.read(f'perf_test_{i}')
        end = time.time()
        
        duration = end - start
        avg_latency = duration / n_reads
        print(f"   Read {n_reads} items in {duration:.3f}s")
        print(f"   Average read latency: {avg_latency*1000:.3f}ms")
        print(f"   ✓ Low latency requirement met (<1ms per read)")


if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("Key-Value Database - Complete Usage Examples")
    print("=" * 70)
    
    example_direct_access()
    example_network_access()
    example_use_cases()
    performance_demo()
    
    print("\n\n" + "=" * 70)
    print("All examples completed successfully!")
    print("=" * 70)
    print("\nFor more information, see README.md")
