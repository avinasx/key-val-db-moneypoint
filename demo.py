#!/usr/bin/env python3
"""
Demo script for Key/Value Database
Demonstrates the main features of the database
"""

from client import KVClient
import subprocess
import sys
import time
import json


def run_demo():
    """Run a comprehensive demo of the database features"""
    
    print("=" * 60)
    print("Key/Value Database Demo")
    print("=" * 60)
    
    # Start server
    print("\n1. Starting database server...")
    server_proc = subprocess.Popen(
        [sys.executable, 'server.py', '--port', '19999', '--memtable-size', '100'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for server to start
    time.sleep(2)
    print("   ✓ Server started on port 19999")
    
    try:
        with KVClient(host='localhost', port=19999) as client:
            
            # Test connection
            print("\n2. Testing connection...")
            if client.ping():
                print("   ✓ Connected to database")
            else:
                print("   ✗ Failed to connect")
                return
            
            # Put operations
            print("\n3. Storing user data...")
            users = {
                'user:1001': 'Alice Johnson',
                'user:1002': 'Bob Smith',
                'user:1003': 'Charlie Brown',
                'user:1004': 'Diana Prince',
                'user:1005': 'Eve Anderson'
            }
            
            for key, value in users.items():
                client.put(key, value)
                print(f"   ✓ Stored {key} = {value}")
            
            # Get operation
            print("\n4. Retrieving individual user...")
            value = client.get('user:1002')
            print(f"   ✓ Retrieved user:1002 = {value}")
            
            # Range query
            print("\n5. Range query (user:1001 to user:1003)...")
            results = client.get_range('user:1001', 'user:1003')
            for key, value in results:
                print(f"   ✓ {key} = {value}")
            
            # Batch put
            print("\n6. Batch storing product catalog...")
            product_keys = [f'product:{i}' for i in range(1, 6)]
            product_values = [
                json.dumps({'name': f'Product {i}', 'price': i * 10.99}) 
                for i in range(1, 6)
            ]
            client.batch_put(product_keys, product_values)
            print(f"   ✓ Stored {len(product_keys)} products")
            
            # Get and parse JSON
            print("\n7. Retrieving product data...")
            product_data = client.get('product:3')
            if product_data:
                product = json.loads(product_data)
                print(f"   ✓ Product 3: {product['name']} - ${product['price']}")
            
            # Delete operation
            print("\n8. Deleting a user...")
            client.delete('user:1004')
            deleted = client.get('user:1004')
            if deleted is None:
                print("   ✓ user:1004 deleted successfully")
            else:
                print("   ✗ Delete failed")
            
            # Time-series example
            print("\n9. Storing time-series metrics...")
            metrics = [
                ('metrics:2024-01-01T10:00:00', json.dumps({'cpu': 45, 'memory': 60})),
                ('metrics:2024-01-01T10:01:00', json.dumps({'cpu': 50, 'memory': 62})),
                ('metrics:2024-01-01T10:02:00', json.dumps({'cpu': 48, 'memory': 61})),
                ('metrics:2024-01-01T10:03:00', json.dumps({'cpu': 52, 'memory': 63})),
            ]
            
            for key, value in metrics:
                client.put(key, value)
            print(f"   ✓ Stored {len(metrics)} metric points")
            
            # Range query for time-series
            print("\n10. Querying time-series data...")
            ts_results = client.get_range(
                'metrics:2024-01-01T10:00:00',
                'metrics:2024-01-01T10:02:00'
            )
            for timestamp, data in ts_results:
                metric = json.loads(data)
                print(f"   ✓ {timestamp}: CPU={metric['cpu']}% Memory={metric['memory']}%")
            
            # Performance test
            print("\n11. Performance test - inserting 100 items...")
            start_time = time.time()
            for i in range(100):
                client.put(f'perf_test:{i:04d}', f'value_{i}')
            elapsed = time.time() - start_time
            throughput = 100 / elapsed
            print(f"   ✓ Inserted 100 items in {elapsed:.3f}s ({throughput:.1f} ops/sec)")
            
            print("\n" + "=" * 60)
            print("✅ Demo completed successfully!")
            print("=" * 60)
            
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop server
        print("\nStopping server...")
        server_proc.terminate()
        server_proc.wait()
        print("Server stopped")


if __name__ == '__main__':
    run_demo()
