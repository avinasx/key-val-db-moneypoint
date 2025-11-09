"""
Tests for the Key-Value Database
"""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest

from kvdb import KeyValueDB
from server import KVDBServer
from client import KVDBClient


class TestKeyValueDB(unittest.TestCase):
    """Test the core database functionality."""
    
    def setUp(self):
        """Set up a temporary database for testing."""
        self.test_dir = tempfile.mkdtemp()
        self.db = KeyValueDB(self.test_dir)
    
    def tearDown(self):
        """Clean up test database."""
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_put_and_read(self):
        """Test basic put and read operations."""
        # Test string values
        self.assertTrue(self.db.put('key1', 'value1'))
        self.assertEqual(self.db.read('key1'), 'value1')
        
        # Test numeric values
        self.assertTrue(self.db.put('key2', 42))
        self.assertEqual(self.db.read('key2'), 42)
        
        # Test complex values
        complex_value = {'name': 'John', 'age': 30}
        self.assertTrue(self.db.put('key3', complex_value))
        self.assertEqual(self.db.read('key3'), complex_value)
    
    def test_read_nonexistent_key(self):
        """Test reading a key that doesn't exist."""
        result = self.db.read('nonexistent')
        self.assertIsNone(result)
    
    def test_overwrite_value(self):
        """Test overwriting an existing value."""
        self.db.put('key1', 'original')
        self.assertEqual(self.db.read('key1'), 'original')
        
        self.db.put('key1', 'updated')
        self.assertEqual(self.db.read('key1'), 'updated')
    
    def test_delete(self):
        """Test delete operation."""
        self.db.put('key1', 'value1')
        self.assertEqual(self.db.read('key1'), 'value1')
        
        self.assertTrue(self.db.delete('key1'))
        self.assertIsNone(self.db.read('key1'))
        
        # Delete non-existent key
        self.assertFalse(self.db.delete('nonexistent'))
    
    def test_batch_put(self):
        """Test batch put operation."""
        keys = ['batch1', 'batch2', 'batch3']
        values = ['value1', 'value2', 'value3']
        
        self.assertTrue(self.db.batch_put(keys, values))
        
        for key, expected_value in zip(keys, values):
            self.assertEqual(self.db.read(key), expected_value)
    
    def test_batch_put_mismatched_lengths(self):
        """Test batch put with mismatched key/value lengths."""
        keys = ['key1', 'key2']
        values = ['value1']
        
        self.assertFalse(self.db.batch_put(keys, values))
    
    def test_read_key_range(self):
        """Test reading a range of keys."""
        # Insert test data
        self.db.put('apple', 'red')
        self.db.put('banana', 'yellow')
        self.db.put('cherry', 'red')
        self.db.put('date', 'brown')
        self.db.put('elderberry', 'purple')
        
        # Read range
        result = self.db.read_key_range('banana', 'date')
        
        # Should include banana, cherry, and date
        self.assertEqual(len(result), 3)
        self.assertEqual(result['banana'], 'yellow')
        self.assertEqual(result['cherry'], 'red')
        self.assertEqual(result['date'], 'brown')
        
        # Should not include apple or elderberry
        self.assertNotIn('apple', result)
        self.assertNotIn('elderberry', result)
    
    def test_read_key_range_empty(self):
        """Test reading an empty range."""
        self.db.put('apple', 'red')
        self.db.put('cherry', 'red')
        
        result = self.db.read_key_range('banana', 'banana')
        self.assertEqual(len(result), 0)
    
    def test_persistence(self):
        """Test data persistence across database restarts."""
        # Insert data
        self.db.put('persistent_key', 'persistent_value')
        self.db.close()
        
        # Reopen database
        new_db = KeyValueDB(self.test_dir)
        
        # Verify data persisted
        self.assertEqual(new_db.read('persistent_key'), 'persistent_value')
        
        new_db.close()
    
    def test_wal_recovery(self):
        """Test WAL recovery after unclean shutdown."""
        # Write some data and ensure it's in WAL
        self.db.put('wal_key1', 'wal_value1')
        self.db.put('wal_key2', 'wal_value2')
        
        # Simulate unclean shutdown (don't call close)
        # Manually close without calling close() method
        del self.db
        
        # Reopen database - should recover from WAL
        self.db = KeyValueDB(self.test_dir)
        
        # Verify data recovered
        self.assertEqual(self.db.read('wal_key1'), 'wal_value1')
        self.assertEqual(self.db.read('wal_key2'), 'wal_value2')


class TestNetworkServer(unittest.TestCase):
    """Test the network server and client."""
    
    @classmethod
    def setUpClass(cls):
        """Start server for all tests."""
        cls.test_dir = tempfile.mkdtemp()
        cls.server = KVDBServer('localhost', 19999, cls.test_dir)
        
        # Start server in background thread
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        
        # Give server time to start
        time.sleep(0.5)
    
    @classmethod
    def tearDownClass(cls):
        """Stop server after all tests."""
        cls.server.stop()
        shutil.rmtree(cls.test_dir, ignore_errors=True)
    
    def test_client_put_and_read(self):
        """Test client put and read operations."""
        with KVDBClient('localhost', 19999) as client:
            # Put value
            result = client.put('test_key', 'test_value')
            self.assertTrue(result)
            
            # Read value
            value = client.read('test_key')
            self.assertEqual(value, 'test_value')
    
    def test_client_delete(self):
        """Test client delete operation."""
        with KVDBClient('localhost', 19999) as client:
            # Put and delete
            client.put('delete_key', 'delete_value')
            result = client.delete('delete_key')
            self.assertTrue(result)
            
            # Verify deleted
            value = client.read('delete_key')
            self.assertIsNone(value)
    
    def test_client_batch_put(self):
        """Test client batch put operation."""
        with KVDBClient('localhost', 19999) as client:
            keys = ['batch_a', 'batch_b', 'batch_c']
            values = [1, 2, 3]
            
            result = client.batch_put(keys, values)
            self.assertTrue(result)
            
            # Verify all values
            for key, expected in zip(keys, values):
                self.assertEqual(client.read(key), expected)
    
    def test_client_read_key_range(self):
        """Test client read key range operation."""
        with KVDBClient('localhost', 19999) as client:
            # Setup data
            client.put('range_a', 1)
            client.put('range_b', 2)
            client.put('range_c', 3)
            client.put('range_d', 4)
            
            # Read range
            result = client.read_key_range('range_b', 'range_c')
            
            self.assertEqual(len(result), 2)
            self.assertEqual(result['range_b'], 2)
            self.assertEqual(result['range_c'], 3)
    
    def test_multiple_clients(self):
        """Test multiple clients can connect simultaneously."""
        def client_operation(client_id):
            with KVDBClient('localhost', 19999) as client:
                key = f'multi_client_{client_id}'
                value = f'value_{client_id}'
                client.put(key, value)
                result = client.read(key)
                self.assertEqual(result, value)
        
        # Create multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=client_operation, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()


class TestPerformance(unittest.TestCase):
    """Test performance characteristics."""
    
    def setUp(self):
        """Set up a temporary database for testing."""
        self.test_dir = tempfile.mkdtemp()
        self.db = KeyValueDB(self.test_dir)
    
    def tearDown(self):
        """Clean up test database."""
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_read_latency(self):
        """Test read latency is low."""
        # Insert test data
        for i in range(1000):
            self.db.put(f'key_{i}', f'value_{i}')
        
        # Measure read latency
        start = time.time()
        for i in range(100):
            self.db.read(f'key_{i}')
        end = time.time()
        
        avg_latency = (end - start) / 100
        
        # Read latency should be very low (< 1ms per read)
        self.assertLess(avg_latency, 0.001, f"Average read latency too high: {avg_latency*1000:.2f}ms")
    
    def test_write_latency(self):
        """Test write latency is reasonable."""
        # Measure write latency
        start = time.time()
        for i in range(100):
            self.db.put(f'perf_key_{i}', f'perf_value_{i}')
        end = time.time()
        
        avg_latency = (end - start) / 100
        
        # Write latency should be reasonable (< 10ms per write with WAL)
        self.assertLess(avg_latency, 0.01, f"Average write latency too high: {avg_latency*1000:.2f}ms")


if __name__ == '__main__':
    unittest.main()
