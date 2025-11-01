"""
Unit Tests for Key/Value Database
Tests storage engine, server, and client functionality
"""

import unittest
import os
import shutil
import time
import threading
from storage_engine import MemTable, WAL, SSTable, StorageEngine
from server import KVServer
from client import KVClient


class TestMemTable(unittest.TestCase):
    """Test MemTable functionality"""
    
    def setUp(self):
        self.memtable = MemTable(max_size=10)
    
    def test_put_and_get(self):
        self.memtable.put('key1', 'value1')
        self.assertEqual(self.memtable.get('key1'), 'value1')
    
    def test_delete(self):
        self.memtable.put('key1', 'value1')
        self.memtable.delete('key1')
        self.assertIsNone(self.memtable.get('key1'))
    
    def test_range_query(self):
        self.memtable.put('a', '1')
        self.memtable.put('b', '2')
        self.memtable.put('c', '3')
        self.memtable.put('d', '4')
        
        result = self.memtable.get_range('b', 'c')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ('b', '2'))
        self.assertEqual(result[1], ('c', '3'))
    
    def test_is_full(self):
        for i in range(10):
            self.memtable.put(f'key{i}', f'value{i}')
        self.assertTrue(self.memtable.is_full())
    
    def test_sorting(self):
        self.memtable.put('z', '1')
        self.memtable.put('a', '2')
        self.memtable.put('m', '3')
        
        keys = list(self.memtable.data.keys())
        self.assertEqual(keys, ['a', 'm', 'z'])


class TestWAL(unittest.TestCase):
    """Test Write-Ahead Log functionality"""
    
    def setUp(self):
        self.test_dir = '/tmp/test_wal'
        os.makedirs(self.test_dir, exist_ok=True)
        self.wal_path = os.path.join(self.test_dir, 'test_wal.log')
        self.wal = WAL(self.wal_path)
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_append_and_replay(self):
        self.wal.append('put', 'key1', 'value1')
        self.wal.append('put', 'key2', 'value2')
        self.wal.append('delete', 'key1')
        
        operations = self.wal.replay()
        self.assertEqual(len(operations), 3)
        self.assertEqual(operations[0]['operation'], 'put')
        self.assertEqual(operations[2]['operation'], 'delete')
    
    def test_clear(self):
        self.wal.append('put', 'key1', 'value1')
        self.wal.clear()
        
        operations = self.wal.replay()
        self.assertEqual(len(operations), 0)


class TestSSTable(unittest.TestCase):
    """Test SSTable functionality"""
    
    def setUp(self):
        self.test_dir = '/tmp/test_sstable'
        os.makedirs(self.test_dir, exist_ok=True)
        self.sstable_path = os.path.join(self.test_dir, 'test_sstable.dat')
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_write_and_read(self):
        data = [('key1', 'value1'), ('key2', 'value2'), ('key3', 'value3')]
        sstable = SSTable.write(self.sstable_path, data)
        
        self.assertEqual(sstable.get('key1'), 'value1')
        self.assertEqual(sstable.get('key2'), 'value2')
        self.assertIsNone(sstable.get('key_not_exist'))
    
    def test_range_query(self):
        data = [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4')]
        sstable = SSTable.write(self.sstable_path, data)
        
        result = sstable.get_range('b', 'c')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ('b', '2'))


class TestStorageEngine(unittest.TestCase):
    """Test StorageEngine functionality"""
    
    def setUp(self):
        self.test_dir = '/tmp/test_storage'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir, exist_ok=True)
        self.engine = StorageEngine(data_dir=self.test_dir, memtable_size=5)
    
    def tearDown(self):
        self.engine.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_put_and_get(self):
        self.engine.put('key1', 'value1')
        self.assertEqual(self.engine.get('key1'), 'value1')
    
    def test_delete(self):
        self.engine.put('key1', 'value1')
        self.engine.delete('key1')
        self.assertIsNone(self.engine.get('key1'))
    
    def test_batch_put(self):
        keys = ['key1', 'key2', 'key3']
        values = ['value1', 'value2', 'value3']
        self.engine.batch_put(keys, values)
        
        self.assertEqual(self.engine.get('key1'), 'value1')
        self.assertEqual(self.engine.get('key2'), 'value2')
        self.assertEqual(self.engine.get('key3'), 'value3')
    
    def test_range_query(self):
        self.engine.put('a', '1')
        self.engine.put('b', '2')
        self.engine.put('c', '3')
        self.engine.put('d', '4')
        
        result = self.engine.get_range('b', 'c')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ('b', '2'))
        self.assertEqual(result[1], ('c', '3'))
    
    def test_memtable_flush(self):
        # Insert more than memtable_size to trigger flush
        for i in range(10):
            self.engine.put(f'key{i}', f'value{i}')
        
        # Should have created SSTables
        self.assertGreater(len(self.engine.sstables), 0)
        
        # Data should still be retrievable
        self.assertEqual(self.engine.get('key0'), 'value0')
        self.assertEqual(self.engine.get('key9'), 'value9')
    
    def test_crash_recovery(self):
        # Put some data
        self.engine.put('key1', 'value1')
        self.engine.put('key2', 'value2')
        
        # Simulate crash by creating new engine with same data dir
        new_engine = StorageEngine(data_dir=self.test_dir, memtable_size=5)
        
        # Data should be recovered
        self.assertEqual(new_engine.get('key1'), 'value1')
        self.assertEqual(new_engine.get('key2'), 'value2')
        
        new_engine.close()


class TestServerClient(unittest.TestCase):
    """Test Server and Client integration"""
    
    def setUp(self):
        self.test_dir = '/tmp/test_server_client'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Start server in a separate thread
        self.server = KVServer(host='localhost', port=19999, 
                              data_dir=self.test_dir, memtable_size=100)
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        
        # Wait for server to start
        time.sleep(1)
        
        # Create client
        self.client = KVClient(host='localhost', port=19999)
        self.client.connect()
    
    def tearDown(self):
        self.client.disconnect()
        self.server.stop()
        time.sleep(1.5)  # Give server time to fully shut down
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_ping(self):
        self.assertTrue(self.client.ping())
    
    def test_put_and_get(self):
        self.assertTrue(self.client.put('test_key', 'test_value'))
        self.assertEqual(self.client.get('test_key'), 'test_value')
    
    def test_delete(self):
        self.client.put('test_key', 'test_value')
        self.assertTrue(self.client.delete('test_key'))
        self.assertIsNone(self.client.get('test_key'))
    
    def test_batch_put(self):
        keys = ['key1', 'key2', 'key3']
        values = ['value1', 'value2', 'value3']
        self.assertTrue(self.client.batch_put(keys, values))
        
        self.assertEqual(self.client.get('key1'), 'value1')
        self.assertEqual(self.client.get('key2'), 'value2')
    
    def test_range_query(self):
        self.client.put('a', '1')
        self.client.put('b', '2')
        self.client.put('c', '3')
        self.client.put('d', '4')
        
        result = self.client.get_range('b', 'c')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ('b', '2'))
        self.assertEqual(result[1], ('c', '3'))
    
    def test_large_dataset(self):
        # Test with larger dataset
        num_items = 50
        for i in range(num_items):
            self.client.put(f'key_{i:04d}', f'value_{i}')
        
        # Verify all items
        for i in range(num_items):
            value = self.client.get(f'key_{i:04d}')
            self.assertEqual(value, f'value_{i}')


class TestConcurrency(unittest.TestCase):
    """Test concurrent access"""
    
    def setUp(self):
        self.test_dir = '/tmp/test_concurrency'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir, exist_ok=True)
        self.engine = StorageEngine(data_dir=self.test_dir, memtable_size=100)
    
    def tearDown(self):
        self.engine.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_concurrent_writes(self):
        def write_worker(start, end):
            for i in range(start, end):
                self.engine.put(f'key_{i}', f'value_{i}')
        
        threads = []
        for i in range(4):
            t = threading.Thread(target=write_worker, args=(i*25, (i+1)*25))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify all writes succeeded
        for i in range(100):
            value = self.engine.get(f'key_{i}')
            self.assertEqual(value, f'value_{i}')


def run_tests():
    """Run all tests"""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()
