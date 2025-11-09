"""
Client library for Key-Value Database
Provides a simple interface to interact with the database server.
"""

import json
import socket
from typing import Any, Dict, List, Optional


class KVDBClient:
    """
    Client for the Key-Value database server.
    
    Example usage:
        client = KVDBClient('localhost', 9999)
        client.connect()
        client.put('key1', 'value1')
        value = client.read('key1')
        client.close()
        
    Or using context manager:
        with KVDBClient('localhost', 9999) as client:
            client.put('key1', 'value1')
            value = client.read('key1')
    """
    
    def __init__(self, host: str = "localhost", port: int = 9999):
        """
        Initialize the client.
        
        Args:
            host: Server host
            port: Server port
        """
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.buffer = ""
    
    def connect(self):
        """Connect to the server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
        except Exception as e:
            raise ConnectionError(f"Failed to connect to server: {e}")
    
    def _send_request(self, request: dict) -> dict:
        """
        Send a request to the server and receive the response.
        
        Args:
            request: Request dictionary
            
        Returns:
            Response dictionary
        """
        if not self.socket:
            raise RuntimeError("Not connected to server. Call connect() first.")
        
        try:
            # Send request
            message = json.dumps(request) + '\n'
            self.socket.sendall(message.encode('utf-8'))
            
            # Receive response
            while '\n' not in self.buffer:
                data = self.socket.recv(4096).decode('utf-8')
                if not data:
                    raise ConnectionError("Server closed connection")
                self.buffer += data
            
            # Extract one response
            response_str, self.buffer = self.buffer.split('\n', 1)
            response = json.loads(response_str)
            
            # Check for errors
            if response.get('status') == 'error':
                raise RuntimeError(f"Server error: {response.get('message')}")
            
            return response
        
        except Exception as e:
            raise RuntimeError(f"Communication error: {e}")
    
    def put(self, key: str, value: Any) -> bool:
        """
        Store a key-value pair.
        
        Args:
            key: The key to store
            value: The value to store
            
        Returns:
            True if successful
        """
        request = {'command': 'put', 'key': key, 'value': value}
        response = self._send_request(request)
        return response.get('result', False)
    
    def read(self, key: str) -> Optional[Any]:
        """
        Read a value by key.
        
        Args:
            key: The key to read
            
        Returns:
            The value if found, None otherwise
        """
        request = {'command': 'read', 'key': key}
        response = self._send_request(request)
        return response.get('result')
    
    def read_key_range(self, start_key: str, end_key: str) -> Dict[str, Any]:
        """
        Read all key-value pairs in a range (inclusive).
        
        Args:
            start_key: Start of the range
            end_key: End of the range
            
        Returns:
            Dictionary of key-value pairs in the range
        """
        request = {'command': 'read_key_range', 'start_key': start_key, 'end_key': end_key}
        response = self._send_request(request)
        return response.get('result', {})
    
    def batch_put(self, keys: List[str], values: List[Any]) -> bool:
        """
        Store multiple key-value pairs atomically.
        
        Args:
            keys: List of keys
            values: List of values
            
        Returns:
            True if successful
        """
        request = {'command': 'batch_put', 'keys': keys, 'values': values}
        response = self._send_request(request)
        return response.get('result', False)
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful
        """
        request = {'command': 'delete', 'key': key}
        response = self._send_request(request)
        return response.get('result', False)
    
    def close(self):
        """Close the connection."""
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def main():
    """Demo script showing client usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Key-Value Database Client')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=9999, help='Server port')
    
    args = parser.parse_args()
    
    print("Key-Value Database Client")
    print("=" * 50)
    
    with KVDBClient(args.host, args.port) as client:
        # Demo operations
        print("\n1. Putting single values...")
        client.put('name', 'John Doe')
        client.put('age', 30)
        client.put('city', 'New York')
        print("   Done!")
        
        print("\n2. Reading single values...")
        print(f"   name: {client.read('name')}")
        print(f"   age: {client.read('age')}")
        print(f"   city: {client.read('city')}")
        
        print("\n3. Batch putting values...")
        keys = ['product1', 'product2', 'product3']
        values = ['Laptop', 'Mouse', 'Keyboard']
        client.batch_put(keys, values)
        print("   Done!")
        
        print("\n4. Reading key range...")
        range_result = client.read_key_range('product1', 'product3')
        for k, v in range_result.items():
            print(f"   {k}: {v}")
        
        print("\n5. Deleting a key...")
        client.delete('age')
        print(f"   age after delete: {client.read('age')}")
        
        print("\nDemo completed successfully!")


if __name__ == '__main__':
    main()
