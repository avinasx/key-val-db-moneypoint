"""
Client Library for Key/Value Database
Provides easy-to-use interface for connecting to the database server
"""

import socket
import json
from typing import Any, List, Optional, Tuple


class KVClient:
    """Client for Key/Value Database"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999, timeout: float = 30.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        self.buffer = ""
        
    def connect(self) -> None:
        """Connect to the database server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self.timeout)
        self.socket.connect((self.host, self.port))
    
    def disconnect(self) -> None:
        """Disconnect from the database server"""
        if self.socket:
            self.socket.close()
            self.socket = None
            self.buffer = ""
    
    def _send_request(self, request: dict) -> dict:
        """Send request and receive response"""
        if not self.socket:
            raise ConnectionError("Not connected to server. Call connect() first.")
        
        # Send request
        message = json.dumps(request) + '\n'
        self.socket.send(message.encode('utf-8'))
        
        # Receive response
        while '\n' not in self.buffer:
            data = self.socket.recv(4096).decode('utf-8')
            if not data:
                raise ConnectionError("Connection closed by server")
            self.buffer += data
        
        response_str, self.buffer = self.buffer.split('\n', 1)
        return json.loads(response_str)
    
    def put(self, key: str, value: Any) -> bool:
        """Store a key-value pair"""
        request = {
            'command': 'put',
            'key': key,
            'value': value
        }
        response = self._send_request(request)
        return response.get('status') == 'ok'
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve value for a key"""
        request = {
            'command': 'get',
            'key': key
        }
        response = self._send_request(request)
        if response.get('status') == 'ok':
            return response.get('value')
        return None
    
    def get_range(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """Retrieve all key-value pairs in range [start_key, end_key]"""
        request = {
            'command': 'get_range',
            'start_key': start_key,
            'end_key': end_key
        }
        response = self._send_request(request)
        if response.get('status') == 'ok':
            return [tuple(item) for item in response.get('data', [])]
        return []
    
    def batch_put(self, keys: List[str], values: List[Any]) -> bool:
        """Store multiple key-value pairs in batch"""
        if len(keys) != len(values):
            raise ValueError("Keys and values must have the same length")
        
        request = {
            'command': 'batch_put',
            'keys': keys,
            'values': values
        }
        response = self._send_request(request)
        return response.get('status') == 'ok'
    
    def delete(self, key: str) -> bool:
        """Delete a key"""
        request = {
            'command': 'delete',
            'key': key
        }
        response = self._send_request(request)
        return response.get('status') == 'ok'
    
    def ping(self) -> bool:
        """Check if server is responsive"""
        try:
            request = {'command': 'ping'}
            response = self._send_request(request)
            return response.get('status') == 'ok'
        except Exception:
            return False
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        return False


def main():
    """Interactive client CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Key/Value Database Client')
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=9999, help='Server port (default: 9999)')
    
    args = parser.parse_args()
    
    print(f"Connecting to {args.host}:{args.port}...")
    
    try:
        with KVClient(host=args.host, port=args.port) as client:
            print("Connected! Type 'help' for commands or 'quit' to exit.\n")
            
            while True:
                try:
                    command = input("> ").strip()
                    
                    if not command:
                        continue
                    
                    if command == 'quit' or command == 'exit':
                        break
                    
                    elif command == 'help':
                        print("""
Available commands:
  put <key> <value>          - Store a key-value pair
  get <key>                  - Retrieve value for a key
  range <start> <end>        - Get all keys in range [start, end]
  batch <k1> <v1> <k2> <v2>  - Batch store key-value pairs
  delete <key>               - Delete a key
  ping                       - Check server connection
  quit/exit                  - Exit the client
                        """)
                    
                    elif command == 'ping':
                        if client.ping():
                            print("Server is alive!")
                        else:
                            print("Server not responding")
                    
                    elif command.startswith('put '):
                        parts = command.split(None, 2)
                        if len(parts) >= 3:
                            key, value = parts[1], parts[2]
                            if client.put(key, value):
                                print(f"Stored: {key} = {value}")
                            else:
                                print("Failed to store")
                        else:
                            print("Usage: put <key> <value>")
                    
                    elif command.startswith('get '):
                        parts = command.split(None, 1)
                        if len(parts) >= 2:
                            key = parts[1]
                            value = client.get(key)
                            if value is not None:
                                print(f"{key} = {value}")
                            else:
                                print(f"Key '{key}' not found")
                        else:
                            print("Usage: get <key>")
                    
                    elif command.startswith('range '):
                        parts = command.split(None, 2)
                        if len(parts) >= 3:
                            start, end = parts[1], parts[2]
                            results = client.get_range(start, end)
                            if results:
                                for key, value in results:
                                    print(f"  {key} = {value}")
                            else:
                                print("No keys found in range")
                        else:
                            print("Usage: range <start_key> <end_key>")
                    
                    elif command.startswith('batch '):
                        parts = command.split()[1:]
                        if len(parts) >= 2 and len(parts) % 2 == 0:
                            keys = parts[0::2]
                            values = parts[1::2]
                            if client.batch_put(keys, values):
                                print(f"Batch stored {len(keys)} pairs")
                            else:
                                print("Failed to batch store")
                        else:
                            print("Usage: batch <k1> <v1> <k2> <v2> ...")
                    
                    elif command.startswith('delete '):
                        parts = command.split(None, 1)
                        if len(parts) >= 2:
                            key = parts[1]
                            if client.delete(key):
                                print(f"Deleted: {key}")
                            else:
                                print("Failed to delete")
                        else:
                            print("Usage: delete <key>")
                    
                    else:
                        print(f"Unknown command: {command}. Type 'help' for available commands.")
                
                except KeyboardInterrupt:
                    print("\nUse 'quit' to exit")
                except Exception as e:
                    print(f"Error: {e}")
    
    except ConnectionRefusedError:
        print(f"Could not connect to server at {args.host}:{args.port}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
