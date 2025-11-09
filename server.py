"""
Network Server for Key-Value Database
Exposes the database over TCP/IP using a simple JSON protocol.
"""

import json
import socket
import threading
from typing import Optional

from kvdb import KeyValueDB


class KVDBServer:
    """
    Network server for the Key-Value database.
    
    Protocol:
    - Requests and responses are JSON objects
    - Each message is terminated with a newline
    - Request format: {"command": "put", "key": "k1", "value": "v1"}
    - Response format: {"status": "ok", "result": ...} or {"status": "error", "message": "..."}
    """
    
    def __init__(self, host: str = "localhost", port: int = 9999, data_dir: str = "data"):
        """
        Initialize the server.
        
        Args:
            host: Host to bind to
            port: Port to listen on
            data_dir: Directory for database files
        """
        self.host = host
        self.port = port
        self.db = KeyValueDB(data_dir)
        self.server_socket: Optional[socket.socket] = None
        self.running = False
    
    def _handle_client(self, client_socket: socket.socket, address):
        """
        Handle a client connection.
        
        Args:
            client_socket: Client socket
            address: Client address
        """
        print(f"Client connected: {address}")
        
        try:
            # Create a buffer for incomplete data
            buffer = ""
            
            while self.running:
                # Receive data
                data = client_socket.recv(4096).decode('utf-8')
                
                if not data:
                    break
                
                buffer += data
                
                # Process complete messages (separated by newlines)
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    
                    if line.strip():
                        response = self._process_request(line)
                        client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
        
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        
        finally:
            client_socket.close()
            print(f"Client disconnected: {address}")
    
    def _process_request(self, request_str: str) -> dict:
        """
        Process a client request.
        
        Args:
            request_str: JSON request string
            
        Returns:
            Response dictionary
        """
        try:
            request = json.loads(request_str)
            command = request.get('command')
            
            if command == 'put':
                key = request.get('key')
                value = request.get('value')
                
                if key is None or value is None:
                    return {'status': 'error', 'message': 'Missing key or value'}
                
                success = self.db.put(key, value)
                if success:
                    return {'status': 'ok', 'result': success}
                else:
                    return {'status': 'error', 'message': 'Failed to put key-value pair', 'result': False}
            
            elif command == 'read':
                key = request.get('key')
                
                if key is None:
                    return {'status': 'error', 'message': 'Missing key'}
                
                value = self.db.read(key)
                return {'status': 'ok', 'result': value}
            
            elif command == 'read_key_range':
                start_key = request.get('start_key')
                end_key = request.get('end_key')
                
                if start_key is None or end_key is None:
                    return {'status': 'error', 'message': 'Missing start_key or end_key'}
                
                result = self.db.read_key_range(start_key, end_key)
                return {'status': 'ok', 'result': result}
            
            elif command == 'batch_put':
                keys = request.get('keys')
                values = request.get('values')
                
                if keys is None or values is None:
                    return {'status': 'error', 'message': 'Missing keys or values'}
                
                success = self.db.batch_put(keys, values)
                if success:
                    return {'status': 'ok', 'result': success}
                else:
                    return {'status': 'error', 'message': 'Failed to batch put key-value pairs', 'result': False}
            
            elif command == 'delete':
                key = request.get('key')
                
                if key is None:
                    return {'status': 'error', 'message': 'Missing key'}
                
                success = self.db.delete(key)
                if success:
                    return {'status': 'ok', 'result': success}
                else:
                    return {'status': 'error', 'message': 'Failed to delete key or key not found', 'result': False}
            
            else:
                return {'status': 'error', 'message': f'Unknown command: {command}'}
        
        except json.JSONDecodeError as e:
            return {'status': 'error', 'message': f'Invalid JSON: {e}'}
        
        except Exception as e:
            return {'status': 'error', 'message': f'Server error: {e}'}
    
    def start(self):
        """Start the server."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            
            print(f"Key-Value Database Server started on {self.host}:{self.port}")
            
            while self.running:
                try:
                    # Set a timeout to periodically check if we should stop
                    self.server_socket.settimeout(1.0)
                    
                    try:
                        client_socket, address = self.server_socket.accept()
                    except socket.timeout:
                        continue
                    
                    # Handle client in a new thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                
                except Exception as e:
                    if self.running:
                        print(f"Error accepting connection: {e}")
        
        except Exception as e:
            print(f"Error starting server: {e}")
        
        finally:
            self.stop()
    
    def stop(self):
        """Stop the server."""
        print("Stopping server...")
        self.running = False
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                print(f"Error closing server socket: {e}")
        
        # Close database
        self.db.close()
        print("Server stopped")


def main():
    """Main entry point for the server."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Key-Value Database Server')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=9999, help='Port to listen on')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    
    args = parser.parse_args()
    
    server = KVDBServer(args.host, args.port, args.data_dir)
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()


if __name__ == '__main__':
    main()
