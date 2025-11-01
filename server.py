"""
Network Server for Key/Value Database
Provides TCP-based network access to the database
"""

import socket
import json
import threading
from typing import Any, Dict
from storage_engine import StorageEngine


class KVServer:
    """TCP Server for Key/Value Database"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 9999, 
                 data_dir: str = './data', memtable_size: int = 1000):
        self.host = host
        self.port = port
        self.storage = StorageEngine(data_dir=data_dir, memtable_size=memtable_size)
        self.server_socket = None
        self.running = False
        self.client_threads = []
        
    def start(self) -> None:
        """Start the server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"Key/Value Database Server started on {self.host}:{self.port}")
        
        try:
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    client_socket, address = self.server_socket.accept()
                    print(f"Connection from {address}")
                    
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                    self.client_threads.append(client_thread)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"Error accepting connection: {e}")
        except KeyboardInterrupt:
            print("\nServer interrupted by user")
        finally:
            self.stop()
    
    def _handle_client(self, client_socket: socket.socket, address: tuple) -> None:
        """Handle client connection"""
        buffer = ""
        # Set timeout to prevent hanging on disconnected clients
        client_socket.settimeout(60.0)
        try:
            while self.running:
                try:
                    data = client_socket.recv(4096).decode('utf-8')
                    if not data:
                        break
                    
                    buffer += data
                    
                    # Process complete messages (ended with newline)
                    while '\n' in buffer:
                        message, buffer = buffer.split('\n', 1)
                        if message.strip():
                            response = self._process_request(message.strip())
                            client_socket.send((json.dumps(response) + '\n').encode('utf-8'))
                
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"Error handling client {address}: {e}")
                    break
        finally:
            client_socket.close()
            print(f"Connection closed from {address}")
    
    def _process_request(self, message: str) -> Dict[str, Any]:
        """Process client request and return response"""
        try:
            request = json.loads(message)
            command = request.get('command')
            
            if command == 'put':
                key = request.get('key')
                value = request.get('value')
                if key is None or value is None:
                    return {'status': 'error', 'message': 'Missing key or value'}
                
                self.storage.put(key, value)
                return {'status': 'ok', 'message': 'Key-value pair stored'}
            
            elif command == 'get':
                key = request.get('key')
                if key is None:
                    return {'status': 'error', 'message': 'Missing key'}
                
                value = self.storage.get(key)
                if value is None:
                    return {'status': 'not_found', 'message': 'Key not found'}
                return {'status': 'ok', 'value': value}
            
            elif command == 'get_range':
                start_key = request.get('start_key')
                end_key = request.get('end_key')
                if start_key is None or end_key is None:
                    return {'status': 'error', 'message': 'Missing start_key or end_key'}
                
                result = self.storage.get_range(start_key, end_key)
                return {'status': 'ok', 'data': result}
            
            elif command == 'batch_put':
                keys = request.get('keys')
                values = request.get('values')
                if keys is None or values is None:
                    return {'status': 'error', 'message': 'Missing keys or values'}
                if not isinstance(keys, list) or not isinstance(values, list):
                    return {'status': 'error', 'message': 'Keys and values must be lists'}
                
                self.storage.batch_put(keys, values)
                return {'status': 'ok', 'message': f'{len(keys)} key-value pairs stored'}
            
            elif command == 'delete':
                key = request.get('key')
                if key is None:
                    return {'status': 'error', 'message': 'Missing key'}
                
                self.storage.delete(key)
                return {'status': 'ok', 'message': 'Key deleted'}
            
            elif command == 'ping':
                return {'status': 'ok', 'message': 'pong'}
            
            else:
                return {'status': 'error', 'message': f'Unknown command: {command}'}
        
        except json.JSONDecodeError:
            return {'status': 'error', 'message': 'Invalid JSON'}
        except Exception as e:
            return {'status': 'error', 'message': f'Internal error: {str(e)}'}
    
    def stop(self) -> None:
        """Stop the server"""
        print("Stopping server...")
        self.running = False
        
        if self.server_socket:
            self.server_socket.close()
        
        # Close storage engine
        self.storage.close()
        
        print("Server stopped")


def main():
    """Main entry point for the server"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Key/Value Database Server')
    parser.add_argument('--host', default='0.0.0.0', help='Server host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=9999, help='Server port (default: 9999)')
    parser.add_argument('--data-dir', default='./data', help='Data directory (default: ./data)')
    parser.add_argument('--memtable-size', type=int, default=1000, 
                        help='Memtable size before flush (default: 1000)')
    
    args = parser.parse_args()
    
    server = KVServer(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        memtable_size=args.memtable_size
    )
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.stop()


if __name__ == '__main__':
    main()
