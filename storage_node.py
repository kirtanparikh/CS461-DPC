import socket
import threading
import time
import os
import sys
from utils import send_json, recv_json, recv_all
from config import MASTER_HOST, MASTER_PORT, HEARTBEAT_INTERVAL


class StorageNode:
    def __init__(self, host, port, storage_dir):

        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.running = True

        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)

        print(f"Storage Node initialized at {host}:{port}")
        print(f"Storage directory: {storage_dir}")

    def start(self):


        heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        print(f"Storage Node listening on {self.host}:{self.port}")

        try:
            while self.running:
                try:
                    server_socket.settimeout(1.0)
                    client_socket, addr = server_socket.accept()
                    print(f"Connection from {addr}")

                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket,)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                except socket.timeout:
                    continue
        except KeyboardInterrupt:
            print("\nShutting down Storage Node...")
        finally:
            server_socket.close()

    def handle_client(self, client_socket):

        try:
            request = recv_json(client_socket)
            if not request:
                return

            command = request.get('command')

            if command == 'STORE':
                self.handle_store(client_socket, request)
            elif command == 'RETRIEVE':
                self.handle_retrieve(client_socket, request)
            else:
                response = {'status': 'error', 'message': 'Unknown command'}
                send_json(client_socket, response)
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def handle_store(self, client_socket, request):
        try:
            chunk_id = request.get('chunk_id')
            chunk_size = request.get('size')

            if not chunk_id or chunk_size is None:
                response = {'status': 'error', 'message': 'Missing chunk_id or size'}
                send_json(client_socket, response)
                return

            # Receive raw binary chunk data
            chunk_data = recv_all(client_socket, chunk_size)

            if chunk_data is None:
                response = {'status': 'error', 'message': 'Failed to receive chunk data'}
                send_json(client_socket, response)
                return

            # Write raw chunk data to file
            chunk_path = os.path.join(self.storage_dir, chunk_id)
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)

            print(f"Stored chunk {chunk_id} ({len(chunk_data)} bytes)")

            response = {'status': 'success', 'message': f'Chunk {chunk_id} stored'}
            send_json(client_socket, response)
        except Exception as e:
            print(f"Error storing chunk: {e}")
            response = {'status': 'error', 'message': str(e)}
            send_json(client_socket, response)

    def handle_retrieve(self, client_socket, request):
        try:
            chunk_id = request.get('chunk_id')

            if not chunk_id:
                response = {'status': 'error', 'message': 'Missing chunk_id'}
                send_json(client_socket, response)
                return

            chunk_path = os.path.join(self.storage_dir, chunk_id)

            if not os.path.exists(chunk_path):
                response = {'status': 'error', 'message': f'Chunk {chunk_id} not found'}
                send_json(client_socket, response)
                return

            # Read raw chunk data from file
            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()

            # Get chunk size
            chunk_size = len(chunk_data)

            print(f"Retrieved chunk {chunk_id} ({chunk_size} bytes)")

            # Send metadata response
            response = {
                'status': 'success',
                'chunk_id': chunk_id,
                'size': chunk_size
            }
            send_json(client_socket, response)

            # Send raw binary chunk data
            client_socket.sendall(chunk_data)
        except Exception as e:
            print(f"Error retrieving chunk: {e}")
            response = {'status': 'error', 'message': str(e)}
            send_json(client_socket, response)

    def send_heartbeats(self):

        while self.running:
            try:
                master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                master_socket.connect((MASTER_HOST, MASTER_PORT))

                heartbeat = {
                    'command': 'HEARTBEAT',
                    'node_id': f"{self.host}:{self.port}",
                    'host': self.host,
                    'port': self.port
                }

                send_json(master_socket, heartbeat)
                response = recv_json(master_socket)

                if response and response.get('status') == 'success':
                    print(f"Heartbeat sent to master")

                master_socket.close()
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")

            time.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python storage_node.py <host> <port> <storage_dir>")
        print("Example: python storage_node.py 127.0.0.1 9001 ./node1_storage")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    storage_dir = sys.argv[3]

    node = StorageNode(host, port, storage_dir)
    node.start()

