import socket
import threading
import time
from collections import defaultdict
from utils import send_json, recv_json
from config import MASTER_HOST, MASTER_PORT, REPLICATION_FACTOR, FAILURE_TIMEOUT


class MasterNode:
    def __init__(self, host, port):

        self.host = host
        self.port = port
        self.running = True

        self.file_metadata = {}  # filename -> [chunk_ids]
        self.chunk_locations = defaultdict(list)  # chunk_id -> [(host, port)]
        self.storage_nodes = {}  # node_id -> {'host': x, 'port': y, 'last_heartbeat': time}

        self.metadata_lock = threading.Lock()
        self.nodes_lock = threading.Lock()

        print(f"Master Node initialized at {host}:{port}")

    def start(self):
        monitor_thread = threading.Thread(target=self.monitor_node_health)
        monitor_thread.daemon = True
        monitor_thread.start()

        replication_thread = threading.Thread(target=self.check_replication)
        replication_thread.daemon = True
        replication_thread.start()

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)

        print(f"Master Node listening on {self.host}:{self.port}")

        try:
            while self.running:
                try:
                    server_socket.settimeout(1.0)
                    client_socket, addr = server_socket.accept()

                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket,)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                except socket.timeout:
                    continue
        except KeyboardInterrupt:
            print("\nShutting down Master Node...")
        finally:
            server_socket.close()

    def handle_client(self, client_socket):

        try:
            request = recv_json(client_socket)
            if not request:
                return

            command = request.get('command')

            if command == 'HEARTBEAT':
                self.handle_heartbeat(client_socket, request)
            elif command == 'UPLOAD':
                self.handle_upload_request(client_socket, request)
            elif command == 'DOWNLOAD':
                self.handle_download_request(client_socket, request)
            elif command == 'LIST_FILES':
                self.handle_list_files(client_socket)
            elif command == 'REPORT_CHUNK':
                self.handle_chunk_report(client_socket, request)
            else:
                response = {'status': 'error', 'message': 'Unknown command'}
                send_json(client_socket, response)
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def handle_heartbeat(self, client_socket, request):

        node_id = request.get('node_id')
        host = request.get('host')
        port = request.get('port')

        with self.nodes_lock:
            self.storage_nodes[node_id] = {
                'host': host,
                'port': port,
                'last_heartbeat': time.time()
            }

        response = {'status': 'success', 'message': 'Heartbeat received'}
        send_json(client_socket, response)

    def handle_upload_request(self, client_socket, request):

        try:
            filename = request.get('filename')
            chunk_ids = request.get('chunk_ids')

            if not filename or not chunk_ids:
                response = {'status': 'error', 'message': 'Missing filename or chunk_ids'}
                send_json(client_socket, response)
                return

            alive_nodes = self.get_alive_nodes()

            if len(alive_nodes) < REPLICATION_FACTOR:
                response = {
                    'status': 'error',
                    'message': f'Not enough storage nodes. Need {REPLICATION_FACTOR}, have {len(alive_nodes)}'
                }
                send_json(client_socket, response)
                return

            chunk_assignments = {}
            for chunk_id in chunk_ids:

                selected_nodes = self.select_nodes_for_chunk(alive_nodes, REPLICATION_FACTOR)
                chunk_assignments[chunk_id] = selected_nodes

            with self.metadata_lock:
                self.file_metadata[filename] = chunk_ids

            response = {
                'status': 'success',
                'chunk_assignments': chunk_assignments
            }
            send_json(client_socket, response)

            print(f"Upload request for {filename} with {len(chunk_ids)} chunks")
        except Exception as e:
            print(f"Error handling upload request: {e}")
            response = {'status': 'error', 'message': str(e)}
            send_json(client_socket, response)

    def handle_download_request(self, client_socket, request):

        try:
            filename = request.get('filename')

            if not filename:
                response = {'status': 'error', 'message': 'Missing filename'}
                send_json(client_socket, response)
                return

            with self.metadata_lock:
                if filename not in self.file_metadata:
                    response = {'status': 'error', 'message': f'File {filename} not found'}
                    send_json(client_socket, response)
                    return

                chunk_ids = self.file_metadata[filename]

            chunk_locations = {}
            for chunk_id in chunk_ids:
                locations = self.chunk_locations.get(chunk_id, [])

                alive_locations = [
                    loc for loc in locations
                    if f"{loc[0]}:{loc[1]}" in self.get_alive_nodes()
                ]
                chunk_locations[chunk_id] = alive_locations

            response = {
                'status': 'success',
                'chunk_ids': chunk_ids,
                'chunk_locations': chunk_locations
            }
            send_json(client_socket, response)

            print(f"Download request for {filename}")
        except Exception as e:
            print(f"Error handling download request: {e}")
            response = {'status': 'error', 'message': str(e)}
            send_json(client_socket, response)

    def handle_list_files(self, client_socket):

        with self.metadata_lock:
            files = list(self.file_metadata.keys())

        response = {
            'status': 'success',
            'files': files
        }
        send_json(client_socket, response)

    def handle_chunk_report(self, client_socket, request):

        try:
            chunk_id = request.get('chunk_id')
            locations = request.get('locations')  # [(host, port), ...]

            if not chunk_id or not locations:
                response = {'status': 'error', 'message': 'Missing chunk_id or locations'}
                send_json(client_socket, response)
                return

            with self.metadata_lock:
                self.chunk_locations[chunk_id] = locations

            response = {'status': 'success', 'message': 'Chunk location recorded'}
            send_json(client_socket, response)

            print(f"Recorded locations for chunk {chunk_id}: {locations}")
        except Exception as e:
            print(f"Error handling chunk report: {e}")
            response = {'status': 'error', 'message': str(e)}
            send_json(client_socket, response)

    def get_alive_nodes(self):

        alive_nodes = []
        current_time = time.time()

        with self.nodes_lock:
            for node_id, node_info in self.storage_nodes.items():
                if current_time - node_info['last_heartbeat'] < FAILURE_TIMEOUT:
                    alive_nodes.append(node_id)

        return alive_nodes

    def select_nodes_for_chunk(self, alive_nodes, count):

        import random
        selected_node_ids = random.sample(alive_nodes, min(count, len(alive_nodes)))

        selected_nodes = []
        with self.nodes_lock:
            for node_id in selected_node_ids:
                node_info = self.storage_nodes[node_id]
                selected_nodes.append((node_info['host'], node_info['port']))

        return selected_nodes

    def monitor_node_health(self):

        while self.running:
            time.sleep(5)

            current_time = time.time()
            failed_nodes = []

            with self.nodes_lock:
                for node_id, node_info in self.storage_nodes.items():
                    if current_time - node_info['last_heartbeat'] > FAILURE_TIMEOUT:
                        if node_id not in failed_nodes:
                            failed_nodes.append(node_id)
                            print(f"WARNING: Node {node_id} has failed!")

            if failed_nodes:
                self.handle_node_failures(failed_nodes)

    def handle_node_failures(self, failed_nodes):

        with self.metadata_lock:

            affected_chunks = []
            for chunk_id, locations in self.chunk_locations.items():

                new_locations = [
                    loc for loc in locations
                    if f"{loc[0]}:{loc[1]}" not in failed_nodes
                ]

                if len(new_locations) != len(locations):
                    self.chunk_locations[chunk_id] = new_locations
                    affected_chunks.append((chunk_id, new_locations))

            print(f"Found {len(affected_chunks)} chunks affected by node failures")

    def check_replication(self):

        while self.running:
            time.sleep(30)  # Check every 30 seconds

            with self.metadata_lock:
                for chunk_id, locations in self.chunk_locations.items():
                    if len(locations) < REPLICATION_FACTOR:
                        print(f"WARNING: Chunk {chunk_id} under-replicated: {len(locations)}/{REPLICATION_FACTOR}")


if __name__ == "__main__":
    master = MasterNode(MASTER_HOST, MASTER_PORT)
    master.start()
