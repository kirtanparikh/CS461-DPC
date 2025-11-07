import socket
import sys
import os
import hashlib
from utils import send_json, recv_json, recv_all
from config import MASTER_HOST, MASTER_PORT, CHUNK_SIZE


class Client:
    def __init__(self):

        self.master_host = MASTER_HOST
        self.master_port = MASTER_PORT

    def upload_file(self, filepath):

        if not os.path.exists(filepath):
            print(f"Error: File {filepath} not found")
            return

        filename = os.path.basename(filepath)
        print(f"Uploading {filename}...")

        chunks = self.partition_file(filepath)
        print(f"File partitioned into {len(chunks)} chunks")

        chunk_ids = [chunk['id'] for chunk in chunks]
        chunk_assignments = self.request_upload(filename, chunk_ids)

        if not chunk_assignments:
            print("Failed to get chunk assignments from master")
            return

        success_count = 0
        for chunk in chunks:
            chunk_id = chunk['id']
            chunk_data = chunk['data']
            assigned_nodes = chunk_assignments.get(chunk_id, [])

            if not assigned_nodes:
                print(f"No nodes assigned for chunk {chunk_id}")
                continue

            stored_locations = []
            for node_host, node_port in assigned_nodes:
                if self.store_chunk(node_host, node_port, chunk_id, chunk_data):
                    stored_locations.append((node_host, node_port))

            if stored_locations:
                self.report_chunk_storage(chunk_id, stored_locations)
                success_count += 1

        print(f"Upload complete: {success_count}/{len(chunks)} chunks stored")

    def download_file(self, filename, output_path):

        print(f"Downloading {filename}...")

        download_info = self.request_download(filename)

        if not download_info:
            print(f"Failed to get download info for {filename}")
            return

        chunk_ids = download_info['chunk_ids']
        chunk_locations = download_info['chunk_locations']

        print(f"File has {len(chunk_ids)} chunks")

        chunks = []
        for chunk_id in chunk_ids:
            locations = chunk_locations.get(chunk_id, [])

            if not locations:
                print(f"No locations available for chunk {chunk_id}")
                return

            chunk_data = None
            for node_host, node_port in locations:
                chunk_data = self.retrieve_chunk(node_host, node_port, chunk_id)
                if chunk_data:
                    break

            if not chunk_data:
                print(f"Failed to retrieve chunk {chunk_id}")
                return

            chunks.append(chunk_data)

        self.reassemble_file(chunks, output_path)
        print(f"Download complete: {output_path}")

    def list_files(self):

        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((self.master_host, self.master_port))

            request = {'command': 'LIST_FILES'}
            send_json(master_socket, request)

            response = recv_json(master_socket)
            master_socket.close()

            if response and response.get('status') == 'success':
                files = response.get('files', [])
                if files:
                    print("Files in storage:")
                    for f in files:
                        print(f"  - {f}")
                else:
                    print("No files in storage")
            else:
                print("Failed to list files")
        except Exception as e:
            print(f"Error listing files: {e}")

    def partition_file(self, filepath):

        chunks = []

        with open(filepath, 'rb') as f:
            chunk_number = 0
            while True:
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break

                chunk_id = self.generate_chunk_id(chunk_data, chunk_number)

                chunks.append({
                    'id': chunk_id,
                    'data': chunk_data
                })

                chunk_number += 1

        return chunks

    def generate_chunk_id(self, chunk_data, chunk_number):

        hash_obj = hashlib.sha256(chunk_data)
        hash_hex = hash_obj.hexdigest()
        return f"chunk_{chunk_number}_{hash_hex[:16]}"

    def request_upload(self, filename, chunk_ids):

        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((self.master_host, self.master_port))

            request = {
                'command': 'UPLOAD',
                'filename': filename,
                'chunk_ids': chunk_ids
            }

            send_json(master_socket, request)
            response = recv_json(master_socket)
            master_socket.close()

            if response and response.get('status') == 'success':
                return response.get('chunk_assignments', {})
            else:
                print(f"Master error: {response.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            print(f"Error requesting upload: {e}")
            return None

    def request_download(self, filename):

        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((self.master_host, self.master_port))

            request = {
                'command': 'DOWNLOAD',
                'filename': filename
            }

            send_json(master_socket, request)
            response = recv_json(master_socket)
            master_socket.close()

            if response and response.get('status') == 'success':
                return response
            else:
                print(f"Master error: {response.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            print(f"Error requesting download: {e}")
            return None

    def store_chunk(self, node_host, node_port, chunk_id, chunk_data):
        try:
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_socket.connect((node_host, node_port))

            # Get chunk size
            chunk_size = len(chunk_data)

            # Create request metadata (no data field, just metadata)
            request_metadata = {
                'command': 'STORE',
                'chunk_id': chunk_id,
                'size': chunk_size
            }

            # Send metadata via JSON
            send_json(node_socket, request_metadata)

            # Send raw binary chunk data
            node_socket.sendall(chunk_data)

            # Receive response
            response = recv_json(node_socket)
            node_socket.close()

            if response and response.get('status') == 'success':
                print(f"  Stored chunk {chunk_id} on {node_host}:{node_port}")
                return True
            else:
                print(f"  Failed to store chunk {chunk_id} on {node_host}:{node_port}")
                return False
        except Exception as e:
            print(f"  Error storing chunk on {node_host}:{node_port}: {e}")
            return False

    def retrieve_chunk(self, node_host, node_port, chunk_id):
        try:
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_socket.connect((node_host, node_port))

            # Send retrieve request
            request = {
                'command': 'RETRIEVE',
                'chunk_id': chunk_id
            }

            send_json(node_socket, request)

            # Receive metadata response
            response = recv_json(node_socket)

            if response and response.get('status') == 'success':
                # Get chunk size from response
                chunk_size = response.get('size')

                if chunk_size is None:
                    print(f"  Error: No size in response for chunk {chunk_id}")
                    node_socket.close()
                    return None

                # Receive raw binary chunk data
                chunk_data = recv_all(node_socket, chunk_size)

                node_socket.close()

                if chunk_data is None:
                    print(f"  Failed to receive data for chunk {chunk_id} from {node_host}:{node_port}")
                    return None

                print(f"  Retrieved chunk {chunk_id} from {node_host}:{node_port}")
                return chunk_data
            else:
                print(f"  Failed to retrieve chunk {chunk_id} from {node_host}:{node_port}")
                node_socket.close()
                return None
        except Exception as e:
            print(f"  Error retrieving chunk from {node_host}:{node_port}: {e}")
            return None

    def report_chunk_storage(self, chunk_id, locations):

        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((self.master_host, self.master_port))

            request = {
                'command': 'REPORT_CHUNK',
                'chunk_id': chunk_id,
                'locations': locations
            }

            send_json(master_socket, request)
            response = recv_json(master_socket)
            master_socket.close()
        except Exception as e:
            print(f"Error reporting chunk storage: {e}")

    def reassemble_file(self, chunks, output_path):

        with open(output_path, 'wb') as f:
            for chunk_data in chunks:
                f.write(chunk_data)


def print_usage():

    print("Usage:")
    print("  python client.py upload <filepath>")
    print("  python client.py download <filename> <output_path>")
    print("  python client.py list")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    client = Client()
    command = sys.argv[1].lower()

    if command == 'upload':
        if len(sys.argv) != 3:
            print("Usage: python client.py upload <filepath>")
            sys.exit(1)
        filepath = sys.argv[2]
        client.upload_file(filepath)

    elif command == 'download':
        if len(sys.argv) != 4:
            print("Usage: python client.py download <filename> <output_path>")
            sys.exit(1)
        filename = sys.argv[2]
        output_path = sys.argv[3]
        client.download_file(filename, output_path)

    elif command == 'list':
        client.list_files()

    else:
        print(f"Unknown command: {command}")
        print_usage()
        sys.exit(1)

