from flask import Flask, render_template, request, jsonify, send_file
import os
import sys
import hashlib
from client import Client
import tempfile
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()

client = Client()

@app.route('/')
def index():

    return render_template('index.html')

@app.route('/api/upload', methods=['POST'])
def upload_file():

    try:
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'No file provided'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'status': 'error', 'message': 'No file selected'}), 400

        filename = secure_filename(file.filename)
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(temp_path)

        file_size = os.path.getsize(temp_path)

        client.upload_file(temp_path)

        os.remove(temp_path)

        return jsonify({
            'status': 'success',
            'message': f'File "{filename}" uploaded successfully',
            'filename': filename,
            'size': file_size
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):

    try:

        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f'download_{filename}')

        client.download_file(filename, temp_path)

        return send_file(temp_path, as_attachment=True, download_name=filename)

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/files', methods=['GET'])
def list_files():

    try:
        import socket
        from utils import send_json, recv_json
        from config import MASTER_HOST, MASTER_PORT

        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((MASTER_HOST, MASTER_PORT))

        request_data = {'command': 'LIST_FILES'}
        send_json(master_socket, request_data)

        response = recv_json(master_socket)
        master_socket.close()

        if response and response.get('status') == 'success':
            files = response.get('files', [])
            return jsonify({'status': 'success', 'files': files})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to list files'}), 500

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/status', methods=['GET'])
def get_status():

    try:
        import socket
        from config import MASTER_HOST, MASTER_PORT

        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.settimeout(2)
            master_socket.connect((MASTER_HOST, MASTER_PORT))
            master_socket.close()
            master_status = 'online'
        except:
            master_status = 'offline'

        return jsonify({
            'status': 'success',
            'master': master_status,
            'message': 'System is ' + ('ready' if master_status == 'online' else 'not ready')
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    print("Starting Web Frontend for Distributed File Storage...")
    print("Access the interface at: http://localhost:5000")
    print("\nMake sure the following are running:")
    print("  1. Master Node (python master_node.py)")
    print("  2. Storage Nodes (python storage_node.py ...)")
    print("\nPress Ctrl+C to stop the web server\n")

    app.run(host='0.0.0.0', port=5000, debug=True)

