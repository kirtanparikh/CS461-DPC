import socket
import json

def send_json(sock, data):
    try:
        message = json.dumps(data)
        sock.sendall(message.encode('utf-8'))
        sock.sendall(b'<<END>>')
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Error sending message: {e}")
        return False
    return True

def recv_json(sock):
    buffer = ""
    try:
        while True:
            data = sock.recv(1024).decode('utf-8')
            if not data:
                return None
            buffer += data
            if '<<END>>' in buffer:
                message, _ = buffer.split('<<END>>', 1)
                return json.loads(message)
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Error receiving message: {e}")
        return None
