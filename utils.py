import socket
import json
import struct

def recv_all(sock, length):
    """
    Receive exactly 'length' bytes from socket.
    Handles partial reads and loops until all bytes are received.
    Returns the buffer of 'length' bytes, or None if socket closes.
    """
    buffer = b''
    while len(buffer) < length:
        try:
            chunk = sock.recv(length - len(buffer))
            if not chunk:
                # Socket closed
                return None
            buffer += chunk
        except socket.error as e:
            print(f"Error receiving data: {e}")
            return None
    return buffer

def send_json(sock, data):
    """
    Send JSON data using length-prefixed protocol.
    Sends a 4-byte header with message length, then the JSON message.
    """
    try:
        # Serialize data to JSON byte string
        message = json.dumps(data).encode('utf-8')

        # Get the length of the message
        length = len(message)

        # Pack the length into a 4-byte header (network byte order)
        header = struct.pack("!I", length)

        # Send the 4-byte length header
        sock.sendall(header)

        # Send the JSON message
        sock.sendall(message)
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Error sending message: {e}")
        return False
    return True

def recv_json(sock):
    """
    Receive JSON data using length-prefixed protocol.
    Reads a 4-byte header to get message length, then reads the full message.
    """
    try:
        # Read 4-byte length prefix
        header_bytes = recv_all(sock, 4)
        if header_bytes is None:
            return None

        # Unpack the header to get message length
        length = struct.unpack("!I", header_bytes)[0]

        # Read the full JSON message
        message_bytes = recv_all(sock, length)
        if message_bytes is None:
            return None

        # Decode and parse JSON
        return json.loads(message_bytes.decode('utf-8'))
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Error receiving message: {e}")
        return None
