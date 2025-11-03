# Distributed Cloud File Storage (Mini Dropbox)

A fault-tolerant distributed file storage system with a modern web interface.

## Quick Start

### Step 1: Install Flask

```powershell
pip install flask
```

### Step 2: Open 5 Terminals

**Terminal 1 - Master Node:**

```powershell
python master_node.py
```

**Terminal 2 - Storage Node 1:**

```powershell
python storage_node.py 127.0.0.1 9001 ./node1_storage
```

**Terminal 3 - Storage Node 2:**

```powershell
python storage_node.py 127.0.0.1 9002 ./node2_storage
```

**Terminal 4 - Storage Node 3:**

```powershell
python storage_node.py 127.0.0.1 9003 ./node3_storage
```

**Terminal 5 - Web Interface:**

```powershell
python web_frontend.py
```

### Step 3: Open Browser

Go to: **http://localhost:5000**

## How to Use

1. Check Status - Green dot means system is ready
2. Upload Files - Drag and drop or click to browse
3. Download Files - Click download button next to any file
4. Refresh - Update file list

## Test Fault Tolerance

1. Upload a file via web interface
2. Stop one storage node (Ctrl+C)
3. Wait 15 seconds
4. Download still works

System survives up to 2 node failures.

## Command Line (Alternative)

```powershell
# Upload
python client.py upload myfile.txt

# Download
python client.py download myfile.txt output.txt

# List files
python client.py list
```

## Configuration

Edit `config.py` to change:

- Chunk size (default: 1MB)
- Replication factor (default: 3)
- Heartbeat interval (default: 5 sec)

## Troubleshooting

**System Offline?**

- Make sure all 5 terminals are running
- Wait 10 seconds for nodes to connect

**Upload Fails?**

- Check all 3 storage nodes are running
- File size must be under 100MB

**Port Conflict?**

- Close previous instances (Ctrl+C)
- Or change ports in `config.py`

## Project Structure

```
CS461 DPC/
├── web_frontend.py     # Web server
├── templates/
│   └── index.html      # Web interface
├── master_node.py      # Master node
├── storage_node.py     # Storage nodes
├── client.py           # CLI client
├── config.py           # Settings
├── utils.py            # Utilities
└── node*_storage/      # Storage dirs
```

## How It Works

- **Upload**: File -> chunks -> replicate 3x -> store on nodes
- **Download**: Retrieve chunks -> reassemble -> download
- **Fault Tolerance**: If node fails, use replicas
- **Monitoring**: Heartbeats every 5 sec, failure detected in 15 sec

## Tech Stack

- Python 3 + Sockets
- Flask web server
- JSON protocol over TCP
- HTML/CSS/JavaScript frontend
