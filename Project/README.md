# Distributed Data Processing System

A coordinator–worker distributed computing system written in C using
**TCP sockets**, **epoll** (non-blocking I/O), and a custom binary protocol.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                   COORDINATOR                        │
│  • Splits input file into chunks                     │
│  • Assigns chunks to workers via TCP                 │
│  • Monitors heartbeats; detects/recovers failures    │
│  • Aggregates partial results → final answer         │
│  • Uses epoll for non-blocking I/O                   │
└───────────────┬──────────────────────────────────────┘
                │ TCP (port 9000)
       ┌────────┴────────┐
       ▼                 ▼
  ┌─────────┐       ┌─────────┐       ┌─────────┐
  │ Worker1 │       │ Worker2 │  ...  │ WorkerN │
  │ (task)  │       │ (task)  │       │ (task)  │
  └─────────┘       └─────────┘       └─────────┘
```

### Mandatory Features
| Feature | Implementation |
|---------|---------------|
| Coordinator splits dataset | `load_and_split_file()` in coordinator.c |
| Workers process chunks | `process_chunk()` in worker.c |
| Coordinator aggregates | `print_final_result()` in coordinator.c |
| TCP communication | `net_utils.c` – binary framing over TCP |

### Additional Features (all four implemented)
| Feature | Implementation |
|---------|---------------|
| Dynamic worker registration | Workers connect at any time; assigned IDs |
| Fault tolerance | Heartbeat timeout → chunk reassignment |
| Non-blocking I/O + epoll | `epoll_create1`, `EPOLLET`, `O_NONBLOCK` |
| Multiple task types | `TASK_WORD_COUNT`, `TASK_SUM_NUMBERS`, `TASK_LINE_COUNT` |

---

## Files

```
dds/
├── Makefile
├── README.md
├── include/
│   ├── protocol.h      # Packet layout, message types, constants
│   └── net_utils.h     # send_all, recv_all, send_msg, recv_msg
├── src/
│   ├── net_utils.c     # Network helper implementations
│   ├── coordinator.c   # Master process
│   └── worker.c        # Worker process
└── data/               # Created by `make test_data`
    ├── sample.txt
    └── numbers.txt
```

---

## Build

### Requirements
- GCC 7+ (uses C11 and `_GNU_SOURCE`)
- Linux kernel ≥ 2.6.17 (for `epoll_create1`)
- `make`
- `python3` (only for `make test_data`)

### Steps

```bash
# 1. Enter the project directory
cd dds

# 2. Build both binaries
make

# 3. (Optional) Generate sample test data
make test_data
```

Expected output:
```
Build complete.  Binaries: ./coordinator  ./worker
```

---

## Running the System

### Terminal layout (use separate terminal tabs or tmux panes)

**Machine 1 – Coordinator**
```bash
./coordinator data/sample.txt 1 2048
#              ^filename       ^task ^chunk_size_bytes
```

Task codes:
| Code | Task |
|------|------|
| 1 | Word count |
| 2 | Sum of numbers |
| 3 | Line count |

**Machine 2 – Worker 1**
```bash
./worker 127.0.0.1
```

**Machine 3 – Worker 2**
```bash
./worker 127.0.0.1
```

**Machine 4 – Worker 3 (optional, join mid-run to test dynamic registration)**
```bash
./worker 127.0.0.1
```

Workers can be started **before or after** the coordinator begins distributing
chunks.  New workers will be registered and immediately receive pending chunks.

---

## Testing Guide

### Test 1 – Word Count

```bash
# Generate data
make test_data

# Terminal 1
./coordinator data/sample.txt 1 1024

# Terminal 2
./worker 127.0.0.1

# Terminal 3
./worker 127.0.0.1
```

**Verify** by comparing coordinator output against:
```bash
wc -w data/sample.txt
```
Both numbers must match.

---

### Test 2 – Sum of Numbers

```bash
# Terminal 1
./coordinator data/numbers.txt 2 512

# Terminal 2
./worker 127.0.0.1

# Terminal 3
./worker 127.0.0.1
```

**Verify** with Python:
```bash
python3 -c "print(sum(int(x) for x in open('data/numbers.txt').read().split()))"
```

---

### Test 3 – Line Count

```bash
./coordinator data/sample.txt 3 2048
./worker 127.0.0.1   # in another terminal
```

**Verify**:
```bash
wc -l data/sample.txt
```

---

### Test 4 – Fault Tolerance (Worker Crash)

```bash
# Terminal 1 – coordinator with large file and many chunks
./coordinator data/sample.txt 1 512

# Terminal 2 – start worker 1
./worker 127.0.0.1

# Terminal 3 – start worker 2
./worker 127.0.0.1

# After a few seconds, kill worker 2 with Ctrl+C
# Watch the coordinator log: it detects the timeout and reassigns the chunk
# Final result must still be correct
```

---

### Test 5 – Dynamic Worker Registration

```bash
./coordinator data/sample.txt 1 256

# Start with one worker
./worker 127.0.0.1

# After ~5 seconds, add a second worker in a new terminal
./worker 127.0.0.1

# Add a third worker while processing is still running
./worker 127.0.0.1
```

The coordinator assigns pending chunks to each new worker as they join.

---

## Protocol Reference

Every TCP message has a 16-byte header (`PktHeader`) followed by an
optional variable-length payload.

| Field | Size | Description |
|-------|------|-------------|
| magic | 4 B | 0xDEADBEEF sanity check |
| msg_type | 1 B | See table below |
| task_type | 1 B | 1=words 2=sum 3=lines |
| seq_num | 2 B | Sequence counter (for retransmission) |
| worker_id | 2 B | Coordinator-assigned worker ID |
| chunk_id | 2 B | Chunk index |
| payload_len | 4 B | Bytes that follow |

Message types:

| Code | Name | Direction |
|------|------|-----------|
| 1 | MSG_REGISTER | Worker → Coord |
| 2 | MSG_REGISTER_ACK | Coord → Worker |
| 3 | MSG_TASK_ASSIGN | Coord → Worker |
| 4 | MSG_TASK_ACK | Worker → Coord |
| 5 | MSG_RESULT | Worker → Coord |
| 6 | MSG_RESULT_ACK | Coord → Worker |
| 7 | MSG_HEARTBEAT | Worker → Coord |
| 8 | MSG_HEARTBEAT_ACK | Coord → Worker |
| 9 | MSG_TASK_REASSIGN | Coord → Worker |
| 10 | MSG_SHUTDOWN | Coord → Worker |
| 11 | MSG_WORKER_LEAVE | Worker → Coord |

---

## Configuration Constants (include/protocol.h)

| Constant | Default | Meaning |
|----------|---------|---------|
| COORD_PORT | 9000 | TCP port coordinator listens on |
| MAX_WORKERS | 64 | Max simultaneous workers |
| MAX_CHUNK_SIZE | 65536 | Max bytes per chunk |
| HEARTBEAT_SECS | 5 | Seconds between worker heartbeats |
| WORKER_TIMEOUT | 15 | Seconds before coordinator marks worker dead |
| MAX_RETRIES | 5 | Result-send retransmission attempts |

---

## Git Setup

```bash
cd dds
git init
git add .
git commit -m "Initial commit: Distributed Data Processing System"

# Link to GitHub remote (replace with your URL)
git remote add origin https://github.com/YOURUSER/dds.git
git push -u origin main
```

