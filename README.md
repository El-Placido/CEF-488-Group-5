# Distributed Data Processing System (DDS)

A coordinator–worker distributed system written in **C** using **UDP sockets**
with retransmission, sequence numbers, `epoll`-based non-blocking I/O, dynamic
worker registration, fault tolerance, and configurable processing tasks.

---

## Directory Layout

```
dds/
├── include/
│   ├── protocol.h      # Wire protocol: message types, structs, helpers
│   └── net_util.h      # UDP socket helpers: reliable send, epoll utils
├── src/
│   ├── coordinator.c   # Master node — splits data, assigns chunks, aggregates
│   └── worker.c        # Worker node — receives chunks, processes, returns results
├── data/
│   ├── generate_data.py  # Script to produce test files
│   ├── words.txt         # (generated) word-count test file
│   ├── numbers.txt       # (generated) sum-numbers test file
│   └── lines.txt         # (generated) line-count test file
├── Makefile
└── README.md
```

---

## Features

| Requirement | Implementation |
|---|---|
| Coordinator splits file into chunks | `load_file_chunks()` in `coordinator.c` |
| Workers process chunks (word count / sum / line count) | `process_chunk()` in `worker.c` |
| Coordinator aggregates results | `grand_total` accumulator, `print_final_result()` |
| UDP with retransmission + sequence numbers | `udp_send_reliable()` in `net_util.h` |
| Dynamic worker registration | Workers register at any time via `MSG_REGISTER` |
| Fault tolerance (dead worker → reassign) | `check_worker_timeouts()` + chunk state machine |
| Non-blocking I/O + `epoll` | `epoll_create1` / `epoll_wait` event loop in coordinator |
| Configurable task type | Passed as CLI argument; coordinator broadcasts to workers |

---

## Build

### Prerequisites

```bash
# Ubuntu / Debian
sudo apt update
sudo apt install gcc make python3
```

### Compile

```bash
cd dds
make          # builds both 'coordinator' and 'worker'
make clean    # removes binaries
```

You should see zero compiler warnings. If any appear, please report them.

---

## Generate Test Data

```bash
python3 data/generate_data.py words    # creates data/words.txt
python3 data/generate_data.py numbers  # creates data/numbers.txt
python3 data/generate_data.py lines    # creates data/lines.txt
```

Each command also prints the **expected result** you can compare against the
coordinator's final output.

---

## Running the System

You need **at least three terminals** open in the `dds/` directory.

### Task type argument

| Value | Task |
|---|---|
| `1` | Word count (default) |
| `2` | Sum of integers |
| `3` | Line count |

---

### Test 1 — Word Count

**Terminal 1 (Coordinator):**
```bash
./coordinator data/words.txt 1
```

**Terminal 2 (Worker 1):**
```bash
./worker 127.0.0.1 9001
```

**Terminal 3 (Worker 2):**
```bash
./worker 127.0.0.1 9002
```

The coordinator splits `words.txt` into 4 KB chunks and distributes them.
Workers count words in their chunks and return results. The coordinator sums
the partial counts and prints the total.

Compare the output against the expected count printed by `generate_data.py`.

---

### Test 2 — Sum of Numbers

**Terminal 1:**
```bash
./coordinator data/numbers.txt 2
```

**Terminal 2:**
```bash
./worker 127.0.0.1 9001
```

**Terminal 3:**
```bash
./worker 127.0.0.1 9002
```

---

### Test 3 — Line Count

```bash
./coordinator data/lines.txt 3
./worker 127.0.0.1 9001
./worker 127.0.0.1 9002
```

---

### Test 4 — Dynamic Worker Registration

Start the coordinator, then start workers **one at a time** at different times
to verify they can join mid-processing:

```bash
# Terminal 1
./coordinator data/words.txt 1

# Terminal 2 (join immediately)
./worker 127.0.0.1 9001

# Terminal 3 (join 5 seconds later — coordinator will have already sent some chunks)
sleep 5 && ./worker 127.0.0.1 9002
```

---

### Test 5 — Fault Tolerance

Start coordinator and two workers. After a few chunks are assigned, kill one
worker with `Ctrl+C`. Watch the coordinator output: after `WORKER_TIMEOUT`
seconds (default 10 s) the coordinator will log the timeout and reassign that
worker's chunk to the surviving worker.

```bash
# Terminal 1
./coordinator data/words.txt 1

# Terminal 2
./worker 127.0.0.1 9001

# Terminal 3 — kill after a few seconds
./worker 127.0.0.1 9002
# Press Ctrl+C in Terminal 3 to simulate failure
```

---

### Test 6 — Three or More Workers

```bash
./coordinator data/words.txt 1
./worker 127.0.0.1 9001
./worker 127.0.0.1 9002
./worker 127.0.0.1 9003
```

Chunks are distributed round-robin to the first available idle worker.

---

## Protocol Reference

All messages follow a fixed **9-byte header**:

```
 0       1       2-3      4-5         6-9
 type    (pad)   seq      worker_id   payload_len
```

All multi-byte fields are **network byte order** (big-endian).

| Message | Direction | Purpose |
|---|---|---|
| `MSG_REGISTER` | Worker → Coordinator | Announce presence, send listen port |
| `MSG_REGISTER_ACK` | Coordinator → Worker | Confirm ID and task type |
| `MSG_CHUNK_ASSIGN` | Coordinator → Worker | Deliver a data chunk |
| `MSG_CHUNK_ACK` | Worker → Coordinator | Confirm chunk received |
| `MSG_RESULT` | Worker → Coordinator | Return computed result |
| `MSG_RESULT_ACK` | Coordinator → Worker | Confirm result received |
| `MSG_HEARTBEAT` | Worker → Coordinator | Liveness probe (every 3 s) |
| `MSG_HEARTBEAT_ACK` | Coordinator → Worker | Acknowledge probe |
| `MSG_SHUTDOWN` | Coordinator → Worker | Stop all workers at the end |

Retransmission: up to **5 attempts**, **2-second timeout** each, for reliable
messages (`MSG_REGISTER`, `MSG_RESULT`). Sequence numbers allow the receiver to
detect and discard duplicates.

---

## Configuration Constants (`include/protocol.h`)

| Constant | Default | Description |
|---|---|---|
| `COORDINATOR_PORT` | 9000 | UDP port coordinator listens on |
| `MAX_CHUNK_SIZE` | 4096 | Bytes per chunk |
| `HEARTBEAT_INTERVAL` | 3 | Seconds between worker heartbeats |
| `WORKER_TIMEOUT` | 10 | Seconds before declaring a worker dead |
| `RETRANSMIT_TIMEOUT` | 2 | Seconds before retransmitting |
| `MAX_RETRANSMITS` | 5 | Max retransmit attempts |
| `MAX_WORKERS` | 64 | Maximum concurrent workers |
| `MAX_CHUNKS` | 1024 | Maximum file chunks |

---

## Git Log

Initialize a repository and commit as you go:

```bash
cd dds
git init
git add .
git commit -m "Initial commit: coordinator, worker, protocol, Makefile, README"
```

---

## Known Limitations

- All nodes must be reachable via UDP on `localhost` (or a LAN with no NAT).
- File size is limited to `MAX_CHUNKS × MAX_CHUNK_SIZE` = 4 MB by default;
  increase `MAX_CHUNKS` in `protocol.h` for larger files.
- The coordinator is single-process; it uses `epoll` for I/O concurrency but
  not multi-threading.
