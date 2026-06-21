# Distributed Data Processing System

## Build
make all

## Run
# Terminal 1
./coordinator

# Terminal 2
./worker 127.0.0.1

# Terminal 3
./worker 127.0.0.1

# Terminal 4 (For Word Count)
./client 127.0.0.1 data/sample.txt 1

# Terminal 4 (For Number Summing - alternate test case)
./client 127.0.0.1 data/sample_numbers.txt 2

## Architecture
- coordinator: splits file, distributes chunks, aggregates results
- worker: registers, receives chunks, processes, returns results
- client: submits jobs and receives final result

## Protocol
UDP with sequence numbers and retransmission (up to 5 retries, 500ms apart)

## Features Implemented
1. Dynamic worker registration
2. Fault tolerance with chunk reassignment
3. Non-blocking I/O with epoll
4. Configurable task types (word count, sum)
