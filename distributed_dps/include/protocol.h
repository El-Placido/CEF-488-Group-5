#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>

#define COORDINATOR_PORT 9000
#define MAX_WORKERS      16
#define MAX_PAYLOAD      4096
#define RETRANSMIT_MS    500     /* retransmit after 500ms */
#define MAX_RETRIES      5
#define HEARTBEAT_SEC    3       /* workers send heartbeat every 3s */
#define WORKER_TIMEOUT   10      /* coordinator drops worker after 10s */

/* Message types */
#define MSG_REGISTER      0x01
#define MSG_REGISTER_ACK  0x02
#define MSG_CHUNK_ASSIGN  0x03
#define MSG_CHUNK_ACK     0x04
#define MSG_RESULT        0x05
#define MSG_RESULT_ACK    0x06
#define MSG_HEARTBEAT     0x07
#define MSG_HEARTBEAT_ACK 0x08
#define MSG_TASK_SUBMIT   0x09
#define MSG_TASK_DONE     0x0A

/* Task types */
#define TASK_WORD_COUNT   0x01
#define TASK_SUM_NUMBERS  0x02

/* Packet header — packed so no padding bytes are inserted */
typedef struct __attribute__((packed)) {
    uint32_t seq_num;
    uint8_t  msg_type;
    uint16_t payload_len;
} PacketHeader;

/* Full packet */
typedef struct __attribute__((packed)) {
    PacketHeader header;
    char payload[MAX_PAYLOAD];
} Packet;

/* Chunk assignment payload */
typedef struct __attribute__((packed)) {
    uint32_t chunk_id;
    uint8_t  task_type;
    uint32_t data_len;
    char     data[MAX_PAYLOAD - 9];
} ChunkPayload;

/* Result payload */
typedef struct __attribute__((packed)) {
    uint32_t chunk_id;
    int64_t  result;   /* word count OR sum, both fit in int64_t */
} ResultPayload;

/* Worker registration ACK payload */
typedef struct __attribute__((packed)) {
    uint32_t worker_id;
} RegisterAckPayload;

#endif /* PROTOCOL_H */
