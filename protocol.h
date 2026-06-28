#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>

/* ─── Port & sizing constants ─────────────────────────────────────────────── */
#define COORDINATOR_PORT   9000
#define MAX_WORKERS        64
#define MAX_CHUNK_SIZE     4096   /* bytes per chunk payload                   */
#define MAX_FILENAME       256
#define MAX_RESULT_SIZE    512
#define HEARTBEAT_INTERVAL 3      /* seconds between worker heartbeats         */
#define WORKER_TIMEOUT     10     /* seconds before worker declared dead       */
#define RETRANSMIT_TIMEOUT 2      /* seconds before retransmitting a packet    */
#define MAX_RETRANSMITS    5      /* max retransmit attempts before giving up  */

/* ─── Message type tags ───────────────────────────────────────────────────── */
typedef enum {
    MSG_REGISTER        = 1,  /* worker → coordinator: "I am here"            */
    MSG_REGISTER_ACK    = 2,  /* coordinator → worker: "Registered, ID=N"     */
    MSG_CHUNK_ASSIGN    = 3,  /* coordinator → worker: here is your chunk      */
    MSG_CHUNK_ACK       = 4,  /* worker → coordinator: chunk received          */
    MSG_RESULT          = 5,  /* worker → coordinator: here is my result       */
    MSG_RESULT_ACK      = 6,  /* coordinator → worker: result received         */
    MSG_HEARTBEAT       = 7,  /* worker → coordinator: I am alive              */
    MSG_HEARTBEAT_ACK   = 8,  /* coordinator → worker: acknowledged            */
    MSG_TASK_TYPE       = 9,  /* coordinator → worker: which task to perform   */
    MSG_SHUTDOWN        = 10, /* coordinator → worker: stop processing         */
    MSG_DONE            = 11  /* coordinator broadcasts: all work complete     */
} msg_type_t;

/* ─── Processing task types ───────────────────────────────────────────────── */
typedef enum {
    TASK_WORD_COUNT  = 1,  /* count words in the chunk                        */
    TASK_SUM_NUMBERS = 2,  /* sum integers found in the chunk                 */
    TASK_LINE_COUNT  = 3   /* count newline-terminated lines in the chunk     */
} task_type_t;

/* ─── Wire-format message header (fixed, always present) ─────────────────── */
/*  All multi-byte integers are sent in network byte order (big-endian).      */
typedef struct __attribute__((packed)) {
    uint8_t  type;        /* msg_type_t value                                 */
    uint16_t seq;         /* sequence number for retransmission logic         */
    uint16_t worker_id;   /* sender/target worker ID (0 = coordinator)        */
    uint32_t payload_len; /* byte length of the payload that follows          */
} msg_header_t;

/* ─── Payload structures ──────────────────────────────────────────────────── */

/* MSG_REGISTER — sent by worker to coordinator on startup */
typedef struct __attribute__((packed)) {
    uint16_t listen_port; /* UDP port this worker listens on                  */
} payload_register_t;

/* MSG_REGISTER_ACK — coordinator replies with the assigned ID */
typedef struct __attribute__((packed)) {
    uint16_t assigned_id; /* unique worker ID for this session                */
    uint8_t  task_type;   /* task_type_t: what kind of processing to perform  */
} payload_register_ack_t;

/* MSG_CHUNK_ASSIGN — coordinator sends a data chunk to a worker */
typedef struct __attribute__((packed)) {
    uint32_t chunk_id;               /* chunk sequence number                 */
    uint32_t data_len;               /* number of valid bytes in data[]       */
    char     data[MAX_CHUNK_SIZE];   /* raw chunk bytes                       */
} payload_chunk_t;

/* MSG_CHUNK_ACK — worker confirms receipt of a chunk */
typedef struct __attribute__((packed)) {
    uint32_t chunk_id;  /* mirrors the chunk_id from MSG_CHUNK_ASSIGN         */
} payload_chunk_ack_t;

/* MSG_RESULT — worker sends its computed result back */
typedef struct __attribute__((packed)) {
    uint32_t chunk_id;                 /* which chunk this result belongs to  */
    int64_t  value;                    /* numeric result (word/line/sum count) */
    char     detail[MAX_RESULT_SIZE];  /* human-readable result summary       */
} payload_result_t;

/* MSG_RESULT_ACK — coordinator confirms result receipt */
typedef struct __attribute__((packed)) {
    uint32_t chunk_id;
} payload_result_ack_t;

/* ─── Utility: build a complete message into a caller-supplied buffer ──────
 *  Returns total bytes written (header + payload), or -1 on error.           */
static inline int build_message(uint8_t *buf, size_t buf_size,
                                msg_type_t type, uint16_t seq,
                                uint16_t worker_id,
                                const void *payload, uint32_t payload_len)
{
    if (buf_size < sizeof(msg_header_t) + payload_len) return -1;

    msg_header_t hdr;
    hdr.type        = (uint8_t)type;
    hdr.seq         = htons(seq);
    hdr.worker_id   = htons(worker_id);
    hdr.payload_len = htonl(payload_len);

    memcpy(buf, &hdr, sizeof(hdr));
    if (payload && payload_len > 0)
        memcpy(buf + sizeof(hdr), payload, payload_len);

    return (int)(sizeof(hdr) + payload_len);
}

/* ─── Utility: parse header from a received buffer ─────────────────────────
 *  Fills *hdr with host-byte-order values.
 *  Returns pointer to the start of the payload (buf + sizeof header).        */
static inline const uint8_t *parse_header(const uint8_t *buf, size_t buf_len,
                                           msg_header_t *hdr)
{
    if (buf_len < sizeof(msg_header_t)) return NULL;
    const msg_header_t *wire = (const msg_header_t *)buf;
    hdr->type        = wire->type;
    hdr->seq         = ntohs(wire->seq);
    hdr->worker_id   = ntohs(wire->worker_id);
    hdr->payload_len = ntohl(wire->payload_len);
    return buf + sizeof(msg_header_t);
}

#endif /* PROTOCOL_H */
