/*
 * protocol.h - Distributed Data Processing System
 * Defines all shared message types, constants, and packet structures
 * used by the coordinator and workers over TCP.
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <sys/types.h>

/* ─── Port & size constants ─────────────────────────────────────────────── */
#define COORD_PORT        9000          /* Coordinator listens on this port  */
#define MAX_WORKERS       64            /* Maximum simultaneous workers      */
#define MAX_CHUNK_SIZE    65536         /* Maximum bytes per chunk (64 KB)   */
#define MAX_RESULT_SIZE   32768         /* Maximum bytes in a result payload */
#define MAX_FILENAME      256           /* Maximum filename length           */
#define HEARTBEAT_SECS    5             /* Worker sends heartbeat every N s  */
#define WORKER_TIMEOUT    15            /* Coordinator drops worker after N s*/
#define RETRANSMIT_MS     2000          /* Retransmit after N milliseconds   */
#define MAX_RETRIES       5             /* Give up after this many retries   */

/* ─── Message type codes ────────────────────────────────────────────────── */
typedef enum {
    MSG_REGISTER        = 1,   /* Worker → Coordinator: "I'm here"          */
    MSG_REGISTER_ACK    = 2,   /* Coordinator → Worker: "Registered OK"     */
    MSG_TASK_ASSIGN     = 3,   /* Coordinator → Worker: here is your chunk  */
    MSG_TASK_ACK        = 4,   /* Worker → Coordinator: chunk received      */
    MSG_RESULT          = 5,   /* Worker → Coordinator: here is my result   */
    MSG_RESULT_ACK      = 6,   /* Coordinator → Worker: result received     */
    MSG_HEARTBEAT       = 7,   /* Worker → Coordinator: I'm still alive     */
    MSG_HEARTBEAT_ACK   = 8,   /* Coordinator → Worker: acknowledged        */
    MSG_TASK_REASSIGN   = 9,   /* Coordinator → Worker: extra/failed chunk  */
    MSG_SHUTDOWN        = 10,  /* Coordinator → Worker: all done, exit      */
    MSG_WORKER_LEAVE    = 11,  /* Worker → Coordinator: I'm leaving cleanly */
} MsgType;

/* ─── Task / processing mode ────────────────────────────────────────────── */
typedef enum {
    TASK_WORD_COUNT  = 1,   /* Count words in text                         */
    TASK_SUM_NUMBERS = 2,   /* Sum all integers found in the data          */
    TASK_LINE_COUNT  = 3,   /* Count newline-terminated lines              */
} TaskType;

/* ─── Fixed-size packet header (sent before every message) ─────────────── */
/*
 * Every TCP message begins with this 16-byte header, followed by
 * `payload_len` bytes of payload whose layout depends on `msg_type`.
 */
typedef struct __attribute__((packed)) {
    uint32_t magic;          /* Must be 0xDEADBEEF – sanity check          */
    uint8_t  msg_type;       /* One of the MsgType values above            */
    uint8_t  task_type;      /* One of the TaskType values (where relevant)*/
    uint16_t seq_num;        /* Sequence number for retransmission logic   */
    uint16_t worker_id;      /* Assigned worker id (0 = not yet assigned)  */
    uint16_t chunk_id;       /* Which chunk this message concerns          */
    uint32_t payload_len;    /* Bytes that follow this header              */
} PktHeader;

#define MAGIC_NUMBER  0xDEADBEEFU

/* ─── Payload structures ────────────────────────────────────────────────── */

/* MSG_REGISTER payload (Worker → Coordinator) */
typedef struct __attribute__((packed)) {
    char hostname[64];       /* Worker's hostname for logging              */
} RegisterPayload;

/* MSG_REGISTER_ACK payload (Coordinator → Worker) */
typedef struct __attribute__((packed)) {
    uint16_t assigned_id;    /* Permanent id for this worker session       */
} RegisterAckPayload;

/* MSG_TASK_ASSIGN payload (Coordinator → Worker) */
typedef struct __attribute__((packed)) {
    uint16_t chunk_id;       /* Index of this chunk                        */
    uint32_t chunk_len;      /* Number of data bytes that follow struct    */
    uint8_t  task_type;      /* TASK_WORD_COUNT, TASK_SUM_NUMBERS, etc.   */
    /* chunk_len raw bytes of data follow immediately after this struct    */
} TaskAssignPayload;

/* MSG_RESULT payload (Worker → Coordinator) */
typedef struct __attribute__((packed)) {
    uint16_t chunk_id;       /* Which chunk produced this result           */
    int64_t  result_value;   /* Numeric result (word count, sum, …)        */
} ResultPayload;

/* ─── Helper: full message sizes ────────────────────────────────────────── */
#define HDR_SIZE   sizeof(PktHeader)

#endif /* PROTOCOL_H */

