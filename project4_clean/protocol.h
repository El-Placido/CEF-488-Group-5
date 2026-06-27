/*
 * protocol.h
 * ==========
 * Wire protocol for Project 4: Distributed Data Processing System.
 *
 * TRANSPORT: UDP
 * ---------------
 * UDP gives us no delivery guarantee, no ordering guarantee, and no
 * congestion control.  We build a minimal reliability layer on top of it
 * using sequence numbers and a request/acknowledgement pattern:
 *
 *   - Every message carries a 4-byte sequence number.
 *   - The coordinator retransmits a chunk if no result arrives within a
 *     timeout (fault tolerance against lost packets AND dead workers).
 *   - Workers re-send their result if they don't receive an ACK, using
 *     the same sequence number so the coordinator can de-duplicate.
 *
 * MESSAGE TYPES
 * -------------
 *   MSG_REGISTER     worker → coordinator   "I am available for work"
 *   MSG_REGISTER_ACK coordinator → worker   "Registered, your worker_id=X"
 *   MSG_TASK         coordinator → worker   "Here is a chunk to process"
 *   MSG_RESULT       worker → coordinator   "Here is my result for chunk N"
 *   MSG_RESULT_ACK   coordinator → worker   "Got your result, here's more work"
 *   MSG_LEAVE        worker → coordinator   "I am leaving gracefully"
 *   MSG_NO_MORE_WORK coordinator → worker   "All chunks done, you may exit"
 *   MSG_HEARTBEAT    worker → coordinator   "I am still alive" (idle keep-alive)
 *
 * WIRE FORMAT
 * -----------
 * Every datagram begins with a fixed 12-byte header:
 *
 *   Offset Size  Field
 *     0     1    msg_type     (one of MSG_* above)
 *     1     1    task_id      (which map/reduce task: TASK_WORDCOUNT, etc.)
 *     2     2    reserved     (zero, alignment)
 *     4     4    seq_num      (sequence number, network byte order)
 *     8     4    chunk_id     (which chunk this message refers to, or
 *                              0xFFFFFFFF if not applicable)
 *
 * Followed by a type-specific payload (see structs below).
 *
 * All multi-byte integers are sent in NETWORK byte order.
 *
 * CEF488 Group Project — Project 4: Distributed Data Processing System
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>

/* ====================================================================
 * Message types
 * ==================================================================== */
#define MSG_REGISTER      0x01   /* worker → coordinator */
#define MSG_REGISTER_ACK  0x02   /* coordinator → worker */
#define MSG_TASK          0x03   /* coordinator → worker */
#define MSG_RESULT        0x04   /* worker → coordinator */
#define MSG_RESULT_ACK    0x05   /* coordinator → worker */
#define MSG_LEAVE         0x06   /* worker → coordinator */
#define MSG_NO_MORE_WORK  0x07   /* coordinator → worker */
#define MSG_HEARTBEAT     0x08   /* worker → coordinator */

/* ====================================================================
 * Task identifiers — which map/reduce function to apply
 *
 * This is what makes "different processing tasks" configurable: the
 * coordinator tells every worker which task_id to use, and the worker
 * looks it up in its local task registry (see tasks.h).
 * ==================================================================== */
#define TASK_WORDCOUNT    1   /* count total words in the chunk          */
#define TASK_LINECOUNT    2   /* count total lines in the chunk          */
#define TASK_SUMNUMBERS   3   /* sum all whitespace-separated integers   */
#define TASK_CHARCOUNT    4   /* count total non-whitespace characters   */

/* ====================================================================
 * Size limits
 * ==================================================================== */
#define MAX_CHUNK_BYTES   1024   /* bytes of file data per chunk         */
#define MAX_DATAGRAM      1100   /* header + max chunk, fits one UDP pkt */
#define CHUNK_ID_NONE     0xFFFFFFFFu

/* ====================================================================
 * Fixed 12-byte header
 * ==================================================================== */
typedef struct {
    uint8_t  msg_type;
    uint8_t  task_id;
    uint16_t reserved;
    uint32_t seq_num;
    uint32_t chunk_id;
} __attribute__((packed)) msg_header;

/* ====================================================================
 * MSG_REGISTER payload — empty (header only)
 * MSG_REGISTER_ACK payload — assigned worker_id
 * ==================================================================== */
typedef struct {
    uint32_t worker_id;
} __attribute__((packed)) register_ack_payload;

/* ====================================================================
 * MSG_TASK payload — chunk_id is in the header; this carries the
 * actual chunk bytes.  total_len tells the worker how many of the
 * MAX_CHUNK_BYTES are valid (the last chunk may be shorter).
 * ==================================================================== */
typedef struct {
    uint32_t data_len;                  /* valid bytes in `data`        */
    char     data[MAX_CHUNK_BYTES];     /* raw chunk bytes (not a C str)*/
} __attribute__((packed)) task_payload;

/* ====================================================================
 * MSG_RESULT payload — the worker's computed value for chunk_id.
 * We use a signed 64-bit integer, large enough for word counts, line
 * counts, character counts, and sums of numbers in a text file.
 * ==================================================================== */
typedef struct {
    int64_t result_value;
} __attribute__((packed)) result_payload;

/* ====================================================================
 * Byte order helpers for the header
 * ==================================================================== */
static inline void header_to_network(msg_header *h)
{
    h->seq_num   = htonl(h->seq_num);
    h->chunk_id  = htonl(h->chunk_id);
    h->reserved  = htons(h->reserved);
}

static inline void header_to_host(msg_header *h)
{
    h->seq_num   = ntohl(h->seq_num);
    h->chunk_id  = ntohl(h->chunk_id);
    h->reserved  = ntohs(h->reserved);
}

/* ====================================================================
 * 64-bit value byte order helpers (manual, since htobe64 portability
 * varies across libc versions — we roll our own to keep this header
 * self-contained).
 * ==================================================================== */
static inline uint64_t hton64(uint64_t v)
{
    uint32_t hi = (uint32_t)(v >> 32);
    uint32_t lo = (uint32_t)(v & 0xFFFFFFFFu);
    return ((uint64_t)htonl(lo) << 32) | htonl(hi);
}
static inline uint64_t ntoh64(uint64_t v) { return hton64(v); /* symmetric */ }

#endif /* PROTOCOL_H */
