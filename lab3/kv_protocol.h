/*
 * kv_protocol.h  —  Lab 3: Advanced Socket Programming & Protocol Design
 *
 * This header defines the binary protocol shared between the TCP/UDP
 * server and the client.  Every multi-byte integer field is transmitted
 * in NETWORK byte order (big-endian) using htonl() / ntohl().
 *
 * MESSAGE FORMAT (both TCP and UDP)
 * ──────────────────────────────────
 *  ┌─────────────────────────────────────┐
 *  │  kv_request  (8 bytes fixed header) │
 *  │  + key (null-terminated string)     │
 *  │  + value (null-terminated string,   │
 *  │    only for OP_SET)                 │
 *  └─────────────────────────────────────┘
 *
 *  ┌─────────────────────────────────────┐
 *  │  kv_response (8 bytes fixed header) │
 *  │  + value (null-terminated string,   │
 *  │    only for OP_GET response)        │
 *  └─────────────────────────────────────┘
 *
 * The `length` field in BOTH structs holds the TOTAL packet size in
 * bytes (header + all payload).  The receiver reads the header first,
 * converts `length` from network order, then reads (length - 8) more
 * bytes of payload.
 *
 * Reference: TLPI Chapter 59 (Sockets: Internet Domains)
 *            TLPI Chapter 61 (Advanced Socket Topics)
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif

#ifndef KV_PROTOCOL_H
#define KV_PROTOCOL_H

/* ====================================================================
 * Feature-test macros — must come before any system include.
 * Enables POSIX-2008 + BSD/glibc extensions (flock, epoll, etc.)
 * ==================================================================== */

#include <stdint.h>
#include <arpa/inet.h>      /* htonl(), ntohl(), htons(), ntohs()        */
#include <string.h>         /* memcpy(), memset(), strlen()              */

/* ====================================================================
 * Opcodes  (stored in kv_request.opcode, network byte order)
 * ==================================================================== */
#define OP_SET   1   /* Store key → value                               */
#define OP_GET   2   /* Retrieve value for key                          */
#define OP_DEL   3   /* Delete key (and its value)                      */

/* ====================================================================
 * Status codes  (stored in kv_response.status, network byte order)
 * ==================================================================== */
#define STATUS_SUCCESS    0  /* Operation completed successfully         */
#define STATUS_NOT_FOUND  1  /* Key does not exist (GET or DEL)         */
#define STATUS_ERROR      2  /* General server error                    */
#define STATUS_BAD_REQ    3  /* Malformed request (wrong length, etc.)  */

/* ====================================================================
 * Maximum sizes
 * ==================================================================== */
#define KV_MAX_KEY_LEN    128   /* bytes, including null terminator     */
#define KV_MAX_VAL_LEN    512   /* bytes, including null terminator     */

/*
 * Maximum total packet size:
 *   8 (header) + 128 (key) + 512 (value) = 648 bytes
 * This is well below the typical PIPE_BUF (4096) so writes to UDP are
 * also safe from fragmentation in most environments.
 */
#define KV_MAX_PACKET_LEN (8 + KV_MAX_KEY_LEN + KV_MAX_VAL_LEN)

/* ====================================================================
 * Wire structures
 *
 * IMPORTANT: ALL multi-byte fields are in NETWORK (big-endian) byte
 * order on the wire.  Use htonl() before sending and ntohl() after
 * receiving.  Never read them directly without converting first.
 *
 * __attribute__((packed)) prevents the compiler from inserting padding
 * bytes between fields, ensuring the struct layout matches the wire
 * format exactly (TLPI §59.7).
 * ==================================================================== */

/*
 * kv_request — fixed 8-byte header.
 *
 * Followed immediately by:
 *   • key  (null-terminated, ≤ KV_MAX_KEY_LEN bytes incl. '\0')
 *   • value (null-terminated, ≤ KV_MAX_VAL_LEN bytes incl. '\0')
 *     — only present when opcode == OP_SET
 *
 * length = sizeof(kv_request) + strlen(key)+1 [+ strlen(value)+1 for SET]
 */
struct kv_request {
    uint32_t length;   /* total packet size in bytes (header + payload) */
    uint32_t opcode;   /* OP_SET, OP_GET, or OP_DEL                     */
} __attribute__((packed));

/*
 * kv_response — fixed 8-byte header.
 *
 * Followed immediately by:
 *   • value (null-terminated) — only when status == STATUS_SUCCESS
 *     and the original opcode was OP_GET
 *
 * length = sizeof(kv_response) [+ strlen(value)+1 for GET response]
 */
struct kv_response {
    uint32_t length;   /* total packet size in bytes (header + payload) */
    uint32_t status;   /* STATUS_SUCCESS, STATUS_NOT_FOUND, etc.        */
} __attribute__((packed));

/* ====================================================================
 * Serialisation helpers
 *
 * These inline functions build complete packets into caller-supplied
 * buffers and return the total number of bytes written.  All integers
 * are converted to network byte order automatically.
 *
 * Using inline functions (rather than macros) gives us type checking.
 * ==================================================================== */

/*
 * build_set_request — assemble an OP_SET packet.
 *
 * Writes into `buf` (must be ≥ KV_MAX_PACKET_LEN bytes):
 *   [kv_request header][key\0][value\0]
 *
 * Returns total bytes written, or -1 if key/value are too long.
 */
static inline int build_set_request(uint8_t *buf,
                                    const char *key, const char *val)
{
    size_t klen = strlen(key) + 1;   /* include null terminator */
    size_t vlen = strlen(val) + 1;
    uint32_t total = (uint32_t)(sizeof(struct kv_request) + klen + vlen);

    if (klen > KV_MAX_KEY_LEN || vlen > KV_MAX_VAL_LEN)
        return -1;

    struct kv_request hdr;
    hdr.length = htonl(total);
    hdr.opcode = htonl(OP_SET);

    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), key, klen);
    memcpy(buf + sizeof(hdr) + klen, val, vlen);
    return (int)total;
}

/*
 * build_get_request — assemble an OP_GET packet.
 *
 * Writes into `buf`: [kv_request header][key\0]
 * Returns total bytes written, or -1 if key is too long.
 */
static inline int build_get_request(uint8_t *buf, const char *key)
{
    size_t klen = strlen(key) + 1;
    uint32_t total = (uint32_t)(sizeof(struct kv_request) + klen);

    if (klen > KV_MAX_KEY_LEN) return -1;

    struct kv_request hdr;
    hdr.length = htonl(total);
    hdr.opcode = htonl(OP_GET);

    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), key, klen);
    return (int)total;
}

/*
 * build_del_request — assemble an OP_DEL packet.
 *
 * Writes into `buf`: [kv_request header][key\0]
 * Returns total bytes written, or -1 if key is too long.
 */
static inline int build_del_request(uint8_t *buf, const char *key)
{
    size_t klen = strlen(key) + 1;
    uint32_t total = (uint32_t)(sizeof(struct kv_request) + klen);

    if (klen > KV_MAX_KEY_LEN) return -1;

    struct kv_request hdr;
    hdr.length = htonl(total);
    hdr.opcode = htonl(OP_DEL);

    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), key, klen);
    return (int)total;
}

/*
 * build_response — assemble a response packet.
 *
 * `value` may be NULL (for non-GET responses or error responses).
 * Writes into `buf`: [kv_response header][value\0 (optional)]
 * Returns total bytes written.
 */
static inline int build_response(uint8_t *buf, uint32_t status,
                                 const char *value)
{
    size_t vlen = (value != NULL) ? (strlen(value) + 1) : 0;
    uint32_t total = (uint32_t)(sizeof(struct kv_response) + vlen);

    struct kv_response hdr;
    hdr.length = htonl(total);
    hdr.status = htonl(status);

    memcpy(buf, &hdr, sizeof(hdr));
    if (vlen > 0)
        memcpy(buf + sizeof(hdr), value, vlen);
    return (int)total;
}

#endif /* KV_PROTOCOL_H */
