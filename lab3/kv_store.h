/*
 * kv_store.h  —  In-memory key-value store interface
 *
 * A simple hash table with separate chaining (linked lists per bucket).
 * All keys and values are null-terminated C strings.
 *
 * Thread safety: NOT thread-safe.  The server is single-threaded
 * (epoll-based), so no locking is needed.
 *
 * Reference: TLPI Chapter 3 (system programming concepts),
 *            standard data-structures textbooks.
 */

#ifndef KV_STORE_H
#define KV_STORE_H

/* ====================================================================
 * Configuration
 * ==================================================================== */
#define KV_NUM_BUCKETS   1024   /* must be a power of 2 for fast modulo */

/* ====================================================================
 * Data structures
 * ==================================================================== */

/*
 * kv_entry — one key/value pair in a bucket's linked list.
 */
struct kv_entry {
    char            *key;    /* heap-allocated copy of the key string   */
    char            *value;  /* heap-allocated copy of the value string */
    struct kv_entry *next;   /* next entry in the same bucket           */
};

/*
 * kv_store — the hash table itself.
 *
 * Declare one of these as a global variable in the server.
 */
struct kv_store {
    struct kv_entry *buckets[KV_NUM_BUCKETS];
    int              count;  /* total number of stored key-value pairs  */
};

/* ====================================================================
 * Function prototypes
 * ==================================================================== */

/*
 * kv_store_init — zero out the store.  Call once at startup.
 */
void kv_store_init(struct kv_store *store);

/*
 * kv_store_set — insert or update a key-value pair.
 * Copies both strings.  Returns 0 on success, -1 on malloc failure.
 */
int kv_store_set(struct kv_store *store,
                 const char *key, const char *value);

/*
 * kv_store_get — look up a key.
 * Returns a pointer to the stored value string (do not free it),
 * or NULL if the key does not exist.
 */
const char *kv_store_get(const struct kv_store *store, const char *key);

/*
 * kv_store_delete — remove a key-value pair.
 * Returns 0 if the key was found and removed, -1 if not found.
 */
int kv_store_delete(struct kv_store *store, const char *key);

/*
 * kv_store_destroy — free all memory held by the store.
 * The store struct itself is not freed (it is typically on the stack).
 */
void kv_store_destroy(struct kv_store *store);

#endif /* KV_STORE_H */
