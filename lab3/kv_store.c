/*
 * kv_store.c  —  In-memory key-value hash table implementation
 *
 * Uses the djb2 hash function (by Dan Bernstein) which is fast and
 * produces a good distribution for string keys.
 *
 * Each bucket is a singly-linked list of kv_entry nodes.  On a SET
 * we walk the list to check if the key already exists (update) or
 * append a new node.  On GET/DEL we do the same walk.
 *
 * Reference: TLPI §3.5 (heap memory allocation)
 */


#include "kv_store.h"
#include <stdlib.h>    /* malloc(), free()                 */
#include <string.h>    /* strcmp(), strdup()               */
#include <stdio.h>     /* fprintf() for debug              */

/* ====================================================================
 * Internal helper: djb2 hash function
 *
 * Maps a string to an index in [0, KV_NUM_BUCKETS).
 * The hash is computed by starting with the seed 5381 and for each
 * character c doing: hash = hash * 33 + c   (TLPI-style explanation)
 * The bit-AND with (KV_NUM_BUCKETS - 1) replaces a modulo operation
 * since KV_NUM_BUCKETS is a power of 2.
 * ==================================================================== */
static unsigned int hash_key(const char *key)
{
    unsigned long hash = 5381;
    int c;
    while ((c = (unsigned char)*key++) != '\0')
        hash = hash * 33 + (unsigned long)c;
    return (unsigned int)(hash & (KV_NUM_BUCKETS - 1));
}

/* ====================================================================
 * Public API implementation
 * ==================================================================== */

void kv_store_init(struct kv_store *store)
{
    /* Zero all bucket pointers and the count */
    memset(store, 0, sizeof(*store));
}

int kv_store_set(struct kv_store *store,
                 const char *key, const char *value)
{
    unsigned int idx = hash_key(key);
    struct kv_entry *e = store->buckets[idx];

    /*
     * Walk the bucket's linked list.
     * If the key already exists, update the value in place.
     */
    while (e != NULL) {
        if (strcmp(e->key, key) == 0) {
            /* Key found — replace value */
            char *new_val = strdup(value);
            if (new_val == NULL) return -1;   /* malloc failed */
            free(e->value);
            e->value = new_val;
            return 0;
        }
        e = e->next;
    }

    /*
     * Key not found — allocate a new entry and prepend it to the
     * bucket list (prepending is O(1) and simpler than appending).
     */
    struct kv_entry *node = malloc(sizeof(struct kv_entry));
    if (node == NULL) return -1;

    node->key   = strdup(key);
    node->value = strdup(value);

    if (node->key == NULL || node->value == NULL) {
        free(node->key);
        free(node->value);
        free(node);
        return -1;
    }

    /* Prepend to the bucket's list */
    node->next = store->buckets[idx];
    store->buckets[idx] = node;
    store->count++;
    return 0;
}

const char *kv_store_get(const struct kv_store *store, const char *key)
{
    unsigned int idx = hash_key(key);
    const struct kv_entry *e = store->buckets[idx];

    while (e != NULL) {
        if (strcmp(e->key, key) == 0)
            return e->value;
        e = e->next;
    }
    return NULL;   /* key not found */
}

int kv_store_delete(struct kv_store *store, const char *key)
{
    unsigned int idx = hash_key(key);
    struct kv_entry **pp = &store->buckets[idx];   /* pointer to pointer */

    /*
     * Walk using a pointer-to-pointer so we can splice out the node
     * without a separate "prev" pointer.
     */
    while (*pp != NULL) {
        struct kv_entry *e = *pp;
        if (strcmp(e->key, key) == 0) {
            *pp = e->next;       /* unlink from list  */
            free(e->key);
            free(e->value);
            free(e);
            store->count--;
            return 0;
        }
        pp = &e->next;
    }
    return -1;   /* key not found */
}

void kv_store_destroy(struct kv_store *store)
{
    for (int i = 0; i < KV_NUM_BUCKETS; i++) {
        struct kv_entry *e = store->buckets[i];
        while (e != NULL) {
            struct kv_entry *next = e->next;
            free(e->key);
            free(e->value);
            free(e);
            e = next;
        }
        store->buckets[i] = NULL;
    }
    store->count = 0;
}
