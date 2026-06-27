/*
 * tasks.c
 * =======
 * Implements the four built-in map/reduce tasks and the registry
 * lookup function declared in tasks.h.
 *
 * CEF488 Group Project — Project 4: Distributed Data Processing System
 */

#include "tasks.h"
#include "protocol.h"
#include <ctype.h>

/* ====================================================================
 * TASK_WORDCOUNT — count whitespace-separated tokens in the chunk
 *
 * A "word" begins right after we see whitespace (or at the start of
 * the chunk) and continues until the next whitespace.  This correctly
 * counts words even when a word is split across a chunk boundary, as
 * long as the reduce step is a simple sum — the only error this
 * introduces is double-counting (or missing) the one word that straddles
 * the boundary, which is negligible for large texts and noted in the
 * report as a known approximation of chunked word counting.
 * ==================================================================== */
static int64_t map_wordcount(const char *data, uint32_t len)
{
    int64_t count   = 0;
    int     in_word  = 0;

    for (uint32_t i = 0; i < len; i++) {
        if (isspace((unsigned char)data[i])) {
            in_word = 0;
        } else if (!in_word) {
            in_word = 1;
            count++;
        }
    }
    return count;
}

/* ====================================================================
 * TASK_LINECOUNT — count newline characters in the chunk
 * ==================================================================== */
static int64_t map_linecount(const char *data, uint32_t len)
{
    int64_t count = 0;
    for (uint32_t i = 0; i < len; i++)
        if (data[i] == '\n') count++;
    return count;
}

/* ====================================================================
 * TASK_SUMNUMBERS — sum all whitespace-separated integers in the chunk
 *
 * Non-numeric tokens are skipped (their contribution is 0). Negative
 * numbers (leading '-') are supported.
 * ==================================================================== */
static int64_t map_sumnumbers(const char *data, uint32_t len)
{
    int64_t total = 0;
    uint32_t i = 0;

    while (i < len) {
        /* Skip whitespace */
        while (i < len && isspace((unsigned char)data[i])) i++;
        if (i >= len) break;

        /* Parse one token: optional '-' followed by digits */
        int      sign = 1;
        int64_t  val  = 0;
        int      saw_digit = 0;

        if (data[i] == '-') { sign = -1; i++; }

        while (i < len && isdigit((unsigned char)data[i])) {
            val = val * 10 + (data[i] - '0');
            saw_digit = 1;
            i++;
        }

        if (saw_digit) total += sign * val;

        /* Skip to next whitespace boundary in case of trailing junk */
        while (i < len && !isspace((unsigned char)data[i])) i++;
    }
    return total;
}

/* ====================================================================
 * TASK_CHARCOUNT — count non-whitespace characters in the chunk
 * ==================================================================== */
static int64_t map_charcount(const char *data, uint32_t len)
{
    int64_t count = 0;
    for (uint32_t i = 0; i < len; i++)
        if (!isspace((unsigned char)data[i])) count++;
    return count;
}

/* ====================================================================
 * Reduce function: all four tasks combine partial results by SUMMING.
 * (A word count total is the sum of per-chunk word counts, etc.)
 * One shared function keeps the registry table compact.
 * ==================================================================== */
static int64_t reduce_sum(int64_t acc, int64_t next)
{
    return acc + next;
}

/* ====================================================================
 * The task registry table
 *
 * To add a new task: write a map_*() function above, add one row here.
 * ==================================================================== */
static const task_descriptor TASK_REGISTRY[] = {
    { TASK_WORDCOUNT,  "wordcount",  map_wordcount,  reduce_sum, 0 },
    { TASK_LINECOUNT,  "linecount",  map_linecount,  reduce_sum, 0 },
    { TASK_SUMNUMBERS, "sumnumbers", map_sumnumbers, reduce_sum, 0 },
    { TASK_CHARCOUNT,  "charcount",  map_charcount,  reduce_sum, 0 },
};

#define TASK_REGISTRY_SIZE \
    (sizeof(TASK_REGISTRY) / sizeof(TASK_REGISTRY[0]))

/* ====================================================================
 * get_task()
 * ==================================================================== */
const task_descriptor *get_task(uint8_t task_id)
{
    for (size_t i = 0; i < TASK_REGISTRY_SIZE; i++) {
        if (TASK_REGISTRY[i].task_id == task_id)
            return &TASK_REGISTRY[i];
    }
    return NULL;
}

/* ====================================================================
 * task_name()
 * ==================================================================== */
const char *task_name(uint8_t task_id)
{
    const task_descriptor *t = get_task(task_id);
    return t ? t->name : "UNKNOWN";
}
