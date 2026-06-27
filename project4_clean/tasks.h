/*
 * tasks.h
 * =======
 * Pluggable map/reduce task registry for Project 4.
 *
 * WHY THIS FILE EXISTS
 * ---------------------
 * The project requires "support for different processing tasks
 * (configurable map/reduce functions)".  Rather than hard-coding one
 * algorithm into the worker, we define a small function-pointer table:
 * each task has a MAP function (applied to one chunk by a worker) and
 * a REDUCE function (applied by the coordinator to combine all chunk
 * results into the final answer).
 *
 * Adding a new task means writing two small functions and adding one
 * line to the registry table in tasks.c — no other file needs to change.
 *
 * CEF488 Group Project — Project 4: Distributed Data Processing System
 */

#ifndef TASKS_H
#define TASKS_H

#include <stdint.h>
#include <stddef.h>

/*
 * map_fn_t — applied by a WORKER to one chunk of raw bytes.
 *   data     – pointer to the chunk's bytes (NOT null-terminated)
 *   data_len – number of valid bytes in data
 * Returns a single int64_t partial result for this chunk.
 */
typedef int64_t (*map_fn_t)(const char *data, uint32_t data_len);

/*
 * reduce_fn_t — applied by the COORDINATOR to combine two partial
 * results into one.  Called repeatedly (a left fold) over all chunk
 * results to produce the final answer.
 */
typedef int64_t (*reduce_fn_t)(int64_t accumulator, int64_t next_value);

/*
 * task_descriptor — one entry in the task registry.
 */
typedef struct {
    uint8_t      task_id;     /* TASK_* constant from protocol.h        */
    const char  *name;        /* human-readable name for logging        */
    map_fn_t     map;         /* per-chunk computation                  */
    reduce_fn_t  reduce;      /* combine step                           */
    int64_t      identity;    /* reduce() identity element (0 for sums) */
} task_descriptor;

/*
 * get_task() — look up a task_descriptor by task_id.
 * Returns NULL if task_id is not registered.
 */
const task_descriptor *get_task(uint8_t task_id);

/*
 * task_name() — convenience wrapper, returns "UNKNOWN" if not found.
 */
const char *task_name(uint8_t task_id);

#endif /* TASKS_H */
