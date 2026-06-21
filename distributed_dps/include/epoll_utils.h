#ifndef EPOLL_UTILS_H
#define EPOLL_UTILS_H

#include <sys/epoll.h>

/* Create an epoll instance. Returns epoll fd or -1. */
int epoll_create_instance(void);

/* Add fd to epoll instance for EPOLLIN (read) events. */
int epoll_add(int epoll_fd, int fd);

/* Remove fd from epoll instance. */
int epoll_remove(int epoll_fd, int fd);

/* Wait for events. Returns number of events, -1 on error.
   timeout_ms = -1 to block indefinitely. */
int epoll_wait_events(int epoll_fd,
                      struct epoll_event *events,
                      int max_events,
                      int timeout_ms);

#endif /* EPOLL_UTILS_H */
