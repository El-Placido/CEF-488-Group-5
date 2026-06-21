#include "epoll_utils.h"
#include <unistd.h>
#include <string.h>

int epoll_create_instance(void) {
    return epoll_create1(0);
}

int epoll_add(int epoll_fd, int fd) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLET; // Edge-Triggered mode
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_remove(int epoll_fd, int fd) {
    return epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
}

int epoll_wait_events(int epoll_fd, struct epoll_event *events, int max_events, int timeout_ms) {
    return epoll_wait(epoll_fd, events, max_events, timeout_ms);
}
