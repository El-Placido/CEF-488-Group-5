#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/epoll.h>

#define PORT 8080
#define MAX_EVENTS 10
#define BUFFER_SIZE 4096

int set_nonblocking(int fd) {

    int flags = fcntl(fd, F_GETFL, 0);

    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return 0;
}

int main() {

    int server_fd, client_fd;

    struct sockaddr_in address;

    int addrlen = sizeof(address);

    struct epoll_event ev, events[MAX_EVENTS];

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    set_nonblocking(server_fd);

    address.sin_family = AF_INET;

    address.sin_addr.s_addr = INADDR_ANY;

    address.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));

    listen(server_fd, 10);

    int epfd = epoll_create1(0);

    ev.events = EPOLLIN;

    ev.data.fd = server_fd;

    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    printf("epoll server running on port %d\n", PORT);

    while(1) {

        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for(int i=0;i<nfds;i++) {

            if(events[i].data.fd == server_fd) {

                client_fd = accept(server_fd,
                                  (struct sockaddr *)&address,
                                  (socklen_t*)&addrlen);

                printf("New client connected: fd=%d\n", client_fd);

                char buffer[BUFFER_SIZE];

                read(client_fd, buffer, BUFFER_SIZE);

                FILE *file = fopen("www/index.html", "r");

                char file_buffer[BUFFER_SIZE];

                fread(file_buffer, 1, BUFFER_SIZE, file);

                char response[BUFFER_SIZE];

                sprintf(response,
                        "HTTP/1.0 200 OK\r\n"
                        "Content-Type: text/html\r\n\r\n%s",
                        file_buffer);

                write(client_fd, response, strlen(response));

                fclose(file);

                close(client_fd);
            }
        }
    }

    return 0;
}
