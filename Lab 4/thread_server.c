#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>

#define PORT 8080
#define THREADS 4
#define BUFFER_SIZE 4096

int server_fd;

void *worker(void *arg) {

    while(1) {

        struct sockaddr_in client;

        int len = sizeof(client);

        int client_fd = accept(server_fd,
                              (struct sockaddr *)&client,
                              (socklen_t*)&len);

        char buffer[BUFFER_SIZE];

        read(client_fd, buffer, BUFFER_SIZE);

        printf("Handled by thread\n");

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

    return NULL;
}

int main() {

    struct sockaddr_in address;

    pthread_t threads[THREADS];

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    address.sin_family = AF_INET;

    address.sin_addr.s_addr = INADDR_ANY;

    address.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));

    listen(server_fd, 10);

    printf("Thread server running on port %d\n", PORT);

    for(int i=0;i<THREADS;i++) {

        pthread_create(&threads[i], NULL, worker, NULL);
    }

    for(int i=0;i<THREADS;i++) {

        pthread_join(threads[i], NULL);
    }

    return 0;
}
