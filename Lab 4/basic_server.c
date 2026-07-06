#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUFFER_SIZE 4096

int main() {

    int server_fd, client_fd;

    struct sockaddr_in address;

    int addrlen = sizeof(address);

    char buffer[BUFFER_SIZE];

    FILE *file;

    char file_buffer[BUFFER_SIZE];

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if(server_fd == -1) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));

    listen(server_fd, 5);

    printf("Server running on port %d\n", PORT);

    while(1) {

        client_fd = accept(server_fd,
                          (struct sockaddr *)&address,
                          (socklen_t*)&addrlen);

        read(client_fd, buffer, BUFFER_SIZE);

        printf("Request:\n%s\n", buffer);

        file = fopen("www/index.html", "r");

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

    close(server_fd);

    return 0;
}
