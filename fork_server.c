#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/wait.h>

#define PORT 8080
#define BUFFER_SIZE 4096

void handle_client(int client_fd) {

    char buffer[BUFFER_SIZE];

    read(client_fd, buffer, BUFFER_SIZE);

    printf("Client request handled by PID %d\n", getpid());

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

int main() {

    int server_fd, client_fd;

    struct sockaddr_in address;

    int addrlen = sizeof(address);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    address.sin_family = AF_INET;

    address.sin_addr.s_addr = INADDR_ANY;

    address.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));

    listen(server_fd, 5);

    printf("Fork server running on port %d\n", PORT);

    while(1) {

        client_fd = accept(server_fd,
                          (struct sockaddr *)&address,
                          (socklen_t*)&addrlen);

        pid_t pid = fork();

        if(pid == 0) {

            close(server_fd);

            handle_client(client_fd);

            exit(0);
        }

        else {

            close(client_fd);

            waitpid(-1, NULL, WNOHANG);
        }
    }

    return 0;
}
