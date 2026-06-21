#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include "protocol.h"
#include "udp_utils.h"
#include "epoll_utils.h"

uint32_t my_worker_id = 0;
uint32_t global_seq = 100;

int64_t do_word_count(const char *data, uint32_t len) {
    int64_t count = 0;
    int in_word = 0;
    for (uint32_t i = 0; i < len; i++) {
        if (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
            in_word = 0;
        } else if (!in_word) {
            in_word = 1;
            count++;
        }
    }
    return count;
}

int64_t do_sum_numbers(const char *data, uint32_t len) {
    int64_t sum = 0;
    char *copy = malloc(len + 1);
    memcpy(copy, data, len);
    copy[len] = '\0';
    
    char *token = strtok(copy, " \t\n\r");
    while (token != NULL) {
        sum += strtol(token, NULL, 10);
        token = strtok(NULL, " \t\n\r");
    }
    free(copy);
    return sum;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <coordinator_ip>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in coord_addr;
    memset(&coord_addr, 0, sizeof(coord_addr));
    coord_addr.sin_family = AF_INET;
    coord_addr.sin_port = htons(COORDINATOR_PORT);
    inet_pton(AF_INET, argv[1], &coord_addr.sin_addr);

    int sockfd = udp_create_socket(0); // Ephemeral local port
    if (sockfd < 0) exit(EXIT_FAILURE);

    printf("Worker running on ephemeral port...\n");

    // Dynamic registration phase
    Packet req, ack;
    udp_build_packet(&req, MSG_REGISTER, global_seq++, NULL, 0);
    printf("Registering with coordinator at %s:%d...\n", argv[1], COORDINATOR_PORT);
    
    if (udp_send_reliable(sockfd, &req, sizeof(PacketHeader), MSG_REGISTER_ACK, &coord_addr, &ack) < 0) {
        fprintf(stderr, "Registration with coordinator failed.\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    RegisterAckPayload *reg_payload = (RegisterAckPayload*)ack.payload;
    my_worker_id = reg_payload->worker_id;
    printf("Successfully registered as Worker ID: %u\n", my_worker_id);

    int epoll_fd = epoll_create_instance();
    epoll_add(epoll_fd, sockfd);
    struct epoll_event events[1];

    time_t last_heartbeat_time = time(NULL);

    while (1) {
        int num_events = epoll_wait_events(epoll_fd, events, 1, 1000); // 1-second break timeout
        time_t now = time(NULL);

        // Keepalive Tick Check
        if (now - last_heartbeat_time >= HEARTBEAT_SEC) {
            Packet hb;
            udp_build_packet(&hb, MSG_HEARTBEAT, global_seq++, &my_worker_id, sizeof(my_worker_id));
            udp_send_raw(sockfd, &hb, sizeof(PacketHeader) + sizeof(my_worker_id), &coord_addr);
            last_heartbeat_time = now;
        }

        if (num_events > 0) {
            Packet incoming;
            struct sockaddr_in sender;
            ssize_t n = udp_recv(sockfd, &incoming, &sender);
            if (n <= 0) continue;

            if (incoming.header.msg_type == MSG_CHUNK_ASSIGN) {
                ChunkPayload *chunk = (ChunkPayload*)incoming.payload;
                printf("Assigned chunk ID: %u, task type: %u. Computing...\n", chunk->chunk_id, chunk->task_type);

                // Immediate Acknowledge Back to Master
                Packet chunk_ack;
                udp_build_packet(&chunk_ack, MSG_CHUNK_ACK, incoming.header.seq_num, NULL, 0);
                udp_send_raw(sockfd, &chunk_ack, sizeof(PacketHeader), &coord_addr);

                // Compute tasks based on incoming directive structural boundaries
                int64_t calculation = 0;
                if (chunk->task_type == TASK_WORD_COUNT) {
                    calculation = do_word_count(chunk->data, chunk->data_len);
                } else if (chunk->task_type == TASK_SUM_NUMBERS) {
                    calculation = do_sum_numbers(chunk->data, chunk->data_len);
                }

                // Process reliable outcome dropoff
                ResultPayload res_out;
                res_out.chunk_id = chunk->chunk_id;
                res_out.result = calculation;

                Packet res_packet;
                udp_build_packet(&res_packet, MSG_RESULT, global_seq++, &res_out, sizeof(ResultPayload));
                printf("Sending Result for Chunk ID %u: %lld\n", chunk->chunk_id, (long long)calculation);
                
                udp_send_reliable(sockfd, &res_packet, sizeof(PacketHeader) + sizeof(ResultPayload), MSG_RESULT_ACK, &coord_addr, NULL);
            }
        }
    }

    close(sockfd);
    return 0;
}
