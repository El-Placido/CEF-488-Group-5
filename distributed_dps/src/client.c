#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "protocol.h"
#include "udp_utils.h"

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <coordinator_ip> <filename> <task_type: 1=WordCount, 2=Sum>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *coord_ip = argv[1];
    char *filename = argv[2];
    uint8_t task_type = (uint8_t)atoi(argv[3]);

    struct sockaddr_in coord_addr;
    memset(&coord_addr, 0, sizeof(coord_addr));
    coord_addr.sin_family = AF_INET;
    coord_addr.sin_port = htons(COORDINATOR_PORT);
    inet_pton(AF_INET, coord_ip, &coord_addr.sin_addr);

    int sockfd = udp_create_socket(0); // Ephemeral port
    if (sockfd < 0) exit(EXIT_FAILURE);

    // Build the task submission data frame structure payload
    Packet submit_pkt;
    char payload_buf[MAX_PAYLOAD];
    payload_buf[0] = task_type;
    size_t fn_len = strlen(filename);
    if (fn_len >= MAX_PAYLOAD - 1) {
        fprintf(stderr, "File path string argument is too long.\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    memcpy(&payload_buf[1], filename, fn_len + 1); // include null terminator

    uint16_t total_payload_len = 1 + fn_len + 1;
    udp_build_packet(&submit_pkt, MSG_TASK_SUBMIT, 1, payload_buf, total_payload_len);

    printf("Submitting computing request context to master at %s...\n", coord_ip);
    Packet response;
    
    // Send work payload and expect final results
    if (udp_send_reliable(sockfd, &submit_pkt, sizeof(PacketHeader) + total_payload_len, MSG_TASK_DONE, &coord_addr, &response) < 0) {
        fprintf(stderr, "Failed to gather results back from Distributed Processing Master cluster system.\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    int64_t final_aggregated_result = *(int64_t*)response.payload;
    printf("\n=========================================\n");
    printf("FINAL RECEIVED AGGREGATION METRIC RESULT: %lld\n", (long long)final_aggregated_result);
    printf("=========================================\n");

    close(sockfd);
    return 0;
}
