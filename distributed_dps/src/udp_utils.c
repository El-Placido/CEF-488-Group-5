#include "udp_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/select.h>

int udp_create_socket(int port) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("Socket creation failed");
        return -1;
    }

    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(sockfd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Bind failed");
        close(sockfd);
        return -1;
    }

    // Set non-blocking mode for use with epoll/select
    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    return sockfd;
}

void udp_build_packet(Packet *pkt, uint8_t msg_type, uint32_t seq_num, const void *payload, uint16_t payload_len) {
    memset(pkt, 0, sizeof(Packet));
    pkt->header.seq_num = seq_num;
    pkt->header.msg_type = msg_type;
    pkt->header.payload_len = payload_len;
    if (payload && payload_len > 0) {
        memcpy(pkt->payload, payload, payload_len);
    }
}

int udp_send_raw(int sockfd, const Packet *pkt, size_t pkt_size, struct sockaddr_in *dest) {
    return sendto(sockfd, pkt, pkt_size, 0, (struct sockaddr *)dest, sizeof(*dest));
}

ssize_t udp_recv(int sockfd, Packet *pkt_out, struct sockaddr_in *sender_addr) {
    socklen_t addr_len = sizeof(*sender_addr);
    return recvfrom(sockfd, pkt_out, sizeof(Packet), 0, (struct sockaddr *)sender_addr, &addr_len);
}

int udp_send_reliable(int sockfd, const Packet *pkt, size_t pkt_size, uint8_t expected_ack_type, struct sockaddr_in *dest, Packet *ack_out) {
    int retries = 0;
    fd_set read_fds;
    struct timeval tv;

    while (retries < MAX_RETRIES) {
        if (udp_send_raw(sockfd, pkt, pkt_size, dest) < 0) {
            perror("Reliable send raw failed");
            return -1;
        }

        FD_ZERO(&read_fds);
        FD_SET(sockfd, &read_fds);
        tv.tv_sec = 0;
        tv.tv_usec = RETRANSMIT_MS * 1000;

        int retval = select(sockfd + 1, &read_fds, NULL, NULL, &tv);
        if (retval > 0) {
            struct sockaddr_in resp_addr;
            Packet resp_pkt;
            ssize_t recv_len = udp_recv(sockfd, &resp_pkt, &resp_addr);
            
            if (recv_len >= (ssize_t)sizeof(PacketHeader)) {
                if (resp_pkt.header.msg_type == expected_ack_type && resp_pkt.header.seq_num == pkt->header.seq_num) {
                    if (ack_out) {
                        memcpy(ack_out, &resp_pkt, sizeof(Packet));
                    }
                    return 0; // Success
                }
            }
        }
        retries++;
    }
    return -1; // Failed after max retries
}
