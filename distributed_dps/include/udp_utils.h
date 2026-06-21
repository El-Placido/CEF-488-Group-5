#ifndef UDP_UTILS_H
#define UDP_UTILS_H

#include <netinet/in.h>
#include "protocol.h"

/* Create and bind a UDP socket on the given port.
   Pass port=0 for an ephemeral port (workers use this). */
int udp_create_socket(int port);

/* Send a packet and wait for an ACK of the expected type.
   Retransmits up to MAX_RETRIES times, spaced RETRANSMIT_MS apart.
   Returns 0 on success, -1 on failure (no ACK received). */
int udp_send_reliable(int sockfd,
                      const Packet *pkt,
                      size_t pkt_size,
                      uint8_t expected_ack_type,
                      struct sockaddr_in *dest,
                      Packet *ack_out);

/* Send a packet with no reliability (fire and forget). */
int udp_send_raw(int sockfd,
                 const Packet *pkt,
                 size_t pkt_size,
                 struct sockaddr_in *dest);

/* Receive one packet. Returns number of bytes read, -1 on error.
   Fills in sender_addr. */
ssize_t udp_recv(int sockfd, Packet *pkt_out, struct sockaddr_in *sender_addr);

/* Build a packet with the given type, sequence number, and payload. */
void udp_build_packet(Packet *pkt,
                      uint8_t msg_type,
                      uint32_t seq_num,
                      const void *payload,
                      uint16_t payload_len);

#endif /* UDP_UTILS_H */
