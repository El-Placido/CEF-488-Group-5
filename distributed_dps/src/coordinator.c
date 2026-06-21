#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include "protocol.h"
#include "udp_utils.h"
#include "epoll_utils.h"

typedef struct {
    uint32_t worker_id;
    struct sockaddr_in addr;
    time_t last_heartbeat;
    int active;
    int busy;
    uint32_t assigned_chunk_id;
} WorkerInfo;

typedef struct {
    uint32_t chunk_id;
    char data[MAX_PAYLOAD - 9];
    uint32_t data_len;
    uint8_t task_type;
    int done;
    int assigned;
    int64_t result;
} ChunkInfo;

WorkerInfo workers[MAX_WORKERS];
uint32_t worker_count = 0;

ChunkInfo *chunks = NULL;
uint32_t num_chunks = 0;
uint32_t job_task_type = 0;
int job_active = 0;

struct sockaddr_in client_addr;
uint32_t client_seq = 0;
uint32_t coord_seq = 500;

void split_file(const char *filename, uint8_t task_type) {
    FILE *f = fopen(filename, "r");
    if (!f) {
        perror("Failed to open source file target");
        return;
    }

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *buffer = malloc(size + 1);
    size_t read_bytes = fread(buffer, 1, size, f);
    fclose(f);

    uint32_t max_chunk_sz = MAX_PAYLOAD - 9;
    num_chunks = (read_bytes + max_chunk_sz - 1) / max_chunk_sz;
    chunks = malloc(sizeof(ChunkInfo) * num_chunks);

    size_t offset = 0;
    for (uint32_t i = 0; i < num_chunks; i++) {
        chunks[i].chunk_id = i;
        chunks[i].task_type = task_type;
        chunks[i].done = 0;
        chunks[i].assigned = 0;
        chunks[i].result = 0;

        size_t current_chunk_sz = read_bytes - offset;
        if (current_chunk_sz > max_chunk_sz) {
            current_chunk_sz = max_chunk_sz;
        }

        chunks[i].data_len = current_chunk_sz;
        memcpy(chunks[i].data, buffer + offset, current_chunk_sz);
        offset += current_chunk_sz;
    }
    free(buffer);
    job_task_type = task_type;
    job_active = 1;
    printf("File split complete. Generated %u chunks for tasks processing.\n", num_chunks);
}

int find_free_worker(void) {
    for (uint32_t i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].active && !workers[i].busy) {
            return i;
        }
    }
    return -1;
}

void register_worker(Packet *pkt, struct sockaddr_in *addr, int sockfd) {
    for (uint32_t i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].active && 
            workers[i].addr.sin_addr.s_addr == addr->sin_addr.s_addr && 
            workers[i].addr.sin_port == addr->sin_port) {
            workers[i].last_heartbeat = time(NULL);
            return;
        }
    }

    for (uint32_t i = 0; i < MAX_WORKERS; i++) {
        if (!workers[i].active) {
            workers[i].worker_id = i + 1;
            workers[i].addr = *addr;
            workers[i].last_heartbeat = time(NULL);
            workers[i].active = 1;
            workers[i].busy = 0;
            
            Packet ack;
            RegisterAckPayload p;
            p.worker_id = workers[i].worker_id;
            
            udp_build_packet(&ack, MSG_REGISTER_ACK, pkt->header.seq_num, &p, sizeof(p));
            udp_send_raw(sockfd, &ack, sizeof(PacketHeader) + sizeof(p), addr);
            
            printf("Registered Worker ID: %u from port %d\n", workers[i].worker_id, ntohs(addr->sin_port));
            return;
        }
    }
}

void mark_worker_dead(int idx) {
    printf("Worker %u timed out! Dropping from cluster maps.\n", workers[idx].worker_id);
    workers[idx].active = 0;
    if (workers[idx].busy) {
        uint32_t cid = workers[idx].assigned_chunk_id;
        chunks[cid].assigned = 0;
        printf("Requeued uncompleted Chunk ID %u back to open queue scheduler.\n", cid);
    }
}

void dispatch_pending_chunks(int sockfd) {
    if (!job_active) return;

    for (uint32_t i = 0; i < num_chunks; i++) {
        if (!chunks[i].done && !chunks[i].assigned) {
            int w_idx = find_free_worker();
            if (w_idx < 0) return;

            Packet pkt;
            ChunkPayload payload;
            payload.chunk_id = chunks[i].chunk_id;
            payload.task_type = chunks[i].task_type;
            payload.data_len = chunks[i].data_len;
            memcpy(payload.data, chunks[i].data, chunks[i].data_len);

            udp_build_packet(&pkt, MSG_CHUNK_ASSIGN, coord_seq++, &payload, sizeof(ChunkPayload));
            
            workers[w_idx].busy = 1;
            workers[w_idx].assigned_chunk_id = chunks[i].chunk_id;
            chunks[i].assigned = 1;

            printf("Dispatching Chunk ID %u to Worker ID %u\n", chunks[i].chunk_id, workers[w_idx].worker_id);
            udp_send_raw(sockfd, &pkt, sizeof(PacketHeader) + sizeof(ChunkPayload), &workers[w_idx].addr);
        }
    }
}

void check_worker_timeouts(void) {
    time_t now = time(NULL);
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].active && (now - workers[i].last_heartbeat > WORKER_TIMEOUT)) {
            mark_worker_dead(i);
        }
    }
}

void aggregate_results(int sockfd) {
    for (uint32_t i = 0; i < num_chunks; i++) {
        if (!chunks[i].done) return;
    }

    int64_t final_aggregated_value = 0;
    for (uint32_t i = 0; i < num_chunks; i++) {
        if (job_task_type == TASK_WORD_COUNT || job_task_type == TASK_SUM_NUMBERS) {
            final_aggregated_value += chunks[i].result;
        }
    }

    printf("Job complete! Consolidated Aggregation Output: %lld\n", (long long)final_aggregated_value);

    Packet complete_pkt;
    udp_build_packet(&complete_pkt, MSG_TASK_DONE, client_seq, &final_aggregated_value, sizeof(final_aggregated_value));
    udp_send_raw(sockfd, &complete_pkt, sizeof(PacketHeader) + sizeof(final_aggregated_value), &client_addr);

    free(chunks);
    chunks = NULL;
    num_chunks = 0;
    job_active = 0;
}

int main(void) {
    int sockfd = udp_create_socket(COORDINATOR_PORT);
    if (sockfd < 0) exit(EXIT_FAILURE);

    int epoll_fd = epoll_create_instance();
    epoll_add(epoll_fd, sockfd);
    struct epoll_event events[8];

    memset(workers, 0, sizeof(workers));
    printf("Coordinator engine listening actively on UDP Port %d\n", COORDINATOR_PORT);

    while (1) {
        int num_events = epoll_wait_events(epoll_fd, events, 8, 1000);
        
        check_worker_timeouts();
        dispatch_pending_chunks(sockfd);
        if (job_active) {
            aggregate_results(sockfd);
        }

        if (num_events > 0) {
            Packet incoming;
            struct sockaddr_in sender;
            ssize_t n = udp_recv(sockfd, &incoming, &sender);
            if (n <= 0) continue;

            switch (incoming.header.msg_type) {
                case MSG_REGISTER:
                    register_worker(&incoming, &sender, sockfd);
                    break;

                case MSG_HEARTBEAT: {
                    uint32_t wid = *(uint32_t*)incoming.payload;
                    if (wid > 0 && wid <= MAX_WORKERS && workers[wid-1].active) {
                        workers[wid-1].last_heartbeat = time(NULL);
                        Packet hb_ack;
                        udp_build_packet(&hb_ack, MSG_HEARTBEAT_ACK, incoming.header.seq_num, NULL, 0);
                        udp_send_raw(sockfd, &hb_ack, sizeof(PacketHeader), &sender);
                    }
                    break;
                }
                case MSG_CHUNK_ACK:
                    break;

                case MSG_RESULT: {
                    ResultPayload *res = (ResultPayload*)incoming.payload;
                    printf("Received processing results for Chunk ID: %u\n", res->chunk_id);
                    
                    if (job_active && res->chunk_id < num_chunks) {
                        chunks[res->chunk_id].result = res->result;
                        chunks[res->chunk_id].done = 1;
                    }

                    for (int i = 0; i < MAX_WORKERS; i++) {
                        if (workers[i].active && workers[i].addr.sin_port == sender.sin_port && workers[i].addr.sin_addr.s_addr == sender.sin_addr.s_addr) {
                            workers[i].busy = 0;
                        }
                    }

                    Packet res_ack;
                    udp_build_packet(&res_ack, MSG_RESULT_ACK, incoming.header.seq_num, NULL, 0);
                    udp_send_raw(sockfd, &res_ack, sizeof(PacketHeader), &sender);
                    break;
                }
                case MSG_TASK_SUBMIT: {
                    client_addr = sender;
                    client_seq = incoming.header.seq_num;
                    uint8_t t_type = incoming.payload[0];
                    char *filename = &incoming.payload[1];
                    printf("Job submission input received for file target: %s (Type: %d)\n", filename, t_type);
                    split_file(filename, t_type);
                    break;
                }
            }
        }
    }

    close(sockfd);
    return 0;
}
