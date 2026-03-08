#include "rdma/servers/mu_leader.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>

void MuLeader::on_accept(
    rdma_cm_id* /*new_id*/,
    const ConnPrivateData& incoming,
    rdma_conn_param& accept_params
) {
    // Mu leader gives clients a separate client_pool buffer
    // (not the log buffer that followers replicate to)
    if (incoming.type == ConnType::CLIENT) {
        if (!client_pool_) {
            client_pool_ = allocate_rdma_buffer();
            client_mr_ = ibv_reg_mr(pd_, client_pool_, ALIGNED_SIZE,
                                    IBV_ACCESS_LOCAL_WRITE |
                                    IBV_ACCESS_REMOTE_WRITE |
                                    IBV_ACCESS_REMOTE_READ);
            if (!client_mr_) throw std::runtime_error("client_mr reg failed");

            // Override server_creds to point to client_pool
            server_creds_.addr = reinterpret_cast<uintptr_t>(client_pool_);
            server_creds_.rkey = client_mr_->rkey;
        }

        accept_params.responder_resources = 1;
        accept_params.initiator_depth     = 1;
        accept_params.private_data        = &server_creds_;
        accept_params.private_data_len    = sizeof(server_creds_);
    }
}

void MuLeader::run() {
    std::cout << "[MuLeader " << node_id_ << "] Starting sequential replication loop\n";

    const uint32_t majority = CLUSTER_NODES.size() - 1;
    uint32_t current_index = 0;
    auto* local_log = static_cast<char*>(log_pool_);

    // Pre-post receives for all clients
    for (size_t i = 0; i < NUM_CLIENTS; ++i) {
        ibv_recv_wr wr{}, *bad = nullptr;
        wr.wr_id   = i;
        wr.sg_list = nullptr;
        wr.num_sge = 0;
        if (ibv_post_recv(clients_[i].cm_id->qp, &wr, &bad))
            throw std::runtime_error("Failed to post initial recv");
    }

    Queue<uint32_t, NUM_CLIENTS> pending;
    bool should_write = true;
    uint32_t inflight_client = 0;
    int acks = 0;

    ibv_wc wc[32];

    while (true) {
        int n = ibv_poll_cq(cq_, 32, wc);

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[MuLeader] WC error: "
                          << ibv_wc_status_str(wc[i].status)
                          << " opcode: " << wc[i].opcode
                          << " wr_id: " << wc[i].wr_id << "\n";
                throw std::runtime_error("RDMA completion failure");
            }

            if (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                // Client request arrived
                uint32_t client_id = wc[i].imm_data;
                pending.push(client_id);

                ibv_recv_wr next{}, *bad = nullptr;
                next.wr_id = client_id;
                if (ibv_post_recv(clients_[client_id].cm_id->qp, &next, &bad))
                    throw std::runtime_error("Failed to re-post recv");

            } else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
                if (wc[i].wr_id == current_index) {
                    if (++acks >= static_cast<int>(majority)) {
                        // Got quorum — ack back to client
                        ibv_send_wr swr{};
                        swr.wr_id      = current_index;
                        swr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
                        swr.num_sge    = 0;
                        swr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
                        swr.wr.rdma.remote_addr = 0;
                        swr.wr.rdma.rkey        = 0;
                        swr.imm_data  = current_index;

                        ibv_send_wr* bad = nullptr;
                        if (ibv_post_send(clients_[inflight_client].cm_id->qp,
                                          &swr, &bad))
                            throw std::runtime_error("Ack post failed");

                        should_write = true;
                        acks = 0;
                        current_index++;
                    }
                }
            }
        }

        // Replicate next request
        if (should_write) {
            uint32_t client_id;
            if (!pending.pop(client_id)) continue;

            inflight_client = client_id;
            uint32_t slot = current_index % MAX_LOG_ENTRIES;

            local_log[(slot * ENTRY_SIZE) + ENTRY_SIZE - 1] = 1;

            for (auto& peer : peers_) {
                if (peer.id == node_id_ || !peer.id) continue;

                ibv_sge sge{};
                sge.addr   = reinterpret_cast<uintptr_t>(local_log + (slot * ENTRY_SIZE));
                sge.length = ENTRY_SIZE;
                sge.lkey   = log_mr_->lkey;

                ibv_send_wr swr{}, *bad = nullptr;
                swr.wr_id      = current_index;
                swr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
                swr.sg_list    = &sge;
                swr.num_sge    = 1;
                swr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
                swr.wr.rdma.remote_addr = peer.remote_addr + (slot * ENTRY_SIZE);
                swr.wr.rdma.rkey        = peer.rkey;
                swr.imm_data  = current_index;

                if (ibv_post_send(peer.cm_id->qp, &swr, &bad))
                    throw std::runtime_error("Replication post failed");
            }

            should_write = false;
        }
    }
}