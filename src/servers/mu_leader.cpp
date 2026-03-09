#include "rdma/servers/mu_leader.h"
#include "rdma/common.h"

#include <cstring>
#include <iostream>
#include <stdexcept>

struct PendingRequest {
    uint32_t client_id;
    uint32_t lock_id;
};

void MuLeader::run() {
    std::cout << "[MuLeader " << node_id_ << "] Starting lock replication loop\n";
    while (true) {}
    //
    // const uint32_t num_followers = CLUSTER_NODES.size() - 1;
    // auto* local_buf = static_cast<uint8_t*>(buf_);
    //
    // uint64_t lock_slots[MAX_LOCKS] = {};
    //
    // // Pre-post receives for all clients
    // for (size_t i = 0; i < NUM_CLIENTS; ++i) {
    //     ibv_recv_wr wr{}, *bad = nullptr;
    //     wr.wr_id   = i;
    //     wr.sg_list = nullptr;
    //     wr.num_sge = 0;
    //     if (ibv_post_recv(clients_[i].cm_id->qp, &wr, &bad))
    //         throw std::runtime_error("Failed to post initial recv");
    // }
    //
    // Queue<PendingRequest, 256> pending;
    // bool should_write = true;
    // PendingRequest inflight{};
    // int acks = 0;
    //
    // ibv_wc wc[32];
    //
    // while (true) {
    //     int n = ibv_poll_cq(cq_, 32, wc);
    //
    //     for (int i = 0; i < n; ++i) {
    //         if (wc[i].status != IBV_WC_SUCCESS) {
    //             std::cerr << "[MuLeader] WC error: "
    //                       << ibv_wc_status_str(wc[i].status)
    //                       << " opcode: " << wc[i].opcode
    //                       << " wr_id: " << wc[i].wr_id << "\n";
    //             throw std::runtime_error("RDMA completion failure");
    //         }
    //
    //         if (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
    //             // Client request — payload landed in staging, IMM tells us who/what
    //             uint16_t lock_id   = mu_decode_lock_id(wc[i].imm_data);
    //             uint16_t client_id = mu_decode_client_id(wc[i].imm_data);
    //
    //             pending.push({client_id, lock_id});
    //
    //             // Re-post recv for this client
    //             ibv_recv_wr next{}, *bad = nullptr;
    //             next.wr_id   = client_id;
    //             next.sg_list = nullptr;
    //             next.num_sge = 0;
    //             if (ibv_post_recv(clients_[client_id].cm_id->qp, &next, &bad))
    //                 throw std::runtime_error("Failed to re-post recv");
    //
    //         } else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
    //             // Replication or ack send completion
    //             uint32_t tag = static_cast<uint32_t>(wc[i].wr_id >> 32);
    //
    //             if (tag == 0xACKu) continue; // client ack send completed, ignore
    //
    //             // Follower replication write completed
    //             if (!should_write) {
    //                 if (++acks >= static_cast<int>(num_followers)) {
    //                     uint64_t slot = lock_slots[inflight.lock_id];
    //
    //                     // Ack the client
    //                     ibv_send_wr swr{};
    //                     swr.wr_id      = (static_cast<uint64_t>(0xACKu) << 32) | slot;
    //                     swr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    //                     swr.num_sge    = 0;
    //                     swr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    //                     swr.wr.rdma.remote_addr = 0;
    //                     swr.wr.rdma.rkey        = 0;
    //                     swr.imm_data  = mu_encode_imm(inflight.lock_id, slot);
    //
    //                     ibv_send_wr* bad = nullptr;
    //                     if (ibv_post_send(clients_[inflight.client_id].cm_id->qp,
    //                                       &swr, &bad))
    //                         throw std::runtime_error("Ack post failed");
    //
    //                     lock_slots[inflight.lock_id]++;
    //                     should_write = true;
    //                     acks = 0;
    //                 }
    //             }
    //         }
    //     }
    //
    //     if (should_write) {
    //         PendingRequest req;
    //         if (!pending.pop(req)) continue;
    //
    //         inflight = req;
    //         should_write = false;
    //         acks = 0;
    //
    //         uint64_t slot = lock_slots[req.lock_id];
    //
    //         // Write client_id into local log at this lock's slot
    //         *reinterpret_cast<uint64_t*>(
    //             local_buf + lock_log_slot_offset(req.lock_id, slot))
    //             = static_cast<uint64_t>(req.client_id);
    //
    //         // Copy payload from client staging into the log entry
    //         // (For future KV: the data is at client_staging_offset)
    //         // For now the 8-byte client_id is the entry.
    //
    //         // Replicate to followers with WRITE_WITH_IMM
    //         // IMM tells follower: [lock_id:16][slot:16]
    //         for (auto& peer : peers_) {
    //             if (peer.id == node_id_ || !peer.cm_id) continue;
    //
    //             ibv_sge sge{};
    //             sge.addr   = reinterpret_cast<uintptr_t>(
    //                 local_buf + lock_log_slot_offset(req.lock_id, slot));
    //             sge.length = ENTRY_SIZE;
    //             sge.lkey   = mr_->lkey;
    //
    //             ibv_send_wr swr{}, *bad = nullptr;
    //             swr.wr_id      = (static_cast<uint64_t>(req.lock_id) << 32) | peer.id;
    //             swr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    //             swr.sg_list    = &sge;
    //             swr.num_sge    = 1;
    //             swr.send_flags = IBV_SEND_SIGNALED;
    //             swr.wr.rdma.remote_addr = peer.remote_addr + lock_log_slot_offset(req.lock_id, slot);
    //             swr.wr.rdma.rkey        = peer.rkey;
    //             swr.imm_data  = mu_encode_imm(req.lock_id, static_cast<uint16_t>(slot));
    //
    //             if (ibv_post_send(peer.cm_id->qp, &swr, &bad))
    //                 throw std::runtime_error("Replication post failed");
    //         }
    //     }
    // }
}