#include "rdma/servers/mu_leader.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <cstring>
#include <iostream>
#include <stdexcept>

struct LockState {
    size_t inflight = 0;

    Queue<uint32_t, 512> pending;
    uint8_t acks[MAX_LOG_PER_LOCK]{};

    uint64_t commit_index = 0;
    uint64_t current_index = 0;

    uint64_t holder_slot = 0;
    bool locked = false;

    bool commit_dirty = false;
};

void MuLeader::run() {
    std::cout << "[MuLeader " << node_id_ << "] Starting MCS pipelined replication loop\n";

    auto* local_buf = static_cast<uint8_t*>(buf_);

    static constexpr size_t MAX_REPL_WRS = MAX_LOCKS * MAX_INFLIGHT * (MAX_REPLICAS - 1)
                                         + MAX_LOCKS * (MAX_REPLICAS - 1);
    auto* repl_wrs = new ibv_send_wr[MAX_REPL_WRS]();
    auto* repl_sges = new ibv_sge[MAX_REPL_WRS]();

    auto* locks = new LockState[MAX_LOCKS]();

    for (size_t i = 0; i < NUM_CLIENTS; ++i) {
        for (size_t r = 0; r < 16; ++r) {
            ibv_recv_wr wr{}, *bad = nullptr;
            wr.wr_id = i;
            wr.sg_list = nullptr;
            wr.num_sge = 0;
            if (ibv_post_recv(clients_[i].cm_id->qp, &wr, &bad)) {
                throw std::runtime_error("Failed to post initial recv");
            }
        }
    }

    ibv_wc wc[32];

    while (true) {
        const int n = ibv_poll_cq(cq_, 32, wc);

        // ── process all completions ──
        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[MuLeader] WC error: "
                    << ibv_wc_status_str(wc[i].status)
                    << " opcode: " << wc[i].opcode
                    << " wr_id: " << wc[i].wr_id << "\n";
                throw std::runtime_error("RDMA completion failure");
            }

            // ── client request (SEND_WITH_IMM or WRITE_WITH_IMM) ──
            if (wc[i].opcode & IBV_WC_RECV) {
                const uint32_t imm = ntohl(wc[i].imm_data);
                const uint16_t lock_id = mu_decode_lock_id(imm);
                const uint16_t client_id = mu_decode_client_id(imm);
                auto& lock_state = locks[lock_id];
                lock_state.pending.push(imm);

                ibv_recv_wr next{}, *bad = nullptr;
                next.wr_id = client_id;
                next.sg_list = nullptr;
                next.num_sge = 0;
                if (ibv_post_recv(clients_[client_id].cm_id->qp, &next, &bad)) {
                    throw std::runtime_error("Failed to re-post recv");
                }
            }
            // ── replication write completion ──
            else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
                const uint64_t wr_id = wc[i].wr_id;
                if ((wr_id >> 48) == ACK_TAG) continue;  // ack/commit notify — ignore

                const auto lock_id = static_cast<uint16_t>(wr_id >> 32);
                const auto slot = static_cast<uint32_t>(wr_id & 0xFFFFFFFF);
                auto& lock_state = locks[lock_id];

                lock_state.acks[slot]++;

                const uint64_t old_commit = lock_state.commit_index;

                // ── walk up commit index ──
                while (lock_state.commit_index < lock_state.current_index) {
                    if (lock_state.acks[lock_state.commit_index] < QUORUM) break;

                    auto* lock_base = mu_lock_base(local_buf, lock_id);
                    const auto* entry = mu_entry_ptr(lock_base, lock_state.commit_index);
                    const uint32_t entry_imm = mu_read_client_imm(entry);
                    const uint16_t client_id = mu_decode_client_id(entry_imm);
                    const uint32_t op = mu_decode_op(entry_imm);

                    // ── handle unlock ──
                    if (op == MU_OP_CLIENT_UNLOCK) {
                        lock_state.locked = false;

                        ibv_send_wr swr{}, *bad_wr = nullptr;
                        swr.wr_id = (ACK_TAG << 48) | client_id;
                        swr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                        swr.num_sge = 0;
                        swr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;  // unsignaled
                        swr.imm_data = htonl(mu_encode_ack(lock_id, lock_state.commit_index, false));
                        swr.wr.rdma.remote_addr = clients_[client_id].remote_addr;
                        swr.wr.rdma.rkey = clients_[client_id].rkey;
                        if (ibv_post_send(clients_[client_id].cm_id->qp, &swr, &bad_wr)) {
                            throw std::runtime_error("Failed to ack unlock");
                        }
                    }

                    // ── try to grant next queued acquire ──
                    while (!lock_state.locked && lock_state.holder_slot <= lock_state.commit_index) {
                        auto* lock_base2 = mu_lock_base(local_buf, lock_id);
                        const auto* next_entry = mu_entry_ptr(lock_base2, lock_state.holder_slot);
                        const uint32_t next_imm = mu_read_client_imm(next_entry);
                        const uint32_t next_op = mu_decode_op(next_imm);

                        if (next_op == MU_OP_CLIENT_LOCK) {
                            const uint16_t next_client_id = mu_decode_client_id(next_imm);
                            lock_state.locked = true;

                            ibv_send_wr swr{}, *bad_wr = nullptr;
                            swr.wr_id = (ACK_TAG << 48) | next_client_id;
                            swr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                            swr.num_sge = 0;
                            swr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;  // unsignaled
                            swr.imm_data = htonl(mu_encode_ack(lock_id, lock_state.holder_slot, true));
                            swr.wr.rdma.remote_addr = clients_[next_client_id].remote_addr;
                            swr.wr.rdma.rkey = clients_[next_client_id].rkey;
                            if (ibv_post_send(clients_[next_client_id].cm_id->qp, &swr, &bad_wr)) {
                                throw std::runtime_error("Failed to ack lock grant");
                            }
                        }
                        lock_state.holder_slot++;
                    }

                    lock_state.commit_index++;
                    lock_state.inflight--;
                }

                if (lock_state.commit_index != old_commit) {
                    lock_state.commit_dirty = true;
                }
            }
        }

        // ── drain pending → replicate, chained per follower ──
        ibv_send_wr* follower_head[MAX_REPLICAS] = {};
        ibv_send_wr* follower_tail[MAX_REPLICAS] = {};
        size_t repl_count = 0;

        for (uint32_t lock_id = 0; lock_id < MAX_LOCKS; ++lock_id) {
            auto& ls = locks[lock_id];

            // ── commit notification (unsignaled) ──
            if (ls.commit_dirty) {
                for (size_t f = 0; f < peers_.size(); ++f) {
                    if (peers_[f].id == node_id_ || !peers_[f].id) continue;

                    auto& wr = repl_wrs[repl_count];
                    wr = {};
                    wr.wr_id = (ACK_TAG << 48);
                    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                    wr.sg_list = nullptr;
                    wr.num_sge = 0;
                    wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;  // unsignaled
                    wr.imm_data = htonl(mu_encode_commit_notify(lock_id, ls.commit_index));
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr;
                    wr.wr.rdma.rkey = peers_[f].rkey;
                    wr.next = nullptr;

                    if (!follower_head[f]) {
                        follower_head[f] = &wr;
                    } else {
                        follower_tail[f]->next = &wr;
                    }
                    follower_tail[f] = &wr;
                    repl_count++;
                }
                ls.commit_dirty = false;
            }

            // ── drain pending entries ──
            if (ls.inflight >= MAX_INFLIGHT || ls.pending.size() == 0) continue;

            const size_t to_drain = std::min(ls.pending.size(), MAX_INFLIGHT - ls.inflight);

            for (size_t j = 0; j < to_drain; ++j) {
                uint32_t imm;
                if (!ls.pending.pop(imm)) break;

                const uint32_t slot = ls.current_index;
                auto* lock_base = mu_lock_base(local_buf, lock_id);
                auto* entry = mu_entry_ptr(lock_base, slot);
                mu_write_entry(entry, imm);

                for (size_t f = 0; f < peers_.size(); ++f) {
                    if (peers_[f].id == node_id_ || !peers_[f].id) continue;

                    auto& sge = repl_sges[repl_count];
                    sge.addr = reinterpret_cast<uintptr_t>(entry);
                    sge.length = ENTRY_SIZE;
                    sge.lkey = mr_->lkey;

                    auto& wr = repl_wrs[repl_count];
                    wr = {};
                    wr.wr_id = (static_cast<uint64_t>(lock_id) << 32)
                   | static_cast<uint64_t>(slot);
                    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
                    wr.imm_data = htonl(imm);
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr
                        + lock_id * LOCK_REGION_SIZE
                        + LOCK_HEADER_SIZE
                        + slot * ENTRY_SIZE;
                    wr.wr.rdma.rkey = peers_[f].rkey;
                    wr.next = nullptr;

                    if (!follower_head[f]) {
                        follower_head[f] = &wr;
                    } else {
                        follower_tail[f]->next = &wr;
                    }
                    follower_tail[f] = &wr;
                    repl_count++;
                }

                ls.current_index++;
                ls.inflight++;
            }
        }

        // ── ring doorbell once per follower ──
        for (size_t f = 0; f < peers_.size(); ++f) {
            if (!follower_head[f]) continue;
            ibv_send_wr* bad_wr = nullptr;
            if (ibv_post_send(peers_[f].cm_id->qp, follower_head[f], &bad_wr)) {
                throw std::runtime_error("Failed to post chained replication");
            }
        }
    }
}