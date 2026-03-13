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

struct FollowerBatch {
    uint32_t lock_slots[MAX_LOCKS];
    bool has_entry[MAX_LOCKS];
};

void MuLeader::run() {
    uint32_t client_unsignaled[TOTAL_CLIENTS] = {};

    auto post_client_ack = [&](uint16_t client_id, uint32_t imm_data) {
        ibv_send_wr swr{}, *bad_wr = nullptr;
        swr.wr_id = (ACK_TAG << 48) | client_id;
        swr.opcode = IBV_WR_SEND_WITH_IMM;
        swr.num_sge = 0;
        swr.sg_list = nullptr;
        swr.send_flags = IBV_SEND_INLINE;

        client_unsignaled[client_id]++;
        if (client_unsignaled[client_id] >= 512) {
            swr.send_flags |= IBV_SEND_SIGNALED;
            client_unsignaled[client_id] = 0;
        }

        swr.imm_data = htonl(imm_data);

        if (ibv_post_send(clients_[client_id].cm_id->qp, &swr, &bad_wr)) {
            throw std::runtime_error("Failed to post client ack");
        }
    };

    std::cout << "[MuLeader " << node_id_ << "] locks ["
              << lock_start_ << ", " << lock_end_ << ")\n";

    auto* local_buf = static_cast<uint8_t*>(buf_);

    const size_t num_locks = lock_end_ - lock_start_;
    const size_t max_repl_wrs = num_locks * MAX_INFLIGHT * (MAX_REPLICAS - 1)
                              + num_locks * (MAX_REPLICAS - 1);
    auto* repl_wrs = new ibv_send_wr[max_repl_wrs]();
    auto* repl_sges = new ibv_sge[max_repl_wrs]();

    auto* locks = new LockState[MAX_LOCKS]();
    auto* batches = new FollowerBatch[MAX_REPLICAS]();

    uint32_t follower_unsignaled[MAX_REPLICAS] = {};

    for (size_t i = 0; i < TOTAL_CLIENTS; ++i) {
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

    // ── DEBUG COUNTERS ──
    uint64_t poll_iter = 0;
    uint64_t total_recvs = 0;
    uint64_t total_writes = 0;
    uint64_t total_drained = 0;
    uint64_t total_committed = 0;
    uint64_t total_acks_sent = 0;
    uint64_t total_posts_to_followers = 0;
    uint64_t total_send_completions = 0;
    uint64_t total_other_completions = 0;

    while (true) {
        const int n = ibv_poll_cq(cq_, 32, wc);

        poll_iter++;
        // if (poll_iter % 5000000 == 0) {
        //     // also dump first few locks' state
        //     std::cerr << "[MuLeader " << node_id_
        //               << " locks " << lock_start_ << "-" << lock_end_
        //               << "] iter=" << poll_iter
        //               << " recvs=" << total_recvs
        //               << " writes=" << total_writes
        //               << " sends=" << total_send_completions
        //               << " other=" << total_other_completions
        //               << " drained=" << total_drained
        //               << " committed=" << total_committed
        //               << " acks_sent=" << total_acks_sent
        //               << " follower_posts=" << total_posts_to_followers
        //               << "\n";
        //
        //     // dump state of first 3 active locks
        //     int dumped = 0;
        //     for (uint32_t lid = lock_start_; lid < lock_end_ && dumped < 3; ++lid) {
        //         auto& ls = locks[lid];
        //         if (ls.current_index == 0 && ls.pending.size() == 0) continue;
        //         std::cerr << "  lock[" << lid
        //                   << "] pending=" << ls.pending.size()
        //                   << " inflight=" << ls.inflight
        //                   << " current=" << ls.current_index
        //                   << " commit=" << ls.commit_index
        //                   << " holder=" << ls.holder_slot
        //                   << " locked=" << ls.locked
        //                   << " acks[commit]=" << (ls.commit_index < MAX_LOG_PER_LOCK ? (int)ls.acks[ls.commit_index] : -1)
        //                   << "\n";
        //         dumped++;
        //     }
        // }

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[MuLeader] WC error: "
                    << ibv_wc_status_str(wc[i].status)
                    << " opcode: " << wc[i].opcode
                    << " wr_id: " << wc[i].wr_id << "\n";
                throw std::runtime_error("RDMA completion failure");
            }

            if (wc[i].opcode & IBV_WC_RECV) {
                total_recvs++;
                const uint32_t imm = ntohl(wc[i].imm_data);
                const uint16_t lock_id = mu_decode_lock_id(imm);
                const uint16_t client_id = mu_decode_client_id(imm);
                const uint32_t op = mu_decode_op(imm);

                // first recv: print what we got
                if (total_recvs <= 3) {
                    std::cerr << "[MuLeader] recv #" << total_recvs
                              << " lock=" << lock_id
                              << " client=" << client_id
                              << " op=" << (op == MU_OP_CLIENT_LOCK ? "LOCK" : "UNLOCK")
                              << "\n";
                }

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
            else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
                total_writes++;
                const uint64_t wr_id = wc[i].wr_id;
                if ((wr_id >> 48) == ACK_TAG) continue;

                const size_t f = wr_id & 0xFFFF;
                auto& batch = batches[f];

                for (uint32_t lid = lock_start_; lid < lock_end_; ++lid) {
                    if (!batch.has_entry[lid]) continue;
                    auto& ls = locks[lid];
                    for (uint64_t s = ls.commit_index; s <= batch.lock_slots[lid]; ++s) {
                        ls.acks[s]++;
                    }
                    batch.has_entry[lid] = false;
                }

                follower_unsignaled[f] = 0;

                for (uint32_t lid = lock_start_; lid < lock_end_; ++lid) {
                    auto& ls = locks[lid];
                    const uint64_t old_commit = ls.commit_index;

                    while (ls.commit_index < ls.current_index) {
                        if (ls.acks[ls.commit_index] < QUORUM) break;

                        auto* lock_base = mu_lock_base(local_buf, lid);
                        const auto* entry = mu_entry_ptr(lock_base, ls.commit_index);
                        const uint32_t entry_imm = mu_read_client_imm(entry);
                        const uint16_t client_id = mu_decode_client_id(entry_imm);
                        const uint32_t op = mu_decode_op(entry_imm);

                        if (op == MU_OP_CLIENT_UNLOCK) {
                            ls.locked = false;
                            post_client_ack(client_id, mu_encode_ack(lid, ls.commit_index, false));
                            total_acks_sent++;
                        }

                        while (!ls.locked && ls.holder_slot <= ls.commit_index) {
                            const auto* next_entry = mu_entry_ptr(lock_base, ls.holder_slot);
                            const uint32_t next_imm = mu_read_client_imm(next_entry);
                            const uint32_t next_op = mu_decode_op(next_imm);

                            if (next_op == MU_OP_CLIENT_LOCK) {
                                const uint16_t next_client_id = mu_decode_client_id(next_imm);
                                ls.locked = true;
                                post_client_ack(next_client_id,
                                    mu_encode_ack(lid, ls.holder_slot, true));
                                total_acks_sent++;
                            }
                            ls.holder_slot++;
                        }

                        ls.commit_index++;
                        ls.inflight--;
                        total_committed++;
                    }

                    if (ls.commit_index != old_commit) {
                        ls.commit_dirty = true;
                    }
                }
            }
            else if (wc[i].opcode == IBV_WC_SEND) {
                total_send_completions++;
            }
            else {
                total_other_completions++;
                std::cerr << "[MuLeader] unexpected opcode=" << wc[i].opcode
                          << " wr_id=" << wc[i].wr_id << "\n";
            }
        }

        // drain pending → replicate
        ibv_send_wr* follower_head[MAX_REPLICAS] = {};
        ibv_send_wr* follower_tail[MAX_REPLICAS] = {};
        size_t repl_count = 0;

        for (uint32_t lock_id = lock_start_; lock_id < lock_end_; ++lock_id) {
            auto& ls = locks[lock_id];

            if (ls.commit_dirty) {
                auto* lock_base = mu_lock_base(local_buf, lock_id);
                mu_write_commit_index(lock_base, ls.commit_index);

                for (size_t f = 0; f < peers_.size(); ++f) {
                    if (peers_[f].id == node_id_ || !peers_[f].id) continue;

                    auto& sge = repl_sges[repl_count];
                    sge.addr = reinterpret_cast<uintptr_t>(lock_base);
                    sge.length = LOCK_HEADER_SIZE;
                    sge.lkey = mr_->lkey;

                    auto& wr = repl_wrs[repl_count];
                    wr = {};
                    wr.wr_id = (ACK_TAG << 48);
                    wr.opcode = IBV_WR_RDMA_WRITE;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.send_flags = IBV_SEND_INLINE;
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr
                        + lock_id * LOCK_REGION_SIZE;
                    wr.wr.rdma.rkey = peers_[f].rkey;
                    wr.next = nullptr;

                    if (!follower_head[f]) {
                        follower_head[f] = &wr;
                    } else {
                        follower_tail[f]->next = &wr;
                    }
                    follower_tail[f] = &wr;
                    follower_unsignaled[f]++;
                    repl_count++;
                }
                ls.commit_dirty = false;
            }

            if (ls.inflight >= MAX_INFLIGHT || ls.pending.size() == 0) continue;

            const size_t to_drain = std::min(ls.pending.size(), MAX_INFLIGHT - ls.inflight);

            for (size_t j = 0; j < to_drain; ++j) {
                uint32_t imm;
                if (!ls.pending.pop(imm)) break;

                total_drained++;

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
                    wr.wr_id = f;
                    wr.opcode = IBV_WR_RDMA_WRITE;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.send_flags = IBV_SEND_INLINE;
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr
                        + lock_id * LOCK_REGION_SIZE
                        + LOCK_HEADER_SIZE
                        + slot * ENTRY_SIZE;
                    wr.wr.rdma.rkey = peers_[f].rkey;
                    wr.next = nullptr;

                    batches[f].lock_slots[lock_id] = slot;
                    batches[f].has_entry[lock_id] = true;

                    if (!follower_head[f]) {
                        follower_head[f] = &wr;
                    } else {
                        follower_tail[f]->next = &wr;
                    }
                    follower_tail[f] = &wr;
                    follower_unsignaled[f]++;
                    repl_count++;
                }

                ls.current_index++;
                ls.inflight++;
            }
        }

        for (size_t f = 0; f < peers_.size(); ++f) {
            if (!follower_head[f]) continue;

            total_posts_to_followers++;

            follower_tail[f]->send_flags |= IBV_SEND_SIGNALED;
            follower_tail[f]->wr_id = f;

            ibv_send_wr* bad_wr = nullptr;
            if (ibv_post_send(peers_[f].cm_id->qp, follower_head[f], &bad_wr)) {
                throw std::runtime_error("Failed to post chained replication");
            }
        }
    }
}