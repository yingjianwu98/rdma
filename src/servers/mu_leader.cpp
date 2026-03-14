#include "rdma/servers/mu_leader.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <cstring>
#include <chrono>
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
    using clock = std::chrono::steady_clock;
    using ns = std::chrono::nanoseconds;

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

    // ── TIMING COUNTERS (cumulative nanoseconds) ──
    uint64_t t_poll_ns = 0;           // ibv_poll_cq call itself
    uint64_t t_recv_ns = 0;           // processing recv completions
    uint64_t t_write_ack_ns = 0;      // processing write completions (ack increment)
    uint64_t t_write_commit_ns = 0;   // processing write completions (commit advancement)
    uint64_t t_drain_ns = 0;          // drain pending → build WR chains
    uint64_t t_post_ns = 0;           // ibv_post_send to followers
    uint64_t t_total_ns = 0;          // entire loop iteration

    // how many iterations had actual work (n > 0)
    uint64_t busy_iters = 0;
    // how many iterations drained something
    uint64_t drain_iters = 0;

    auto last_dump = clock::now();

    while (true) {
        auto iter_start = clock::now();

        // ── Phase 1: Poll CQ ──
        auto t0 = clock::now();
        const int n = ibv_poll_cq(cq_, 32, wc);
        auto t1 = clock::now();
        t_poll_ns += std::chrono::duration_cast<ns>(t1 - t0).count();

        poll_iter++;
        if (n > 0) busy_iters++;

        // ── Phase 2: Process completions ──
        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[MuLeader] WC error: "
                    << ibv_wc_status_str(wc[i].status)
                    << " opcode: " << wc[i].opcode
                    << " wr_id: " << wc[i].wr_id << "\n";
                throw std::runtime_error("RDMA completion failure");
            }

            if (wc[i].opcode & IBV_WC_RECV) {
                auto r0 = clock::now();
                total_recvs++;
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
                auto r1 = clock::now();
                t_recv_ns += std::chrono::duration_cast<ns>(r1 - r0).count();
            }
            else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
                total_writes++;
                const uint64_t wr_id = wc[i].wr_id;
                if ((wr_id >> 48) == ACK_TAG) continue;

                const size_t f = wr_id & 0xFFFF;
                auto& batch = batches[f];

                // ── Ack increment phase ──
                auto wa0 = clock::now();
                for (uint32_t lid = lock_start_; lid < lock_end_; ++lid) {
                    if (!batch.has_entry[lid]) continue;
                    auto& ls = locks[lid];
                    for (uint64_t s = ls.commit_index; s <= batch.lock_slots[lid]; ++s) {
                        ls.acks[s]++;
                    }
                    batch.has_entry[lid] = false;
                }
                auto wa1 = clock::now();
                t_write_ack_ns += std::chrono::duration_cast<ns>(wa1 - wa0).count();

                follower_unsignaled[f] = 0;

                // ── Commit advancement phase ──
                auto wc0 = clock::now();
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
                auto wc1 = clock::now();
                t_write_commit_ns += std::chrono::duration_cast<ns>(wc1 - wc0).count();
            }
            else if (wc[i].opcode == IBV_WC_SEND) {
                total_send_completions++;
            }
            else {
                total_other_completions++;
            }
        }

        // ── Phase 3: Drain pending → replicate ──
        auto d0 = clock::now();

        ibv_send_wr* follower_head[MAX_REPLICAS] = {};
        ibv_send_wr* follower_tail[MAX_REPLICAS] = {};
        size_t repl_count = 0;
        bool did_drain = false;

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

            did_drain = true;
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
        auto d1 = clock::now();
        t_drain_ns += std::chrono::duration_cast<ns>(d1 - d0).count();
        if (did_drain) drain_iters++;

        // ── Phase 4: Post to followers ──
        auto p0 = clock::now();
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
        auto p1 = clock::now();
        t_post_ns += std::chrono::duration_cast<ns>(p1 - p0).count();

        auto iter_end = clock::now();
        t_total_ns += std::chrono::duration_cast<ns>(iter_end - iter_start).count();

        // ── Periodic dump (every 5 seconds wall-clock) ──
        if (poll_iter % 1000000 == 0) {
            auto now = clock::now();
            double elapsed_s = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_dump).count() / 1000.0;

            if (elapsed_s >= 5.0) {
                double total_s = t_total_ns / 1e9;

                std::cerr << "\n[MuLeader " << node_id_
                          << " locks " << lock_start_ << "-" << lock_end_
                          << "] ── TIMING DUMP ──\n"
                          << "  wall_elapsed:    " << elapsed_s << " s\n"
                          << "  poll_iters:      " << poll_iter << "\n"
                          << "  busy_iters:      " << busy_iters
                          << " (" << (100.0 * busy_iters / poll_iter) << "%)\n"
                          << "  drain_iters:     " << drain_iters << "\n"
                          << "  recvs=" << total_recvs
                          << " writes=" << total_writes
                          << " sends=" << total_send_completions
                          << " drained=" << total_drained
                          << " committed=" << total_committed
                          << " acks_sent=" << total_acks_sent
                          << " follower_posts=" << total_posts_to_followers
                          << "\n"
                          << "  ── TIME BREAKDOWN (cumulative) ──\n"
                          << "  t_poll:          " << (t_poll_ns / 1e6) << " ms ("
                          << (100.0 * t_poll_ns / t_total_ns) << "%)\n"
                          << "  t_recv:          " << (t_recv_ns / 1e6) << " ms ("
                          << (100.0 * t_recv_ns / t_total_ns) << "%)\n"
                          << "  t_write_ack:     " << (t_write_ack_ns / 1e6) << " ms ("
                          << (100.0 * t_write_ack_ns / t_total_ns) << "%)\n"
                          << "  t_write_commit:  " << (t_write_commit_ns / 1e6) << " ms ("
                          << (100.0 * t_write_commit_ns / t_total_ns) << "%)\n"
                          << "  t_drain:         " << (t_drain_ns / 1e6) << " ms ("
                          << (100.0 * t_drain_ns / t_total_ns) << "%)\n"
                          << "  t_post:          " << (t_post_ns / 1e6) << " ms ("
                          << (100.0 * t_post_ns / t_total_ns) << "%)\n"
                          << "  t_total:         " << (t_total_ns / 1e6) << " ms\n"
                          << "  ── PER-OP AVERAGES ──\n";

                if (total_recvs > 0)
                    std::cerr << "  avg recv:        " << (t_recv_ns / total_recvs) << " ns\n";
                if (total_writes > 0) {
                    std::cerr << "  avg write_ack:   " << (t_write_ack_ns / total_writes) << " ns\n";
                    std::cerr << "  avg write_commit:" << (t_write_commit_ns / total_writes) << " ns\n";
                }
                if (drain_iters > 0)
                    std::cerr << "  avg drain iter:  " << (t_drain_ns / drain_iters) << " ns\n";
                if (total_posts_to_followers > 0)
                    std::cerr << "  avg post:        " << (t_post_ns / total_posts_to_followers) << " ns\n";

                // dump first 3 active locks
                int dumped = 0;
                for (uint32_t lid = lock_start_; lid < lock_end_ && dumped < 3; ++lid) {
                    auto& ls = locks[lid];
                    if (ls.current_index == 0 && ls.pending.size() == 0) continue;
                    std::cerr << "  lock[" << lid
                              << "] pending=" << ls.pending.size()
                              << " inflight=" << ls.inflight
                              << " current=" << ls.current_index
                              << " commit=" << ls.commit_index
                              << " holder=" << ls.holder_slot
                              << " locked=" << ls.locked
                              << " acks[commit]=" << (ls.commit_index < MAX_LOG_PER_LOCK ? (int)ls.acks[ls.commit_index] : -1)
                              << "\n";
                    dumped++;
                }
                std::cerr << std::endl;

                // Reset for next interval
                last_dump = now;
                t_poll_ns = t_recv_ns = t_write_ack_ns = t_write_commit_ns = 0;
                t_drain_ns = t_post_ns = t_total_ns = 0;
                poll_iter = busy_iters = drain_iters = 0;
                total_recvs = total_writes = total_send_completions = 0;
                total_other_completions = total_drained = total_committed = 0;
                total_acks_sent = total_posts_to_followers = 0;
            }
        }
    }
}