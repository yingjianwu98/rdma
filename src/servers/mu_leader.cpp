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

// track what each follower batch covers
struct FollowerBatch {
    uint32_t lock_slots[MAX_LOCKS];  // highest slot sent per lock this batch
    bool has_entry[MAX_LOCKS];       // did we send anything for this lock?
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
        // no wr.wr.rdma needed — SEND doesn't use remote_addr/rkey

        if (ibv_post_send(clients_[client_id].cm_id->qp, &swr, &bad_wr)) {
            throw std::runtime_error("Failed to post client ack");
        }
    };

    std::cout << "[MuLeader " << node_id_ << "] Starting MCS pipelined replication loop\n";

    auto* local_buf = static_cast<uint8_t*>(buf_);

    static constexpr size_t MAX_REPL_WRS = MAX_LOCKS * MAX_INFLIGHT * (MAX_REPLICAS - 1)
                                         + MAX_LOCKS * (MAX_REPLICAS - 1);
    auto* repl_wrs = new ibv_send_wr[MAX_REPL_WRS]();
    auto* repl_sges = new ibv_sge[MAX_REPL_WRS]();

    auto* locks = new LockState[MAX_LOCKS]();
    auto* batches = new FollowerBatch[MAX_REPLICAS]();

    // per-follower unsignaled count (need to signal before SQ fills)
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

            // ── client request ──
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
            // ── replication batch completion ──
            else if (wc[i].opcode == IBV_WC_RDMA_WRITE) {
                const uint64_t wr_id = wc[i].wr_id;
                if ((wr_id >> 48) == ACK_TAG) continue;

                // wr_id encodes follower index for batch completions
                const size_t f = wr_id & 0xFFFF;
                auto& batch = batches[f];

                // bulk-ack: every slot sent in this batch is now acked by this follower
                for (uint32_t lid = 0; lid < MAX_LOCKS; ++lid) {
                    if (!batch.has_entry[lid]) continue;
                    auto& ls = locks[lid];
                    for (uint64_t s = ls.commit_index; s <= batch.lock_slots[lid]; ++s) {
                        ls.acks[s]++;
                    }
                    batch.has_entry[lid] = false;
                }

                // reset unsignaled counter
                follower_unsignaled[f] = 0;

                // ── walk commit for all locks ──
                for (uint32_t lid = 0; lid < MAX_LOCKS; ++lid) {
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
                            }
                            ls.holder_slot++;
                        }

                        ls.commit_index++;
                        ls.inflight--;
                    }

                    if (ls.commit_index != old_commit) {
                        ls.commit_dirty = true;
                    }
                }
            }
        }

        // ── drain pending → replicate, chained per follower ──
        ibv_send_wr* follower_head[MAX_REPLICAS] = {};
        ibv_send_wr* follower_tail[MAX_REPLICAS] = {};
        size_t repl_count = 0;

        for (uint32_t lock_id = 0; lock_id < MAX_LOCKS; ++lock_id) {
            auto& ls = locks[lock_id];

            // ── commit notification (unsignaled, no data) ──
            if (ls.commit_dirty) {
                // write commit index into local buffer header too
                auto* lock_base = mu_lock_base(local_buf, lock_id);
                mu_write_commit_index(lock_base, ls.commit_index);

                for (size_t f = 0; f < peers_.size(); ++f) {
                    if (peers_[f].id == node_id_ || !peers_[f].id) continue;

                    auto& sge = repl_sges[repl_count];
                    sge.addr = reinterpret_cast<uintptr_t>(lock_base);  // the 8-byte header
                    sge.length = LOCK_HEADER_SIZE;
                    sge.lkey = mr_->lkey;

                    auto& wr = repl_wrs[repl_count];
                    wr = {};
                    wr.wr_id = (ACK_TAG << 48);
                    wr.opcode = IBV_WR_RDMA_WRITE;  // plain write, no IMM
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.send_flags = IBV_SEND_INLINE;
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr
                        + lock_id * LOCK_REGION_SIZE;  // write to lock header
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
                    wr.wr_id = f;  // just follower index — batch tracking handles the rest
                    wr.opcode = IBV_WR_RDMA_WRITE;  // plain WRITE, no IMM needed
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.send_flags = IBV_SEND_INLINE;  // unsignaled by default
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr
                        + lock_id * LOCK_REGION_SIZE
                        + LOCK_HEADER_SIZE
                        + slot * ENTRY_SIZE;
                    wr.wr.rdma.rkey = peers_[f].rkey;
                    wr.next = nullptr;

                    // track highest slot for batch ack
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

        // ── ring doorbell once per follower, signal only the tail ──
        for (size_t f = 0; f < peers_.size(); ++f) {
            if (!follower_head[f]) continue;

            // signal the tail WR so we get exactly 1 CQE per follower per batch
            follower_tail[f]->send_flags |= IBV_SEND_SIGNALED;
            follower_tail[f]->wr_id = f;  // ensure the signaled WR has follower index

            ibv_send_wr* bad_wr = nullptr;
            if (ibv_post_send(peers_[f].cm_id->qp, follower_head[f], &bad_wr)) {
                throw std::runtime_error("Failed to post chained replication");
            }
        }
    }
}