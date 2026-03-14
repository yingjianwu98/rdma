#include "rdma/servers/mu_leader.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <cstring>
#include <iostream>
#include <stdexcept>

// ─── Per-lock state ───────────────────────────────────────────────────────────

struct LockState {
    Queue<uint32_t, 512> pending;

    // Per-slot ack count — each entry tracks how many followers confirmed it.
    // No batch tracking needed; we increment directly from wr_id.
    uint8_t acks[MAX_LOG_PER_LOCK]{};

    uint64_t commit_index  = 0;
    uint64_t current_index = 0;
    uint64_t holder_slot   = 0;

    size_t inflight = 0;
    bool   locked   = false;
};

// ─── WR-ID encoding ──────────────────────────────────────────────────────────
//
// We encode (lock_id, slot, follower_index) into the 64-bit wr_id so that
// when a write completion arrives we know EXACTLY which lock+slot it was for.
// No scanning, no batch arrays.
//
// Layout: [ tag:16 | lock_id:16 | slot:24 | follower:8 ]

namespace wrid {
    constexpr uint64_t REPL_TAG    = 0x0001;
    constexpr uint64_t COMMIT_TAG  = 0x0002;
    constexpr uint64_t ACK_TAG_VAL = 0xFFFF;

    inline uint64_t encode_repl(uint16_t lock_id, uint32_t slot, uint8_t follower) {
        return (REPL_TAG << 48)
             | (static_cast<uint64_t>(lock_id) << 32)
             | (static_cast<uint64_t>(slot & 0xFFFFFF) << 8)
             | follower;
    }

    inline uint64_t encode_commit(uint16_t lock_id) {
        return (COMMIT_TAG << 48) | (static_cast<uint64_t>(lock_id) << 32);
    }

    inline uint64_t encode_ack(uint16_t client_id) {
        return (ACK_TAG_VAL << 48) | client_id;
    }

    inline uint16_t tag(uint64_t id)       { return static_cast<uint16_t>(id >> 48); }
    inline uint16_t lock_id(uint64_t id)   { return static_cast<uint16_t>(id >> 32); }
    inline uint32_t slot(uint64_t id)      { return static_cast<uint32_t>((id >> 8) & 0xFFFFFF); }
    inline uint8_t  follower(uint64_t id)  { return static_cast<uint8_t>(id & 0xFF); }
}

// ─── Main event loop ─────────────────────────────────────────────────────────

void MuLeader::run() {
    std::cout << "[MuLeader " << node_id_ << "] locks ["
              << lock_start_ << ", " << lock_end_ << ")\n";

    auto* local_buf = static_cast<uint8_t*>(buf_);

    // ── Allocate lock state ──
    auto* locks = new LockState[MAX_LOCKS]();

    // ── Unsignaled send tracking ──
    uint32_t client_unsignaled[TOTAL_CLIENTS] = {};
    uint32_t follower_unsignaled[MAX_REPLICAS] = {};
    constexpr uint32_t SIGNAL_INTERVAL = 64;

    // ── Count active followers ──
    size_t num_followers = 0;
    size_t follower_ids[MAX_REPLICAS];
    for (size_t f = 0; f < peers_.size(); ++f) {
        if (peers_[f].id == node_id_ || !peers_[f].id) continue;
        follower_ids[num_followers++] = f;
    }

    // ── Pre-post recvs for all clients ──
    for (size_t i = 0; i < TOTAL_CLIENTS; ++i) {
        for (size_t r = 0; r < 16; ++r) {
            ibv_recv_wr wr{}, *bad = nullptr;
            wr.wr_id = i;
            wr.sg_list = nullptr;
            wr.num_sge = 0;
            if (ibv_post_recv(clients_[i].cm_id->qp, &wr, &bad))
                throw std::runtime_error("Failed to post initial recv");
        }
    }

    // ── Scratch space for building WR chains ──
    // Max WRs per iteration: each lock can drain MAX_INFLIGHT entries,
    // each entry goes to num_followers, plus commit header writes.
    const size_t num_locks = lock_end_ - lock_start_;
    const size_t max_wrs = num_locks * (MAX_INFLIGHT + 1) * num_followers;
    auto* wr_pool  = new ibv_send_wr[max_wrs]();
    auto* sge_pool = new ibv_sge[max_wrs]();

    // ── Helper: post ack to client ──
    auto post_client_ack = [&](uint16_t client_id, uint32_t imm_data) {
        ibv_send_wr swr{}, *bad_wr = nullptr;
        swr.wr_id    = wrid::encode_ack(client_id);
        swr.opcode   = IBV_WR_SEND_WITH_IMM;
        swr.num_sge  = 0;
        swr.sg_list  = nullptr;
        swr.send_flags = IBV_SEND_INLINE;

        if (++client_unsignaled[client_id] >= SIGNAL_INTERVAL) {
            swr.send_flags |= IBV_SEND_SIGNALED;
            client_unsignaled[client_id] = 0;
        }

        swr.imm_data = htonl(imm_data);

        if (ibv_post_send(clients_[client_id].cm_id->qp, &swr, &bad_wr))
            throw std::runtime_error("Failed to post client ack");
    };

    // ── Helper: try to advance commit and grant locks ──
    auto try_commit = [&](uint32_t lid) {
        auto& ls = locks[lid];
        auto* lock_base = mu_lock_base(local_buf, lid);

        while (ls.commit_index < ls.current_index) {
            if (ls.acks[ls.commit_index] < QUORUM) break;

            const auto* entry = mu_entry_ptr(lock_base, ls.commit_index);
            const uint32_t entry_imm = mu_read_client_imm(entry);
            const uint16_t client_id = mu_decode_client_id(entry_imm);
            const uint32_t op        = mu_decode_op(entry_imm);

            // Ack unlocks immediately
            if (op == MU_OP_CLIENT_UNLOCK) {
                ls.locked = false;
                post_client_ack(client_id,
                    mu_encode_ack(static_cast<uint8_t>(lid), ls.commit_index, false));
            }

            // Grant to next waiting LOCK holder(s)
            while (!ls.locked && ls.holder_slot <= ls.commit_index) {
                const auto* next_entry = mu_entry_ptr(lock_base, ls.holder_slot);
                const uint32_t next_imm = mu_read_client_imm(next_entry);
                const uint32_t next_op  = mu_decode_op(next_imm);

                if (next_op == MU_OP_CLIENT_LOCK) {
                    const uint16_t next_client = mu_decode_client_id(next_imm);
                    ls.locked = true;
                    post_client_ack(next_client,
                        mu_encode_ack(static_cast<uint8_t>(lid), ls.holder_slot, true));
                }
                ls.holder_slot++;
            }

            ls.commit_index++;
            ls.inflight--;
        }
    };

    // ── Main loop ──
    ibv_wc wc[64];

    while (true) {
        // ────────────────────────────────────────────────────────────────────
        // Phase 1: Poll completions
        // ────────────────────────────────────────────────────────────────────
        const int n = ibv_poll_cq(cq_, 64, wc);

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[MuLeader] WC error: "
                          << ibv_wc_status_str(wc[i].status)
                          << " opcode=" << wc[i].opcode
                          << " wr_id=0x" << std::hex << wc[i].wr_id
                          << std::dec << "\n";
                throw std::runtime_error("RDMA completion failure");
            }

            const uint64_t id = wc[i].wr_id;
            const uint16_t t  = wrid::tag(id);

            if (wc[i].opcode & IBV_WC_RECV) {
                // ── Client request arrived ──
                const uint32_t imm = ntohl(wc[i].imm_data);
                const uint16_t lock_id  = mu_decode_lock_id(imm);
                const uint16_t client_id = mu_decode_client_id(imm);

                locks[lock_id].pending.push(imm);

                // Re-post recv
                ibv_recv_wr next{}, *bad = nullptr;
                next.wr_id   = client_id;
                next.sg_list = nullptr;
                next.num_sge = 0;
                if (ibv_post_recv(clients_[client_id].cm_id->qp, &next, &bad))
                    throw std::runtime_error("Failed to re-post recv");
            }
            else if (t == wrid::REPL_TAG) {
                // ── Replication write completed for a specific (lock, slot) ──
                const uint16_t lid  = wrid::lock_id(id);
                const uint32_t slot = wrid::slot(id);

                locks[lid].acks[slot]++;
                try_commit(lid);
            }
            else if (t == wrid::COMMIT_TAG) {
                // commit header write — nothing to do
            }
            else if (t == wrid::ACK_TAG_VAL) {
                // signaled client ack — nothing to do
            }
            else if (wc[i].opcode == IBV_WC_SEND) {
                // unsignaled send completion
            }
        }

        // ────────────────────────────────────────────────────────────────────
        // Phase 2: Drain pending → replicate to followers
        // ────────────────────────────────────────────────────────────────────

        // Build per-follower WR chains
        ibv_send_wr* f_head[MAX_REPLICAS] = {};
        ibv_send_wr* f_tail[MAX_REPLICAS] = {};
        size_t wr_idx = 0;

        for (uint32_t lid = lock_start_; lid < lock_end_; ++lid) {
            auto& ls = locks[lid];
            if (ls.pending.size() == 0 || ls.inflight >= MAX_INFLIGHT) continue;

            auto* lock_base = mu_lock_base(local_buf, lid);
            const size_t to_drain = std::min(ls.pending.size(),
                                              MAX_INFLIGHT - ls.inflight);

            for (size_t j = 0; j < to_drain; ++j) {
                uint32_t imm;
                if (!ls.pending.pop(imm)) break;

                const uint32_t slot = ls.current_index;
                auto* entry = mu_entry_ptr(lock_base, slot);
                mu_write_entry(entry, imm);

                // Post to each follower with a unique wr_id encoding (lid, slot, f)
                for (size_t fi = 0; fi < num_followers; ++fi) {
                    const size_t f = follower_ids[fi];

                    auto& sge = sge_pool[wr_idx];
                    sge.addr   = reinterpret_cast<uintptr_t>(entry);
                    sge.length = ENTRY_SIZE;
                    sge.lkey   = mr_->lkey;

                    auto& wr = wr_pool[wr_idx];
                    wr = {};
                    wr.wr_id   = wrid::encode_repl(
                        static_cast<uint16_t>(lid), slot, static_cast<uint8_t>(f));
                    wr.opcode  = IBV_WR_RDMA_WRITE;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.send_flags = IBV_SEND_INLINE;
                    wr.wr.rdma.remote_addr = peers_[f].remote_addr
                        + lid * LOCK_REGION_SIZE
                        + LOCK_HEADER_SIZE
                        + slot * ENTRY_SIZE;
                    wr.wr.rdma.rkey = peers_[f].rkey;
                    wr.next = nullptr;

                    // Signal periodically to avoid SQ overflow
                    follower_unsignaled[f]++;
                    if (follower_unsignaled[f] >= SIGNAL_INTERVAL) {
                        wr.send_flags |= IBV_SEND_SIGNALED;
                        follower_unsignaled[f] = 0;
                    }

                    if (!f_head[f]) f_head[f] = &wr;
                    else            f_tail[f]->next = &wr;
                    f_tail[f] = &wr;

                    wr_idx++;
                }

                ls.current_index++;
                ls.inflight++;
            }

            // Also replicate commit header if we committed anything
            // (write new commit_index to followers so MuFollower can see it)
            mu_write_commit_index(lock_base, ls.commit_index);

            for (size_t fi = 0; fi < num_followers; ++fi) {
                const size_t f = follower_ids[fi];

                auto& sge = sge_pool[wr_idx];
                sge.addr   = reinterpret_cast<uintptr_t>(lock_base);
                sge.length = LOCK_HEADER_SIZE;
                sge.lkey   = mr_->lkey;

                auto& wr = wr_pool[wr_idx];
                wr = {};
                wr.wr_id   = wrid::encode_commit(static_cast<uint16_t>(lid));
                wr.opcode  = IBV_WR_RDMA_WRITE;
                wr.sg_list = &sge;
                wr.num_sge = 1;
                wr.send_flags = IBV_SEND_INLINE;
                wr.wr.rdma.remote_addr = peers_[f].remote_addr
                    + lid * LOCK_REGION_SIZE;
                wr.wr.rdma.rkey = peers_[f].rkey;
                wr.next = nullptr;

                follower_unsignaled[f]++;
                if (follower_unsignaled[f] >= SIGNAL_INTERVAL) {
                    wr.send_flags |= IBV_SEND_SIGNALED;
                    follower_unsignaled[f] = 0;
                }

                if (!f_head[f]) f_head[f] = &wr;
                else            f_tail[f]->next = &wr;
                f_tail[f] = &wr;

                wr_idx++;
            }
        }

        // ────────────────────────────────────────────────────────────────────
        // Phase 3: Post chained WRs to each follower
        // ────────────────────────────────────────────────────────────────────

        for (size_t fi = 0; fi < num_followers; ++fi) {
            const size_t f = follower_ids[fi];
            if (!f_head[f]) continue;

            // Ensure the tail is signaled so we always get at least one
            // completion per batch (for the repl entries — not just commits).
            f_tail[f]->send_flags |= IBV_SEND_SIGNALED;
            follower_unsignaled[f] = 0;

            ibv_send_wr* bad_wr = nullptr;
            if (ibv_post_send(peers_[f].cm_id->qp, f_head[f], &bad_wr))
                throw std::runtime_error("Failed to post chained replication");
        }
    }

    delete[] locks;
    delete[] wr_pool;
    delete[] sge_pool;
}