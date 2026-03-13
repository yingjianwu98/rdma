#include "rdma/strategies/faa_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

constexpr uint64_t FAA_TAG = 0x00100000;
constexpr uint64_t REPLICATE_TAG = 0x00200000;
constexpr uint64_t RELEASE_TAG = 0x00300000;
constexpr uint64_t READ_TAG = 0x00400000;
constexpr uint64_t NEXT_READ_TAG = 0x00500000;
constexpr uint64_t NOTIFY_TAG = 0x00600000;
constexpr uint64_t TAG_MASK = 0x00F00000;

static inline uint64_t make_wr_id(uint64_t ctx, uint64_t tag, uint32_t idx = 0) {
    return (ctx << 32) | tag | (idx & 0xFFFFF);
}

static inline uint64_t wr_ctx(uint64_t wr_id) { return wr_id >> 32; }
static inline uint64_t wr_tag(uint64_t wr_id) { return wr_id & TAG_MASK; }

constexpr int NOTIFY_SPIN_ROUNDS = 100000;

namespace {
    [[noreturn]] void throw_wc_error(const char* where, const ibv_wc& wc) {
        throw std::runtime_error(
            std::string(where) + " failed: status=" + std::to_string(wc.status) +
            " vendor_err=" + std::to_string(wc.vendor_err));
    }

    void wait_exact(ibv_cq* cq, uint64_t ctx, uint64_t tag, const char* where) {
        ibv_wc wc_batch[32];
        for (;;) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            if (pulled < 0) throw std::runtime_error(std::string(where) + ": poll failed");
            for (int i = 0; i < pulled; ++i) {
                if (wc_batch[i].status != IBV_WC_SUCCESS) throw_wc_error(where, wc_batch[i]);
                if (wr_ctx(wc_batch[i].wr_id) == ctx && wr_tag(wc_batch[i].wr_id) == tag) return;
            }
        }
    }

    void wait_n(ibv_cq* cq, uint64_t ctx, uint64_t tag, int n, const char* where) {
        ibv_wc wc_batch[32];
        int seen = 0;
        while (seen < n) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            if (pulled < 0) throw std::runtime_error(std::string(where) + ": poll failed");
            for (int i = 0; i < pulled; ++i) {
                if (wc_batch[i].status != IBV_WC_SUCCESS) throw_wc_error(where, wc_batch[i]);
                if (wr_ctx(wc_batch[i].wr_id) == ctx && wr_tag(wc_batch[i].wr_id) == tag) {
                    if (++seen >= n) return;
                }
            }
        }
    }
}

uint64_t FaaStrategy::acquire(Client& client, int op_id, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();

    if (conns.empty()) throw std::runtime_error("FaaStrategy: no connections");

    const uint64_t ctx = static_cast<uint32_t>(op_id);

    // 1. get ticket
    state->metadata = 0xDEAD;

    ibv_sge faa_sge{};
    faa_sge.addr = reinterpret_cast<uintptr_t>(&state->metadata);
    faa_sge.length = 8;
    faa_sge.lkey = mr->lkey;

    ibv_send_wr faa_wr{}, *bad_faa = nullptr;
    faa_wr.wr_id = make_wr_id(ctx, FAA_TAG);
    faa_wr.sg_list = &faa_sge;
    faa_wr.num_sge = 1;
    faa_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    faa_wr.send_flags = IBV_SEND_SIGNALED;
    faa_wr.wr.atomic.remote_addr = conns[0].addr + lock_control_offset(lock_id);
    faa_wr.wr.atomic.rkey = conns[0].rkey;
    faa_wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(conns[0].id->qp, &faa_wr, &bad_faa))
        throw std::runtime_error("FAA post failed: " + std::string(std::strerror(errno)));

    wait_exact(cq, ctx, FAA_TAG, "FAA");
    my_ticket_ = state->metadata;

    // 2. replicate slot to all servers
    state->next_frontier = encode_slot(client.id(), false);

    ibv_sge rep_sge{};
    rep_sge.addr = reinterpret_cast<uintptr_t>(&state->next_frontier);
    rep_sge.length = 8;
    rep_sge.lkey = mr->lkey;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = make_wr_id(ctx, REPLICATE_TAG, static_cast<uint32_t>(i));
        wr.sg_list = &rep_sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, my_ticket_);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad))
            throw std::runtime_error("Replicate post failed");
    }

    wait_n(cq, ctx, REPLICATE_TAG, static_cast<int>(QUORUM), "Replicate");

    // 3. ticket 0 = no predecessor, we hold the lock
    next_client_id_ = UINT32_MAX;  // reset — we'll try to learn during spin

    if (my_ticket_ == 0) return my_ticket_;

    // 4. wait for predecessor to release
    auto* notify_ptr = reinterpret_cast<volatile uint64_t*>(&state->notify_signal);
    *notify_ptr = NOTIFY_CLEAR;

    const uint64_t prev_slot = my_ticket_ - 1;
    const uint64_t next_slot = my_ticket_ + 1;

    for (;;) {
        // Bulk read: predecessor slot + next slot from all servers
        // We use learn_results[0..N-1] for prev_slot reads (READ_TAG)
        // We use frontier_values[0..N-1] for next_slot reads (NEXT_READ_TAG)

        for (size_t i = 0; i < conns.size(); ++i) {
            state->learn_results[i] = EMPTY_SLOT;
            state->frontier_values[i] = EMPTY_SLOT;
        }

        for (size_t i = 0; i < conns.size(); ++i) {
            // read predecessor slot
            {
                ibv_sge sge{};
                sge.addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]);
                sge.length = 8;
                sge.lkey = mr->lkey;

                ibv_send_wr wr{}, *bad = nullptr;
                wr.wr_id = make_wr_id(ctx, READ_TAG, static_cast<uint32_t>(i));
                wr.sg_list = &sge;
                wr.num_sge = 1;
                wr.opcode = IBV_WR_RDMA_READ;
                wr.send_flags = IBV_SEND_SIGNALED;
                wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, prev_slot);
                wr.wr.rdma.rkey = conns[i].rkey;

                if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                    throw std::runtime_error("Prev slot read post failed");
            }

            // read next slot (speculative — learn successor early)
            {
                ibv_sge sge{};
                sge.addr = reinterpret_cast<uintptr_t>(&state->frontier_values[i]);
                sge.length = 8;
                sge.lkey = mr->lkey;

                ibv_send_wr wr{}, *bad = nullptr;
                wr.wr_id = make_wr_id(ctx, NEXT_READ_TAG, static_cast<uint32_t>(i));
                wr.sg_list = &sge;
                wr.num_sge = 1;
                wr.opcode = IBV_WR_RDMA_READ;
                wr.send_flags = IBV_SEND_SIGNALED;
                wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, next_slot);
                wr.wr.rdma.rkey = conns[i].rkey;

                if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                    throw std::runtime_error("Next slot read post failed");
            }
        }

        // Wait for all reads (2 * conns.size() completions)
        ibv_wc wc_batch[32];
        int prev_responses = 0;
        int next_responses = 0;
        const int total_reads = static_cast<int>(conns.size()) * 2;
        int total_done = 0;
        bool prev_resolved = false;

        while (total_done < total_reads) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            if (pulled < 0) throw std::runtime_error("Bulk read: poll failed");

            for (int i = 0; i < pulled; ++i) {
                if (wc_batch[i].status != IBV_WC_SUCCESS)
                    throw_wc_error("Bulk read", wc_batch[i]);

                if (wr_ctx(wc_batch[i].wr_id) == ctx) {
                    uint64_t tag = wr_tag(wc_batch[i].wr_id);
                    if (tag == READ_TAG) { ++prev_responses; ++total_done; }
                    else if (tag == NEXT_READ_TAG) { ++next_responses; ++total_done; }
                }
            }

            // Check if predecessor is done (quorum)
            if (!prev_resolved) {
                int done_count = 0;
                for (size_t i = 0; i < conns.size(); ++i) {
                    if (state->learn_results[i] != EMPTY_SLOT && is_done(state->learn_results[i]))
                        ++done_count;
                }
                if (done_count >= static_cast<int>(QUORUM)) {
                    prev_resolved = true;
                }
            }

            // Try to learn next client (quorum)
            if (next_client_id_ == UINT32_MAX) {
                for (size_t j = 0; j < conns.size(); ++j) {
                    uint64_t val = state->frontier_values[j];
                    if (val == EMPTY_SLOT) continue;

                    uint32_t cid = decode_client(val);
                    int count = 0;
                    for (size_t k = 0; k < conns.size(); ++k) {
                        if (state->frontier_values[k] != EMPTY_SLOT &&
                            decode_client(state->frontier_values[k]) == cid)
                            ++count;
                    }
                    if (count >= static_cast<int>(QUORUM)) {
                        next_client_id_ = cid;
                        break;
                    }
                }
            }

            // If predecessor resolved, we can break early
            if (prev_resolved) break;
        }

        if (prev_resolved) return my_ticket_;

        // Spin on local notify signal
        for (int spin = 0; spin < NOTIFY_SPIN_ROUNDS; ++spin) {
            if (*notify_ptr != NOTIFY_CLEAR) return my_ticket_;
        }
    }
}

void FaaStrategy::release(Client& client, int op_id, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();
    const auto& peers = client.peers();

    const uint64_t ctx = static_cast<uint32_t>(op_id);

    // 1. mark slot done on all servers
    state->next_frontier = encode_slot(client.id(), true);

    {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&state->next_frontier);
        sge.length = 8;
        sge.lkey = mr->lkey;

        for (size_t i = 0; i < conns.size(); ++i) {
            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(ctx, RELEASE_TAG, static_cast<uint32_t>(i));
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, my_ticket_);
            wr.wr.rdma.rkey = conns[i].rkey;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                throw std::runtime_error("Release post failed");
        }

        wait_n(cq, ctx, RELEASE_TAG, static_cast<int>(QUORUM), "Release");
    }

    // 2. If we already learned the next client during acquire, skip the read
    if (next_client_id_ == UINT32_MAX) {
        // Slow path: read next slot now
        const uint64_t next_slot = my_ticket_ + 1;

        for (size_t i = 0; i < conns.size(); ++i) {
            state->learn_results[i] = EMPTY_SLOT;

            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]);
            sge.length = 8;
            sge.lkey = mr->lkey;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(ctx, NEXT_READ_TAG, static_cast<uint32_t>(i));
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, next_slot);
            wr.wr.rdma.rkey = conns[i].rkey;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                throw std::runtime_error("Release read post failed");
        }

        ibv_wc wc_batch[32];
        int responses = 0;

        while (responses < static_cast<int>(conns.size())) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            if (pulled < 0) throw std::runtime_error("Next-slot read: poll failed");
            for (int i = 0; i < pulled; ++i) {
                if (wc_batch[i].status != IBV_WC_SUCCESS) throw_wc_error("Next-slot read", wc_batch[i]);
                if (wr_ctx(wc_batch[i].wr_id) == ctx && wr_tag(wc_batch[i].wr_id) == NEXT_READ_TAG)
                    ++responses;
            }

            for (size_t j = 0; j < conns.size(); ++j) {
                uint64_t val = state->learn_results[j];
                if (val == EMPTY_SLOT) continue;

                uint32_t cid = decode_client(val);
                int count = 0;
                for (size_t k = 0; k < conns.size(); ++k) {
                    if (state->learn_results[k] != EMPTY_SLOT &&
                        decode_client(state->learn_results[k]) == cid)
                        ++count;
                }
                if (count >= static_cast<int>(QUORUM)) {
                    next_client_id_ = cid;
                    break;
                }
            }
            if (next_client_id_ != UINT32_MAX) break;
        }
    }

    // 3. notify next waiter via peer RDMA write
    if (next_client_id_ != UINT32_MAX && next_client_id_ < peers.size()) {
        const auto& peer = peers[next_client_id_];

        if (peer.id != nullptr) {
            state->next_frontier = GO_SIGNAL;

            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(&state->next_frontier);
            sge.length = 8;
            sge.lkey = mr->lkey;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(ctx, NOTIFY_TAG);
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = peer.addr + offsetof(LocalState, notify_signal);
            wr.wr.rdma.rkey = peer.rkey;

            if (ibv_post_send(peer.id->qp, &wr, &bad))
                throw std::runtime_error("Notify post failed");

            wait_exact(cq, ctx, NOTIFY_TAG, "Notify");
        }
    }
}

void FaaStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
}