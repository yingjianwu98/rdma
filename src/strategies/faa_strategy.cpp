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

static inline uint64_t wr_ctx(uint64_t wr_id) {
    return wr_id >> 32;
}

static inline uint64_t wr_tag(uint64_t wr_id) {
    return wr_id & TAG_MASK;
}

constexpr int NOTIFY_SPIN_ROUNDS = 100000;

namespace {
    [[noreturn]] void throw_wc_error(const char* where, const ibv_wc& wc) {
        throw std::runtime_error(
            std::string(where) + " failed: status=" + std::to_string(wc.status) +
            " vendor_err=" + std::to_string(wc.vendor_err));
    }

    void wait_for_exact_completion(ibv_cq* cq, uint64_t want_ctx, uint64_t want_tag, const char* where) {
        ibv_wc wc_batch[32];

        for (;;) {
            const int pulled = ibv_poll_cq(cq, 32, wc_batch);
            if (pulled < 0) {
                throw std::runtime_error(std::string(where) + ": ibv_poll_cq failed");
            }
            if (pulled == 0) {
                continue;
            }

            for (int i = 0; i < pulled; ++i) {
                const ibv_wc& wc = wc_batch[i];

                if (wc.status != IBV_WC_SUCCESS) {
                    throw_wc_error(where, wc);
                }

                if (wr_ctx(wc.wr_id) == want_ctx && wr_tag(wc.wr_id) == want_tag) {
                    return;
                }
            }
        }
    }

    void wait_for_n_completions(ibv_cq* cq,
                                uint64_t want_ctx,
                                uint64_t want_tag,
                                int want_count,
                                const char* where) {
        ibv_wc wc_batch[32];
        int seen = 0;

        while (seen < want_count) {
            const int pulled = ibv_poll_cq(cq, 32, wc_batch);
            if (pulled < 0) {
                throw std::runtime_error(std::string(where) + ": ibv_poll_cq failed");
            }
            if (pulled == 0) {
                continue;
            }

            for (int i = 0; i < pulled; ++i) {
                const ibv_wc& wc = wc_batch[i];

                if (wc.status != IBV_WC_SUCCESS) {
                    throw_wc_error(where, wc);
                }

                if (wr_ctx(wc.wr_id) == want_ctx && wr_tag(wc.wr_id) == want_tag) {
                    ++seen;
                    if (seen >= want_count) {
                        return;
                    }
                }
            }
        }
    }
}

uint64_t FaaStrategy::acquire(Client& client, int /*op_id*/, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();

    if (conns.empty()) {
        throw std::runtime_error("FaaStrategy: no connections");
    }

    const uint64_t faa_ctx = lock_id;

    // 1) FAA against leader control word.
    state->metadata = 0xDEAD;

    ibv_sge faa_sge{};
    faa_sge.addr = reinterpret_cast<uintptr_t>(&state->metadata);
    faa_sge.length = 8;
    faa_sge.lkey = mr->lkey;

    ibv_send_wr faa_wr{}, *bad_faa = nullptr;
    faa_wr.wr_id = make_wr_id(faa_ctx, FAA_TAG);
    faa_wr.sg_list = &faa_sge;
    faa_wr.num_sge = 1;
    faa_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    faa_wr.send_flags = IBV_SEND_SIGNALED;
    faa_wr.wr.atomic.remote_addr = conns[0].addr + lock_control_offset(lock_id);
    faa_wr.wr.atomic.rkey = conns[0].rkey;
    faa_wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(conns[0].id->qp, &faa_wr, &bad_faa)) {
        throw std::runtime_error("FAA post failed: " + std::string(std::strerror(errno)));
    }

    wait_for_exact_completion(cq, faa_ctx, FAA_TAG, "FAA");

    my_ticket_ = state->metadata;

    // 2) Replicate my "not-done" slot value to all replicas.
    state->next_frontier = encode_slot(client.id(), false);

    ibv_sge rep_sge{};
    rep_sge.addr = reinterpret_cast<uintptr_t>(&state->next_frontier);
    rep_sge.length = 8;
    rep_sge.lkey = mr->lkey;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = make_wr_id(my_ticket_, REPLICATE_TAG, static_cast<uint32_t>(i));
        wr.sg_list = &rep_sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, my_ticket_);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("Replicate post failed");
        }
    }

    wait_for_n_completions(
        cq,
        my_ticket_,
        REPLICATE_TAG,
        static_cast<int>(QUORUM),
        "Replicate quorum");

    if (my_ticket_ == 0) {
        return my_ticket_;
    }

    // 3) Wait for predecessor to release.
    auto* notify_ptr = reinterpret_cast<volatile uint64_t*>(&state->metadata);
    *notify_ptr = NOTIFY_CLEAR;

    const uint64_t prev_slot = my_ticket_ - 1;

    for (;;) {

        {
            for (size_t i = 0; i < conns.size(); ++i) {
                state->learn_results[i] = EMPTY_SLOT;

                ibv_sge sge{};
                sge.addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]);
                sge.length = 8;
                sge.lkey = mr->lkey;

                ibv_send_wr wr{}, *bad = nullptr;
                wr.wr_id = make_wr_id(my_ticket_, READ_TAG, static_cast<uint32_t>(i));
                wr.sg_list = &sge;
                wr.num_sge = 1;
                wr.opcode = IBV_WR_RDMA_READ;
                wr.send_flags = IBV_SEND_SIGNALED;
                wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, prev_slot);
                wr.wr.rdma.rkey = conns[i].rkey;

                if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
                    throw std::runtime_error("Fallback read post failed");
                }
            }

            wait_for_n_completions(
                cq,
                my_ticket_,
                READ_TAG,
                static_cast<int>(conns.size()),
                "Fallback read");

            int done_count = 0;
            for (size_t i = 0; i < conns.size(); ++i) {
                if (state->learn_results[i] != EMPTY_SLOT &&
                    is_done(state->learn_results[i])) {
                    ++done_count;
                    }
            }

            if (done_count >= static_cast<int>(QUORUM)) {
                return my_ticket_;
            }
        }

        // Fast path: predecessor sent GO directly to my metadata.
        for (int spin = 0; spin < NOTIFY_SPIN_ROUNDS; ++spin) {
            if (*notify_ptr != NOTIFY_CLEAR) {
                return my_ticket_;
            }
        }

        // Slow path: read predecessor slot from all replicas and see if a quorum says "done".
        // for (size_t i = 0; i < conns.size(); ++i) {
        //     state->learn_results[i] = EMPTY_SLOT;
        //
        //     ibv_sge sge{};
        //     sge.addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]);
        //     sge.length = 8;
        //     sge.lkey = mr->lkey;
        //
        //     ibv_send_wr wr{}, *bad = nullptr;
        //     wr.wr_id = make_wr_id(my_ticket_, READ_TAG, static_cast<uint32_t>(i));
        //     wr.sg_list = &sge;
        //     wr.num_sge = 1;
        //     wr.opcode = IBV_WR_RDMA_READ;
        //     wr.send_flags = IBV_SEND_SIGNALED;
        //     wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, prev_slot);
        //     wr.wr.rdma.rkey = conns[i].rkey;
        //
        //     if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
        //         throw std::runtime_error("Fallback read post failed");
        //     }
        // }
        //
        // wait_for_n_completions(
        //     cq,
        //     my_ticket_,
        //     READ_TAG,
        //     static_cast<int>(conns.size()),
        //     "Fallback read");
        //
        // int done_count = 0;
        // for (size_t i = 0; i < conns.size(); ++i) {
        //     if (state->learn_results[i] != EMPTY_SLOT &&
        //         is_done(state->learn_results[i])) {
        //         ++done_count;
        //     }
        // }
        //
        // if (done_count >= static_cast<int>(QUORUM)) {
        //     return my_ticket_;
        // }
    }
}

void FaaStrategy::release(Client& client, int /*op_id*/, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();
    const auto& peers = client.peers();

    // 1) Mark my slot done on all replicas.
    state->next_frontier = encode_slot(client.id(), true);

    {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&state->next_frontier);
        sge.length = 8;
        sge.lkey = mr->lkey;

        for (size_t i = 0; i < conns.size(); ++i) {
            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(my_ticket_, RELEASE_TAG, static_cast<uint32_t>(i));
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, my_ticket_);
            wr.wr.rdma.rkey = conns[i].rkey;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
                throw std::runtime_error("Release post failed");
            }
        }

        wait_for_n_completions(
            cq,
            my_ticket_,
            RELEASE_TAG,
            static_cast<int>(QUORUM),
            "Release quorum");
    }

    // 2) Discover who owns the next slot.
    const uint64_t next_slot = my_ticket_ + 1;

    for (size_t i = 0; i < conns.size(); ++i) {
        state->learn_results[i] = EMPTY_SLOT;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]);
        sge.length = 8;
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = make_wr_id(my_ticket_, NEXT_READ_TAG, static_cast<uint32_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, next_slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("Release read post failed");
        }
    }

    wait_for_n_completions(
        cq,
        my_ticket_,
        NEXT_READ_TAG,
        static_cast<int>(conns.size()),
        "Next-slot read");

    uint32_t next_client_id = UINT32_MAX;

    for (size_t i = 0; i < conns.size(); ++i) {
        const uint64_t val = state->learn_results[i];
        if (val == EMPTY_SLOT) {
            continue;
        }

        const uint32_t cid = decode_client(val);
        int count = 0;

        for (size_t j = 0; j < conns.size(); ++j) {
            if (state->learn_results[j] != EMPTY_SLOT &&
                decode_client(state->learn_results[j]) == cid) {
                ++count;
            }
        }

        if (count >= static_cast<int>(QUORUM)) {
            next_client_id = cid;
            break;
        }
    }

    // 3) Notify the next waiter directly, if we could identify one.
    if (next_client_id != UINT32_MAX && next_client_id < peers.size()) {
        const auto& peer = peers[next_client_id];

        if (peer.id != nullptr) {
            state->next_frontier = GO_SIGNAL;

            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(&state->next_frontier);
            sge.length = 8;
            sge.lkey = mr->lkey;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(my_ticket_, NOTIFY_TAG);
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = peer.addr + offsetof(LocalState, metadata);
            wr.wr.rdma.rkey = peer.rkey;

            if (ibv_post_send(peer.id->qp, &wr, &bad)) {
                throw std::runtime_error("Notify post failed");
            }

            wait_for_exact_completion(cq, my_ticket_, NOTIFY_TAG, "Notify");
        }
    }
}

void FaaStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // No cleanup needed
}
