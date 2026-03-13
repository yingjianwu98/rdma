#include "rdma/strategies/faa_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

constexpr uint64_t FAA_TAG = 0x00A00000;
constexpr uint64_t REPLICATE_TAG = 0x00B00000;
constexpr uint64_t RELEASE_TAG = 0x00C00000;
constexpr uint64_t READ_TAG = 0x00D00000;
constexpr uint64_t NOTIFY_TAG = 0x00E00000;
constexpr uint64_t TAG_MASK = 0x00F00000;

static inline uint64_t make_wr_id(uint64_t ctx, uint64_t tag, uint32_t idx = 0) {
    return (ctx << 32) | tag | (idx & 0xFFFFF);
}

static inline uint64_t wr_ctx(uint64_t wr_id) { return wr_id >> 32; }
static inline uint64_t wr_tag(uint64_t wr_id) { return wr_id & TAG_MASK; }

constexpr int NOTIFY_SPIN_ROUNDS = 1000;

uint64_t FaaStrategy::acquire(Client& client, int /*op_id*/, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();

    if (conns.empty()) throw std::runtime_error("FaaStrategy: no connections");

    const uint64_t faa_ctx = lock_id;

    state->metadata = 0xDEAD;

    ibv_sge faa_sge{
        .addr = reinterpret_cast<uintptr_t>(&state->metadata),
        .length = 8,
        .lkey = mr->lkey
    };

    ibv_send_wr faa_wr{}, *bad_faa = nullptr;
    faa_wr.wr_id = make_wr_id(faa_ctx, FAA_TAG);
    faa_wr.sg_list = &faa_sge;
    faa_wr.num_sge = 1;
    faa_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    faa_wr.send_flags = IBV_SEND_SIGNALED;
    faa_wr.wr.atomic.remote_addr = conns[0].addr + lock_control_offset(lock_id);
    faa_wr.wr.atomic.rkey = conns[0].rkey;
    faa_wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(conns[0].id->qp, &faa_wr, &bad_faa))
        throw std::runtime_error("FAA post failed: " + std::string(strerror(errno)));

    {
        ibv_wc wc{};
        while (true) {
            int n = ibv_poll_cq(cq, 1, &wc);
            if (n < 0) throw std::runtime_error("Poll CQ failed");
            if (n > 0 && wr_ctx(wc.wr_id) == faa_ctx && wr_tag(wc.wr_id) == FAA_TAG) {
                if (wc.status != IBV_WC_SUCCESS)
                    throw std::runtime_error("FAA failed: status " + std::to_string(wc.status));
                break;
            }
        }
    }

    std::atomic_thread_fence(std::memory_order_acquire);
    my_ticket_ = state->metadata;

      std::cerr << "[FAA Client " << client.id() << "] lock=" << lock_id
              << " ticket=" << my_ticket_ << "\n";

    state->next_frontier = encode_slot(client.id(), false);

    ibv_sge rep_sge{
        .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
        .length = 8,
        .lkey = mr->lkey
    };

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = make_wr_id(my_ticket_, REPLICATE_TAG, static_cast<uint32_t>(i));
        wr.sg_list = &rep_sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, my_ticket_);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad))
            throw std::runtime_error("Replicate post failed");
    }

    {
        int acks = 0;
        ibv_wc wc_batch[32];
        while (acks < static_cast<int>(QUORUM)) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            for (int j = 0; j < pulled; ++j) {
                if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
                if (wr_ctx(wc_batch[j].wr_id) == my_ticket_ &&
                    wr_tag(wc_batch[j].wr_id) == REPLICATE_TAG) {
                    acks++;
                }
            }
        }
    }


    if (my_ticket_ == 0) {
        std::cerr << "[FAA Client " << client.id() << "] lock=" << lock_id
                  << " ticket=0, acquired immediately\n";
        return my_ticket_;
    }

    auto* notify_ptr = reinterpret_cast<volatile uint64_t*>(&state->metadata);
    *notify_ptr = NOTIFY_CLEAR;
    std::atomic_thread_fence(std::memory_order_seq_cst);

    const uint64_t prev_slot = my_ticket_ - 1;

    std::cerr << "[FAA Client " << client.id() << "] lock=" << lock_id
              << " ticket=" << my_ticket_ << " waiting for predecessor " << (my_ticket_ - 1) << "\n";

    while (true) {
        for (int spin = 0; spin < NOTIFY_SPIN_ROUNDS; ++spin) {
            if (*notify_ptr != NOTIFY_CLEAR) {
                std::cout << "We fast pathed!" << std::endl;
                return my_ticket_;
            }
        }

        ibv_wc wc_batch[32];

        for (size_t i = 0; i < conns.size(); ++i) {
            state->learn_results[i] = 0;

            ibv_sge sge{
                .addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]),
                .length = 8,
                .lkey = mr->lkey
            };
            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(my_ticket_, READ_TAG, static_cast<uint32_t>(i));
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, prev_slot);
            wr.wr.rdma.rkey = conns[i].rkey;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                throw std::runtime_error("Fallback read post failed");
        }

        int responses = 0;
        while (responses < static_cast<int>(conns.size())) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            for (int j = 0; j < pulled; ++j) {
                if (wr_ctx(wc_batch[j].wr_id) == my_ticket_ &&
                    wr_tag(wc_batch[j].wr_id) == READ_TAG) {
                    responses++;
                }
            }
        }

        int done_count = 0;
        for (size_t i = 0; i < conns.size(); ++i) {
            if (state->learn_results[i] != EMPTY_SLOT &&
                is_done(state->learn_results[i])) {
                done_count++;
            }
        }

        if (done_count >= static_cast<int>(QUORUM)) {
            return my_ticket_;
        }

        std::cerr << "[FAA Client " << client.id() << "] lock=" << lock_id
          << " ticket=" << my_ticket_ << " slow-path: done_count=" << done_count
          << "/" << QUORUM << " vals=[";
        for (size_t i = 0; i < conns.size(); ++i) {
            if (i > 0) std::cerr << ", ";
            std::cerr << "0x" << std::hex << state->learn_results[i] << std::dec;
        }
        std::cerr << "]\n";
    }
}


void FaaStrategy::release(Client& client, int /*op_id*/, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();
    const auto& peers = client.peers();

    ibv_wc wc_batch[32];

    state->next_frontier = encode_slot(client.id(), true);

    {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
            .length = 8,
            .lkey = mr->lkey
        };

        for (size_t i = 0; i < conns.size(); ++i) {
            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(my_ticket_, RELEASE_TAG, static_cast<uint32_t>(i));
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, my_ticket_);
            wr.wr.rdma.rkey = conns[i].rkey;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                throw std::runtime_error("Release post failed");
        }

        int acks = 0;
        while (acks < static_cast<int>(QUORUM)) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            for (int j = 0; j < pulled; ++j) {
                if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
                if (wr_ctx(wc_batch[j].wr_id) == my_ticket_ &&
                    wr_tag(wc_batch[j].wr_id) == RELEASE_TAG) {
                    acks++;
                }
            }
        }
    }

    const uint64_t next_slot = my_ticket_ + 1;

    for (size_t i = 0; i < conns.size(); ++i) {
        state->learn_results[i] = EMPTY_SLOT;

        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = make_wr_id(my_ticket_, READ_TAG, static_cast<uint32_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, next_slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad))
            throw std::runtime_error("Release read post failed");
    }

    {
        int responses = 0;
        while (responses < static_cast<int>(conns.size())) {
            int pulled = ibv_poll_cq(cq, 32, wc_batch);
            for (int j = 0; j < pulled; ++j) {
                if (wr_ctx(wc_batch[j].wr_id) == my_ticket_ &&
                    wr_tag(wc_batch[j].wr_id) == READ_TAG) {
                    responses++;
                }
            }
        }
    }

    uint32_t next_client_id = UINT32_MAX;
    for (size_t i = 0; i < conns.size(); ++i) {
        const uint64_t val = state->learn_results[i];
        if (val == EMPTY_SLOT) continue;

        const uint32_t cid = decode_client(val);
        int count = 0;
        for (size_t j = 0; j < conns.size(); ++j) {
            if (state->learn_results[j] != EMPTY_SLOT &&
                decode_client(state->learn_results[j]) == cid) {
                count++;
            }
        }
        if (count >= static_cast<int>(QUORUM)) {
            next_client_id = cid;
            break;
        }
    }


    if (next_client_id != UINT32_MAX && next_client_id < peers.size()) {
        const auto& peer = peers[next_client_id];

        if (peer.id != nullptr) {
            state->next_frontier = GO_SIGNAL;

            ibv_sge sge{
                .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
                .length = 8,
                .lkey = mr->lkey
            };

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_wr_id(my_ticket_, NOTIFY_TAG);
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = peer.addr + offsetof(LocalState, metadata);
            wr.wr.rdma.rkey = peer.rkey;

            if (ibv_post_send(peer.id->qp, &wr, &bad))
                throw std::runtime_error("Notify post failed");

            ibv_wc wc{};
            while (true) {
                int n = ibv_poll_cq(cq, 1, &wc);
                if (n > 0 &&
                    wr_ctx(wc.wr_id) == my_ticket_ &&
                    wr_tag(wc.wr_id) == NOTIFY_TAG)
                    break;
            }
        }
    }
}


void FaaStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // No cleanup needed
}
