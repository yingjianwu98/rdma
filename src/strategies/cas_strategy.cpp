#include "rdma/strategies/cas_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

static void advance_frontier(
    LocalState* state, const uint64_t slot,
    const std::vector<RemoteNode>& conns, const ibv_mr* mr
) {
    state->next_frontier = slot;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = 0x111000 | i;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + FRONTIER_OFFSET;
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("advance_frontier post failed");
        }
    }
}

uint64_t CasStrategy::acquire(Client& client, int op_id, uint32_t /*lock_id*/) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();

    if (conns.empty()) throw std::runtime_error("CasStrategy: no connections");

    while (true) {
        const uint64_t expected = target_slot_ - 1;

        // ── Step 1: CAS on the sequencer (connections[0]) ──

        state->cas_results[0] = 0xFEFEFEFEFEFEFEFE;

        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->cas_results[0]),
            .length = 8,
            .lkey = mr->lkey
        };

        ibv_send_wr wr{}, *bad_wr;
        wr.wr_id = (static_cast<uint64_t>(op_id) << 32) | 0x123;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.atomic.remote_addr = conns[0].addr + FRONTIER_OFFSET;
        wr.wr.atomic.rkey = conns[0].rkey;
        wr.wr.atomic.compare_add = expected;
        wr.wr.atomic.swap = target_slot_;

        if (ibv_post_send(conns[0].id->qp, &wr, &bad_wr)) {
            std::cerr << "Post failed: " << strerror(errno) << std::endl;
            continue;
        }

        // ── Step 2: Poll for our CAS completion ──

        ibv_wc wc{};
        const uint64_t expected_id = (static_cast<uint64_t>(op_id) << 32) | 0x123;

        while (true) {
            int n = ibv_poll_cq(cq, 1, &wc);
            if (n < 0) throw std::runtime_error("Poll CQ failed");
            if (n > 0 && wc.wr_id == expected_id) break;
        }

        if (wc.status != IBV_WC_SUCCESS) {
            std::cerr << "CAS completion error: " << wc.status << std::endl;
            continue;
        }

        const uint64_t result = state->cas_results[0];

        // ── Step 3: Did we win? ──

        if (result == expected) {
            // We won — replicate our client_id to all nodes at this slot
            state->next_frontier = static_cast<uint64_t>(client.id());

            ibv_sge replication_sge{
                .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
                .length = 8,
                .lkey = mr->lkey
            };

            const uint64_t log_offset = target_slot_ * 8;

            for (size_t i = 0; i < conns.size(); ++i) {
                ibv_send_wr rep_wr{}, *bad = nullptr;
                rep_wr.wr_id = (target_slot_ << 32) | static_cast<uint32_t>(i);
                rep_wr.sg_list = &replication_sge;
                rep_wr.num_sge = 1;
                rep_wr.opcode = IBV_WR_RDMA_WRITE;
                rep_wr.send_flags = IBV_SEND_SIGNALED;
                rep_wr.wr.rdma.remote_addr = conns[i].addr + log_offset;
                rep_wr.wr.rdma.rkey = conns[i].rkey;

                if (ibv_post_send(conns[i].id->qp, &rep_wr, &bad)) {
                    throw std::runtime_error("Replication post failed");
                }
            }

            // Wait for quorum acks on replication
            int acks = 0;
            ibv_wc wc_batch[32];
            while (acks < static_cast<int>(QUORUM)) {
                int pulled = ibv_poll_cq(cq, 32, wc_batch);
                for (int j = 0; j < pulled; ++j) {
                    if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
                    if ((wc_batch[j].wr_id >> 32) == target_slot_) acks++;
                }
            }

            return target_slot_;
        }

        // ── Lost — adjust target_slot based on what we read back ──
        if (result % 2 != 0) {
            target_slot_ = result + 2; // odd = in-progress, skip past it
        }
        else {
            target_slot_ = result + 1; // even = clean, try next
        }
    }
}

void CasStrategy::release(Client& client, int /*op_id*/, uint32_t /*lock_id*/) {
    auto* state = static_cast<LocalState*>(client.buffer());
    const auto& conns = client.connections();
    const auto* mr = client.mr();

    advance_frontier(state, target_slot_ + 1, conns, mr);
    target_slot_ += 2;
}

void CasStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // CAS: no reset needed — target_slot_ tracks state across calls
}
