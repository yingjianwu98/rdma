#include "rdma/strategies/cas_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

// ─── advance_frontier: CAS frontier from old_val → new_val, fire-and-forget ───

static void advance_frontier(
    LocalState* state, const uint64_t old_val, const uint64_t new_val,
    uint32_t lock_id, int op_id,
    const std::vector<RemoteNode>& conns, const ibv_mr* mr,
    ibv_cq* cq
) {
    const size_t node = lock_id % conns.size();
    std::cerr << "advance_frontier: conns=" << conns.size()
              << " node=" << node << " lock=" << lock_id
              << " op=" << op_id << std::endl;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->cas_results[i]),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op_id) << 32) | 0x111000 | i;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_control_offset(lock_id);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = i == node ? old_val : std::min(old_val, old_val-1);
        wr.wr.atomic.swap = new_val;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("advance_frontier post failed");
        }
    }

    int done = 0;
    ibv_wc wcs[16];
    while (done < static_cast<int>(QUORUM)) {
        int n = ibv_poll_cq(cq, 16, wcs);
        for (int i = 0; i < n; ++i) {
            bool is_ours = (wcs[i].wr_id >> 32) == static_cast<uint64_t>(op_id);
            bool is_advance = (wcs[i].wr_id & 0xFFF000) == 0x111000;
            if (is_ours && is_advance) done++;
        }
    }
}

uint64_t CasStrategy::acquire(Client& client, int op_id, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();

     const size_t node = lock_id % conns.size();
    // std::cout << "Reaching out to node: " << node << std::endl;

    while (true) {
        const uint64_t expected = target_slot_ - 1;

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
        wr.wr.atomic.remote_addr = conns[node].addr + lock_control_offset(lock_id);
        wr.wr.atomic.rkey = conns[node].rkey;
        wr.wr.atomic.compare_add = expected;
        wr.wr.atomic.swap = target_slot_;

        if (ibv_post_send(conns[node].id->qp, &wr, &bad_wr)) {
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
            // We won — replicate our client_id to this lock's log on all nodes
            state->next_frontier = static_cast<uint64_t>(client.id());

            // ibv_sge replication_sge{
            //     .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
            //     .length = 8,
            //     .lkey = mr->lkey
            // };
            //
            // for (size_t i = 0; i < conns.size(); ++i) {
            //     ibv_send_wr rep_wr{}, *bad = nullptr;
            //     rep_wr.wr_id = (target_slot_ << 32) | static_cast<uint32_t>(i);
            //     rep_wr.sg_list = &replication_sge;
            //     rep_wr.num_sge = 1;
            //     rep_wr.opcode = IBV_WR_RDMA_WRITE;
            //     rep_wr.send_flags = IBV_SEND_SIGNALED;
            //     rep_wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, target_slot_);
            //     rep_wr.wr.rdma.rkey = conns[i].rkey;
            //
            //     if (ibv_post_send(conns[i].id->qp, &rep_wr, &bad)) {
            //         throw std::runtime_error("Replication post failed");
            //     }
            // }
            //
            // // Wait for quorum acks on replication
            // int acks = 0;
            // ibv_wc wc_batch[32];
            // while (acks < static_cast<int>(QUORUM)) {
            //     int pulled = ibv_poll_cq(cq, 32, wc_batch);
            //     for (int j = 0; j < pulled; ++j) {
            //         if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
            //         if ((wc_batch[j].wr_id >> 32) == target_slot_) acks++;
            //     }
            // }

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

void CasStrategy::release(Client& client, int op_id, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    const auto& conns = client.connections();
    const auto* mr = client.mr();
    auto* cq = client.cq();

    advance_frontier(state, target_slot_, target_slot_ + 1, lock_id, op_id, conns, mr, cq);
    target_slot_ += 2;
}

void CasStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // CAS: no reset needed — target_slot_ tracks state across calls
}