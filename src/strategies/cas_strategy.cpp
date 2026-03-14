#include "rdma/strategies/cas_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

// ─── advance_frontier: CAS frontier from old_val → new_val, fire-and-forget ───

#include <cstdint>
#include <sstream>
#include <stdexcept>

static constexpr uint64_t TAG_ACQUIRE = 0x1;
static constexpr uint64_t TAG_ADVANCE = 0x2;

static inline uint64_t make_wr_id(int op_id, uint64_t tag, size_t conn) {
    return (static_cast<uint64_t>(op_id) << 32) | (tag << 16) | conn;
}

static inline uint64_t wr_tag(uint64_t wr_id) {
    return (wr_id >> 16) & 0xFFFF;
}

static inline uint32_t wr_seq(uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

uint64_t CasStrategy::acquire(Client& client, int op_id, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    auto* mr = client.mr();
    const auto& conns = client.connections();

     const size_t node = lock_id % conns.size();
    // std::cout << "Reaching out to node: " << node << std::endl;

    ibv_wc wcs[16];

    while (true) {
        // std::cout << "Trying to get next lock: " << op_id << " lockid=" << lock_id << '\n';
        const uint64_t expected = target_slot_ - 1;

        state->cas_results[node] = 0xFEFEFEFEFEFEFEFE;

        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->cas_results[0]),
            .length = 8,
            .lkey = mr->lkey
        };

        ibv_send_wr wr{}, *bad_wr;
        wr.wr_id = make_wr_id(op_id, TAG_ACQUIRE, node);
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


        bool got_acquire = false;
        while (!got_acquire) {
            // std::cout << "Trying to get completion for lock: " << op_id
            //           << " lockid=" << lock_id << '\n';

            const int n = ibv_poll_cq(cq, 16, wcs);
            if (n < 0) throw std::runtime_error("Poll CQ failed");
            if (n == 0) continue;

            for (int i = 0; i < n; ++i) {
                if (const ibv_wc& c = wcs[i]; c.status != IBV_WC_SUCCESS) {
                    std::cerr << "CQE error: wr_id=0x" << std::hex << c.wr_id
                        << std::dec << " status=" << c.status << '\n';
                    continue;
                }

                if (wr_tag(wcs[i].wr_id) == TAG_ACQUIRE &&
                    wr_seq(wcs[i].wr_id) == static_cast<uint32_t>(op_id)) {
                    got_acquire = true;
                    break;
                }
            }
        }

        const uint64_t result = state->cas_results[node];

        // ── Step 3: Did we win? ──

        if (result == expected) {
            // std::cout << "We got the next lock: " << result << " lockid=" << lock_id << '\n';
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

        // std::cout << "We failed to get the next lock: " << result << " lockid=" << lock_id << '\n';


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

    const size_t node = lock_id % conns.size();
    const auto old_val = target_slot_;
    const auto new_val = old_val + 1;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->cas_results[i]),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = make_wr_id(op_id, TAG_ADVANCE, i);
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = ++signal_count_ % 100 == 0 ? IBV_SEND_SIGNALED : 0;
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

    // std::cout << "Advanced next to: " << new_val << " lockid=" << lock_id << '\n';

    target_slot_ += 2;
}

void CasStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // CAS: no reset needed — target_slot_ tracks state across calls
}