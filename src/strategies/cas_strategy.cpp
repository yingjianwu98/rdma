#include "rdma/strategies/cas_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

// ─── advance_frontier: CAS frontier from old_val → new_val, fire-and-forget ───

#pragma once

#include <cstdint>
#include <stdexcept>

namespace wrid {
    // Layout (64 bits total):
    //
    // [ seq:32 ][ conn_idx:8 ][ op_type:8 ][ lock_id:16 ]
    //
    // lock_id  : up to 65535 locks
    // op_type  : up to 255 op kinds
    // conn_idx : up to 255 connections
    // seq      : 32-bit sequence number

    constexpr uint64_t LOCK_BITS = 16;
    constexpr uint64_t OP_BITS   = 8;
    constexpr uint64_t CONN_BITS = 8;
    constexpr uint64_t SEQ_BITS  = 32;

    static_assert(LOCK_BITS + OP_BITS + CONN_BITS + SEQ_BITS == 64);

    constexpr uint64_t LOCK_SHIFT = 0;
    constexpr uint64_t OP_SHIFT   = LOCK_SHIFT + LOCK_BITS;
    constexpr uint64_t CONN_SHIFT = OP_SHIFT + OP_BITS;
    constexpr uint64_t SEQ_SHIFT  = CONN_SHIFT + CONN_BITS;

    constexpr uint64_t LOCK_MASK = (1ULL << LOCK_BITS) - 1;
    constexpr uint64_t OP_MASK   = (1ULL << OP_BITS) - 1;
    constexpr uint64_t CONN_MASK = (1ULL << CONN_BITS) - 1;
    constexpr uint64_t SEQ_MASK  = (1ULL << SEQ_BITS) - 1;

    enum class OpType : uint8_t {
        Acquire = 1,
        Release = 2,
        Advance = 3,
        Read    = 4,
        Write   = 5,
        Cas     = 6,
        Faa     = 7
    };

    struct Decoded {
        uint16_t lock_id;
        uint8_t  op_type;
        uint8_t  conn_idx;
        uint32_t seq;
    };

    inline uint64_t pack(uint16_t lock_id, uint8_t op_type, uint32_t seq, uint8_t conn_idx) {
        return (static_cast<uint64_t>(lock_id)  << LOCK_SHIFT) |
               (static_cast<uint64_t>(op_type)  << OP_SHIFT)   |
               (static_cast<uint64_t>(conn_idx) << CONN_SHIFT) |
               (static_cast<uint64_t>(seq)      << SEQ_SHIFT);
    }

    inline uint64_t pack(uint16_t lock_id, OpType op_type, uint32_t seq, uint8_t conn_idx) {
        return pack(lock_id, static_cast<uint8_t>(op_type), seq, conn_idx);
    }

    inline uint16_t lock_id(uint64_t wr_id) {
        return static_cast<uint16_t>((wr_id >> LOCK_SHIFT) & LOCK_MASK);
    }

    inline uint8_t op_type(uint64_t wr_id) {
        return static_cast<uint8_t>((wr_id >> OP_SHIFT) & OP_MASK);
    }

    inline uint8_t conn_idx(uint64_t wr_id) {
        return static_cast<uint8_t>((wr_id >> CONN_SHIFT) & CONN_MASK);
    }

    inline uint32_t seq(uint64_t wr_id) {
        return static_cast<uint32_t>((wr_id >> SEQ_SHIFT) & SEQ_MASK);
    }

    inline Decoded decode(uint64_t wr_id) {
        return Decoded{
            .lock_id = lock_id(wr_id),
            .op_type = op_type(wr_id),
            .conn_idx = conn_idx(wr_id),
            .seq = seq(wr_id)
        };
    }
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
        std::cout << "Trying to get next lock: " << op_id << " lockid=" << lock_id << '\n';
        const uint64_t expected = target_slot_ - 1;

        state->cas_results[0] = 0xFEFEFEFEFEFEFEFE;

        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->cas_results[0]),
            .length = 8,
            .lkey = mr->lkey
        };

        ibv_send_wr wr{}, *bad_wr;
        wr.wr_id = wrid::pack(
            static_cast<uint16_t>(lock_id),
            static_cast<uint8_t>(wrid::OpType::Acquire),
            static_cast<uint32_t>(op_id),
            static_cast<uint8_t>(node)
        );
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
            std::cout << "Trying to get completion for lock: " << op_id
                      << " lockid=" << lock_id << '\n';

            const int n = ibv_poll_cq(cq, 16, wcs);
            if (n < 0) throw std::runtime_error("Poll CQ failed");
            if (n == 0) continue;

            for (int i = 0; i < n; ++i) {
                const ibv_wc& c = wcs[i];

                if (c.status != IBV_WC_SUCCESS) {
                    std::cerr << "CQE error: wr_id=0x" << std::hex << c.wr_id
                              << std::dec << " status=" << c.status << '\n';
                    continue;
                }

                auto d = wrid::decode(c.wr_id);

                std::cout << "CQE: lock=" << d.lock_id
                          << " seq=" << d.seq
                          << " op=" << static_cast<int>(d.op_type)
                          << " conn=" << static_cast<int>(d.conn_idx)
                          << '\n';

                if (d.lock_id == static_cast<uint16_t>(lock_id) &&
                    d.seq == static_cast<uint32_t>(op_id) &&
                    d.op_type == static_cast<uint8_t>(wrid::OpType::Acquire)) {
                    got_acquire = true;
                    break;
                    }

                if (d.op_type == static_cast<uint8_t>(wrid::OpType::Advance)) {
                    continue;
                }

                std::cerr << "Unexpected CQE: wr_id=0x" << std::hex << c.wr_id
                          << std::dec
                          << " lock=" << d.lock_id
                          << " seq=" << d.seq
                          << " op=" << static_cast<int>(d.op_type)
                          << " conn=" << static_cast<int>(d.conn_idx)
                          << '\n';
            }
        }

        const uint64_t result = state->cas_results[0];

        // ── Step 3: Did we win? ──

        if (result == expected) {
            std::cout << "We got the next lock: " << result << " lockid=" << lock_id << '\n';
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

        std::cout << "We failed to get the next lock: " << result << " lockid=" << lock_id << '\n';


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
        wr.wr_id = wrid::pack(
            static_cast<uint16_t>(lock_id),
            static_cast<uint8_t>(wrid::OpType::Advance),
            static_cast<uint32_t>(op_id),
            static_cast<uint8_t>(i)
        );
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

    std::cout << "Advanced next to: " << new_val << " lockid=" << lock_id << '\n';

    target_slot_ += 2;
}

void CasStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // CAS: no reset needed — target_slot_ tracks state across calls
}