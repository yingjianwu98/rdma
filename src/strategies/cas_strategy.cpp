#include "rdma/strategies/cas_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

// ─── advance_frontier: CAS frontier from old_val → new_val, fire-and-forget ───

namespace wrid {
    // Layout (64 bits total):
    //
    // [ gen:16 ][ op:32 ][ type:8 ][ index:8 ]
    //
    // - gen   : optional generation / epoch / client-local rollover tag
    // - op    : your operation id
    // - type  : acquire / advance / replication / etc.
    // - index : connection index or small sub-id

    constexpr uint64_t INDEX_BITS = 8;
    constexpr uint64_t TYPE_BITS  = 8;
    constexpr uint64_t OP_BITS    = 32;
    constexpr uint64_t GEN_BITS   = 16;

    static_assert(INDEX_BITS + TYPE_BITS + OP_BITS + GEN_BITS == 64);

    constexpr uint64_t INDEX_SHIFT = 0;
    constexpr uint64_t TYPE_SHIFT  = INDEX_SHIFT + INDEX_BITS;
    constexpr uint64_t OP_SHIFT    = TYPE_SHIFT  + TYPE_BITS;
    constexpr uint64_t GEN_SHIFT   = OP_SHIFT    + OP_BITS;

    constexpr uint64_t bitmask(uint64_t bits) {
        return (1ull << bits) - 1;
    }

    constexpr uint64_t INDEX_MASK = bitmask(INDEX_BITS) << INDEX_SHIFT;
    constexpr uint64_t TYPE_MASK  = bitmask(TYPE_BITS)  << TYPE_SHIFT;
    constexpr uint64_t OP_MASK    = bitmask(OP_BITS)    << OP_SHIFT;
    constexpr uint64_t GEN_MASK   = bitmask(GEN_BITS)   << GEN_SHIFT;

    enum class Type : uint8_t {
        AcquireCas      = 1,
        AdvanceFrontier = 2,
        Replication     = 3,
        Release         = 4,
    };

    constexpr uint64_t make(uint16_t gen, uint32_t op, Type type, uint8_t index) {
        return (static_cast<uint64_t>(gen)   << GEN_SHIFT)  |
               (static_cast<uint64_t>(op)    << OP_SHIFT)   |
               (static_cast<uint64_t>(type)  << TYPE_SHIFT) |
               (static_cast<uint64_t>(index) << INDEX_SHIFT);
    }

    constexpr uint16_t gen(uint64_t wr_id) {
        return static_cast<uint16_t>((wr_id & GEN_MASK) >> GEN_SHIFT);
    }

    constexpr uint32_t op(uint64_t wr_id) {
        return static_cast<uint32_t>((wr_id & OP_MASK) >> OP_SHIFT);
    }

    constexpr Type type(uint64_t wr_id) {
        return static_cast<Type>((wr_id & TYPE_MASK) >> TYPE_SHIFT);
    }

    constexpr uint8_t index(uint64_t wr_id) {
        return static_cast<uint8_t>((wr_id & INDEX_MASK) >> INDEX_SHIFT);
    }

    constexpr bool matches(uint64_t wr_id, uint16_t expected_gen, uint32_t expected_op, Type expected_type) {
        return gen(wr_id) == expected_gen &&
               op(wr_id) == expected_op &&
               type(wr_id) == expected_type;
    }

    constexpr bool matches(uint64_t wr_id, uint32_t expected_op, Type expected_type) {
        return op(wr_id) == expected_op &&
               type(wr_id) == expected_type;
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
        wr.wr_id = wrid::make(
            0,
            static_cast<uint32_t>(op_id),
            wrid::Type::AcquireCas,
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
            std::cout << "Trying to get completion for lock: " << op_id << " lockid=" << lock_id << '\n';
            const int n = ibv_poll_cq(cq, 16, wcs);
            if (n < 0) throw std::runtime_error("Poll CQ failed");
            if (n == 0) continue;
            for (int i = 0; i < n; ++i) {
                const ibv_wc& c = wcs[i];
                if (c.status != IBV_WC_SUCCESS) {
                    std::cerr << "CQE error: wr_id=0x" << std::hex << c.wr_id << std::dec << " status=" << c.status << '\n';
                    continue;
                }

                if (wrid::matches(c.wr_id, static_cast<uint32_t>(op_id), wrid::Type::AcquireCas)) {
                    got_acquire = true;
                    break;
                }

                if (wrid::type(c.wr_id) == wrid::Type::AdvanceFrontier) continue;

                std::cerr << "Unexpected CQE: wr_id=0x" << std::hex << c.wr_id
                  << std::dec
                  << " gen=" << wrid::gen(c.wr_id)
                  << " op=" << wrid::op(c.wr_id)
                  << " type=" << static_cast<int>(wrid::type(c.wr_id))
                  << " index=" << static_cast<int>(wrid::index(c.wr_id))
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
        wr.wr_id = wrid::make(
           0,
           static_cast<uint32_t>(op_id),
           wrid::Type::AdvanceFrontier,
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