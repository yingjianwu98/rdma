#include "rdma/strategies/faa_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

constexpr uint64_t FAA_MAGIC_ID = 0xFFFFFFFFFFFFFFFF;

uint64_t FaaStrategy::acquire(Client& client, int /*op_id*/, uint32_t /*lock_id*/) {
    auto* cq  = client.cq();
    auto* mr  = client.mr();
    const auto& conns = client.connections();

    if (conns.empty()) throw std::runtime_error("FaaStrategy: no connections");

    const auto& sequencer = conns[0];

    ibv_sge ticket_sge{};
    ticket_sge.addr   = reinterpret_cast<uintptr_t>(mr->addr);
    ticket_sge.length = 8;
    ticket_sge.lkey   = mr->lkey;

    ibv_send_wr faa_wr{}, *bad_faa = nullptr;
    faa_wr.wr_id                = FAA_MAGIC_ID;
    faa_wr.sg_list              = &ticket_sge;
    faa_wr.num_sge              = 1;
    faa_wr.opcode               = IBV_WR_ATOMIC_FETCH_AND_ADD;
    faa_wr.send_flags           = IBV_SEND_SIGNALED;
    faa_wr.wr.atomic.remote_addr = sequencer.addr + (ALIGNED_SIZE - 8);
    faa_wr.wr.atomic.rkey       = sequencer.rkey;
    faa_wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(sequencer.id->qp, &faa_wr, &bad_faa)) {
        throw std::runtime_error("FAA post failed");
    }

    // ── Step 2: Poll until we get our ticket back ──

    ibv_wc wc{};
    while (true) {
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n < 0) throw std::runtime_error("Poll CQ failed");
        if (n > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "FAA failed with status: " + std::to_string(wc.status));
            }
            if (wc.wr_id == FAA_MAGIC_ID) break;
        }
    }

    std::atomic_thread_fence(std::memory_order_acquire);
    const uint64_t my_ticket  = *static_cast<uint64_t*>(mr->addr);
    const uint64_t log_offset = my_ticket * 8;

    // ── Step 3: Replicate to all nodes ──

    uint64_t* local_data = static_cast<uint64_t*>(mr->addr) + 1;
    *local_data = 0xDEADBEEF;

    ibv_sge write_sge{};
    write_sge.addr   = reinterpret_cast<uintptr_t>(local_data);
    write_sge.length = 8;
    write_sge.lkey   = mr->lkey;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id      = (my_ticket << 32) | static_cast<uint32_t>(i);
        wr.sg_list    = &write_sge;
        wr.num_sge    = 1;
        wr.opcode     = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + log_offset;
        wr.wr.rdma.rkey        = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("Replication post failed");
        }
    }

    // ── Step 4: Wait for quorum acks ──

    int acks = 0;
    ibv_wc wc_batch[32];
    while (acks < static_cast<int>(QUORUM)) {
        int pulled = ibv_poll_cq(cq, 32, wc_batch);
        for (int j = 0; j < pulled; ++j) {
            if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
            if ((wc_batch[j].wr_id >> 32) == my_ticket) {
                acks++;
            }
        }
    }

    return my_ticket;
}

void FaaStrategy::release(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // FAA: ticket is consumed, no explicit release needed
}

void FaaStrategy::cleanup(Client& /*client*/, int /*op_id*/, uint32_t /*lock_id*/) {
    // FAA: no cleanup needed
}