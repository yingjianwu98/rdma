#include "rdma/strategies/tas_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

// ─── Protocol constants ───

static constexpr uint64_t DISCOVER_FRONTIER_ID = 0xABC000;
static constexpr uint64_t ADVANCE_FRONTIER_ID = 0x111000;
static constexpr uint64_t COMMIT_ID = 0xDEF000;
static constexpr uint64_t OP_MASK = 0xFFF000;
static constexpr uint64_t LEARN_ID = 0x999000;
static constexpr uint64_t RESET_ID = 0x777000;
static constexpr uint64_t SENTINEL = 0xFEFEFEFEFEFEFEFE;

static uint64_t discover_frontier(
    LocalState* state, const int op, uint32_t lock_id,
    const std::vector<RemoteNode>& conns, ibv_cq* cq, const ibv_mr* mr
) {
    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->frontier_values[i]),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op) << 32) | DISCOVER_FRONTIER_ID | i;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_control_offset(lock_id);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (int ret = ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            fprintf(stderr, "Post failed: %s (errno: %d) | Addr: %p | LKey: %u\n",
                    strerror(ret), ret, (void*)sge.addr, sge.lkey);
            throw std::runtime_error("Failed to discover frontier");
        }
    }

    int received = 0;
    uint64_t max_v = 0;
    ibv_wc wcs[MAX_REPLICAS];

    while (received < static_cast<int>(QUORUM)) {
        int n = ibv_poll_cq(cq, MAX_REPLICAS, wcs);
        for (int i = 0; i < n; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "discover_frontier WC error: " + std::to_string(wcs[i].status));
            }
            bool is_current = (wcs[i].wr_id >> 32) == static_cast<uint64_t>(op);
            bool is_discover = (wcs[i].wr_id & OP_MASK) == DISCOVER_FRONTIER_ID;
            if (is_current && is_discover) {
                max_v = std::max(max_v, state->frontier_values[wcs[i].wr_id & 0xFFF]);
                received++;
            }
        }
    }
    return max_v;
}

static int commit_cas(
    LocalState* state, const int op, const uint64_t slot, const uint32_t cid,
    uint32_t lock_id,
    const std::vector<RemoteNode>& conns, ibv_cq* cq, const ibv_mr* mr
) {
    std::fill_n(state->cas_results, conns.size(), SENTINEL);

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->cas_results[i]),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op) << 32) | COMMIT_ID | i;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, slot);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = EMPTY_SLOT;
        wr.wr.atomic.swap = static_cast<uint64_t>(cid);

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("commit_cas post failed");
        }
    }

    int responses = 0, wins = 0;
    ibv_wc wc{};
    while (responses < static_cast<int>(QUORUM)) {
        if (ibv_poll_cq(cq, 1, &wc) > 0) {
            bool is_current = (wc.wr_id >> 32) == static_cast<uint64_t>(op);
            bool is_cas = (wc.wr_id & OP_MASK) == COMMIT_ID;
            if (is_current && is_cas) {
                if (state->cas_results[wc.wr_id & 0xFFF] == EMPTY_SLOT) wins++;
                responses++;
            }
        }
    }
    return wins;
}

static bool learn_majority(
    LocalState* state, const int op, const uint64_t slot, const uint32_t client_id,
    uint32_t lock_id,
    const std::vector<RemoteNode>& conns, ibv_cq* cq, const ibv_mr* mr
) {
    std::fill_n(state->learn_results, conns.size(), SENTINEL);

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op) << 32) | LEARN_ID | i;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("learn_majority post failed");
        }
    }

    int reads_done = 0;
    ibv_wc wc{};
    while (reads_done < static_cast<int>(conns.size())) {
        if (ibv_poll_cq(cq, 1, &wc) > 0) {
            if ((wc.wr_id >> 32) == static_cast<uint64_t>(op) &&
                (wc.wr_id & OP_MASK) == LEARN_ID) {
                reads_done++;
            }
        }
    }

    uint64_t quorum_winner = EMPTY_SLOT;
    uint64_t lowest_id = EMPTY_SLOT;
    bool found_quorum = false;
    bool found_any = false;

    for (size_t i = 0; i < conns.size(); ++i) {
        const uint64_t val = state->learn_results[i];
        if (val == SENTINEL || val == EMPTY_SLOT) continue;

        found_any = true;
        if (val < lowest_id) lowest_id = val;

        int count = 0;
        for (size_t j = 0; j < conns.size(); ++j) {
            if (state->learn_results[j] == val) count++;
        }
        if (count >= static_cast<int>(QUORUM) && val < quorum_winner) {
            quorum_winner = val;
            found_quorum = true;
        }
    }

    if (found_quorum) return quorum_winner == static_cast<uint64_t>(client_id);
    if (found_any) return lowest_id == static_cast<uint64_t>(client_id);
    return false;
}

static void advance_frontier(
    LocalState* state, const uint64_t slot, uint32_t lock_id,
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
        wr.wr_id = ADVANCE_FRONTIER_ID | i;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_control_offset(lock_id);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("advance_frontier post failed");
        }
    }
}

static void synra_reset(
    LocalState* state, const uint32_t client_id, uint32_t lock_id,
    const std::vector<RemoteNode>& conns, ibv_cq* cq, const ibv_mr* mr
) {
    uint64_t current_idx = discover_frontier(state, 0, lock_id, conns, cq, mr);

    if (current_idx % 2 == 0) return; // already clean

    uint64_t next_slot = current_idx + 1;
    state->metadata = static_cast<uint64_t>(client_id);

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{
            .addr = reinterpret_cast<uintptr_t>(&state->metadata),
            .length = 8,
            .lkey = mr->lkey
        };
        ibv_send_wr wr{}, *bad;
        wr.wr_id = RESET_ID | i;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(lock_id, next_slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
            throw std::runtime_error("synra_reset post failed");
        }
    }

    int responses = 0;
    ibv_wc wc{};
    while (responses < static_cast<int>(conns.size())) {
        if (ibv_poll_cq(cq, 1, &wc) > 0) {
            if ((wc.wr_id & OP_MASK) == RESET_ID) responses++;
        }
    }

    advance_frontier(state, next_slot, lock_id, conns, mr);
}

uint64_t TasStrategy::acquire(Client& client, const int op_id, uint32_t lock_id) {
    auto* state = static_cast<LocalState*>(client.buffer());
    auto* cq = client.cq();
    const auto* mr = client.mr();
    const auto& conns = client.connections();

    while (true) {
        const uint64_t max_val = discover_frontier(state, op_id, lock_id, conns, cq, mr);

        if (max_val % 2 != 0) continue;
        const uint64_t next_slot = max_val + 1;

        if (commit_cas(state, op_id, next_slot, client.id(), lock_id, conns, cq, mr) >= static_cast<int>(QUORUM)) {
            advance_frontier(state, next_slot, lock_id, conns, mr);
            return next_slot;
        }

        if (learn_majority(state, op_id, next_slot, client.id(), lock_id, conns, cq, mr)) {
            advance_frontier(state, next_slot, lock_id, conns, mr);
            return next_slot;
        }
    }
}

void TasStrategy::release(Client& client, int /*op_id*/, uint32_t lock_id) {
    // synra_reset(
    //         static_cast<LocalState*>(client.buffer()),
    //         client.id(), lock_id,
    //         client.connections(),
    //         client.cq(),
    //         client.mr()
    //     );
}

void TasStrategy::cleanup(Client& client, int /*op_id*/, uint32_t lock_id) {
    synra_reset(
        static_cast<LocalState*>(client.buffer()),
        client.id(), lock_id,
        client.connections(),
        client.cq(),
        client.mr()
    );
}
