#include "rdma/local_ticket_faa_lock_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kConnBits = 8;
constexpr uint64_t kRoundBits = 8;
constexpr uint64_t kPhaseBits = 8;
constexpr uint64_t kSlotBits = 16;
constexpr uint64_t kConnShift = 0;
constexpr uint64_t kRoundShift = kConnShift + kConnBits;
constexpr uint64_t kPhaseShift = kRoundShift + kRoundBits;
constexpr uint64_t kSlotShift = kPhaseShift + kPhaseBits;
constexpr uint64_t kGenerationShift = kSlotShift + kSlotBits;
constexpr uint64_t kConnMask = (1ULL << kConnBits) - 1;
constexpr uint64_t kRoundMask = (1ULL << kRoundBits) - 1;
constexpr uint64_t kPhaseMask = (1ULL << kPhaseBits) - 1;
constexpr uint64_t kSlotMask = (1ULL << kSlotBits) - 1;
constexpr uint64_t kGenerationMask = (1ULL << kSlotShift) - 1;

enum class LocalTicketFaaPhase : uint8_t {
    idle = 0,
    replicate_ticket = 1,
    wait_turn_local = 2,
};

struct RegisteredLocalTicketFaaBuffers {
    uint64_t* replicate_results = nullptr;
};

struct LocalTicketFaaOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t req_id = 0;
    uint8_t round = 0;
    uint64_t ticket = 0;
    uint64_t waiter_id = 0;
    LocalTicketFaaPhase phase = LocalTicketFaaPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;
    uint32_t quorum_hits = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

struct alignas(64) LocalTicketTurn {
    std::atomic<uint64_t> ticket{0};
    std::atomic<uint64_t> turn{0};
};

std::array<LocalTicketTurn, MAX_LOCKS> g_local_ticket_turns{};

uint64_t encode_waiter(const uint16_t client_id, const uint16_t op_slot, const uint32_t req_id) {
    return (static_cast<uint64_t>(client_id) << 47)
         | (static_cast<uint64_t>(op_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

uint64_t encode_wr_id(const LocalTicketFaaOpCtx& op, const LocalTicketFaaPhase phase, const uint8_t conn_index) {
    return ((static_cast<uint64_t>(op.generation) & kGenerationMask) << kGenerationShift)
         | ((static_cast<uint64_t>(op.slot) & kSlotMask) << kSlotShift)
         | ((static_cast<uint64_t>(phase) & kPhaseMask) << kPhaseShift)
         | ((static_cast<uint64_t>(op.round) & kRoundMask) << kRoundShift)
         | ((static_cast<uint64_t>(conn_index) & kConnMask) << kConnShift);
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> kGenerationShift);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> kSlotShift) & kSlotMask);
}

LocalTicketFaaPhase wr_phase(const uint64_t wr_id) {
    return static_cast<LocalTicketFaaPhase>((wr_id >> kPhaseShift) & kPhaseMask);
}

uint8_t wr_round(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kRoundShift) & kRoundMask);
}

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kConnShift) & kConnMask);
}

size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

uint64_t* row_ptr(uint64_t* base, const uint32_t slot, const size_t replica_count) {
    return base + row_offset(slot, replica_count);
}

RegisteredLocalTicketFaaBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    const size_t matrix_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    if (matrix_bytes > buffer_size) {
        throw std::runtime_error("local_ticket_faa pipeline: registered client buffer too small");
    }

    RegisteredLocalTicketFaaBuffers buffers{};
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base);
    return buffers;
}

void post_replicate_ticket(
    Client& client,
    LocalTicketFaaOpCtx& op,
    const RegisteredLocalTicketFaaBuffers& buffers,
    const bool replicate_with_cas
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    op.round++;
    op.phase = LocalTicketFaaPhase::replicate_ticket;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());
    op.quorum_hits = 0;

    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = replicate_with_cas ? (EMPTY_SLOT - 1) : op.waiter_id;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, LocalTicketFaaPhase::replicate_ticket, static_cast<uint8_t>(i));
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        if (replicate_with_cas) {
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.ticket);
            wr.wr.atomic.rkey = conns[i].rkey;
            wr.wr.atomic.compare_add = EMPTY_SLOT;
            wr.wr.atomic.swap = op.waiter_id;
        } else {
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.ticket);
            wr.wr.rdma.rkey = conns[i].rkey;
        }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("local_ticket_faa pipeline: replicate ticket post failed");
        }
    }
}

} // namespace

LocalTicketFaaLockPipelineConfig load_local_ticket_faa_lock_pipeline_config() {
    LocalTicketFaaLockPipelineConfig config{};
    config.active_window = std::max<size_t>(1, LOCAL_TICKET_FAA_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, LOCAL_TICKET_FAA_CQ_BATCH);
    config.zipf_skew = LOCAL_TICKET_FAA_ZIPF_SKEW;
    config.replicate_with_cas = LOCAL_TICKET_FAA_REPLICATE_USE_CAS;
    return config;
}

size_t local_ticket_faa_lock_pipeline_client_buffer_size(const LocalTicketFaaLockPipelineConfig& config) {
    const size_t matrix_bytes = align_up(config.active_window * CLUSTER_NODES.size() * sizeof(uint64_t), 64);
    return align_up(matrix_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_local_ticket_faa_lock_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const LocalTicketFaaLockPipelineConfig& config
) {
    if (TOTAL_MACHINES != 1) {
        throw std::runtime_error("local_ticket_faa pipeline requires TOTAL_MACHINES == 1");
    }

    const auto& conns = client.connections();
    if (conns.empty()) throw std::runtime_error("local_ticket_faa pipeline: no server connections");
    if (config.active_window > 0x7FFFu) throw std::runtime_error("local_ticket_faa pipeline: active window too large");

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<LocalTicketFaaOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto submit_op = [&](const uint32_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = slot;
        op.lock_id = picker.next();
        op.req_id = next_req_id++;
        op.round = 0;
        op.ticket = g_local_ticket_turns[op.lock_id].ticket.fetch_add(1, std::memory_order_seq_cst);
        op.waiter_id = encode_waiter(static_cast<uint16_t>(client.id()), static_cast<uint16_t>(slot), op.req_id);
        op.phase = LocalTicketFaaPhase::idle;
        op.responses = 0;
        op.response_target = 0;
        op.quorum_hits = 0;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        post_replicate_ticket(client, op, buffers, config.replicate_with_cas);
        submitted++;
        active++;
    };

    auto finish_op = [&](LocalTicketFaaOpCtx& op, const uint32_t slot) {
        latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - op.started_at).count();
        g_local_ticket_turns[op.lock_id].turn.fetch_add(1, std::memory_order_seq_cst);
        lock_counts[op.lock_id]++;
        op.active = false;
        op.phase = LocalTicketFaaPhase::idle;
        completed++;
        active--;

        if (submitted < NUM_OPS_PER_CLIENT) {
            submit_op(slot);
        }
    };

    auto try_acquire = [&](LocalTicketFaaOpCtx& op, const uint32_t slot) {
        if (g_local_ticket_turns[op.lock_id].turn.load(std::memory_order_seq_cst) == op.ticket) {
            finish_op(op, slot);
        } else {
            op.phase = LocalTicketFaaPhase::wait_turn_local;
        }
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(static_cast<uint32_t>(active));
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        for (uint32_t slot = 0; slot < ops.size(); ++slot) {
            auto& op = ops[slot];
            if (!op.active || op.phase != LocalTicketFaaPhase::wait_turn_local) continue;
            try_acquire(op, slot);
        }

        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) throw std::runtime_error("local_ticket_faa pipeline: CQ poll failed");
        if (polled == 0) continue;

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "local_ticket_faa pipeline: completion failed status=" + std::to_string(wc.status)
                    + " vendor=" + std::to_string(wc.vendor_err));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) continue;
            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) continue;
            const LocalTicketFaaPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) continue;
            if (wr_round(wc.wr_id) != op.round) continue;

            op.responses++;

            if (phase == LocalTicketFaaPhase::replicate_ticket) {
                auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (!config.replicate_with_cas || results[idx] == EMPTY_SLOT) {
                    op.quorum_hits++;
                }
                const uint32_t remaining = op.response_target - op.responses;
                if (op.quorum_hits >= QUORUM) {
                    try_acquire(op, slot);
                    continue;
                }
                if (op.quorum_hits + remaining < QUORUM) {
                    throw std::runtime_error("local_ticket_faa pipeline: ticket replication cannot reach quorum");
                }
                if (op.responses < op.response_target) continue;
                if (op.quorum_hits < QUORUM) {
                    throw std::runtime_error("local_ticket_faa pipeline: ticket replication failed to reach quorum");
                }
                try_acquire(op, slot);
            }
        }
    }
}
