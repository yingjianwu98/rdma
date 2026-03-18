#include "rdma/pipelines/ticket_faa_lock_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
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
constexpr uint64_t TICKET_FAA_DONE_BIT = 1ULL << 63;
constexpr uint8_t kReleaseTurnConnIndex = 0xFF;

enum class TicketFaaPhase : uint8_t {
    idle = 0,
    faa_ticket = 1,
    replicate_ticket = 2,
    wait_turn_read = 3,
    wait_turn_spin = 4,
    release_parallel = 5,
};

struct RegisteredTicketFaaBuffers {
    uint64_t* ticket_results = nullptr;
    uint64_t* replicate_results = nullptr;
    uint64_t* turn_reads = nullptr;
    uint64_t* mark_done_values = nullptr;
    uint64_t* mark_done_results = nullptr;
    uint64_t* release_results = nullptr;
};

struct TicketFaaOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t owner_node = 0;
    uint32_t req_id = 0;
    uint8_t round = 0;
    uint64_t ticket = 0;
    uint64_t waiter_id = 0;
    uint64_t last_turn = 0;
    uint32_t turn_spin_remaining = 0;
    TicketFaaPhase phase = TicketFaaPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;
    uint32_t quorum_hits = 0;
    uint32_t release_mark_done_responses = 0;
    uint32_t release_mark_done_quorum = 0;
    bool release_turn_done = false;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t encode_waiter(const uint16_t client_id, const uint16_t op_slot, const uint32_t req_id) {
    return (static_cast<uint64_t>(client_id) << 47)
         | (static_cast<uint64_t>(op_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

uint64_t encode_wr_id(const TicketFaaOpCtx& op, const TicketFaaPhase phase, const uint8_t conn_index) {
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

TicketFaaPhase wr_phase(const uint64_t wr_id) {
    return static_cast<TicketFaaPhase>((wr_id >> kPhaseShift) & kPhaseMask);
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

uint64_t waiter_mark_done(const uint64_t waiter) {
    return waiter | TICKET_FAA_DONE_BIT;
}

void ensure_log_slot_in_bounds(const uint32_t lock_id, const uint64_t slot, const char* context) {
    if (slot >= MAX_LOG_PER_LOCK) {
        throw std::runtime_error(
            std::string("ticket_faa pipeline: log overflow during ") + context
            + " lock=" + std::to_string(lock_id)
            + " slot=" + std::to_string(slot)
            + " max_log_per_lock=" + std::to_string(MAX_LOG_PER_LOCK));
    }
}

uint32_t spin_budget_for_distance(const uint64_t distance) {
    if (distance <= 1) return TICKET_FAA_TURN_SPIN_VERY_NEAR;
    if (distance <= 2) return TICKET_FAA_TURN_SPIN_NEAR;
    if (distance <= 4) return TICKET_FAA_TURN_SPIN_MID;
    return TICKET_FAA_TURN_SPIN_FAR;
}

RegisteredTicketFaaBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    RegisteredTicketFaaBuffers buffers{};
    const size_t scalar_bytes = align_up(active_window * sizeof(uint64_t), 64);
    const size_t matrix_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);

    buffers.ticket_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.turn_reads = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.mark_done_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.mark_done_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.release_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("ticket_faa pipeline: registered client buffer too small");
    }
    return buffers;
}

void post_ticket_faa(Client& client, TicketFaaOpCtx& op, const RegisteredTicketFaaBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.ticket_results[op.slot];
    *result = 0;
    const auto& owner = conns[op.owner_node];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    op.round++;
    op.phase = TicketFaaPhase::faa_ticket;
    op.responses = 0;
    op.response_target = 1;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, TicketFaaPhase::faa_ticket, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.atomic.remote_addr = owner.addr + lock_control_offset(op.lock_id);
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("ticket_faa pipeline: FAA ticket post failed");
    }
}

void post_replicate_ticket(
    Client& client,
    TicketFaaOpCtx& op,
    const RegisteredTicketFaaBuffers& buffers,
    const bool replicate_with_cas
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    ensure_log_slot_in_bounds(op.lock_id, op.ticket, "replicate_ticket");

    op.round++;
    op.phase = TicketFaaPhase::replicate_ticket;
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
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::replicate_ticket, static_cast<uint8_t>(i));
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
            throw std::runtime_error("ticket_faa pipeline: replicate ticket post failed");
        }
    }
}

void post_turn_read(Client& client, TicketFaaOpCtx& op, const RegisteredTicketFaaBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.turn_reads[op.slot];
    *result = EMPTY_SLOT;
    const auto& owner = conns[op.owner_node];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    op.round++;
    op.phase = TicketFaaPhase::wait_turn_read;
    op.responses = 0;
    op.response_target = 1;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, TicketFaaPhase::wait_turn_read, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = owner.addr + lock_turn_offset(op.lock_id);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("ticket_faa pipeline: turn read post failed");
    }
}

void post_release_parallel(
    Client& client,
    TicketFaaOpCtx& op,
    const RegisteredTicketFaaBuffers& buffers,
    const bool replicate_with_cas
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    auto* mark_done_value = &buffers.mark_done_values[op.slot];
    auto* mark_done_results = row_ptr(buffers.mark_done_results, op.slot, conns.size());

    ensure_log_slot_in_bounds(op.lock_id, op.ticket, "release_parallel");

    *mark_done_value = waiter_mark_done(op.waiter_id);
    auto* turn_result = &buffers.release_results[op.slot];
    *turn_result = 0;

    ibv_sge turn_sge{};
    turn_sge.addr = reinterpret_cast<uintptr_t>(turn_result);
    turn_sge.length = sizeof(uint64_t);
    turn_sge.lkey = mr->lkey;

    op.round++;
    op.phase = TicketFaaPhase::release_parallel;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size() + 1);
    op.release_mark_done_responses = 0;
    op.release_mark_done_quorum = 0;
    op.release_turn_done = false;

    for (size_t i = 0; i < conns.size(); ++i) {
        mark_done_results[i] = 0;

        ibv_sge mark_done_sge{};
        mark_done_sge.length = sizeof(uint64_t);
        mark_done_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::release_parallel, static_cast<uint8_t>(i));
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &mark_done_sge;
        wr.num_sge = 1;
        if (replicate_with_cas) {
            mark_done_sge.addr = reinterpret_cast<uintptr_t>(&mark_done_results[i]);
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.ticket);
            wr.wr.atomic.rkey = conns[i].rkey;
            wr.wr.atomic.compare_add = op.waiter_id;
            wr.wr.atomic.swap = waiter_mark_done(op.waiter_id);
        } else {
            mark_done_sge.addr = reinterpret_cast<uintptr_t>(mark_done_value);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.ticket);
            wr.wr.rdma.rkey = conns[i].rkey;
        }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: mark done post failed");
        }
    }

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, TicketFaaPhase::release_parallel, kReleaseTurnConnIndex);
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &turn_sge;
    wr.num_sge = 1;
    wr.wr.atomic.remote_addr = owner.addr + lock_turn_offset(op.lock_id);
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("ticket_faa pipeline: release turn post failed");
    }
}

} // namespace

TicketFaaLockPipelineConfig load_ticket_faa_lock_pipeline_config() {
    TicketFaaLockPipelineConfig config{};
    config.active_window = std::max<size_t>(1, TICKET_FAA_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, TICKET_FAA_CQ_BATCH);
    config.zipf_skew = TICKET_FAA_ZIPF_SKEW;
    config.replicate_with_cas = TICKET_FAA_REPLICATE_USE_CAS;
    config.shard_owner = TICKET_FAA_SHARD_OWNER;
    return config;
}

size_t ticket_faa_lock_pipeline_client_buffer_size(const TicketFaaLockPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t scalar_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t matrix_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    return align_up(scalar_bytes * 4 + matrix_bytes * 2 + PAGE_SIZE, PAGE_SIZE);
}

void run_ticket_faa_lock_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TicketFaaLockPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) throw std::runtime_error("ticket_faa pipeline: no server connections");
    if (config.active_window > 0x7FFFu) throw std::runtime_error("ticket_faa pipeline: active window too large");

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<TicketFaaOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto begin_release = [&](TicketFaaOpCtx& op) {
        latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - op.started_at).count();
        post_release_parallel(client, op, buffers, config.replicate_with_cas);
    };

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = picker.next();
        op.owner_node = config.shard_owner ? (op.lock_id % static_cast<uint32_t>(conns.size())) : 0;
        op.req_id = next_req_id++;
        op.round = 0;
        op.ticket = 0;
        op.waiter_id = encode_waiter(static_cast<uint16_t>(client.id()), static_cast<uint16_t>(slot), op.req_id);
        op.last_turn = 0;
        op.turn_spin_remaining = 0;
        op.phase = TicketFaaPhase::idle;
        op.responses = 0;
        op.response_target = 0;
        op.quorum_hits = 0;
        op.release_mark_done_responses = 0;
        op.release_mark_done_quorum = 0;
        op.release_turn_done = false;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        post_ticket_faa(client, op, buffers);
        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        for (auto& op : ops) {
            if (!op.active || op.phase != TicketFaaPhase::wait_turn_spin) continue;
            if (op.turn_spin_remaining > 0) {
                --op.turn_spin_remaining;
            }
            if (op.turn_spin_remaining == 0) {
                post_turn_read(client, op, buffers);
            }
        }

        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) throw std::runtime_error("ticket_faa pipeline: CQ poll failed");
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "ticket_faa pipeline: completion failed status=" + std::to_string(wc.status)
                    + " vendor=" + std::to_string(wc.vendor_err));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) continue;
            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) continue;
            const TicketFaaPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) continue;
            if (wr_round(wc.wr_id) != op.round) continue;

            if (phase == TicketFaaPhase::faa_ticket) {
                op.ticket = buffers.ticket_results[op.slot];
                post_replicate_ticket(client, op, buffers, config.replicate_with_cas);
                continue;
            }

            op.responses++;

            if (phase == TicketFaaPhase::replicate_ticket) {
                auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (!config.replicate_with_cas || results[idx] == EMPTY_SLOT) {
                    op.quorum_hits++;
                }
                const uint32_t remaining = op.response_target - op.responses;
                if (op.quorum_hits >= QUORUM) {
                    if (op.ticket == 0) {
                        begin_release(op);
                    } else {
                        post_turn_read(client, op, buffers);
                    }
                    continue;
                }
                if (op.quorum_hits + remaining < QUORUM) {
                    throw std::runtime_error("ticket_faa pipeline: ticket replication cannot reach quorum");
                }
                if (op.responses < op.response_target) continue;
                if (op.quorum_hits < QUORUM) {
                    throw std::runtime_error("ticket_faa pipeline: ticket replication failed to reach quorum");
                }
                if (op.ticket == 0) {
                    begin_release(op);
                } else {
                    post_turn_read(client, op, buffers);
                }
                continue;
            }

            if (phase == TicketFaaPhase::wait_turn_read) {
                op.last_turn = buffers.turn_reads[op.slot];
                if (op.last_turn == op.ticket) {
                    begin_release(op);
                } else {
                    const uint64_t distance = op.ticket > op.last_turn ? (op.ticket - op.last_turn) : 0;
                    const uint32_t spin_budget = spin_budget_for_distance(distance);
                    if (spin_budget == 0) {
                        post_turn_read(client, op, buffers);
                    } else {
                        op.turn_spin_remaining = spin_budget;
                        op.phase = TicketFaaPhase::wait_turn_spin;
                    }
                }
                continue;
            }

            if (phase == TicketFaaPhase::release_parallel) {
                const uint8_t idx = wr_conn(wc.wr_id);
                if (idx == kReleaseTurnConnIndex) {
                    op.release_turn_done = true;
                } else {
                    auto* mark_done_results = row_ptr(buffers.mark_done_results, op.slot, conns.size());
                    op.release_mark_done_responses++;
                    if (!config.replicate_with_cas || mark_done_results[idx] == op.waiter_id) {
                        op.release_mark_done_quorum++;
                    }
                    const uint32_t remaining = static_cast<uint32_t>(conns.size()) - op.release_mark_done_responses;
                    if (op.release_mark_done_quorum + remaining < QUORUM) {
                        throw std::runtime_error("ticket_faa pipeline: mark done failed to reach quorum");
                    }
                }
                if (!op.release_turn_done || op.release_mark_done_quorum < QUORUM) continue;
                lock_counts[op.lock_id]++;
                op.active = false;
                op.phase = TicketFaaPhase::idle;
                completed++;
                active--;
                if (submitted < NUM_OPS_PER_CLIENT) submit_op(slot);
            }
        }
    }
}
