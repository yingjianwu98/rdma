#include "rdma/pipelines/ticket_faa_lock_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
constexpr uint64_t kLogLiveBit = 1ULL << 63;
constexpr uint64_t kLogTicketMask = (1ULL << 31) - 1;
constexpr uint8_t kReleaseTurnConnIndex = 0xFF;
constexpr uint64_t kDetachedTurnWrTag = 0xFF00000000000000ULL;

constexpr uint32_t kTurnReleaseWrite = 0;
constexpr uint32_t kTurnReleaseCas = 1;
constexpr uint32_t kTurnReleaseFaa = 2;

enum class TicketFaaPhase : uint8_t {
    idle = 0,
    faa_ticket = 1,
    replicate_ticket = 2,
    replicate_ticket_spin = 3,
    wait_turn_read = 4,
    wait_turn_spin = 5,
    release_parallel = 6,
};

struct RegisteredTicketFaaBuffers {
    uint64_t* ticket_results = nullptr;
    uint64_t* replicate_results = nullptr;
    uint64_t* turn_reads = nullptr;
    uint64_t* release_log_values = nullptr;
    uint64_t* release_log_results = nullptr;
    uint64_t* detached_turn_results = nullptr;
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
    uint64_t physical_log_slot = 0;
    uint64_t last_turn = 0;
    uint32_t replicate_retry_spin_remaining = 0;
    uint32_t turn_spin_remaining = 0;
    TicketFaaPhase phase = TicketFaaPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;
    uint32_t quorum_hits = 0;
    uint32_t release_log_responses = 0;
    uint32_t release_log_quorum = 0;
    bool release_owner_log_done = false;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

struct DetachedTurnCtx {
    bool active = false;
    uint32_t turn_mode = kTurnReleaseWrite;
    uint64_t expected_old = 0;
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

bool is_detached_turn_wr_id(const uint64_t wr_id) {
    return (wr_id & kDetachedTurnWrTag) == kDetachedTurnWrTag;
}

uint64_t make_detached_turn_wr_id(const uint32_t ctx_index) {
    return kDetachedTurnWrTag | static_cast<uint64_t>(ctx_index);
}

uint32_t detached_turn_ctx_index(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & 0x00FFFFFFu);
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

size_t detached_turn_ctx_capacity(const size_t active_window) {
    return std::max<size_t>(active_window * 8, 64);
}

uint64_t ticket_faa_physical_log_slot(const uint64_t ticket) {
    return ticket % TICKET_FAA_LOG_CAPACITY;
}

uint64_t pack_ticket_faa_log_free(const uint64_t ticket) {
    return (ticket & kLogTicketMask) << 32;
}

uint64_t pack_ticket_faa_log_live(const uint64_t ticket, const uint16_t client_id, const uint16_t op_slot) {
    return kLogLiveBit
         | ((ticket & kLogTicketMask) << 32)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(op_slot);
}

uint64_t ticket_faa_log_expected_free_value(const uint64_t ticket) {
    return ticket < TICKET_FAA_LOG_CAPACITY ? EMPTY_SLOT : pack_ticket_faa_log_free(ticket);
}

uint64_t ticket_faa_log_next_free_value(const uint64_t ticket) {
    return pack_ticket_faa_log_free(ticket + TICKET_FAA_LOG_CAPACITY);
}

void ensure_log_slot_in_bounds(const uint32_t lock_id, const uint64_t ticket, const char* context) {
    const uint64_t physical = ticket_faa_physical_log_slot(ticket);
    if (physical >= MAX_LOG_PER_LOCK) {
        throw std::runtime_error(
            std::string("ticket_faa pipeline: wrapped log overflow during ") + context
            + " lock=" + std::to_string(lock_id)
            + " ticket=" + std::to_string(ticket)
            + " physical=" + std::to_string(physical)
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
    const size_t detached_turn_bytes = align_up(detached_turn_ctx_capacity(active_window) * sizeof(uint64_t), 64);

    buffers.ticket_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.turn_reads = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.release_log_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.release_log_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.detached_turn_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += detached_turn_bytes;

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
    const RegisteredTicketFaaBuffers& buffers
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    ensure_log_slot_in_bounds(op.lock_id, op.ticket, "replicate_ticket");
    op.physical_log_slot = ticket_faa_physical_log_slot(op.ticket);
    const uint64_t compare_value = ticket_faa_log_expected_free_value(op.ticket);
    const uint64_t swap_value = pack_ticket_faa_log_live(op.ticket, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));

    op.round++;
    op.phase = TicketFaaPhase::replicate_ticket;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());
    op.quorum_hits = 0;

    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = EMPTY_SLOT - 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::replicate_ticket, static_cast<uint8_t>(i));
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.physical_log_slot);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = compare_value;
        wr.wr.atomic.swap = swap_value;

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
    const TicketFaaLockPipelineConfig& config,
    std::vector<DetachedTurnCtx>& detached_turn_ctxs
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    auto* release_log_values = row_ptr(buffers.release_log_values, op.slot, conns.size());
    auto* release_log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
    const uint64_t live_value = pack_ticket_faa_log_live(op.ticket, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));
    const uint64_t free_value = ticket_faa_log_next_free_value(op.ticket);

    ensure_log_slot_in_bounds(op.lock_id, op.ticket, "release_parallel");

    op.round++;
    op.phase = TicketFaaPhase::release_parallel;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());
    op.release_log_responses = 0;
    op.release_log_quorum = 0;
    op.release_owner_log_done = false;

    DetachedTurnCtx* detached_turn_ctx = nullptr;
    uint32_t detached_turn_idx = 0;
    if (config.release_turn_mode != kTurnReleaseWrite) {
        for (uint32_t i = 0; i < detached_turn_ctxs.size(); ++i) {
            if (!detached_turn_ctxs[i].active) {
                detached_turn_ctx = &detached_turn_ctxs[i];
                detached_turn_idx = i;
                break;
            }
        }
        if (detached_turn_ctx == nullptr) {
            throw std::runtime_error("ticket_faa pipeline: detached turn context pool exhausted");
        }

        detached_turn_ctx->active = true;
        detached_turn_ctx->turn_mode = config.release_turn_mode;
        detached_turn_ctx->expected_old = op.ticket;
        buffers.detached_turn_results[detached_turn_idx] = 0;
    }

    if (config.release_turn_mode == kTurnReleaseWrite) {
        uint64_t turn_value = op.ticket + 1;

        ibv_sge turn_sge{};
        turn_sge.addr = reinterpret_cast<uintptr_t>(&turn_value);
        turn_sge.length = sizeof(uint64_t);
        turn_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::release_parallel, kReleaseTurnConnIndex);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_INLINE;
        wr.sg_list = &turn_sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = owner.addr + lock_turn_offset(op.lock_id);
        wr.wr.rdma.rkey = owner.rkey;

        if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: release turn post failed");
        }
    } else {
        auto* turn_result = &buffers.detached_turn_results[detached_turn_idx];

        ibv_sge turn_sge{};
        turn_sge.addr = reinterpret_cast<uintptr_t>(turn_result);
        turn_sge.length = sizeof(uint64_t);
        turn_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_detached_turn_wr_id(detached_turn_idx);
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &turn_sge;
        wr.num_sge = 1;
        if (config.release_turn_mode == kTurnReleaseCas) {
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = owner.addr + lock_turn_offset(op.lock_id);
            wr.wr.atomic.rkey = owner.rkey;
            wr.wr.atomic.compare_add = op.ticket;
            wr.wr.atomic.swap = op.ticket + 1;
        } else {
            wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
            wr.wr.atomic.remote_addr = owner.addr + lock_turn_offset(op.lock_id);
            wr.wr.atomic.rkey = owner.rkey;
            wr.wr.atomic.compare_add = 1;
        }

        if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: release turn post failed");
        }
    }

    for (size_t i = 0; i < conns.size(); ++i) {
        auto* log_value = &release_log_values[i];
        auto* log_result = &release_log_results[i];
        *log_value = free_value;
        *log_result = EMPTY_SLOT - 1;

        ibv_sge log_sge{};
        log_sge.length = sizeof(uint64_t);
        log_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::release_parallel, static_cast<uint8_t>(i));
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &log_sge;
        wr.num_sge = 1;
        if (config.release_log_with_cas) {
            log_sge.addr = reinterpret_cast<uintptr_t>(log_result);
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.physical_log_slot);
            wr.wr.atomic.rkey = conns[i].rkey;
            wr.wr.atomic.compare_add = live_value;
            wr.wr.atomic.swap = free_value;
        } else {
            log_sge.addr = reinterpret_cast<uintptr_t>(log_value);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags |= IBV_SEND_INLINE;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.physical_log_slot);
            wr.wr.rdma.rkey = conns[i].rkey;
        }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: release log post failed");
        }
    }
}

} // namespace

TicketFaaLockPipelineConfig load_ticket_faa_lock_pipeline_config() {
    TicketFaaLockPipelineConfig config{};
    config.active_window = std::max<size_t>(1, TICKET_FAA_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, TICKET_FAA_CQ_BATCH);
    config.zipf_skew = TICKET_FAA_ZIPF_SKEW;
    config.shard_owner = TICKET_FAA_SHARD_OWNER;
    config.release_log_with_cas = TICKET_FAA_RELEASE_LOG_USE_CAS;
    config.release_turn_mode = TICKET_FAA_RELEASE_TURN_MODE;
    return config;
}

size_t ticket_faa_lock_pipeline_client_buffer_size(const TicketFaaLockPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t scalar_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t matrix_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    const size_t detached_turn_bytes = align_up(detached_turn_ctx_capacity(config.active_window) * sizeof(uint64_t), 64);
    return align_up(scalar_bytes * 2 + matrix_bytes * 3 + detached_turn_bytes + PAGE_SIZE, PAGE_SIZE);
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
    std::vector<DetachedTurnCtx> detached_turn_ctxs(detached_turn_ctx_capacity(config.active_window));
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto begin_release = [&](TicketFaaOpCtx& op) {
        latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - op.started_at).count();
        post_release_parallel(client, op, buffers, config, detached_turn_ctxs);
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
        op.physical_log_slot = 0;
        op.last_turn = 0;
        op.replicate_retry_spin_remaining = 0;
        op.turn_spin_remaining = 0;
        op.phase = TicketFaaPhase::idle;
        op.responses = 0;
        op.response_target = 0;
        op.quorum_hits = 0;
        op.release_log_responses = 0;
        op.release_log_quorum = 0;
        op.release_owner_log_done = false;
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
            if (!op.active) continue;
            if (op.phase == TicketFaaPhase::replicate_ticket_spin) {
                if (op.replicate_retry_spin_remaining > 0) {
                    --op.replicate_retry_spin_remaining;
                }
                if (op.replicate_retry_spin_remaining == 0) {
                    post_replicate_ticket(client, op, buffers);
                }
            } else if (op.phase == TicketFaaPhase::wait_turn_spin) {
                if (op.turn_spin_remaining > 0) {
                    --op.turn_spin_remaining;
                }
                if (op.turn_spin_remaining == 0) {
                    post_turn_read(client, op, buffers);
                }
            }
        }

        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) throw std::runtime_error("ticket_faa pipeline: CQ poll failed");
        if (polled == 0) continue;

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "ticket_faa pipeline: completion failed status=" + std::to_string(wc.status)
                    + " vendor=" + std::to_string(wc.vendor_err));
            }

            if (is_detached_turn_wr_id(wc.wr_id)) {
                const uint32_t ctx_index = detached_turn_ctx_index(wc.wr_id);
                if (ctx_index >= detached_turn_ctxs.size()) {
                    throw std::runtime_error("ticket_faa pipeline: detached turn context out of range");
                }
                auto& ctx = detached_turn_ctxs[ctx_index];
                if (!ctx.active) continue;
                if (ctx.turn_mode == kTurnReleaseCas && buffers.detached_turn_results[ctx_index] != ctx.expected_old) {
                    throw std::runtime_error("ticket_faa pipeline: turn CAS release failed");
                }
                ctx.active = false;
                continue;
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
                post_replicate_ticket(client, op, buffers);
                continue;
            }

            op.responses++;

            if (phase == TicketFaaPhase::replicate_ticket) {
                auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (results[idx] == ticket_faa_log_expected_free_value(op.ticket)) {
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
                    op.replicate_retry_spin_remaining = TICKET_FAA_REPLICATE_RETRY_SPIN;
                    op.phase = TicketFaaPhase::replicate_ticket_spin;
                    continue;
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
                auto* release_log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
                op.release_log_responses++;
                if (!config.release_log_with_cas
                    || release_log_results[idx] == pack_ticket_faa_log_live(op.ticket, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot))) {
                    op.release_log_quorum++;
                    if (idx == op.owner_node) {
                        op.release_owner_log_done = true;
                    }
                } else if (idx == op.owner_node) {
                    throw std::runtime_error("ticket_faa pipeline: owner wrapped log release CAS failed");
                }

                const uint32_t remaining = static_cast<uint32_t>(conns.size()) - op.release_log_responses;
                if (op.release_log_quorum + remaining < QUORUM) {
                    throw std::runtime_error("ticket_faa pipeline: wrapped log release failed to reach quorum");
                }
                if (op.release_owner_log_done && op.release_log_quorum >= QUORUM) {
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
}
