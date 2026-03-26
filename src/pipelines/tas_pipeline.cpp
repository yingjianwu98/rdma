#include "rdma/pipelines/tas_pipeline.h"

// Round-based one-sided TAS primitive: every contender races on the same owner
// word, losers stop on the failed owner CAS, and the single winner quorum-CASes
// its client id into the round's flat-log slot.

#include "rdma/client.h"
#include "rdma/common.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kTasAcquireTag = 0xE1ULL;
constexpr uint64_t kTasReplicateTag = 0xE2ULL;
constexpr uint64_t kTasBarrierIncrementTag = 0xE3ULL;
constexpr uint64_t kTasBarrierReadTag = 0xE4ULL;
constexpr uint64_t kTasTagShift = 56;
constexpr uint64_t kTasGenerationShift = 24;
constexpr uint64_t kTasSlotShift = 8;
constexpr uint64_t kTasGenerationMask = 0xFFFFFFFFULL;
constexpr uint64_t kTasSlotMask = 0xFFFFULL;
constexpr uint64_t kTasPayloadMask = 0x00FFFFFFFFFFFFFFULL;

enum class TasPhase : uint8_t {
    idle = 0,
    acquire = 1,
    replicate = 2,
};

struct RegisteredTasBuffers {
    uint64_t* acquire_results = nullptr;
    uint64_t* replicate_results = nullptr;
    uint64_t* barrier_increment_result = nullptr;
    uint64_t* barrier_read_result = nullptr;
};

struct TasOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    size_t round = 0;
    TasPhase phase = TasPhase::idle;
    uint32_t responses = 0;
    uint32_t successes = 0;
    size_t latency_index = 0;
    uint64_t* acquire_result = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t pack_tas_entry(const uint16_t client_id) {
    return static_cast<uint64_t>(client_id);
}

uint64_t make_acquire_wr_id(const TasOpCtx& op) {
    return (kTasAcquireTag << kTasTagShift)
         | ((static_cast<uint64_t>(op.generation) & kTasGenerationMask) << kTasGenerationShift)
         | ((static_cast<uint64_t>(op.slot) & kTasSlotMask) << kTasSlotShift);
}

uint64_t make_replicate_wr_id(const TasOpCtx& op, const uint8_t conn_index) {
    return (kTasReplicateTag << kTasTagShift)
         | ((static_cast<uint64_t>(op.generation) & kTasGenerationMask) << kTasGenerationShift)
         | ((static_cast<uint64_t>(op.slot) & kTasSlotMask) << kTasSlotShift)
         | static_cast<uint64_t>(conn_index);
}

uint64_t make_barrier_increment_wr_id(const size_t round) {
    return (kTasBarrierIncrementTag << kTasTagShift)
         | (static_cast<uint64_t>(round) & kTasPayloadMask);
}

uint64_t make_barrier_read_wr_id(const size_t round) {
    return (kTasBarrierReadTag << kTasTagShift)
         | (static_cast<uint64_t>(round) & kTasPayloadMask);
}

uint8_t wr_tag(const uint64_t wr_id) {
    return static_cast<uint8_t>(wr_id >> kTasTagShift);
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> kTasGenerationShift) & kTasGenerationMask);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> kTasSlotShift) & kTasSlotMask);
}

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>(wr_id & 0xFFu);
}

uint64_t wr_payload(const uint64_t wr_id) {
    return wr_id & kTasPayloadMask;
}

bool is_acquire_wr_id(const uint64_t wr_id) {
    return wr_tag(wr_id) == kTasAcquireTag;
}

bool is_replicate_wr_id(const uint64_t wr_id) {
    return wr_tag(wr_id) == kTasReplicateTag;
}

bool is_barrier_increment_wr_id(const uint64_t wr_id) {
    return wr_tag(wr_id) == kTasBarrierIncrementTag;
}

bool is_barrier_read_wr_id(const uint64_t wr_id) {
    return wr_tag(wr_id) == kTasBarrierReadTag;
}

size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

uint64_t* row_ptr(uint64_t* base, const uint32_t slot, const size_t replica_count) {
    return base + row_offset(slot, replica_count);
}

RegisteredTasBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;

    RegisteredTasBuffers buffers{};

    const size_t acquire_bytes = align_up(active_window * sizeof(uint64_t), 64);
    buffers.acquire_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += acquire_bytes;

    const size_t replicate_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += replicate_bytes;

    const size_t scalar_bytes = align_up(sizeof(uint64_t), 64);
    buffers.barrier_increment_result = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;

    buffers.barrier_read_result = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("tas pipeline: registered client buffer too small");
    }

    return buffers;
}

void post_acquire(const Client& client, TasOpCtx& op) {
    auto* mr = client.mr();
    const auto& owner = client.connections().front();
    *op.acquire_result = EMPTY_SLOT - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.acquire_result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_acquire_wr_id(op);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = owner.addr + tas_owner_word_offset();
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = static_cast<uint64_t>(op.round);
    wr.wr.atomic.swap = static_cast<uint64_t>(op.round + 1);

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("tas pipeline: acquire CAS post failed");
    }

    op.phase = TasPhase::acquire;
}

void post_replicate(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
    const uint64_t entry = pack_tas_entry(static_cast<uint16_t>(client.id()));

    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = EMPTY_SLOT - 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_replicate_wr_id(op, static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = conns[i].addr + tas_log_slot_offset(op.round);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = EMPTY_SLOT;
        wr.wr.atomic.swap = entry;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("tas pipeline: replicate CAS post failed");
        }
    }

    op.phase = TasPhase::replicate;
    op.responses = 0;
    op.successes = 0;
}

void post_barrier_increment(const Client& client, uint64_t* result, const uint64_t increment, const size_t round) {
    auto* mr = client.mr();
    const auto& owner = client.connections().front();
    *result = EMPTY_SLOT - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_barrier_increment_wr_id(round);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = owner.addr + tas_round_done_offset();
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = increment;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("tas pipeline: barrier FAA post failed");
    }
}

void post_barrier_read(const Client& client, uint64_t* result, const size_t round) {
    auto* mr = client.mr();
    const auto& owner = client.connections().front();
    *result = EMPTY_SLOT - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_barrier_read_wr_id(round);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = owner.addr + tas_round_done_offset();
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("tas pipeline: barrier read post failed");
    }
}

} // namespace

TasPipelineConfig load_tas_pipeline_config() {
    TasPipelineConfig config{};
    config.active_window = std::max<size_t>(1, TAS_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, TAS_CQ_BATCH);
    config.rounds = std::max<size_t>(1, TAS_ROUNDS);
    config.log_capacity = TAS_LOG_CAPACITY;
    return config;
}

size_t tas_pipeline_client_buffer_size(const TasPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t acquire_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t replicate_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    const size_t scalar_bytes = align_up(sizeof(uint64_t), 64);
    return align_up(acquire_bytes + replicate_bytes + (2 * scalar_bytes) + PAGE_SIZE, PAGE_SIZE);
}

size_t tas_pipeline_latency_count(const TasPipelineConfig& config) {
    return config.active_window * config.rounds;
}

void run_tas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TasPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("tas pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(kTasSlotMask)) {
        throw std::runtime_error("tas pipeline: active window exceeds wr_id slot encoding");
    }
    if (conns.size() > 0xFFu) {
        throw std::runtime_error("tas pipeline: too many replicas for wr_id encoding");
    }
    if (config.rounds > config.log_capacity) {
        throw std::runtime_error("tas pipeline: rounds exceed flat log capacity");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<TasOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);

    auto poll_until = [&](auto&& done_predicate, auto&& handle_completion) {
        while (!done_predicate()) {
            const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
            if (polled < 0) {
                throw std::runtime_error("tas pipeline: CQ poll failed");
            }
            if (polled == 0) {
                continue;
            }

            for (int i = 0; i < polled; ++i) {
                const ibv_wc& wc = completions[i];
                if (wc.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error(
                        "tas pipeline: WC error status=" + std::to_string(wc.status)
                        + " opcode=" + std::to_string(wc.opcode));
                }
                handle_completion(wc);
            }
        }
    };

    for (size_t round = 0; round < config.rounds; ++round) {
        size_t local_round_completed = 0;

        auto complete_slot = [&](TasOpCtx& op, const bool winner) {
            latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - op.started_at).count();
            if (winner) {
                lock_counts[0]++;
            }
            op.active = false;
            op.phase = TasPhase::idle;
            local_round_completed++;
        };

        auto handle_op_completion = [&](const ibv_wc& wc) {
            if (!is_acquire_wr_id(wc.wr_id) && !is_replicate_wr_id(wc.wr_id)) {
                return;
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("tas pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id) || op.round != round) {
                return;
            }

            if (is_acquire_wr_id(wc.wr_id)) {
                if (op.phase != TasPhase::acquire) {
                    return;
                }

                const uint64_t result = *op.acquire_result;
                if (result == static_cast<uint64_t>(round)) {
                    post_replicate(client, op, buffers);
                } else {
                    complete_slot(op, false);
                }
                return;
            }

            if (op.phase != TasPhase::replicate) {
                return;
            }

            const uint8_t idx = wr_conn(wc.wr_id);
            if (idx >= conns.size()) {
                throw std::runtime_error("tas pipeline: replicate replica index out of range");
            }

            auto* replicate_results = row_ptr(buffers.replicate_results, op.slot, conns.size());
            op.responses++;
            if (replicate_results[idx] == EMPTY_SLOT) {
                op.successes++;
            }

            if (op.successes >= QUORUM) {
                complete_slot(op, true);
                return;
            }

            const uint32_t remaining = static_cast<uint32_t>(conns.size()) - op.responses;
            if (op.successes + remaining < QUORUM) {
                throw std::runtime_error("tas pipeline: winner failed to replicate to quorum");
            }
        };

        for (size_t slot = 0; slot < config.active_window; ++slot) {
            auto& op = ops[slot];
            op.active = true;
            op.generation++;
            op.slot = static_cast<uint32_t>(slot);
            op.round = round;
            op.phase = TasPhase::idle;
            op.responses = 0;
            op.successes = 0;
            op.latency_index = round * config.active_window + slot;
            op.acquire_result = &buffers.acquire_results[slot];
            op.started_at = std::chrono::steady_clock::now();
            post_acquire(client, op);
        }

        poll_until(
            [&]() { return local_round_completed == config.active_window; },
            [&](const ibv_wc& wc) {
                if (is_barrier_increment_wr_id(wc.wr_id) || is_barrier_read_wr_id(wc.wr_id)) {
                    return;
                }
                handle_op_completion(wc);
            });

        post_barrier_increment(client, buffers.barrier_increment_result, static_cast<uint64_t>(config.active_window), round);
        bool barrier_increment_done = false;
        poll_until(
            [&]() { return barrier_increment_done; },
            [&](const ibv_wc& wc) {
                if (is_barrier_increment_wr_id(wc.wr_id) && wr_payload(wc.wr_id) == round) {
                    barrier_increment_done = true;
                    return;
                }
                handle_op_completion(wc);
            });

        const uint64_t round_target = static_cast<uint64_t>(round + 1)
                                    * static_cast<uint64_t>(config.active_window)
                                    * static_cast<uint64_t>(TOTAL_CLIENTS);
        while (true) {
            post_barrier_read(client, buffers.barrier_read_result, round);
            bool barrier_read_done = false;
            poll_until(
                [&]() { return barrier_read_done; },
                [&](const ibv_wc& wc) {
                    if (is_barrier_read_wr_id(wc.wr_id) && wr_payload(wc.wr_id) == round) {
                        barrier_read_done = true;
                        return;
                    }
                    handle_op_completion(wc);
                });

            if (*buffers.barrier_read_result >= round_target) {
                break;
            }
        }
    }
}
