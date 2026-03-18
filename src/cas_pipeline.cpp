#include "rdma/cas_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr size_t kConnBits = 8;

enum class OpPhase : uint8_t {
    idle = 0,
    acquire = 1,
    replicate = 2,
    release = 3,
};

struct RegisteredCasBuffers {
    uint64_t* acquire_results = nullptr;
    uint64_t* replicate_value = nullptr;
    uint64_t* release_sinks = nullptr;
};

struct CasOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t owner_node = 0;
    OpPhase phase = OpPhase::idle;
    uint64_t target_slot = 1;
    uint64_t held_slot = 0;
    uint32_t replicate_acks = 0;
    bool quorum_reached = false;
    size_t latency_index = 0;
    uint64_t* acquire_result = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

constexpr uint64_t kReleaseWrTag = 0xFF00000000000000ULL;

bool is_release_wr_id(const uint64_t wr_id) {
    return (wr_id & kReleaseWrTag) == kReleaseWrTag;
}

uint64_t make_release_wr_id(uint64_t seq, const uint8_t conn_index) {
    return kReleaseWrTag | (seq << 8) | static_cast<uint64_t>(conn_index);
}

void ensure_log_slot_in_bounds(const uint32_t lock_id, const uint64_t slot, const char* context) {
    if (slot >= MAX_LOG_PER_LOCK) {
        throw std::runtime_error(
            std::string("CAS pipeline: log overflow during ") + context
            + " lock=" + std::to_string(lock_id)
            + " slot=" + std::to_string(slot)
            + " max_log_per_lock=" + std::to_string(MAX_LOG_PER_LOCK));
    }
}

uint64_t encode_wr_id(const CasOpCtx& op, const OpPhase phase, const uint8_t conn_index) {
    return (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 16)
         | (static_cast<uint64_t>(phase) << kConnBits)
         | static_cast<uint64_t>(conn_index);
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu);
}

OpPhase wr_phase(const uint64_t wr_id) {
    return static_cast<OpPhase>((wr_id >> kConnBits) & 0xFFu);
}

RegisteredCasBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;

    RegisteredCasBuffers buffers{};

    const size_t acquire_bytes = align_up(active_window * sizeof(uint64_t), 64);
    buffers.acquire_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += acquire_bytes;

    const size_t replicate_bytes = align_up(sizeof(uint64_t), 64);
    buffers.replicate_value = reinterpret_cast<uint64_t*>(base + offset);
    offset += replicate_bytes;

    const size_t release_bytes = align_up(replica_count * sizeof(uint64_t), 64);
    buffers.release_sinks = reinterpret_cast<uint64_t*>(base + offset);
    offset += release_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("CAS pipeline: registered client buffer too small");
    }

    return buffers;
}

void post_acquire(Client& client, CasOpCtx& op) {
    const auto& conns = client.connections();
    auto* mr = client.mr();

    *op.acquire_result = std::numeric_limits<uint64_t>::max() - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.acquire_result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, OpPhase::acquire, static_cast<uint8_t>(op.owner_node));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = conns[op.owner_node].addr + lock_control_offset(op.lock_id);
    wr.wr.atomic.rkey = conns[op.owner_node].rkey;
    wr.wr.atomic.compare_add = op.target_slot - 1;
    wr.wr.atomic.swap = op.target_slot;

    if (ibv_post_send(conns[op.owner_node].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("CAS pipeline: acquire post failed");
    }

    op.phase = OpPhase::acquire;
}

void post_replicate(const Client& client, CasOpCtx& op, const uint64_t* replicate_value) {
    const auto& conns = client.connections();
    auto* mr = client.mr();

    ensure_log_slot_in_bounds(op.lock_id, op.held_slot, "replicate");

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(replicate_value);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, OpPhase::replicate, static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.held_slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("CAS pipeline: replicate post failed");
        }
    }

    op.phase = OpPhase::replicate;
    op.replicate_acks = 0;
    op.quorum_reached = false;
}

void post_release(
    Client& client,
    const CasOpCtx& op,
    uint64_t* release_sinks,
    uint64_t& release_sequence,
    uint64_t& release_signal_count,
    const uint32_t release_signal_every,
    const bool release_with_cas
) {
    const auto& conns = client.connections();
    const auto* mr = client.mr();

    for (size_t i = 0; i < conns.size(); ++i) {
        auto* result = release_sinks + i;
        *result = op.held_slot + 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(result);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_release_wr_id(release_sequence++, static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = (++release_signal_count % std::max<uint32_t>(release_signal_every, 1u) == 0)
                            ? IBV_SEND_SIGNALED
                            : 0;
        if (release_with_cas) {
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
            wr.wr.atomic.rkey = conns[i].rkey;
            wr.wr.atomic.compare_add = (i == op.owner_node) ? op.held_slot : std::min(op.held_slot, op.held_slot - 1);
            wr.wr.atomic.swap = op.held_slot + 1;
        } else {
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags |= IBV_SEND_INLINE;
            wr.wr.rdma.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
            wr.wr.rdma.rkey = conns[i].rkey;
        }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("CAS pipeline: release post failed");
        }
    }
}

}  // namespace

CasPipelineConfig load_cas_pipeline_config() {
    CasPipelineConfig config{};
    config.active_window = CAS_ACTIVE_CLIENTS;
    config.cq_batch = std::max<size_t>(1, CAS_CQ_BATCH);
    config.zipf_skew = CAS_ZIPF_SKEW;
    config.shard_owner = CAS_SHARD_OWNER;
    config.release_signal_every = std::max<uint32_t>(1, CAS_RELEASE_SIGNAL_EVERY);
    config.release_with_cas = CAS_RELEASE_USE_CAS;
    return config;
}

size_t cas_pipeline_client_buffer_size(const CasPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t acquire_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t replicate_bytes = align_up(sizeof(uint64_t), 64);
    const size_t release_bytes = align_up(replica_count * sizeof(uint64_t), 64);
    return align_up(acquire_bytes + replicate_bytes + release_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_cas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const CasPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("CAS pipeline: no server connections");
    }

    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("CAS pipeline: active window exceeds wr_id slot encoding");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<CasOpCtx> ops(config.active_window);
    ZipfLockPicker picker(config.zipf_skew);
    std::vector<ibv_wc> completions(config.cq_batch);
    std::array<uint64_t, MAX_LOCKS> frontier_hints{};
    frontier_hints.fill(1);
    *buffers.replicate_value = static_cast<uint64_t>(client.id());

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint64_t release_sequence = 1;
    uint64_t release_signal_count = 0;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = picker.next();
        op.owner_node = config.shard_owner ? (op.lock_id % conns.size()) : 0;
        op.phase = OpPhase::idle;
        op.target_slot = frontier_hints[op.lock_id];
        op.held_slot = 0;
        op.replicate_acks = 0;
        op.quorum_reached = false;
        op.latency_index = submitted;
        op.acquire_result = &buffers.acquire_results[slot];
        op.started_at = std::chrono::steady_clock::now();
        post_acquire(client, op);
        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("CAS pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "CAS pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            if (is_release_wr_id(wc.wr_id)) {
                continue;
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("CAS pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const OpPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            if (phase == OpPhase::acquire) {
                const uint64_t expected = op.target_slot - 1;
                const uint64_t result = *op.acquire_result;

                if (result == expected) {
                    op.held_slot = op.target_slot;
                    post_replicate(client, op, buffers.replicate_value);
                } else {
                    op.target_slot = (result % 2 != 0) ? result + 2 : result + 1;
                    frontier_hints[op.lock_id] = op.target_slot;
                    post_acquire(client, op);
                }
                continue;
            }

            if (phase == OpPhase::replicate) {
                if (!op.quorum_reached) {
                    op.replicate_acks++;
                    if (op.replicate_acks >= QUORUM) {
                        op.quorum_reached = true;
                        latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - op.started_at).count();
                        frontier_hints[op.lock_id] = std::max(frontier_hints[op.lock_id], op.held_slot + 2);
                        post_release(
                            client,
                            op,
                            buffers.release_sinks,
                            release_sequence,
                            release_signal_count,
                            config.release_signal_every,
                            config.release_with_cas);
                        lock_counts[op.lock_id]++;
                        op.active = false;
                        op.phase = OpPhase::idle;
                        completed++;
                        active--;

                        if (submitted < NUM_OPS_PER_CLIENT) {
                            submit_op(slot);
                        }
                    }
                }
            }
        }
    }
}
