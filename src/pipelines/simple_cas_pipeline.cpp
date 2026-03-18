#include "rdma/pipelines/simple_cas_pipeline.h"

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

enum class SimpleCasPhase : uint8_t {
    idle = 0,
    acquire = 1,
    release = 2,
};

struct RegisteredSimpleCasBuffers {
    uint64_t* acquire_results = nullptr;
    uint64_t* release_values = nullptr;
};

struct SimpleCasOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t owner_node = 0;
    SimpleCasPhase phase = SimpleCasPhase::idle;
    size_t latency_index = 0;
    uint64_t* acquire_result = nullptr;
    uint64_t* release_value = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t encode_wr_id(const SimpleCasOpCtx& op, const SimpleCasPhase phase, const uint8_t conn_index) {
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

SimpleCasPhase wr_phase(const uint64_t wr_id) {
    return static_cast<SimpleCasPhase>((wr_id >> kConnBits) & 0xFFu);
}

RegisteredSimpleCasBuffers map_buffers(void* raw_buffer, const size_t buffer_size, const size_t active_window) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    RegisteredSimpleCasBuffers buffers{};
    const size_t scalar_bytes = align_up(active_window * sizeof(uint64_t), 64);

    buffers.acquire_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.release_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("simple_cas pipeline: registered client buffer too small");
    }
    return buffers;
}

void post_acquire(Client& client, SimpleCasOpCtx& op) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    *op.acquire_result = std::numeric_limits<uint64_t>::max() - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.acquire_result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SimpleCasPhase::acquire, static_cast<uint8_t>(op.owner_node));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = owner.addr + lock_control_offset(op.lock_id);
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = 0;
    wr.wr.atomic.swap = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_cas pipeline: acquire post failed");
    }

    op.phase = SimpleCasPhase::acquire;
}

void post_release(Client& client, SimpleCasOpCtx& op) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    *op.release_value = 0;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.release_value);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SimpleCasPhase::release, static_cast<uint8_t>(op.owner_node));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.wr.rdma.remote_addr = owner.addr + lock_control_offset(op.lock_id);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_cas pipeline: release post failed");
    }

    op.phase = SimpleCasPhase::release;
}

} // namespace

SimpleCasPipelineConfig load_simple_cas_pipeline_config() {
    SimpleCasPipelineConfig config{};
    config.active_window = std::max<size_t>(1, SIMPLE_CAS_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, SIMPLE_CAS_CQ_BATCH);
    config.zipf_skew = SIMPLE_CAS_ZIPF_SKEW;
    config.shard_owner = SIMPLE_CAS_SHARD_OWNER;
    return config;
}

size_t simple_cas_pipeline_client_buffer_size(const SimpleCasPipelineConfig& config) {
    const size_t scalar_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    return align_up(scalar_bytes * 2 + PAGE_SIZE, PAGE_SIZE);
}

void run_simple_cas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SimpleCasPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("simple_cas pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("simple_cas pipeline: active window exceeds wr_id slot encoding");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window);
    std::vector<SimpleCasOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = picker.next();
        op.owner_node = config.shard_owner ? (op.lock_id % conns.size()) : 0;
        op.phase = SimpleCasPhase::idle;
        op.latency_index = submitted;
        op.acquire_result = &buffers.acquire_results[slot];
        op.release_value = &buffers.release_values[slot];
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
            throw std::runtime_error("simple_cas pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "simple_cas pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("simple_cas pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const SimpleCasPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            if (phase == SimpleCasPhase::acquire) {
                if (*op.acquire_result == 0) {
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    post_release(client, op);
                } else {
                    post_acquire(client, op);
                }
                continue;
            }

            if (phase == SimpleCasPhase::release) {
                lock_counts[op.lock_id]++;
                op.active = false;
                op.phase = SimpleCasPhase::idle;
                completed++;
                active--;

                if (submitted < NUM_OPS_PER_CLIENT) {
                    submit_op(slot);
                }
            }
        }
    }
}
