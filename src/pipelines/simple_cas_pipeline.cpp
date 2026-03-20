#include "rdma/pipelines/simple_cas_pipeline.h"

// Minimal one-sided CAS flag lock implementation.

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

// Encode a completion id from op generation/slot plus the current phase.
uint64_t encode_wr_id(const SimpleCasOpCtx& op, const SimpleCasPhase phase, const uint8_t conn_index) {
    return (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 16)
         | (static_cast<uint64_t>(phase) << kConnBits)
         | static_cast<uint64_t>(conn_index);
}

// Extract the op generation from a simple_cas work-request id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

// Extract the pipeline slot from a simple_cas work-request id.
uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu);
}

// Extract the phase tag from a simple_cas work-request id.
SimpleCasPhase wr_phase(const uint64_t wr_id) {
    return static_cast<SimpleCasPhase>((wr_id >> kConnBits) & 0xFFu);
}

// Map the shared client MR into the small per-op result buffers used by the
// simple CAS pipeline.
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

// Post the owner-node 0->1 CAS that attempts to acquire the simple flag lock.
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

// Release the flag either with a guarded CAS or a blind write, depending on the
// configured comparison mode.
void post_release(Client& client, SimpleCasOpCtx& op, const SimpleCasPipelineConfig& config) {
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
    if (config.release_with_cas) {
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = owner.addr + lock_control_offset(op.lock_id);
        wr.wr.atomic.rkey = owner.rkey;
        wr.wr.atomic.compare_add = 1;
        wr.wr.atomic.swap = 0;
    } else {
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        wr.wr.rdma.remote_addr = owner.addr + lock_control_offset(op.lock_id);
        wr.wr.rdma.rkey = owner.rkey;
    }

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_cas pipeline: release post failed");
    }

    op.phase = SimpleCasPhase::release;
}

} // namespace

// Load the compile-time simple CAS tuning knobs into the runtime config struct.
SimpleCasPipelineConfig load_simple_cas_pipeline_config() {
    SimpleCasPipelineConfig config{};
    config.active_window = std::max<size_t>(1, SIMPLE_CAS_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, SIMPLE_CAS_CQ_BATCH);
    config.zipf_skew = SIMPLE_CAS_ZIPF_SKEW;
    config.shard_owner = SIMPLE_CAS_SHARD_OWNER;
    config.release_with_cas = SIMPLE_CAS_RELEASE_USE_CAS;
    return config;
}

// Report how much client MR space simple_cas needs for its result buffers.
size_t simple_cas_pipeline_client_buffer_size(const SimpleCasPipelineConfig& config) {
    const size_t scalar_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    return align_up(scalar_bytes * 2 + PAGE_SIZE, PAGE_SIZE);
}

// Run the minimal simple_cas state machine across the configured active window.
void run_simple_cas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SimpleCasPipelineConfig& config
) {
    // Validate that the client has server connections and that the active window
    // fits within the WR-id slot encoding used by this pipeline.
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("simple_cas pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("simple_cas pipeline: active window exceeds wr_id slot encoding");
    }

    // Map the client MR into small per-op result buffers, then create one op
    // context per active-window slot.
    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window);
    std::vector<SimpleCasOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;

    auto submit_op = [&](const size_t slot) {
        // Start one new lock attempt in this op slot:
        // 1. choose a lock id,
        // 2. choose the owner node,
        // 3. initialize local result pointers,
        // 4. post the acquire CAS immediately.
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

    // Fill the pipeline window before entering the completion-driven main loop.
    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        // Drive the state machine entirely from completions. Each op alternates
        // between acquire and release until its lock/unlock pair is complete.
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
                // Acquire succeeds only if the old control-word value was 0. On
                // failure we immediately repost another acquire CAS for the same op.
                if (*op.acquire_result == 0) {
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    post_release(client, op, config);
                } else {
                    post_acquire(client, op);
                }
                continue;
            }

            if (phase == SimpleCasPhase::release) {
                // Release completion finishes the full operation. Retire this op
                // slot, record the completed lock count, and submit the next request.
                if (config.release_with_cas && *op.release_value != 1) {
                    throw std::runtime_error("simple_cas pipeline: release CAS failed");
                }
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
