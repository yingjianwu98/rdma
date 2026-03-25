#include "rdma/pipelines/synra_faa_pipeline.h"

// Client-side one-sided FAA primitive with flat-log CAS replication.

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

constexpr size_t kConnBits = 8;

enum class SynraFaaPhase : uint8_t {
    idle = 0,
    reserve_slot = 1,
    replicate = 2,
};

struct RegisteredSynraFaaBuffers {
    uint64_t* reserve_results = nullptr;
    uint64_t* replicate_results = nullptr;
};

struct SynraFaaOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    SynraFaaPhase phase = SynraFaaPhase::idle;
    uint64_t reserved_slot = 0;
    uint32_t responses = 0;
    uint32_t successes = 0;
    size_t latency_index = 0;
    uint64_t* reserve_result = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t pack_synra_faa_entry(const uint16_t client_id) {
    return static_cast<uint64_t>(client_id);
}

uint64_t encode_wr_id(const SynraFaaOpCtx& op, const SynraFaaPhase phase, const uint8_t conn_index) {
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

SynraFaaPhase wr_phase(const uint64_t wr_id) {
    return static_cast<SynraFaaPhase>((wr_id >> kConnBits) & 0xFFu);
}

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>(wr_id & 0xFFu);
}

size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

uint64_t* row_ptr(uint64_t* base, const uint32_t slot, const size_t replica_count) {
    return base + row_offset(slot, replica_count);
}

RegisteredSynraFaaBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;

    RegisteredSynraFaaBuffers buffers{};

    const size_t reserve_bytes = align_up(active_window * sizeof(uint64_t), 64);
    buffers.reserve_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += reserve_bytes;

    const size_t replicate_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += replicate_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("synra_faa pipeline: registered client buffer too small");
    }

    return buffers;
}

void post_reserve_slot(const Client& client, SynraFaaOpCtx& op) {
    auto* mr = client.mr();
    const auto& node0 = client.connections().front();
    *op.reserve_result = std::numeric_limits<uint64_t>::max() - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.reserve_result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SynraFaaPhase::reserve_slot, 0);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = node0.addr + synra_faa_counter_offset();
    wr.wr.atomic.rkey = node0.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(node0.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("synra_faa pipeline: FAA reserve post failed");
    }

    op.phase = SynraFaaPhase::reserve_slot;
}

void post_replicate(Client& client, SynraFaaOpCtx& op, const RegisteredSynraFaaBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
    const uint64_t entry = pack_synra_faa_entry(static_cast<uint16_t>(client.id()));

    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = std::numeric_limits<uint64_t>::max() - 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, SynraFaaPhase::replicate, static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = conns[i].addr + synra_faa_log_slot_offset(op.reserved_slot);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = EMPTY_SLOT;
        wr.wr.atomic.swap = entry;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("synra_faa pipeline: replicate CAS post failed");
        }
    }

    op.phase = SynraFaaPhase::replicate;
    op.responses = 0;
    op.successes = 0;
}

} // namespace

SynraFaaPipelineConfig load_synra_faa_pipeline_config() {
    SynraFaaPipelineConfig config{};
    config.active_window = std::max<size_t>(1, SYNRA_FAA_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, SYNRA_FAA_CQ_BATCH);
    config.log_capacity = SYNRA_FAA_LOG_CAPACITY;
    return config;
}

size_t synra_faa_pipeline_client_buffer_size(const SynraFaaPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t reserve_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t replicate_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    return align_up(reserve_bytes + replicate_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_synra_faa_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SynraFaaPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("synra_faa pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("synra_faa pipeline: active window exceeds wr_id slot encoding");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<SynraFaaOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.phase = SynraFaaPhase::idle;
        op.reserved_slot = 0;
        op.responses = 0;
        op.successes = 0;
        op.latency_index = submitted;
        op.reserve_result = &buffers.reserve_results[slot];
        op.started_at = std::chrono::steady_clock::now();
        post_reserve_slot(client, op);
        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("synra_faa pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "synra_faa pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("synra_faa pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const SynraFaaPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            if (phase == SynraFaaPhase::reserve_slot) {
                op.reserved_slot = *op.reserve_result;
                if (op.reserved_slot >= config.log_capacity) {
                    throw std::runtime_error("synra_faa pipeline: reserved slot exceeds flat log capacity");
                }
                post_replicate(client, op, buffers);
                continue;
            }

            if (phase == SynraFaaPhase::replicate) {
                auto* replicate_results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (idx >= conns.size()) {
                    throw std::runtime_error("synra_faa pipeline: replicate replica index out of range");
                }
                op.responses++;
                if (replicate_results[idx] == EMPTY_SLOT) {
                    op.successes++;
                }

                if (op.successes >= QUORUM) {
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    lock_counts[0]++;
                    op.active = false;
                    op.phase = SynraFaaPhase::idle;
                    completed++;
                    active--;

                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(slot);
                    }
                    continue;
                }

                const uint32_t remaining = static_cast<uint32_t>(conns.size()) - op.responses;
                if (op.successes + remaining < QUORUM) {
                    throw std::runtime_error("synra_faa pipeline: flat log CAS failed to reach quorum");
                }
            }
        }
    }
}
