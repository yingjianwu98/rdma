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
    uint64_t* replicate_results = nullptr;
    uint64_t* release_control_results = nullptr;
    uint64_t* release_log_results = nullptr;
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
    uint64_t logical_seq = 0;
    uint64_t physical_log_slot = 0;
    uint32_t replicate_responses = 0;
    uint32_t replicate_acks = 0;
    uint32_t release_log_responses = 0;
    uint32_t release_log_acks = 0;
    bool release_owner_log_done = false;
    bool release_log_quorum = false;
    size_t latency_index = 0;
    uint64_t* acquire_result = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

struct CasWrapDebugStats {
    uint64_t wrapped_claim_attempts = 0;
    uint64_t wrapped_claim_cqe_matches = 0;
    uint64_t wrapped_claim_quorum_success = 0;
    uint64_t wrapped_release_attempts = 0;
    uint64_t wrapped_release_cqe_matches = 0;
    uint64_t wrapped_release_quorum_success = 0;
    uint64_t wrapped_claim_quorum_fail = 0;
    uint64_t wrapped_release_quorum_fail = 0;
    uint32_t printed_lines = 0;
};

constexpr uint8_t kReleaseLogConnFlag = 0x80u;
constexpr uint64_t kLogLiveBit = 1ULL << 63;
constexpr uint64_t kLogSeqMask = (1ULL << 31) - 1;

uint64_t cas_logical_seq(const uint64_t held_slot) {
    return held_slot >> 1;
}

uint64_t cas_physical_log_slot(const uint64_t logical_seq) {
    return logical_seq % CAS_LOG_CAPACITY;
}

uint64_t pack_cas_log_free(const uint64_t logical_seq) {
    return (logical_seq & kLogSeqMask) << 32;
}

uint64_t pack_cas_log_live(const uint64_t logical_seq, const uint16_t client_id, const uint16_t active_slot) {
    return kLogLiveBit
         | ((logical_seq & kLogSeqMask) << 32)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(active_slot);
}

uint64_t cas_log_expected_free_value(const uint64_t logical_seq) {
    return logical_seq < CAS_LOG_CAPACITY ? EMPTY_SLOT : pack_cas_log_free(logical_seq);
}

uint64_t cas_log_next_free_value(const uint64_t logical_seq) {
    return pack_cas_log_free(logical_seq + CAS_LOG_CAPACITY);
}

bool cas_log_is_wrapped(const uint64_t logical_seq) {
    return logical_seq >= CAS_LOG_CAPACITY;
}

void ensure_log_seq_in_bounds(const uint32_t lock_id, const uint64_t logical_seq, const char* context) {
    if (cas_physical_log_slot(logical_seq) >= MAX_LOG_PER_LOCK) {
        throw std::runtime_error(
            std::string("CAS pipeline: wrapped log overflow during ") + context
            + " lock=" + std::to_string(lock_id)
            + " logical_seq=" + std::to_string(logical_seq)
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

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>(wr_id & 0xFFu);
}

size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

uint64_t* row_ptr(uint64_t* base, const uint32_t slot, const size_t replica_count) {
    return base + row_offset(slot, replica_count);
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

    const size_t replicate_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += replicate_bytes;

    const size_t release_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    buffers.release_control_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += release_bytes;
    buffers.release_log_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += release_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("CAS pipeline: registered client buffer too small");
    }

    return buffers;
}

void post_acquire(const Client& client, CasOpCtx& op) {
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
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.wr.atomic.remote_addr = conns[op.owner_node].addr + lock_control_offset(op.lock_id);
    wr.wr.atomic.rkey = conns[op.owner_node].rkey;
    wr.wr.atomic.compare_add = op.target_slot - 1;
    wr.wr.atomic.swap = op.target_slot;

    if (ibv_post_send(conns[op.owner_node].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("CAS pipeline: acquire post failed");
    }

    op.phase = OpPhase::acquire;
}

void post_replicate(const Client& client, CasOpCtx& op, const RegisteredCasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    op.logical_seq = cas_logical_seq(op.held_slot);
    op.physical_log_slot = cas_physical_log_slot(op.logical_seq);
    ensure_log_seq_in_bounds(op.lock_id, op.logical_seq, "replicate");
    const uint64_t      compare_value = cas_log_expected_free_value(op.logical_seq);
    const uint64_t swap_value = pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));

    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = EMPTY_SLOT - 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, OpPhase::replicate, static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.physical_log_slot);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = compare_value;
        wr.wr.atomic.swap = swap_value;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("CAS pipeline: replicate post failed");
        }
    }

    op.phase = OpPhase::replicate;
    op.replicate_responses = 0;
    op.replicate_acks = 0;
}

void post_release(
    Client& client,
    CasOpCtx& op,
    const RegisteredCasBuffers& buffers
) {
    const auto& conns = client.connections();
    const auto* mr = client.mr();
    auto* control_results = row_ptr(buffers.release_control_results, op.slot, conns.size());
    auto* log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
    const uint64_t live_value = pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));
    const uint64_t free_value = cas_log_next_free_value(op.logical_seq);
    const auto& owner = conns[op.owner_node];

    op.phase = OpPhase::release;
    op.release_log_responses = 0;
    op.release_log_acks = 0;
    op.release_owner_log_done = false;
    op.release_log_quorum = false;

    auto* control_result = control_results + op.owner_node;
    *control_result = op.held_slot + 1;

    ibv_sge control_sge{};
    control_sge.addr = reinterpret_cast<uintptr_t>(control_result);
    control_sge.length = sizeof(uint64_t);
    control_sge.lkey = mr->lkey;

    ibv_send_wr control_wr{}, *bad_wr = nullptr;
    control_wr.wr_id = encode_wr_id(op, OpPhase::release, static_cast<uint8_t>(op.owner_node));
    control_wr.sg_list = &control_sge;
    control_wr.num_sge = 1;
    control_wr.send_flags = 0;
    control_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    control_wr.send_flags = IBV_SEND_INLINE;
    control_wr.wr.atomic.remote_addr = owner.addr + lock_control_offset(op.lock_id);
    control_wr.wr.atomic.rkey = owner.rkey;
    control_wr.wr.atomic.compare_add = op.held_slot;
    control_wr.wr.atomic.swap = op.held_slot + 1;

    if (ibv_post_send(owner.id->qp, &control_wr, &bad_wr)) {
        throw std::runtime_error("CAS pipeline: release post failed");
    }

    for (size_t i = 0; i < conns.size(); ++i) {
        auto* log_result = log_results + i;
        *log_result = EMPTY_SLOT - 1;

        ibv_sge log_sge{};
        log_sge.addr = reinterpret_cast<uintptr_t>(log_result);
        log_sge.length = sizeof(uint64_t);
        log_sge.lkey = mr->lkey;

        ibv_send_wr log_wr{}, *bad_log = nullptr;
        log_wr.wr_id = encode_wr_id(op, OpPhase::release, static_cast<uint8_t>(kReleaseLogConnFlag | i));
        log_wr.sg_list = &log_sge;
        log_wr.num_sge = 1;
        log_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP ;
        log_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        log_wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.physical_log_slot);
        log_wr.wr.atomic.rkey = conns[i].rkey;
        log_wr.wr.atomic.compare_add = live_value;
        log_wr.wr.atomic.swap = free_value;

        if (ibv_post_send(conns[i].id->qp, &log_wr, &bad_log)) {
            throw std::runtime_error("CAS pipeline: release log post failed");
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
    config.wrap_debug = CAS_WRAP_DEBUG;
    config.wrap_debug_print_limit = CAS_WRAP_DEBUG_PRINT_LIMIT;
    return config;
}

size_t cas_pipeline_client_buffer_size(const CasPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t acquire_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t replicate_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    const size_t release_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    return align_up(acquire_bytes + replicate_bytes + release_bytes * 2 + PAGE_SIZE, PAGE_SIZE);
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
    CasWrapDebugStats wrap_debug_stats{};

    auto maybe_debug_wrap = [&](const std::string& msg) {
        if (!config.wrap_debug || wrap_debug_stats.printed_lines >= config.wrap_debug_print_limit) {
            return;
        }
        std::cout << "[CAS wrap client=" << client.id() << "] " << msg << "\n";
        wrap_debug_stats.printed_lines++;
    };

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
        op.phase = OpPhase::idle;
        op.target_slot = frontier_hints[op.lock_id];
        op.held_slot = 0;
        op.logical_seq = 0;
        op.physical_log_slot = 0;
        op.replicate_responses = 0;
        op.replicate_acks = 0;
        op.release_log_responses = 0;
        op.release_log_acks = 0;
        op.release_owner_log_done = false;
        op.release_log_quorum = false;
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
                    if (config.wrap_debug && cas_log_is_wrapped(cas_logical_seq(op.held_slot))) {
                        wrap_debug_stats.wrapped_claim_attempts++;
                        maybe_debug_wrap(
                            "claim attempt lock=" + std::to_string(op.lock_id)
                            + " held_slot=" + std::to_string(op.held_slot)
                            + " logical_seq=" + std::to_string(cas_logical_seq(op.held_slot))
                            + " physical=" + std::to_string(cas_physical_log_slot(cas_logical_seq(op.held_slot))));
                    }
                    post_replicate(client, op, buffers);
                } else {
                    op.target_slot = (result % 2 != 0) ? result + 2 : result + 1;
                    frontier_hints[op.lock_id] = op.target_slot;
                    post_acquire(client, op);
                }
                continue;
            }

            if (phase == OpPhase::replicate) {
                auto* replicate_results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                op.replicate_responses++;
                const bool wrapped = cas_log_is_wrapped(op.logical_seq);
                if (replicate_results[idx] == cas_log_expected_free_value(op.logical_seq)) {
                    op.replicate_acks++;
                    if (wrapped) {
                        wrap_debug_stats.wrapped_claim_cqe_matches++;
                    }
                } else if (wrapped) {
                    maybe_debug_wrap(
                        "claim mismatch lock=" + std::to_string(op.lock_id)
                        + " logical_seq=" + std::to_string(op.logical_seq)
                        + " physical=" + std::to_string(op.physical_log_slot)
                        + " replica=" + std::to_string(idx)
                        + " expected=" + std::to_string(cas_log_expected_free_value(op.logical_seq))
                        + " got=" + std::to_string(replicate_results[idx]));
                }
                if (op.replicate_acks >= QUORUM) {
                    if (wrapped) {
                        wrap_debug_stats.wrapped_claim_quorum_success++;
                        maybe_debug_wrap(
                            "claim quorum lock=" + std::to_string(op.lock_id)
                            + " logical_seq=" + std::to_string(op.logical_seq)
                            + " physical=" + std::to_string(op.physical_log_slot)
                            + " acks=" + std::to_string(op.replicate_acks));
                    }
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    frontier_hints[op.lock_id] = std::max(frontier_hints[op.lock_id], op.held_slot + 2);
                    post_release(client, op, buffers);
                    if (wrapped) {
                        wrap_debug_stats.wrapped_release_attempts++;
                    }
                    continue;
                }
                const uint32_t remaining = static_cast<uint32_t>(conns.size()) - op.replicate_responses;
                if (op.replicate_acks + remaining < QUORUM) {
                    if (wrapped) {
                        wrap_debug_stats.wrapped_claim_quorum_fail++;
                    }
                    throw std::runtime_error("CAS pipeline: wrapped log replicate failed to reach quorum");
                }
                continue;
            }

            if (phase == OpPhase::release) {
                const uint8_t raw_idx = wr_conn(wc.wr_id);
                const bool is_log_release = (raw_idx & kReleaseLogConnFlag) != 0;
                const size_t replica_index = static_cast<size_t>(raw_idx & ~kReleaseLogConnFlag);

                if (replica_index >= conns.size()) {
                    throw std::runtime_error("CAS pipeline: release replica index out of range");
                }

                if (is_log_release) {
                    auto* log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
                    op.release_log_responses++;
                    const bool wrapped = cas_log_is_wrapped(op.logical_seq);
                    if (log_results[replica_index]
                        == pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot))) {
                        op.release_log_acks++;
                        if (replica_index == op.owner_node) {
                            op.release_owner_log_done = true;
                        }
                        if (wrapped) {
                            wrap_debug_stats.wrapped_release_cqe_matches++;
                        }
                    } else if (wrapped) {
                        maybe_debug_wrap(
                            "release mismatch lock=" + std::to_string(op.lock_id)
                            + " logical_seq=" + std::to_string(op.logical_seq)
                            + " physical=" + std::to_string(op.physical_log_slot)
                            + " replica=" + std::to_string(replica_index)
                            + " expected=" + std::to_string(pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot)))
                            + " got=" + std::to_string(log_results[replica_index]));
                        if (replica_index == op.owner_node) {
                            throw std::runtime_error("CAS pipeline: owner wrapped log release CAS failed");
                        }
                    }
                    if (op.release_log_acks >= QUORUM) {
                        op.release_log_quorum = true;
                        if (wrapped) {
                            wrap_debug_stats.wrapped_release_quorum_success++;
                            maybe_debug_wrap(
                                "release quorum lock=" + std::to_string(op.lock_id)
                                + " logical_seq=" + std::to_string(op.logical_seq)
                                + " physical=" + std::to_string(op.physical_log_slot)
                                + " acks=" + std::to_string(op.release_log_acks));
                        }
                    } else {
                        const uint32_t remaining = static_cast<uint32_t>(conns.size()) - op.release_log_responses;
                        if (op.release_log_acks + remaining < QUORUM) {
                            if (wrapped) {
                                wrap_debug_stats.wrapped_release_quorum_fail++;
                            }
                            throw std::runtime_error("CAS pipeline: wrapped log release failed to reach quorum");
                        }
                    }
                } else {
                    throw std::runtime_error("CAS pipeline: unexpected control release completion");
                }

                if (op.release_owner_log_done && op.release_log_quorum) {
                    lock_counts[op.lock_id]++;
                    op.active = false;
                    op.phase = OpPhase::idle;
                    completed++;
                    active--;

                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(slot);
                    }
                }
                continue;
            }
        }
    }

    if (config.wrap_debug) {
        std::cout << "[CAS wrap client=" << client.id() << "] summary"
                  << " | claim_attempts=" << wrap_debug_stats.wrapped_claim_attempts
                  << " claim_cqe_matches=" << wrap_debug_stats.wrapped_claim_cqe_matches
                  << " claim_quorum_success=" << wrap_debug_stats.wrapped_claim_quorum_success
                  << " claim_quorum_fail=" << wrap_debug_stats.wrapped_claim_quorum_fail
                  << " release_attempts=" << wrap_debug_stats.wrapped_release_attempts
                  << " release_cqe_matches=" << wrap_debug_stats.wrapped_release_cqe_matches
                  << " release_quorum_success=" << wrap_debug_stats.wrapped_release_quorum_success
                  << " release_quorum_fail=" << wrap_debug_stats.wrapped_release_quorum_fail
                  << " printed_lines=" << wrap_debug_stats.printed_lines
                  << "\n";
    }
}
