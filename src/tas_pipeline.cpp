#include "rdma/tas_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"

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

constexpr uint64_t TAS_SENTINEL = 0xFEFEFEFEFEFEFEFEULL;
constexpr size_t TAS_CONN_BITS = 8;

enum class TasPhase : uint8_t {
    idle = 0,
    discover = 1,
    commit = 2,
    learn = 3,
    advance = 4,
};

struct RegisteredTasBuffers {
    uint64_t* discover_values = nullptr;
    uint64_t* commit_results = nullptr;
    uint64_t* learn_values = nullptr;
    uint64_t* advance_acquire_results = nullptr;
    uint64_t* advance_release_results = nullptr;
};

struct TasOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t req_id = 0;
    uint64_t proposer_id = 0;
    TasPhase phase = TasPhase::idle;
    uint64_t frontier = 0;
    uint64_t candidate_slot = 0;
    uint64_t held_slot = 0;
    uint32_t responses = 0;
    uint32_t commit_wins = 0;
    uint32_t advance_pending = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

class ZipfLockPicker {
public:
    explicit ZipfLockPicker(const double skew)
        : skew_(std::max(skew, 0.0))
        , uniform_(0, MAX_LOCKS - 1) {
        if (skew_ > 0.0) {
            std::vector<double> weights(MAX_LOCKS);
            for (size_t i = 0; i < MAX_LOCKS; ++i) {
                weights[i] = 1.0 / std::pow(static_cast<double>(i + 1), skew_);
            }
            zipf_ = std::discrete_distribution<uint32_t>(weights.begin(), weights.end());
            use_zipf_ = true;
        }
    }

    uint32_t next() {
        return use_zipf_ ? zipf_(rng_) : uniform_(rng_);
    }

private:
    double skew_;
    bool use_zipf_ = false;
    std::mt19937 rng_{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> uniform_;
    std::discrete_distribution<uint32_t> zipf_;
};

uint64_t make_proposer_id(const uint16_t client_id, const uint16_t active_slot, const uint32_t req_id) {
    return (static_cast<uint64_t>(client_id) << 48)
         | (static_cast<uint64_t>(active_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

uint64_t encode_wr_id(const TasOpCtx& op, const TasPhase phase, const uint8_t conn_index) {
    return (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 16)
         | (static_cast<uint64_t>(phase) << TAS_CONN_BITS)
         | static_cast<uint64_t>(conn_index);
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu);
}

TasPhase wr_phase(const uint64_t wr_id) {
    return static_cast<TasPhase>((wr_id >> TAS_CONN_BITS) & 0xFFu);
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
    const size_t per_matrix_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);

    buffers.discover_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += per_matrix_bytes;
    buffers.commit_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += per_matrix_bytes;
    buffers.learn_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += per_matrix_bytes;
    buffers.advance_acquire_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += per_matrix_bytes;
    buffers.advance_release_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += per_matrix_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("TAS pipeline: registered client buffer too small");
    }

    return buffers;
}

void post_discover(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* values = row_ptr(buffers.discover_values, op.slot, conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        values[i] = 0;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&values[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TasPhase::discover, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("TAS pipeline: discover post failed");
        }
    }

    op.phase = TasPhase::discover;
    op.responses = 0;
}

void post_commit(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* values = row_ptr(buffers.commit_results, op.slot, conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        values[i] = TAS_SENTINEL;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&values[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TasPhase::commit, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.candidate_slot);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = EMPTY_SLOT;
        wr.wr.atomic.swap = op.proposer_id;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("TAS pipeline: commit post failed");
        }
    }

    op.phase = TasPhase::commit;
    op.responses = 0;
    op.commit_wins = 0;
}

void post_learn_all(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* values = row_ptr(buffers.learn_values, op.slot, conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        values[i] = TAS_SENTINEL;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&values[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TasPhase::learn, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.candidate_slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("TAS pipeline: learn post failed");
        }
    }

    op.phase = TasPhase::learn;
    op.responses = 0;
}

void post_advances(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* acquire_values = row_ptr(buffers.advance_acquire_results, op.slot, conns.size());
    auto* release_values = row_ptr(buffers.advance_release_results, op.slot, conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        acquire_values[i] = TAS_SENTINEL;
        release_values[i] = TAS_SENTINEL;

        ibv_sge sges[2]{};
        sges[0].addr = reinterpret_cast<uintptr_t>(&acquire_values[i]);
        sges[0].length = sizeof(uint64_t);
        sges[0].lkey = mr->lkey;
        sges[1].addr = reinterpret_cast<uintptr_t>(&release_values[i]);
        sges[1].length = sizeof(uint64_t);
        sges[1].lkey = mr->lkey;

        ibv_send_wr wrs[2]{}, *bad_wr = nullptr;
        wrs[0].wr_id = encode_wr_id(op, TasPhase::advance, static_cast<uint8_t>(i));
        wrs[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wrs[0].send_flags = 0;
        wrs[0].sg_list = &sges[0];
        wrs[0].num_sge = 1;
        wrs[0].wr.atomic.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
        wrs[0].wr.atomic.rkey = conns[i].rkey;
        wrs[0].wr.atomic.compare_add = op.frontier;
        wrs[0].wr.atomic.swap = op.held_slot;
        wrs[0].next = &wrs[1];

        wrs[1].wr_id = encode_wr_id(op, TasPhase::advance, static_cast<uint8_t>(i));
        wrs[1].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wrs[1].send_flags = IBV_SEND_SIGNALED;
        wrs[1].sg_list = &sges[1];
        wrs[1].num_sge = 1;
        wrs[1].wr.atomic.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
        wrs[1].wr.atomic.rkey = conns[i].rkey;
        wrs[1].wr.atomic.compare_add = op.held_slot;
        wrs[1].wr.atomic.swap = op.held_slot + 1;

        if (ibv_post_send(conns[i].id->qp, wrs, &bad_wr)) {
            throw std::runtime_error("TAS pipeline: advance post failed");
        }
    }

    op.phase = TasPhase::advance;
    op.advance_pending = static_cast<uint32_t>(conns.size());
}

uint64_t select_winner(const uint64_t* values, const size_t replica_count) {
    uint64_t quorum_winner = EMPTY_SLOT;
    uint64_t lowest_seen = EMPTY_SLOT;
    bool found_quorum = false;
    bool found_any = false;

    for (size_t i = 0; i < replica_count; ++i) {
        const uint64_t value = values[i];
        if (value == TAS_SENTINEL || value == EMPTY_SLOT) {
            continue;
        }

        found_any = true;
        lowest_seen = std::min(lowest_seen, value);

        size_t count = 0;
        for (size_t j = 0; j < replica_count; ++j) {
            if (values[j] == value) {
                count++;
            }
        }
        if (count >= QUORUM) {
            quorum_winner = found_quorum ? std::min(quorum_winner, value) : value;
            found_quorum = true;
        }
    }

    if (found_quorum) {
        return quorum_winner;
    }
    if (found_any) {
        return lowest_seen;
    }
    return EMPTY_SLOT;
}

}  // namespace

TasPipelineConfig load_tas_pipeline_config() {
    TasPipelineConfig config{};
    config.active_window = std::max<size_t>(1, get_uint_env_or("TAS_ACTIVE_WINDOW", 16));
    config.cq_batch = std::max<size_t>(1, get_uint_env_or("TAS_CQ_BATCH", 32));
    config.zipf_skew = get_double_env_or("TAS_ZIPF_SKEW", 0.0);
    return config;
}

size_t tas_pipeline_client_buffer_size(const TasPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t matrix_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    return align_up((matrix_bytes * 5) + PAGE_SIZE, PAGE_SIZE);
}

void run_tas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TasPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("TAS pipeline: no server connections");
    }
    if (config.active_window > 0xFFFFu) {
        throw std::runtime_error("TAS pipeline: active window exceeds wr_id slot capacity");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<TasOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = picker.next();
        op.req_id = next_req_id++;
        op.proposer_id = make_proposer_id(static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot), op.req_id);
        op.frontier = 0;
        op.candidate_slot = 0;
        op.held_slot = 0;
        op.responses = 0;
        op.commit_wins = 0;
        op.advance_pending = 0;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        post_discover(client, op, buffers);
        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("TAS pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "TAS pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("TAS pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const TasPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            if (phase == TasPhase::advance) {
                if (op.advance_pending == 0) {
                    continue;
                }
                op.advance_pending--;
                if (op.advance_pending == 0) {
                    lock_counts[op.lock_id]++;
                    op.active = false;
                    op.phase = TasPhase::idle;
                    completed++;
                    active--;
                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(slot);
                    }
                }
                continue;
            }

            op.responses++;
            if (phase == TasPhase::commit) {
                auto* values = row_ptr(buffers.commit_results, op.slot, conns.size());
                if (values[wc.wr_id & 0xFFu] == EMPTY_SLOT) {
                    op.commit_wins++;
                }
            }

            if (op.responses < conns.size()) {
                continue;
            }

            if (phase == TasPhase::discover) {
                auto* values = row_ptr(buffers.discover_values, op.slot, conns.size());
                uint64_t frontier = 0;
                for (size_t r = 0; r < conns.size(); ++r) {
                    frontier = std::max(frontier, values[r]);
                }
                op.frontier = frontier;
                if ((frontier & 1ULL) != 0) {
                    post_discover(client, op, buffers);
                } else {
                    op.candidate_slot = frontier + 1;
                    post_commit(client, op, buffers);
                }
                continue;
            }

            if (phase == TasPhase::commit) {
                if (op.commit_wins >= SUPER_QUORUM) {
                    op.held_slot = op.candidate_slot;
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    post_advances(client, op, buffers);
                } else {
                    post_learn_all(client, op, buffers);
                }
                continue;
            }

            if (phase == TasPhase::learn) {
                auto* values = row_ptr(buffers.learn_values, op.slot, conns.size());
                const uint64_t winner = select_winner(values, conns.size());
                if (winner == EMPTY_SLOT) {
                    post_discover(client, op, buffers);
                } else if (winner == op.proposer_id) {
                    op.held_slot = op.candidate_slot;
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    post_advances(client, op, buffers);
                } else {
                    post_discover(client, op, buffers);
                }
            }
        }
    }
}
