#include "rdma/tas_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
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
    advance_acquire = 4,
    advance_release = 5,
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
    uint32_t response_target = 0;
    uint32_t commit_wins = 0;
    uint32_t advance_acquire_pending = 0;
    uint32_t advance_release_pending = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

struct TasPipelineStats {
    uint64_t discover_posts = 0;
    uint64_t discover_cqes = 0;
    uint64_t commit_posts = 0;
    uint64_t commit_cqes = 0;
    uint64_t commit_superquorum_wins = 0;
    uint64_t learn_posts = 0;
    uint64_t learn_cqes = 0;
    uint64_t learn_quorum_winner = 0;
    uint64_t learn_lowest_id_winner = 0;
    uint64_t learn_empty_restart = 0;
    uint64_t discover_odd_restart = 0;
    uint64_t advance_acquire_posts = 0;
    uint64_t advance_release_posts = 0;
    uint64_t advance_acquire_cqes = 0;
    uint64_t advance_release_cqes = 0;
    uint64_t empty_polls = 0;
    uint64_t nonempty_polls = 0;
    uint64_t cqes_polled = 0;
    uint64_t active_ops_hwm = 0;
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

void post_discover(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers, TasPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* values = row_ptr(buffers.discover_values, op.slot, conns.size());
    const size_t discover_reads = std::min<size_t>(QUORUM, conns.size());
    const size_t start = conns.empty() ? 0 : (static_cast<size_t>(op.req_id) % conns.size());

    std::fill_n(values, conns.size(), TAS_SENTINEL);

    for (size_t r = 0; r < discover_reads; ++r) {
        const size_t i = (start + r) % conns.size();

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
    op.response_target = static_cast<uint32_t>(discover_reads);
    op.frontier = 0;
    stats.discover_posts++;
}

void post_commit(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers, TasPipelineStats& stats) {
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
    op.response_target = static_cast<uint32_t>(conns.size());
    op.commit_wins = 0;
    stats.commit_posts++;
}

void post_learn_all(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers, TasPipelineStats& stats) {
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
    op.response_target = static_cast<uint32_t>(conns.size());
    stats.learn_posts++;
}

void post_advance_acquire(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers, TasPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* acquire_values = row_ptr(buffers.advance_acquire_results, op.slot, conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        acquire_values[i] = TAS_SENTINEL;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&acquire_values[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TasPhase::advance_acquire, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.atomic.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = op.frontier;
        wr.wr.atomic.swap = op.held_slot;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("TAS pipeline: advance acquire post failed");
        }
    }

    op.phase = TasPhase::advance_acquire;
    op.advance_acquire_pending = static_cast<uint32_t>(conns.size());
    stats.advance_acquire_posts++;
}

void post_advance_release(Client& client, TasOpCtx& op, const RegisteredTasBuffers& buffers, TasPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* release_values = row_ptr(buffers.advance_release_results, op.slot, conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        release_values[i] = TAS_SENTINEL;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&release_values[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TasPhase::advance_release, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.atomic.remote_addr = conns[i].addr + lock_control_offset(op.lock_id);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = op.held_slot;
        wr.wr.atomic.swap = op.held_slot + 1;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("TAS pipeline: advance release post failed");
        }
    }

    op.phase = TasPhase::advance_release;
    op.advance_release_pending = static_cast<uint32_t>(conns.size());
    stats.advance_release_posts++;
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
    config.active_window = std::max<size_t>(1, TAS_ACTIVE_WINDOW);
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
    const bool tas_stats_enabled = get_uint_env_or("TAS_STATS", 1) != 0;
    const bool tas_stats_print_idle = get_uint_env_or("TAS_STATS_PRINT_IDLE", 0) != 0;
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
    TasPipelineStats stats{};
    TasPipelineStats prev_stats{};
    auto last_stats_at = std::chrono::steady_clock::now();

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto print_stats = [&](const TasPipelineStats& delta, const double interval_s) {
        const uint64_t total_polls = delta.empty_polls + delta.nonempty_polls;
        const double nonempty_poll_pct = total_polls == 0
            ? 0.0
            : 100.0 * static_cast<double>(delta.nonempty_polls) / static_cast<double>(total_polls);

        std::ostringstream line1;
        line1 << std::fixed << std::setprecision(2)
              << "[TasStats client=" << client.id() << "] interval=" << interval_s << "s"
              << " | posts: discover=" << delta.discover_posts
              << " commit=" << delta.commit_posts
              << " learn=" << delta.learn_posts
              << " adv_acq=" << delta.advance_acquire_posts
              << " adv_rel=" << delta.advance_release_posts;

        std::ostringstream line2;
        line2 << std::fixed << std::setprecision(0)
              << "[TasStats client=" << client.id() << "] cq"
              << " | discover=" << delta.discover_cqes
              << " commit=" << delta.commit_cqes
              << " learn=" << delta.learn_cqes
              << " adv_acq=" << delta.advance_acquire_cqes
              << " adv_rel=" << delta.advance_release_cqes
              << " total=" << delta.cqes_polled
              << " | polls empty=" << delta.empty_polls
              << " nonempty=" << delta.nonempty_polls
              << " nonempty%=" << std::fixed << std::setprecision(1) << nonempty_poll_pct;

        std::ostringstream line3;
        line3 << std::fixed << std::setprecision(0)
              << "[TasStats client=" << client.id() << "] decisions"
              << " | superq_wins=" << delta.commit_superquorum_wins
              << " learn_quorum=" << delta.learn_quorum_winner
              << " learn_lowest=" << delta.learn_lowest_id_winner
              << " learn_empty_restart=" << delta.learn_empty_restart
              << " discover_odd_restart=" << delta.discover_odd_restart;

        std::ostringstream line4;
        line4 << "[TasStats client=" << client.id() << "] active"
              << " | current=" << active
              << " interval_hwm=" << stats.active_ops_hwm;

        std::cout << line1.str() << "\n"
                  << line2.str() << "\n"
                  << line3.str() << "\n"
                  << line4.str() << "\n";
    };

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
        op.response_target = 0;
        op.commit_wins = 0;
        op.advance_acquire_pending = 0;
        op.advance_release_pending = 0;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        post_discover(client, op, buffers, stats);
        submitted++;
        active++;
        stats.active_ops_hwm = std::max<uint64_t>(stats.active_ops_hwm, active);
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
            stats.empty_polls++;
            if (tas_stats_enabled) {
                const auto now = std::chrono::steady_clock::now();
                if (now - last_stats_at >= std::chrono::seconds(1)) {
                    TasPipelineStats delta{};
                    delta.discover_posts = stats.discover_posts - prev_stats.discover_posts;
                    delta.discover_cqes = stats.discover_cqes - prev_stats.discover_cqes;
                    delta.commit_posts = stats.commit_posts - prev_stats.commit_posts;
                    delta.commit_cqes = stats.commit_cqes - prev_stats.commit_cqes;
                    delta.commit_superquorum_wins = stats.commit_superquorum_wins - prev_stats.commit_superquorum_wins;
                    delta.learn_posts = stats.learn_posts - prev_stats.learn_posts;
                    delta.learn_cqes = stats.learn_cqes - prev_stats.learn_cqes;
                    delta.learn_quorum_winner = stats.learn_quorum_winner - prev_stats.learn_quorum_winner;
                    delta.learn_lowest_id_winner = stats.learn_lowest_id_winner - prev_stats.learn_lowest_id_winner;
                    delta.learn_empty_restart = stats.learn_empty_restart - prev_stats.learn_empty_restart;
                    delta.discover_odd_restart = stats.discover_odd_restart - prev_stats.discover_odd_restart;
                    delta.advance_acquire_posts = stats.advance_acquire_posts - prev_stats.advance_acquire_posts;
                    delta.advance_release_posts = stats.advance_release_posts - prev_stats.advance_release_posts;
                    delta.advance_acquire_cqes = stats.advance_acquire_cqes - prev_stats.advance_acquire_cqes;
                    delta.advance_release_cqes = stats.advance_release_cqes - prev_stats.advance_release_cqes;
                    delta.empty_polls = stats.empty_polls - prev_stats.empty_polls;
                    delta.nonempty_polls = stats.nonempty_polls - prev_stats.nonempty_polls;
                    delta.cqes_polled = stats.cqes_polled - prev_stats.cqes_polled;
                    const bool active_interval = delta.discover_posts != 0 || delta.commit_posts != 0 || delta.learn_posts != 0
                        || delta.advance_acquire_posts != 0 || delta.advance_release_posts != 0
                        || delta.cqes_polled != 0;
                    if (active_interval || tas_stats_print_idle) {
                        const double interval_s = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_stats_at).count();
                        print_stats(delta, interval_s);
                    }
                    prev_stats = stats;
                    last_stats_at = now;
                }
            }
            continue;
        }
        stats.nonempty_polls++;
        stats.cqes_polled += static_cast<uint64_t>(polled);

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

            if (phase == TasPhase::advance_acquire) {
                stats.advance_acquire_cqes++;
                if (op.advance_acquire_pending == 0) {
                    continue;
                }
                op.advance_acquire_pending--;
                if (op.advance_acquire_pending == 0) {
                    post_advance_release(client, op, buffers, stats);
                }
                continue;
            }

            if (phase == TasPhase::advance_release) {
                stats.advance_release_cqes++;
                if (op.advance_release_pending == 0) {
                    continue;
                }
                op.advance_release_pending--;
                if (op.advance_release_pending == 0) {
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
                stats.commit_cqes++;
                auto* values = row_ptr(buffers.commit_results, op.slot, conns.size());
                if (values[wc.wr_id & 0xFFu] == EMPTY_SLOT) {
                    op.commit_wins++;
                }
            } else if (phase == TasPhase::discover) {
                stats.discover_cqes++;
                auto* values = row_ptr(buffers.discover_values, op.slot, conns.size());
                const uint64_t value = values[wc.wr_id & 0xFFu];
                if (value != TAS_SENTINEL) {
                    op.frontier = std::max(op.frontier, value);
                }
            } else if (phase == TasPhase::learn) {
                stats.learn_cqes++;
            }

            if (op.responses < op.response_target) {
                continue;
            }

            if (phase == TasPhase::discover) {
                if ((op.frontier & 1ULL) != 0) {
                    stats.discover_odd_restart++;
                    post_discover(client, op, buffers, stats);
                } else {
                    op.candidate_slot = op.frontier + 1;
                    post_commit(client, op, buffers, stats);
                }
                continue;
            }

            if (phase == TasPhase::commit) {
                if (op.commit_wins >= SUPER_QUORUM) {
                    stats.commit_superquorum_wins++;
                    op.held_slot = op.candidate_slot;
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    post_advance_acquire(client, op, buffers, stats);
                } else {
                    post_learn_all(client, op, buffers, stats);
                }
                continue;
            }

            if (phase == TasPhase::learn) {
                auto* values = row_ptr(buffers.learn_values, op.slot, conns.size());
                const uint64_t winner = select_winner(values, conns.size());
                if (winner == EMPTY_SLOT) {
                    stats.learn_empty_restart++;
                    post_discover(client, op, buffers, stats);
                } else if (winner == op.proposer_id) {
                    bool had_quorum = false;
                    for (size_t r = 0; r < conns.size(); ++r) {
                        size_t count = 0;
                        for (size_t c = 0; c < conns.size(); ++c) {
                            if (values[c] == values[r] && values[r] != TAS_SENTINEL && values[r] != EMPTY_SLOT) {
                                count++;
                            }
                        }
                        if (count >= QUORUM) {
                            had_quorum = true;
                            break;
                        }
                    }
                    if (had_quorum) {
                        stats.learn_quorum_winner++;
                    } else {
                        stats.learn_lowest_id_winner++;
                    }
                    op.held_slot = op.candidate_slot;
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    post_advance_acquire(client, op, buffers, stats);
                } else {
                    stats.learn_empty_restart++;
                    post_discover(client, op, buffers, stats);
                }
            }
        }

        if (tas_stats_enabled) {
            const auto now = std::chrono::steady_clock::now();
            if (now - last_stats_at >= std::chrono::seconds(1)) {
                TasPipelineStats delta{};
                delta.discover_posts = stats.discover_posts - prev_stats.discover_posts;
                delta.discover_cqes = stats.discover_cqes - prev_stats.discover_cqes;
                delta.commit_posts = stats.commit_posts - prev_stats.commit_posts;
                delta.commit_cqes = stats.commit_cqes - prev_stats.commit_cqes;
                delta.commit_superquorum_wins = stats.commit_superquorum_wins - prev_stats.commit_superquorum_wins;
                delta.learn_posts = stats.learn_posts - prev_stats.learn_posts;
                delta.learn_cqes = stats.learn_cqes - prev_stats.learn_cqes;
                delta.learn_quorum_winner = stats.learn_quorum_winner - prev_stats.learn_quorum_winner;
                delta.learn_lowest_id_winner = stats.learn_lowest_id_winner - prev_stats.learn_lowest_id_winner;
                delta.learn_empty_restart = stats.learn_empty_restart - prev_stats.learn_empty_restart;
                delta.discover_odd_restart = stats.discover_odd_restart - prev_stats.discover_odd_restart;
                delta.advance_acquire_posts = stats.advance_acquire_posts - prev_stats.advance_acquire_posts;
                delta.advance_release_posts = stats.advance_release_posts - prev_stats.advance_release_posts;
                delta.advance_acquire_cqes = stats.advance_acquire_cqes - prev_stats.advance_acquire_cqes;
                delta.advance_release_cqes = stats.advance_release_cqes - prev_stats.advance_release_cqes;
                delta.empty_polls = stats.empty_polls - prev_stats.empty_polls;
                delta.nonempty_polls = stats.nonempty_polls - prev_stats.nonempty_polls;
                delta.cqes_polled = stats.cqes_polled - prev_stats.cqes_polled;
                const bool active_interval = delta.discover_posts != 0 || delta.commit_posts != 0 || delta.learn_posts != 0
                    || delta.advance_acquire_posts != 0 || delta.advance_release_posts != 0 || delta.cqes_polled != 0;
                if (active_interval || tas_stats_print_idle) {
                    const double interval_s = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_stats_at).count();
                    print_stats(delta, interval_s);
                }
                prev_stats = stats;
                last_stats_at = now;
            }
        }
    }
}
