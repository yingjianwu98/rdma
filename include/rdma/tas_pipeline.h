#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

class Client;

struct TasPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
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
    uint64_t learn_loser_restart = 0;
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

inline TasPipelineStats& operator+=(TasPipelineStats& lhs, const TasPipelineStats& rhs) {
    lhs.discover_posts += rhs.discover_posts;
    lhs.discover_cqes += rhs.discover_cqes;
    lhs.commit_posts += rhs.commit_posts;
    lhs.commit_cqes += rhs.commit_cqes;
    lhs.commit_superquorum_wins += rhs.commit_superquorum_wins;
    lhs.learn_posts += rhs.learn_posts;
    lhs.learn_cqes += rhs.learn_cqes;
    lhs.learn_quorum_winner += rhs.learn_quorum_winner;
    lhs.learn_lowest_id_winner += rhs.learn_lowest_id_winner;
    lhs.learn_empty_restart += rhs.learn_empty_restart;
    lhs.learn_loser_restart += rhs.learn_loser_restart;
    lhs.discover_odd_restart += rhs.discover_odd_restart;
    lhs.advance_acquire_posts += rhs.advance_acquire_posts;
    lhs.advance_release_posts += rhs.advance_release_posts;
    lhs.advance_acquire_cqes += rhs.advance_acquire_cqes;
    lhs.advance_release_cqes += rhs.advance_release_cqes;
    lhs.empty_polls += rhs.empty_polls;
    lhs.nonempty_polls += rhs.nonempty_polls;
    lhs.cqes_polled += rhs.cqes_polled;
    lhs.active_ops_hwm = std::max(lhs.active_ops_hwm, rhs.active_ops_hwm);
    return lhs;
}

[[nodiscard]] TasPipelineConfig load_tas_pipeline_config();
[[nodiscard]] size_t tas_pipeline_client_buffer_size(const TasPipelineConfig& config);

void run_tas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TasPipelineConfig& config,
    TasPipelineStats* out_stats = nullptr
);
