#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

class Client;

struct FaaPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
};

struct FaaPipelineStats {
    uint64_t faa_ticket_posts = 0;
    uint64_t faa_ticket_cqes = 0;
    uint64_t replicate_posts = 0;
    uint64_t replicate_cqes = 0;
    uint64_t replicate_quorum_wins = 0;
    uint64_t wait_round_posts = 0;
    uint64_t wait_round_cqes = 0;
    uint64_t wait_round_retries = 0;
    uint64_t predecessor_quorum_done = 0;
    uint64_t notify_hits = 0;
    uint64_t notify_spin_entries = 0;
    uint64_t notify_spin_iterations = 0;
    uint64_t notify_spin_hits = 0;
    uint64_t notify_spin_exhausted = 0;
    uint64_t mark_done_posts = 0;
    uint64_t mark_done_cqes = 0;
    uint64_t successor_read_posts = 0;
    uint64_t successor_read_cqes = 0;
    uint64_t successor_learn_quorum = 0;
    uint64_t successor_learned_while_waiting = 0;
    uint64_t successor_learned_on_unlock = 0;
    uint64_t retire_no_successor = 0;
    uint64_t notify_posts = 0;
    uint64_t notify_cqes = 0;
    uint64_t empty_polls = 0;
    uint64_t nonempty_polls = 0;
    uint64_t cqes_polled = 0;
    uint64_t active_ops_hwm = 0;
};

inline FaaPipelineStats& operator+=(FaaPipelineStats& lhs, const FaaPipelineStats& rhs) {
    lhs.faa_ticket_posts += rhs.faa_ticket_posts;
    lhs.faa_ticket_cqes += rhs.faa_ticket_cqes;
    lhs.replicate_posts += rhs.replicate_posts;
    lhs.replicate_cqes += rhs.replicate_cqes;
    lhs.replicate_quorum_wins += rhs.replicate_quorum_wins;
    lhs.wait_round_posts += rhs.wait_round_posts;
    lhs.wait_round_cqes += rhs.wait_round_cqes;
    lhs.wait_round_retries += rhs.wait_round_retries;
    lhs.predecessor_quorum_done += rhs.predecessor_quorum_done;
    lhs.notify_hits += rhs.notify_hits;
    lhs.notify_spin_entries += rhs.notify_spin_entries;
    lhs.notify_spin_iterations += rhs.notify_spin_iterations;
    lhs.notify_spin_hits += rhs.notify_spin_hits;
    lhs.notify_spin_exhausted += rhs.notify_spin_exhausted;
    lhs.mark_done_posts += rhs.mark_done_posts;
    lhs.mark_done_cqes += rhs.mark_done_cqes;
    lhs.successor_read_posts += rhs.successor_read_posts;
    lhs.successor_read_cqes += rhs.successor_read_cqes;
    lhs.successor_learn_quorum += rhs.successor_learn_quorum;
    lhs.successor_learned_while_waiting += rhs.successor_learned_while_waiting;
    lhs.successor_learned_on_unlock += rhs.successor_learned_on_unlock;
    lhs.retire_no_successor += rhs.retire_no_successor;
    lhs.notify_posts += rhs.notify_posts;
    lhs.notify_cqes += rhs.notify_cqes;
    lhs.empty_polls += rhs.empty_polls;
    lhs.nonempty_polls += rhs.nonempty_polls;
    lhs.cqes_polled += rhs.cqes_polled;
    lhs.active_ops_hwm = std::max(lhs.active_ops_hwm, rhs.active_ops_hwm);
    return lhs;
}

[[nodiscard]] FaaPipelineConfig load_faa_pipeline_config();
[[nodiscard]] size_t faa_pipeline_client_buffer_size(const FaaPipelineConfig& config);

void run_faa_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const FaaPipelineConfig& config,
    FaaPipelineStats* out_stats = nullptr
);
