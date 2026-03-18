#pragma once

#include <cstddef>
#include <cstdint>

class Client;

struct TicketFaaLockPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
    bool shard_owner = true;
    bool release_log_with_cas = true;
    uint32_t release_turn_mode = 2;
};

[[nodiscard]] TicketFaaLockPipelineConfig load_ticket_faa_lock_pipeline_config();
[[nodiscard]] size_t ticket_faa_lock_pipeline_client_buffer_size(const TicketFaaLockPipelineConfig& config);

void run_ticket_faa_lock_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TicketFaaLockPipelineConfig& config
);
