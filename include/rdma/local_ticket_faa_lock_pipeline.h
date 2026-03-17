#pragma once

#include <cstddef>
#include <cstdint>

class Client;

struct LocalTicketFaaLockPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
    bool replicate_with_cas = true;
};

[[nodiscard]] LocalTicketFaaLockPipelineConfig load_local_ticket_faa_lock_pipeline_config();
[[nodiscard]] size_t local_ticket_faa_lock_pipeline_client_buffer_size(const LocalTicketFaaLockPipelineConfig& config);

void run_local_ticket_faa_lock_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const LocalTicketFaaLockPipelineConfig& config
);
