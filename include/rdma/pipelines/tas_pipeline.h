#pragma once

// Round-based TAS primitive: all contenders race on one owner CAS, losers fail
// fast, and the single winner quorum-replicates its client id.

#include <cstddef>
#include <cstdint>

class Client;

struct TasPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    size_t rounds = 0;
    size_t log_capacity = 0;
};

[[nodiscard]] TasPipelineConfig load_tas_pipeline_config();
[[nodiscard]] size_t tas_pipeline_client_buffer_size(const TasPipelineConfig& config);
[[nodiscard]] size_t tas_pipeline_latency_count(const TasPipelineConfig& config);

void run_tas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TasPipelineConfig& config
);
