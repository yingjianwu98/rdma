#pragma once

#include <cstddef>
#include <cstdint>

class Client;

struct SimpleCasPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
    bool shard_owner = true;
    bool release_with_cas = false;
};

[[nodiscard]] SimpleCasPipelineConfig load_simple_cas_pipeline_config();
[[nodiscard]] size_t simple_cas_pipeline_client_buffer_size(const SimpleCasPipelineConfig& config);

void run_simple_cas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SimpleCasPipelineConfig& config
);
