#pragma once

#include <cstddef>
#include <cstdint>

class Client;

struct FaaPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
};

[[nodiscard]] FaaPipelineConfig load_faa_pipeline_config();
[[nodiscard]] size_t faa_pipeline_client_buffer_size(const FaaPipelineConfig& config);

void run_faa_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const FaaPipelineConfig& config
);
