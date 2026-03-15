#pragma once

#include <cstddef>
#include <cstdint>

class Client;

struct CasPipelineConfig {
    size_t active_window;
    size_t cq_batch;
    double zipf_skew;
};

[[nodiscard]] CasPipelineConfig load_cas_pipeline_config();
[[nodiscard]] size_t cas_pipeline_client_buffer_size(const CasPipelineConfig& config);

void run_cas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const CasPipelineConfig& config
);
