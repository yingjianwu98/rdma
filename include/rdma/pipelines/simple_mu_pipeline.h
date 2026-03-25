#pragma once

// Client-side RPC pipeline for the simple_mu primitive.

#include <cstddef>
#include <cstdint>

class Client;

struct SimpleMuPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    uint32_t client_send_signal_every = 0;
};

[[nodiscard]] SimpleMuPipelineConfig load_simple_mu_pipeline_config();
[[nodiscard]] size_t simple_mu_pipeline_client_buffer_size(const SimpleMuPipelineConfig& config);

void run_simple_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SimpleMuPipelineConfig& config
);
