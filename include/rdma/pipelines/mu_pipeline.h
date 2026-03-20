#pragma once

// Client-side RPC pipeline for the leader-based MU design.

#include <cstddef>
#include <cstdint>

class Client;

struct MuPipelineConfig {
    size_t active_window;
    size_t cq_batch;
    uint32_t client_send_signal_every;
    double zipf_skew;
};

[[nodiscard]] MuPipelineConfig load_mu_pipeline_config();
[[nodiscard]] size_t mu_pipeline_client_buffer_size(const MuPipelineConfig& config);

void run_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MuPipelineConfig& config
);
