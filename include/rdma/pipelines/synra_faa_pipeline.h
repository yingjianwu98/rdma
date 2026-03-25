#pragma once

// Client-side one-sided FAA primitive with flat-log quorum CAS replication.

#include <cstddef>
#include <cstdint>

class Client;

struct SynraFaaPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    size_t log_capacity = 0;
};

[[nodiscard]] SynraFaaPipelineConfig load_synra_faa_pipeline_config();
[[nodiscard]] size_t synra_faa_pipeline_client_buffer_size(const SynraFaaPipelineConfig& config);

void run_synra_faa_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SynraFaaPipelineConfig& config
);
