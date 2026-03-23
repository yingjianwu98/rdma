#pragma once

// Client-side Synra replicated watch pipeline:
// - Register watcher using replicated RDMA FAA (get slot from super-quorum)
// - Write watcher ID to replicated watcher array with CAS
//
// Memory layout per watchable object:
//   [0:8)   watcher_count (FAA target, replicated across nodes)
//   [8:16)  data_version (for future notification use)
//   [16:N)  watcher_ids[] array (replicated watcher ID slots)

#include <cstddef>
#include <cstdint>

class Client;

struct WatchPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
    bool shard_owner = true;
};

[[nodiscard]] WatchPipelineConfig load_watch_pipeline_config();
[[nodiscard]] size_t watch_pipeline_client_buffer_size(const WatchPipelineConfig& config);

void run_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const WatchPipelineConfig& config
);
