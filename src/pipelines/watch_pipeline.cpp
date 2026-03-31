#include "rdma/pipelines/watch_pipeline.h"

// Synra watch pipeline: Single FAA on owner node + replicated watcher ID writes.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/resource.h>
#include <sched.h>
#include <immintrin.h>

namespace {

// CPU utilization tracking
struct CPUUsage {
    double user_ms;
    double system_ms;
    double total_ms() const { return user_ms + system_ms; }
};

CPUUsage get_thread_cpu_usage() {
    struct rusage usage;
    getrusage(RUSAGE_THREAD, &usage);
    return {
        .user_ms = usage.ru_utime.tv_sec * 1000.0 + usage.ru_utime.tv_usec / 1000.0,
        .system_ms = usage.ru_stime.tv_sec * 1000.0 + usage.ru_stime.tv_usec / 1000.0
    };
}

constexpr uint64_t kConnBits = 8;
constexpr uint64_t kRoundBits = 8;
constexpr uint64_t kPhaseBits = 8;
constexpr uint64_t kSlotBits = 16;
constexpr uint64_t kConnShift = 0;
constexpr uint64_t kRoundShift = kConnShift + kConnBits;
constexpr uint64_t kPhaseShift = kRoundShift + kRoundBits;
constexpr uint64_t kSlotShift = kPhaseShift + kPhaseBits;
constexpr uint64_t kGenerationShift = kSlotShift + kSlotBits;
constexpr uint64_t kConnMask = (1ULL << kConnBits) - 1;
constexpr uint64_t kRoundMask = (1ULL << kRoundBits) - 1;
constexpr uint64_t kPhaseMask = (1ULL << kPhaseBits) - 1;
constexpr uint64_t kSlotMask = (1ULL << kSlotBits) - 1;
constexpr uint64_t kGenerationMask = (1ULL << kSlotShift) - 1;

enum class WatchPhase : uint8_t {
    idle = 0,
    faa_slot = 1,          // Registration: FAA to get watcher slot from owner node
    write_id = 2,          // Registration: Write client ID to watcher array (super-quorum)
    read_count = 3,        // Notification: Read watcher count from owner
    read_watcher_ids = 4,  // Notification: Read all watcher IDs from owner
    notify_watchers = 5,   // Notification: Broadcast invalidations to all watchers
};

constexpr size_t MAX_NOTIFY_BATCH = 1024;  // Max watchers to notify per batch (matches Mu)

struct RegisteredWatchBuffers {
    uint64_t* faa_results = nullptr;         // FAA slot result (single per op)
    uint64_t* write_results = nullptr;       // Write completion results (per replica)
    uint64_t* count_result = nullptr;        // Watcher count read result
    uint64_t* watcher_ids_buffer = nullptr;  // Buffer for reading watcher IDs
    uint64_t* notify_results = nullptr;      // Notification write results
};

struct WatchOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t object_id = 0;
    uint32_t owner_node = 0;
    uint8_t round = 0;
    uint64_t watcher_slot = 0;           // Assigned slot from FAA
    uint64_t watcher_id = 0;             // Our unique watcher ID
    WatchPhase phase = WatchPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;

    // Notification phase fields
    uint64_t total_watchers = 0;         // Total watchers for this object
    uint32_t notify_sent = 0;            // Number of notifications sent
    uint32_t notify_completed = 0;       // Number of notifications completed

    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};

    // Detailed timing metrics for notification phase breakdown
    std::chrono::steady_clock::time_point read_count_start{};
    std::chrono::steady_clock::time_point read_count_end{};
    std::chrono::steady_clock::time_point read_watcher_ids_start{};
    std::chrono::steady_clock::time_point read_watcher_ids_end{};
    std::chrono::steady_clock::time_point post_notify_start{};
    std::chrono::steady_clock::time_point post_notify_end{};
    std::chrono::steady_clock::time_point wait_notify_start{};
    std::chrono::steady_clock::time_point wait_notify_end{};

    // Bottleneck diagnosis metrics
    uint64_t post_count = 0;              // Number of ibv_post_send calls
    uint64_t poll_attempts = 0;           // Number of ibv_poll_cq calls
    uint64_t poll_completions = 0;        // Total completions polled
    uint64_t max_pending = 0;             // Peak queue depth
    double post_min_us = 1e9;             // Fastest post
    double post_max_us = 0;               // Slowest post
    double post_sum_us = 0;               // Sum for average

    // CPU utilization tracking
    CPUUsage cpu_before_post{};
    CPUUsage cpu_after_post{};
    CPUUsage cpu_before_wait{};
    CPUUsage cpu_after_wait{};
};

// Encode a unique watcher ID from client ID, operation slot, and request ID.
uint64_t encode_watcher_id(const uint16_t client_id, const uint16_t op_slot, const uint32_t req_id) {
    return (static_cast<uint64_t>(client_id) << 47)
         | (static_cast<uint64_t>(op_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

// Encode generation, slot, phase, round, and connection index into one WR id.
uint64_t encode_wr_id(const WatchOpCtx& op, const WatchPhase phase, const uint8_t conn_index) {
    return ((static_cast<uint64_t>(op.generation) & kGenerationMask) << kGenerationShift)
         | ((static_cast<uint64_t>(op.slot) & kSlotMask) << kSlotShift)
         | ((static_cast<uint64_t>(phase) & kPhaseMask) << kPhaseShift)
         | ((static_cast<uint64_t>(op.round) & kRoundMask) << kRoundShift)
         | ((static_cast<uint64_t>(conn_index) & kConnMask) << kConnShift);
}

// Extract generation, slot, phase, round, and connection index from WR id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> kGenerationShift);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> kSlotShift) & kSlotMask);
}

WatchPhase wr_phase(const uint64_t wr_id) {
    return static_cast<WatchPhase>((wr_id >> kPhaseShift) & kPhaseMask);
}

uint8_t wr_round(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kRoundShift) & kRoundMask);
}

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kConnShift) & kConnMask);
}

// Compute row-major offset for per-op-by-replica buffer.
uint64_t* row_ptr(uint64_t* base, const uint32_t row, const size_t cols) {
    return base + (static_cast<size_t>(row) * cols);
}

// Map client buffer into per-op result arrays.
RegisteredWatchBuffers map_buffers(void* raw_buffer, const size_t buffer_size,
                                   const size_t active_window, const size_t num_replicas) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    RegisteredWatchBuffers buffers{};

    // FAA results: single result per op (not per replica)
    const size_t faa_bytes = align_up(active_window * sizeof(uint64_t), 64);
    const size_t write_bytes = align_up(active_window * num_replicas * sizeof(uint64_t), 64);
    const size_t count_bytes = align_up(active_window * sizeof(uint64_t), 64);
    const size_t ids_bytes = align_up(active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);
    const size_t notify_bytes = align_up(active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);

    buffers.faa_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += faa_bytes;
    buffers.write_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += write_bytes;
    buffers.count_result = reinterpret_cast<uint64_t*>(base + offset);
    offset += count_bytes;
    buffers.watcher_ids_buffer = reinterpret_cast<uint64_t*>(base + offset);
    offset += ids_bytes;
    buffers.notify_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += notify_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("watch pipeline: registered client buffer too small");
    }
    return buffers;
}

// Post FAA to owner node to get watcher slot assignment (single FAA, not replicated).
void post_faa_slot(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.faa_results[op.slot];

    op.round++;
    op.phase = WatchPhase::faa_slot;
    op.responses = 0;
    op.response_target = 1;

    // FAA only on owner node (like Synra paper)
    const auto& owner = conns[op.owner_node];
    *result = 0;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, WatchPhase::faa_slot, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.atomic.remote_addr = owner.addr + watch_counter_offset(op.object_id);
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("watch pipeline: FAA slot post failed");
    }
}

// Write watcher ID to super-quorum after getting slot.
void post_write_id(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.write_results, op.slot, conns.size());

    op.round++;
    op.phase = WatchPhase::write_id;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());

    const uint64_t write_offset = watch_id_slot_offset(op.object_id, op.watcher_slot);
    // if (op.slot == 0) {  // Log first operation only
    //     std::cerr << "[DEBUG] WRITE_ID: object_id=" << op.object_id
    //               << " watcher_slot=" << op.watcher_slot
    //               << " watch_id_slot_offset=" << write_offset
    //               << " (MAX_WATCHERS=" << MAX_WATCHERS_PER_OBJECT << ")" << std::endl;
    // }

    // Write our watcher ID to all nodes in parallel
    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = op.watcher_id;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, WatchPhase::write_id, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + write_offset;
        wr.wr.rdma.rkey = conns[i].rkey;

        // if (op.slot == 0 && i == 0) {
        //     std::cerr << "[DEBUG] WRITE_ID to node " << i << ": remote_addr=0x" << std::hex
        //               << wr.wr.rdma.remote_addr << std::dec
        //               << " rkey=" << wr.wr.rdma.rkey << std::endl;
        // }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("watch pipeline: write ID post failed");
        }
    }
}

// Read watcher count from owner node to start notification.
void post_read_count(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    uint64_t* result = &buffers.count_result[op.slot];
    *result = 0;

    op.round++;
    op.phase = WatchPhase::read_count;
    op.responses = 0;
    op.response_target = 1;

    // TIMING: Start read_count phase
    op.read_count_start = std::chrono::steady_clock::now();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, WatchPhase::read_count, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = owner.addr + watch_counter_offset(op.object_id);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("watch pipeline: read count post failed");
    }
}

// Read watcher IDs from owner node (batched if needed).
void post_read_watcher_ids(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];

    // Limit reads to MAX_NOTIFY_BATCH watchers at a time
    const uint64_t read_count = std::min(op.total_watchers, static_cast<uint64_t>(MAX_NOTIFY_BATCH));
    uint64_t* ids_buf = &buffers.watcher_ids_buffer[op.slot * MAX_NOTIFY_BATCH];

    op.round++;
    op.phase = WatchPhase::read_watcher_ids;
    op.responses = 0;
    op.response_target = 1;

    // TIMING: Start read_watcher_ids phase
    op.read_watcher_ids_start = std::chrono::steady_clock::now();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(ids_buf);
    sge.length = read_count * sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, WatchPhase::read_watcher_ids, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = owner.addr + watch_id_slot_offset(op.object_id, 0);  // Start from slot 0
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("watch pipeline: read watcher IDs post failed");
    }
}

// Broadcast invalidations to all watchers (write to metadata area to simulate).
void post_notify_watchers(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();

    // Calculate how many watchers remain to be notified
    const uint64_t watchers_remaining = op.total_watchers - op.notify_sent;
    const uint64_t notify_count = std::min(watchers_remaining, static_cast<uint64_t>(MAX_NOTIFY_BATCH));
    uint64_t* notify_buf = &buffers.notify_results[op.slot * MAX_NOTIFY_BATCH];

    op.round++;
    op.phase = WatchPhase::notify_watchers;
    op.responses = 0;
    const uint32_t batch_start = op.notify_sent;

    // TIMING: Start posting notify writes (CPU overhead)
    op.post_notify_start = std::chrono::steady_clock::now();
    op.cpu_before_post = get_thread_cpu_usage();

    // OPTIMIZATION: Batch post per-QP with linked work requests
    // Group writes by target QP and batch them to reduce ibv_post_send() overhead
    constexpr uint64_t SIGNAL_STRIDE = 128;
    const size_t num_qps = conns.size();

    // Allocate per-QP batches
    std::vector<std::vector<ibv_send_wr>> qp_wrs(num_qps);
    std::vector<std::vector<ibv_sge>> qp_sges(num_qps);

    // Pre-allocate reasonable capacity per QP
    for (size_t qp_idx = 0; qp_idx < num_qps; ++qp_idx) {
        qp_wrs[qp_idx].reserve(notify_count / num_qps + 64);
        qp_sges[qp_idx].reserve(notify_count / num_qps + 64);
    }

    uint64_t signaled_count = 0;

    // Build per-QP batches
    for (uint64_t i = 0; i < notify_count; ++i) {
        notify_buf[i] = 1;  // Invalidation flag

        const uint32_t target_node = static_cast<uint32_t>(i % conns.size());
        const bool should_signal = ((i % SIGNAL_STRIDE) == 0) || (i == notify_count - 1);

        // Setup SGE
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&notify_buf[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;
        qp_sges[target_node].push_back(sge);

        // Setup WR
        ibv_send_wr wr{};
        wr.wr_id = encode_wr_id(op, WatchPhase::notify_watchers, static_cast<uint8_t>(target_node));
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.sg_list = &qp_sges[target_node].back();
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[target_node].addr + WATCH_TABLE_SIZE + ((batch_start + i) * sizeof(uint64_t));
        wr.wr.rdma.rkey = conns[target_node].rkey;
        wr.send_flags = (should_signal ? IBV_SEND_SIGNALED : 0) | IBV_SEND_INLINE;
        wr.next = nullptr;  // Will link later
        qp_wrs[target_node].push_back(wr);

        if (should_signal) {
            signaled_count++;
        }
    }

    // Link WRs within each QP's batch and post
    uint64_t actually_posted = 0;
    for (size_t qp_idx = 0; qp_idx < num_qps; ++qp_idx) {
        if (qp_wrs[qp_idx].empty()) continue;

        // Link the WRs
        for (size_t i = 0; i < qp_wrs[qp_idx].size(); ++i) {
            qp_wrs[qp_idx][i].sg_list = &qp_sges[qp_idx][i];  // Fix pointer after vector resize
            if (i < qp_wrs[qp_idx].size() - 1) {
                qp_wrs[qp_idx][i].next = &qp_wrs[qp_idx][i + 1];
            }
        }

        // Post entire batch for this QP
        auto post_start = std::chrono::steady_clock::now();
        ibv_send_wr* bad_wr = nullptr;
        if (ibv_post_send(conns[qp_idx].id->qp, &qp_wrs[qp_idx][0], &bad_wr)) {
            std::cerr << "[Client " << client.id() << " error] watch pipeline: batch notify post failed for QP " << qp_idx
                      << " (posted " << actually_posted << "/" << notify_count << ")\n";
            continue;  // Try other QPs
        }
        auto post_end = std::chrono::steady_clock::now();

        // Track metrics
        double post_us = std::chrono::duration_cast<std::chrono::nanoseconds>(post_end - post_start).count() / 1000.0;
        op.post_count++;
        op.post_min_us = std::min(op.post_min_us, post_us);
        op.post_max_us = std::max(op.post_max_us, post_us);
        op.post_sum_us += post_us;

        actually_posted += qp_wrs[qp_idx].size();
        op.max_pending = std::max(op.max_pending, actually_posted - op.notify_completed);
    }

    // TIMING: End posting notify writes (CPU overhead)
    op.post_notify_end = std::chrono::steady_clock::now();
    op.cpu_after_post = get_thread_cpu_usage();

    // Update response_target to only expect signaled completions
    op.response_target = static_cast<uint32_t>(signaled_count);
    op.notify_sent += static_cast<uint32_t>(actually_posted);

    // TIMING: Start waiting for completions (NIC latency)
    op.wait_notify_start = std::chrono::steady_clock::now();
    op.cpu_before_wait = get_thread_cpu_usage();

    // If queue was completely full (posted 0), force completion to avoid infinite loop
    if (actually_posted == 0 && notify_count > 0) {
        op.notify_sent = op.total_watchers;  // Force completion
    }
}

} // namespace

// Load watch pipeline config from compile-time constants.
WatchPipelineConfig load_watch_pipeline_config() {
    WatchPipelineConfig config{};
    config.active_window = std::max<size_t>(1, WATCH_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, WATCH_CQ_BATCH);
    config.zipf_skew = WATCH_ZIPF_SKEW;
    config.shard_owner = WATCH_SHARD_OWNER;
    return config;
}

// Report required client buffer size for watch pipeline.
size_t watch_pipeline_client_buffer_size(const WatchPipelineConfig& config) {
    const size_t num_replicas = CLUSTER_NODES.size();
    const size_t faa_bytes = align_up(config.active_window * num_replicas * sizeof(uint64_t), 64);
    const size_t write_bytes = align_up(config.active_window * num_replicas * sizeof(uint64_t), 64);
    const size_t count_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t ids_bytes = align_up(config.active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);
    const size_t notify_bytes = align_up(config.active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);
    return align_up(faa_bytes + write_bytes + count_bytes + ids_bytes + notify_bytes + PAGE_SIZE, PAGE_SIZE);
}

// Main watch pipeline: two-phase benchmark (registration, then notification).
void run_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const WatchPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("watch pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("watch pipeline: active window exceeds wr_id slot encoding");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(),
                              config.active_window, conns.size());
    std::vector<WatchOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);  // Reuse for object selection

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 0;

    // Two-phase benchmark: all ops are registrations, then fixed number of notifications
    // Registration: Use all NUM_OPS operations to register watchers
    // Notification: Fixed 2000 total notifications to test notification performance
    constexpr size_t TOTAL_NOTIFICATIONS = 2000;  // Fixed across all experiments
    const size_t notification_ops = TOTAL_NOTIFICATIONS / TOTAL_CLIENTS;  // Per-client share = 250
    const size_t registration_ops = NUM_OPS_PER_CLIENT;  // All ops are registrations
    bool in_registration_phase = true;

    // Verification statistics
    uint64_t total_registrations = 0;
    uint64_t total_notifications_sent = 0;
    uint64_t total_watchers_seen = 0;
    uint64_t max_watchers = 0;
    uint64_t min_watchers = UINT64_MAX;
    size_t zero_watcher_objects = 0;
    uint64_t invalid_watcher_ids = 0;

    // Phase timing for separate throughput reporting
    auto registration_start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point registration_end_time;
    std::chrono::steady_clock::time_point notification_start_time;
    bool registration_timing_done = false;
    bool notification_timing_started = false;

    // Detailed notification phase metrics collection
    std::vector<uint64_t> read_count_latencies;
    std::vector<uint64_t> read_watcher_ids_latencies;
    std::vector<uint64_t> post_notify_latencies;
    std::vector<uint64_t> wait_notify_latencies;
    std::vector<uint64_t> notification_watcher_counts;  // Track watcher count per notification
    read_count_latencies.reserve(notification_ops);
    read_watcher_ids_latencies.reserve(notification_ops);
    post_notify_latencies.reserve(notification_ops);
    wait_notify_latencies.reserve(notification_ops);
    notification_watcher_counts.reserve(notification_ops);

    // Track phase-separated latency indices
    size_t registration_latency_start = 0;
    size_t notification_latency_start = registration_ops;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.object_id = picker.next();  // Random object (like lock_id)
        op.owner_node = config.shard_owner ? (op.object_id % conns.size()) : 0;
        op.phase = WatchPhase::idle;
        op.latency_index = submitted;
        op.watcher_id = encode_watcher_id(client.id(), static_cast<uint16_t>(slot), next_req_id++);
        op.started_at = std::chrono::steady_clock::now();

        if (in_registration_phase) {
            // Registration phase: FAA to get slot
            post_faa_slot(client, op, buffers);
        } else {
            // Notification phase: read watcher count
            if (!notification_timing_started) {
                notification_start_time = std::chrono::steady_clock::now();
                notification_timing_started = true;
            }
            op.notify_sent = 0;
            op.notify_completed = 0;
            post_read_count(client, op, buffers);
        }
        submitted++;
        active++;
    };

    // Fill pipeline
    const size_t total_ops = registration_ops + notification_ops;

    while (active < config.active_window && submitted < total_ops) {
        submit_op(active);
    }

    // Main completion loop with adaptive backoff
    uint32_t empty_polls = 0;
    while (completed < total_ops) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()),
                                      completions.data());
        if (polled < 0) {
            throw std::runtime_error("watch pipeline: CQ poll failed");
        }

        // Track polling metrics for all active notify_watchers operations
        for (auto& op : ops) {
            if (op.active && op.phase == WatchPhase::notify_watchers) {
                op.poll_attempts++;
                if (polled > 0) {
                    op.poll_completions += polled;
                }
            }
        }

        // Adaptive backoff: reduce CPU spinning when no completions
        if (polled > 0) {
            empty_polls = 0;  // Reset on successful poll
        } else {
            empty_polls++;
            if (empty_polls < 100) {
                // Phase 1: Tight spin for low latency (first 100 empty polls)
                continue;
            } else if (empty_polls < 1000) {
                // Phase 2: CPU pause hint to reduce power (next 900 polls)
                _mm_pause();
            } else {
                // Phase 3: Yield to OS scheduler after 1000 empty polls
                sched_yield();
            }
        }

        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                const uint32_t slot = wr_slot(wc.wr_id);
                const WatchPhase phase = wr_phase(wc.wr_id);
                const uint8_t conn_idx = wr_conn(wc.wr_id);
                const char* phase_name = "unknown";
                if (phase == WatchPhase::faa_slot) phase_name = "faa_slot";
                else if (phase == WatchPhase::write_id) phase_name = "write_id";
                else if (phase == WatchPhase::read_count) phase_name = "read_count";
                else if (phase == WatchPhase::read_watcher_ids) phase_name = "read_watcher_ids";
                else if (phase == WatchPhase::notify_watchers) phase_name = "notify_watchers";

                std::cerr << "[ERROR] WC failed: status=" << wc.status
                          << " vendor_err=" << wc.vendor_err
                          << " phase=" << phase_name
                          << " slot=" << slot
                          << " conn=" << static_cast<int>(conn_idx)
                          << " opcode=" << wc.opcode
                          << " byte_len=" << wc.byte_len << std::endl;
                throw std::runtime_error("watch pipeline: WC error status=" + std::to_string(wc.status));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("watch pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const WatchPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            const uint8_t conn_idx = wr_conn(wc.wr_id);
            op.responses++;

            if (phase == WatchPhase::faa_slot) {
                // Got slot from single FAA (no super-quorum checking needed)
                op.watcher_slot = buffers.faa_results[op.slot];

                // Now replicate watcher_id to all nodes
                post_write_id(client, op, buffers);
                continue;
            }

            if (phase == WatchPhase::write_id) {
                // Wait for quorum writes to complete
                if (op.responses >= QUORUM) {
                    // Track registration completion
                    total_registrations++;

                    // Complete write_id regardless of phase (handles late completions)
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    op.active = false;
                    op.phase = WatchPhase::idle;
                    completed++;
                    active--;

                    // Check if registration phase is complete - switch immediately when count reached
                    if (in_registration_phase && completed >= registration_ops) {
                        in_registration_phase = false;
                        if (!registration_timing_done) {
                            registration_end_time = std::chrono::steady_clock::now();
                            registration_timing_done = true;
                        }
                    }

                    if (submitted < total_ops) {
                        submit_op(slot);
                    }
                }
                continue;
            }

            if (phase == WatchPhase::read_count) {
                // TIMING: End read_count phase
                op.read_count_end = std::chrono::steady_clock::now();

                // Got watcher count, now read all watcher IDs (notification phase only)
                op.total_watchers = buffers.count_result[op.slot];

                // Track verification stats
                total_watchers_seen += op.total_watchers;
                max_watchers = std::max(max_watchers, op.total_watchers);
                if (op.total_watchers > 0) {
                    min_watchers = std::min(min_watchers, op.total_watchers);
                } else {
                    zero_watcher_objects++;
                }

                if (op.total_watchers > 0) {
                    post_read_watcher_ids(client, op, buffers);
                } else {
                    // No watchers (edge case), complete notification with zero broadcasts
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;
                    op.active = false;
                    op.phase = WatchPhase::idle;
                    completed++;
                    active--;
                    if (submitted < total_ops) {
                        submit_op(slot);
                    }
                }
                continue;
            }

            if (phase == WatchPhase::read_watcher_ids) {
                // TIMING: End read_watcher_ids phase
                op.read_watcher_ids_end = std::chrono::steady_clock::now();

                // Got watcher IDs, now broadcast notifications

                // Validate watcher IDs
                const uint64_t* watcher_ids = &buffers.watcher_ids_buffer[op.slot * MAX_NOTIFY_BATCH];
                for (size_t i = 0; i < op.total_watchers && i < MAX_NOTIFY_BATCH; ++i) {
                    // Extract client ID from watcher ID (top 17 bits)
                    const uint16_t client_from_id = static_cast<uint16_t>(watcher_ids[i] >> 47);
                    if (client_from_id >= TOTAL_CLIENTS) {
                        invalid_watcher_ids++;
                    }
                }

                post_notify_watchers(client, op, buffers);
                continue;
            }

            if (phase == WatchPhase::notify_watchers) {
                // Count completed notifications
                op.notify_completed++;

                if (op.notify_completed >= op.response_target) {
                    // TIMING: End waiting for completions (NIC latency)
                    op.wait_notify_end = std::chrono::steady_clock::now();
                    op.cpu_after_wait = get_thread_cpu_usage();
                    // Completed current batch - check if more watchers remain
                    if (op.notify_sent < op.total_watchers) {
                        // More watchers to notify - send next batch
                        post_notify_watchers(client, op, buffers);
                        continue;
                    }

                    // All notifications sent! Track total notifications sent
                    total_notifications_sent += op.notify_sent;

                    // Collect detailed timing metrics for this notification operation
                    if (op.read_count_end > op.read_count_start) {
                        read_count_latencies.push_back(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                op.read_count_end - op.read_count_start).count());
                    }
                    if (op.read_watcher_ids_end > op.read_watcher_ids_start) {
                        read_watcher_ids_latencies.push_back(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                op.read_watcher_ids_end - op.read_watcher_ids_start).count());
                    }
                    if (op.post_notify_end > op.post_notify_start) {
                        post_notify_latencies.push_back(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                op.post_notify_end - op.post_notify_start).count());
                    }
                    if (op.wait_notify_end > op.wait_notify_start) {
                        wait_notify_latencies.push_back(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                op.wait_notify_end - op.wait_notify_start).count());
                    }
                    notification_watcher_counts.push_back(op.total_watchers);

                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    op.active = false;
                    op.phase = WatchPhase::idle;
                    completed++;
                    active--;

                    if (submitted < total_ops) {
                        submit_op(slot);
                    }
                }
            }
        }
    }

    // Print phase-specific throughput to stdout
    if (registration_timing_done && notification_timing_started) {
        const double reg_wall_s = std::chrono::duration_cast<std::chrono::microseconds>(
            registration_end_time - registration_start_time).count() / 1'000'000.0;
        const double notif_wall_s = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - notification_start_time).count() / 1'000'000.0;
        const double reg_throughput = registration_ops / reg_wall_s;
        const double notif_throughput = notification_ops / notif_wall_s;

        std::cout << "\n========================================\n";
        std::cout << " PHASE THROUGHPUT\n";
        std::cout << "========================================\n";
        std::cout << "REGISTRATION PHASE:\n";
        std::cout << "  Ops: " << registration_ops << "\n";
        std::cout << "  Wall Clock: " << std::fixed << std::setprecision(6) << reg_wall_s << " s\n";
        std::cout << "  Throughput: " << static_cast<uint64_t>(reg_throughput) << " ops/s\n";
        std::cout << "\nNOTIFICATION PHASE:\n";
        std::cout << "  Ops: " << notification_ops << "\n";
        std::cout << "  Wall Clock: " << notif_wall_s << " s\n";
        std::cout << "  Throughput: " << static_cast<uint64_t>(notif_throughput) << " ops/s\n";
        std::cout << "========================================\n" << std::flush;
    }

    // VERIFICATION START - ENSURE THIS PRINTS
    std::cerr << "\n[VERIFICATION-START] Client " << client.id() << " reached end of benchmark\n" << std::flush;

    // Print verification statistics (use cerr for immediate visibility)
    std::cerr << "\n========================================\n";
    std::cerr << "[Client " << client.id() << "] Watch Verification\n";
    std::cerr << "========================================\n";
    std::cerr << "REGISTRATION PHASE:\n";
    std::cerr << "  Completed registrations: " << total_registrations << " / " << registration_ops;
    if (total_registrations == registration_ops) {
        std::cerr << " ✓\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }

    std::cerr << "\nNOTIFICATION PHASE:\n";
    std::cerr << "  Completed notifications: " << (completed - total_registrations) << " / " << notification_ops;
    if ((completed - total_registrations) == notification_ops) {
        std::cerr << " ✓\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }
    std::cerr << "  Total RDMA_WRITEs sent: " << total_notifications_sent << "\n";
    std::cerr << "  Avg RDMA_WRITEs/notify: " << (notification_ops > 0 ? total_notifications_sent / notification_ops : 0) << "\n";

    std::cerr << "\nWATCHER STATISTICS:\n";
    std::cerr << "  Total watchers seen: " << total_watchers_seen << "\n";
    std::cerr << "  Avg watchers/object: " << (notification_ops > 0 ? total_watchers_seen / notification_ops : 0) << "\n";
    std::cerr << "  Min watchers: " << (min_watchers == UINT64_MAX ? 0 : min_watchers) << "\n";
    std::cerr << "  Max watchers: " << max_watchers << "\n";
    std::cerr << "  Objects with 0 watchers: " << zero_watcher_objects << "\n";

    std::cerr << "\nCORRECTNESS CHECKS:\n";
    // Expected: Each client registers registration_ops times across 1000 objects
    // With uniform distribution: ~(registration_ops/1000) registrations per object per client
    // Total across 8 clients: ~(registration_ops * 8 / 1000) watchers per object
    const uint64_t expected_avg = (registration_ops * 8) / 1000;
    const uint64_t actual_avg = notification_ops > 0 ? total_watchers_seen / notification_ops : 0;
    std::cerr << "  Expected avg watchers/object: ~" << expected_avg << "\n";
    std::cerr << "  Actual avg watchers/object: " << actual_avg << "\n";

    // Check 1: Watcher counts within 10%
    if (actual_avg < expected_avg * 0.9 || actual_avg > expected_avg * 1.1) {
        std::cerr << "  ✗ Check 1: Watcher count mismatch (outside 10% tolerance)\n";
    } else {
        std::cerr << "  ✓ Check 1: Watcher counts look correct\n";
    }

    // Check 2: All watchers were notified
    if (total_notifications_sent >= total_watchers_seen) {
        std::cerr << "  ✓ Check 2: All watchers received notifications\n";
    } else {
        std::cerr << "  ✗ Check 2: Missing notifications (" << total_notifications_sent << " < " << total_watchers_seen << ")\n";
    }

    // Check 3: No invalid watcher IDs
    if (invalid_watcher_ids == 0) {
        std::cerr << "  ✓ Check 3: All watcher IDs are valid\n";
    } else {
        std::cerr << "  ✗ Check 3: Found " << invalid_watcher_ids << " invalid watcher IDs\n";
    }

    // ===== DETAILED NOTIFICATION PHASE BREAKDOWN =====
    std::cerr << "\n========================================\n";
    std::cerr << "[Client " << client.id() << "] NOTIFICATION PHASE BREAKDOWN\n";
    std::cerr << "========================================\n";

    auto calc_stats = [](const std::vector<uint64_t>& data, const char* name) {
        if (data.empty()) {
            std::cerr << name << ": No data\n";
            return;
        }
        std::vector<uint64_t> sorted = data;
        std::sort(sorted.begin(), sorted.end());

        double sum = 0;
        for (uint64_t val : sorted) sum += val;
        double mean_us = (sum / sorted.size()) / 1000.0;

        auto p = [&](double percentile) -> double {
            size_t idx = static_cast<size_t>(percentile * (sorted.size() - 1));
            return sorted[idx] / 1000.0;
        };

        std::cerr << name << ":\n";
        std::cerr << "  Count: " << sorted.size() << "\n";
        std::cerr << "  Mean:  " << std::fixed << std::setprecision(2) << mean_us << " μs\n";
        std::cerr << "  P50:   " << p(0.50) << " μs\n";
        std::cerr << "  P90:   " << p(0.90) << " μs\n";
        std::cerr << "  P99:   " << p(0.99) << " μs\n";
        std::cerr << "  P99.9: " << p(0.999) << " μs\n";
        std::cerr << "  Max:   " << p(1.0) << " μs\n";
    };

    calc_stats(read_count_latencies, "1. READ_COUNT (RDMA_READ watcher count)");
    calc_stats(read_watcher_ids_latencies, "2. READ_WATCHER_IDS (RDMA_READ watcher IDs)");
    calc_stats(post_notify_latencies, "3. POST_NOTIFY (CPU: posting RDMA_WRITEs)");
    calc_stats(wait_notify_latencies, "4. WAIT_NOTIFY (NIC: waiting for completions)");

    // ===== BOTTLENECK DIAGNOSIS METRICS =====
    std::cout << "\n========================================\n";
    std::cout << "[Client " << client.id() << "] BOTTLENECK DIAGNOSIS\n";
    std::cout << "========================================\n";

    // Collect metrics from all completed notification operations
    uint64_t total_posts = 0;
    double total_post_us = 0;
    double min_post_us = 1e9;
    double max_post_us = 0;
    uint64_t total_poll_attempts = 0;
    uint64_t total_poll_completions = 0;
    uint64_t max_pending_seen = 0;
    double total_post_wall_ms = 0;
    double total_post_cpu_ms = 0;
    double total_wait_wall_ms = 0;
    double total_wait_cpu_ms = 0;
    size_t num_notify_ops = 0;

    for (const auto& op : ops) {
        if (op.post_count > 0) {
            total_posts += op.post_count;
            total_post_us += op.post_sum_us;
            min_post_us = std::min(min_post_us, op.post_min_us);
            max_post_us = std::max(max_post_us, op.post_max_us);
            max_pending_seen = std::max(max_pending_seen, op.max_pending);

            // CPU utilization during POST
            if (op.post_notify_end > op.post_notify_start) {
                double wall_ms = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    op.post_notify_end - op.post_notify_start).count() / 1000000.0;
                double cpu_ms = op.cpu_after_post.total_ms() - op.cpu_before_post.total_ms();
                total_post_wall_ms += wall_ms;
                total_post_cpu_ms += cpu_ms;
            }
        }
        if (op.poll_attempts > 0) {
            total_poll_attempts += op.poll_attempts;
            total_poll_completions += op.poll_completions;

            // CPU utilization during WAIT
            if (op.wait_notify_end > op.wait_notify_start) {
                double wall_ms = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    op.wait_notify_end - op.wait_notify_start).count() / 1000000.0;
                double cpu_ms = op.cpu_after_wait.total_ms() - op.cpu_before_wait.total_ms();
                total_wait_wall_ms += wall_ms;
                total_wait_cpu_ms += cpu_ms;
                num_notify_ops++;
            }
        }
    }

    std::cout << "\nPOST_NOTIFY Overhead:\n";
    if (total_posts > 0) {
        double avg_post_us = total_post_us / total_posts;
        std::cout << "  Total ibv_post_send calls: " << total_posts << "\n";
        std::cout << "  Avg time per post: " << std::fixed << std::setprecision(3) << avg_post_us << " μs\n";
        std::cout << "  Min time per post: " << min_post_us << " μs\n";
        std::cout << "  Max time per post: " << max_post_us << " μs\n";
        std::cout << "  Peak queue depth: " << max_pending_seen << " / " << QP_DEPTH << " ("
                  << std::fixed << std::setprecision(1) << (100.0 * max_pending_seen / QP_DEPTH) << "%)\n";
        if (total_post_wall_ms > 0) {
            double cpu_util = (total_post_cpu_ms / total_post_wall_ms) * 100.0;
            std::cout << "  CPU utilization: " << std::fixed << std::setprecision(1) << cpu_util << "% "
                      << "(wall=" << total_post_wall_ms << "ms, cpu=" << total_post_cpu_ms << "ms)\n";
            if (cpu_util > 80.0) {
                std::cout << "    → HIGH CPU: Posting is CPU-bound\n";
            } else {
                std::cout << "    → LOW CPU: Posting is NOT CPU-bound\n";
            }
        }
    } else {
        std::cout << "  No POST metrics collected\n";
    }

    std::cout << "\nWAIT_NOTIFY Overhead:\n";
    if (total_poll_attempts > 0) {
        double completions_per_poll = static_cast<double>(total_poll_completions) / total_poll_attempts;
        std::cout << "  Total ibv_poll_cq calls: " << total_poll_attempts << "\n";
        std::cout << "  Total completions polled: " << total_poll_completions << "\n";
        std::cout << "  Completions per poll: " << std::fixed << std::setprecision(2) << completions_per_poll << "\n";
        std::cout << "  Poll efficiency: " << std::fixed << std::setprecision(1) << (completions_per_poll * 100.0) << "%\n";
        if (total_wait_wall_ms > 0 && num_notify_ops > 0) {
            double cpu_util = (total_wait_cpu_ms / total_wait_wall_ms) * 100.0;
            double avg_wait_wall = total_wait_wall_ms / num_notify_ops;
            double avg_wait_cpu = total_wait_cpu_ms / num_notify_ops;
            std::cout << "  Avg wait per op: wall=" << std::fixed << std::setprecision(2) << avg_wait_wall
                      << "ms, cpu=" << avg_wait_cpu << "ms\n";
            std::cout << "  CPU utilization: " << std::fixed << std::setprecision(1) << cpu_util << "%\n";
            if (cpu_util > 80.0) {
                std::cout << "    → HIGH CPU: Spinning on poll (CPU bottleneck)\n";
            } else if (cpu_util < 20.0) {
                std::cout << "    → LOW CPU: Waiting for NIC (NIC/network bottleneck)\n";
            } else {
                std::cout << "    → MEDIUM CPU: Mixed CPU/NIC work\n";
            }
        }
    } else {
        std::cout << "  No WAIT metrics collected\n";
    }

    std::cout << "\nBOTTLENECK SUMMARY:\n";
    if (max_pending_seen > QP_DEPTH * 0.9) {
        std::cout << "  ✗ QUEUE OVERFLOW: Peak queue depth " << max_pending_seen << " exceeds 90% of QP_DEPTH=" << QP_DEPTH << "\n";
        std::cout << "    → Increase QP_DEPTH or reduce concurrency\n";
    } else {
        std::cout << "  ✓ Queue depth OK: Peak " << max_pending_seen << " / " << QP_DEPTH << "\n";
    }

    if (total_post_wall_ms > 0 && (total_post_cpu_ms / total_post_wall_ms) > 0.8) {
        std::cout << "  ⚠ POST is CPU-bound: Consider optimizing ibv_post_send() calls\n";
    }

    if (total_wait_wall_ms > 0) {
        double wait_cpu_util = total_wait_cpu_ms / total_wait_wall_ms;
        if (wait_cpu_util > 0.8) {
            std::cout << "  ⚠ WAIT is CPU-bound: Spinning on poll, NIC is keeping up\n";
        } else if (wait_cpu_util < 0.2) {
            std::cout << "  ⚠ WAIT is NIC-bound: CPU waiting for completions, NIC is slow\n";
        }
    }

    std::cerr << "========================================\n";

    // Calculate correlation between watcher count and latencies
    if (!notification_watcher_counts.empty() && notification_watcher_counts.size() == post_notify_latencies.size()) {
        std::cerr << "\nWATCHER SCALING ANALYSIS:\n";
        std::cerr << "  Total notifications: " << notification_watcher_counts.size() << "\n";

        // Group by watcher count ranges
        std::map<std::string, std::vector<uint64_t>> grouped_post_latencies;
        std::map<std::string, std::vector<uint64_t>> grouped_wait_latencies;

        for (size_t i = 0; i < notification_watcher_counts.size(); ++i) {
            uint64_t count = notification_watcher_counts[i];
            std::string bucket;
            if (count == 0) bucket = "0";
            else if (count <= 10) bucket = "1-10";
            else if (count <= 50) bucket = "11-50";
            else if (count <= 100) bucket = "51-100";
            else if (count <= 500) bucket = "101-500";
            else if (count <= 1000) bucket = "501-1000";
            else bucket = "1001+";

            if (i < post_notify_latencies.size()) {
                grouped_post_latencies[bucket].push_back(post_notify_latencies[i]);
            }
            if (i < wait_notify_latencies.size()) {
                grouped_wait_latencies[bucket].push_back(wait_notify_latencies[i]);
            }
        }

        std::cerr << "\nPOST_NOTIFY latency by watcher count:\n";
        for (const auto& [bucket, lats] : grouped_post_latencies) {
            if (lats.empty()) continue;
            auto sorted = lats;
            std::sort(sorted.begin(), sorted.end());
            double mean = 0;
            for (uint64_t v : sorted) mean += v;
            mean = (mean / sorted.size()) / 1000.0;
            double p50 = sorted[sorted.size() / 2] / 1000.0;
            std::cerr << "  " << std::setw(10) << std::left << bucket << ": "
                      << "mean=" << std::fixed << std::setprecision(2) << std::setw(8) << mean << " μs, "
                      << "p50=" << std::setw(8) << p50 << " μs, "
                      << "samples=" << lats.size() << "\n";
        }

        std::cerr << "\nWAIT_NOTIFY latency by watcher count:\n";
        for (const auto& [bucket, lats] : grouped_wait_latencies) {
            if (lats.empty()) continue;
            auto sorted = lats;
            std::sort(sorted.begin(), sorted.end());
            double mean = 0;
            for (uint64_t v : sorted) mean += v;
            mean = (mean / sorted.size()) / 1000.0;
            double p50 = sorted[sorted.size() / 2] / 1000.0;
            std::cerr << "  " << std::setw(10) << std::left << bucket << ": "
                      << "mean=" << std::fixed << std::setprecision(2) << std::setw(8) << mean << " μs, "
                      << "p50=" << std::setw(8) << p50 << " μs, "
                      << "samples=" << lats.size() << "\n";
        }
    }

    std::cerr << "========================================\n";
    // Phase-separated stats now aggregated and printed in main.cpp
}
