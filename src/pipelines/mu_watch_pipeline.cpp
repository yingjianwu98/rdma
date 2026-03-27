#include "rdma/pipelines/mu_watch_pipeline.h"

// Mu watch pipeline: Leader-based watch registration and notification.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

enum class MuWatchPhase : uint8_t {
    idle = 0,
    wait_register_ack = 1,    // Waiting for leader to ACK registration
    wait_notify_ack = 2,       // Waiting for leader to ACK notification
};

constexpr uint64_t MU_RECV_WR_TAG = 0xA1ULL;
constexpr uint64_t MU_SEND_WR_TAG = 0xA2ULL;
constexpr uint64_t MU_WR_TAG_SHIFT = 56;
constexpr uint64_t MU_WR_GEN_SHIFT = 24;
constexpr uint64_t MU_WR_SLOT_SHIFT = 8;
constexpr uint64_t MU_WR_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t MU_WR_SLOT_MASK = 0xFFFFULL;
constexpr size_t MU_CLIENT_RECV_RING_MIN = 32;

struct MuWatchClientBuffers {
    MuResponse* responses = nullptr;
    size_t recv_ring = 0;
};

struct MuWatchOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t object_id = 0;
    uint32_t req_id = 0;
    size_t latency_index = 0;
    MuWatchPhase phase = MuWatchPhase::idle;
    std::chrono::steady_clock::time_point started_at{};
};

// Encode a receive WR id.
uint64_t make_recv_wr_id(const uint32_t recv_slot) {
    return (MU_RECV_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(recv_slot) & MU_WR_SLOT_MASK) << MU_WR_SLOT_SHIFT);
}

// Encode a send WR id.
uint64_t make_send_wr_id(const MuWatchOpCtx& op, const MuWatchPhase phase) {
    return (MU_SEND_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(op.generation) & MU_WR_GEN_MASK) << MU_WR_GEN_SHIFT)
         | ((static_cast<uint64_t>(op.slot) & MU_WR_SLOT_MASK) << MU_WR_SLOT_SHIFT)
         | static_cast<uint64_t>(phase);
}

// Check WR type.
bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_RECV_WR_TAG;
}

bool is_send_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_SEND_WR_TAG;
}

// Extract fields from WR id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_WR_GEN_SHIFT) & MU_WR_GEN_MASK);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_WR_SLOT_SHIFT) & MU_WR_SLOT_MASK);
}

MuWatchPhase wr_phase(const uint64_t wr_id) {
    return static_cast<MuWatchPhase>(wr_id & 0xFFu);
}

// Map client buffer.
MuWatchClientBuffers map_client_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t recv_ring
) {
    MuWatchClientBuffers buffers{};
    auto* base = static_cast<uint8_t*>(raw_buffer);
    const size_t response_bytes = align_up(recv_ring * sizeof(MuResponse), 64);
    if (response_bytes > buffer_size) {
        throw std::runtime_error("MU watch pipeline: client buffer too small");
    }
    buffers.responses = reinterpret_cast<MuResponse*>(base);
    buffers.recv_ring = recv_ring;
    return buffers;
}

// Post receive buffer on leader connection.
void post_recv(Client& client, MuResponse* response, const uint32_t recv_slot) {
    auto* mr = client.mr();
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(response);
    sge.length = sizeof(MuResponse);
    sge.lkey = mr->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_recv_wr_id(recv_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(leader.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MU watch pipeline: failed to post recv");
    }
}

// Send request to leader (register or notify).
void post_request(
    Client& client,
    const MuWatchOpCtx& op,
    const MuRequest& request,
    uint32_t& signal_count,
    const uint32_t signal_every
) {
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&request);
    sge.length = sizeof(MuRequest);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_send_wr_id(op, op.phase);
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_INLINE;
    if (++signal_count % std::max(signal_every, 1u) == 0) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(leader.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MU watch pipeline: send request failed");
    }
}

} // namespace

// Load Mu watch pipeline config.
MuWatchPipelineConfig load_mu_watch_pipeline_config() {
    MuWatchPipelineConfig config{};
    config.active_window = std::max<size_t>(1, MU_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, MU_CQ_BATCH);
    config.client_send_signal_every = MU_CLIENT_SEND_SIGNAL_EVERY;
    config.zipf_skew = MU_ZIPF_SKEW;
    return config;
}

// Report required client buffer size.
size_t mu_watch_pipeline_client_buffer_size(const MuWatchPipelineConfig& config) {
    const size_t recv_ring = std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN);
    const size_t response_bytes = align_up(recv_ring * sizeof(MuResponse), 64);
    return align_up(response_bytes + PAGE_SIZE, PAGE_SIZE);
}

// Main Mu watch pipeline: two-phase benchmark (registration, then notification).
void run_mu_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const MuWatchPipelineConfig& config
) {
    std::cerr << "[DEBUG] run_mu_watch_pipeline started for client " << client.id() << std::endl;
    std::cerr.flush();

    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("MU watch pipeline: no leader connection");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("MU watch pipeline: active window exceeds wr_id slot encoding");
    }

    std::cerr << "[DEBUG] Client " << client.id() << " posting " << std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN) << " receive buffers..." << std::endl;
    std::cerr.flush();

    const size_t recv_ring = std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN);
    auto buffers = map_client_buffers(client.buffer(), client.buffer_size(), recv_ring);

    std::vector<MuWatchOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);  // Reuse for object selection
    std::unordered_map<uint32_t, uint32_t> req_to_slot;
    req_to_slot.reserve(config.active_window * 2);

    // Post all receive buffers upfront
    for (size_t i = 0; i < recv_ring; ++i) {
        post_recv(client, &buffers.responses[i], static_cast<uint32_t>(i));
    }

    std::cerr << "[DEBUG] Client " << client.id() << " posted " << recv_ring << " receive buffers, starting benchmark..." << std::endl;
    std::cerr.flush();

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 0;
    size_t recv_posted = recv_ring;

    // Two-phase benchmark: mostly registration, small number of notifications (match watch_pipeline)
    const size_t registration_ops = NUM_OPS_PER_CLIENT * 99 / 100;
    const size_t notification_ops = NUM_OPS_PER_CLIENT - registration_ops;
    bool in_registration_phase = true;

    // Phase timing for separate throughput reporting
    auto registration_start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point registration_end_time;
    std::chrono::steady_clock::time_point notification_start_time;
    bool registration_timing_done = false;
    bool notification_timing_started = false;

    // Track phase-separated latency indices
    size_t registration_latency_start = 0;
    size_t notification_latency_start = registration_ops;

    // Send signaling control (match mu_pipeline pattern)
    uint32_t signal_count = 0;
    const uint32_t signal_every = config.client_send_signal_every;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.object_id = picker.next();  // Random object
        op.req_id = next_req_id++;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        req_to_slot[op.req_id] = op.slot;

        MuRequest req{};
        req.client_id = static_cast<uint16_t>(client.id());
        req.lock_id = op.object_id;  // Reuse lock_id field for object_id
        req.req_id = op.req_id;

        if (in_registration_phase) {
            // Registration phase: send WatchRegister
            op.phase = MuWatchPhase::wait_register_ack;
            req.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
        } else {
            // Notification phase: send WatchNotify
            if (!notification_timing_started) {
                notification_start_time = std::chrono::steady_clock::now();
                notification_timing_started = true;
            }
            op.phase = MuWatchPhase::wait_notify_ack;
            req.op = static_cast<uint8_t>(MuRpcOp::WatchNotify);
        }

        post_request(client, op, req, signal_count, signal_every);
        submitted++;
        active++;
    };

    // Fill pipeline
    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    std::cerr << "[DEBUG] Client " << client.id() << " submitted " << submitted << " initial requests, entering completion loop..." << std::endl;
    std::cerr.flush();

    // Main completion loop
    uint64_t poll_count = 0;
    while (completed < NUM_OPS_PER_CLIENT) {
        poll_count++;
        if (poll_count % 100000000 == 0) {
            std::cerr << "[DEBUG] Client " << client.id() << " still polling... completed=" << completed << "/" << NUM_OPS_PER_CLIENT << " poll_count=" << poll_count << std::endl;
            std::cerr.flush();
        }
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()),
                                      completions.data());
        if (polled < 0) {
            throw std::runtime_error("MU watch pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        if (completed < 5) {
            std::cerr << "[DEBUG] Client " << client.id() << " got " << polled << " completions" << std::endl;
            std::cerr.flush();
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("MU watch pipeline: WC error status=" + std::to_string(wc.status));
            }

            if (is_recv_wr_id(wc.wr_id)) {
                // Received response from leader
                const uint32_t recv_slot = wr_slot(wc.wr_id);
                if (recv_slot >= buffers.recv_ring) {
                    throw std::runtime_error("MU watch pipeline: recv slot out of range");
                }

                const MuResponse& resp = buffers.responses[recv_slot];

                // Look up op slot from req_id
                const auto it = req_to_slot.find(resp.req_id);
                if (it == req_to_slot.end()) {
                    // Unknown req_id, repost and continue
                    post_recv(client, &buffers.responses[recv_slot], recv_slot);
                    continue;
                }

                const uint32_t op_slot = it->second;
                auto& op = ops[op_slot];

                if (!op.active || op.req_id != resp.req_id) {
                    // Stale response, repost
                    post_recv(client, &buffers.responses[recv_slot], recv_slot);
                    continue;
                }

                if (op.phase == MuWatchPhase::wait_register_ack) {
                    // Registration ACK received - measure and complete
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    req_to_slot.erase(it);
                    op.active = false;
                    op.phase = MuWatchPhase::idle;
                    completed++;
                    active--;

                    // Check if registration phase is complete
                    if (completed >= registration_ops) {
                        in_registration_phase = false;
                        if (!registration_timing_done) {
                            registration_end_time = std::chrono::steady_clock::now();
                            registration_timing_done = true;
                        }
                    }

                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(op_slot);
                    }
                } else if (op.phase == MuWatchPhase::wait_notify_ack) {
                    // Notification ACK received - measure and complete
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    req_to_slot.erase(it);
                    op.active = false;
                    op.phase = MuWatchPhase::idle;
                    completed++;
                    active--;

                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(op_slot);
                    }
                }

                // Repost receive buffer
                post_recv(client, &buffers.responses[recv_slot], recv_slot);
                recv_posted++;
            } else if (is_send_wr_id(wc.wr_id)) {
                // Send completion (signaled sends only)
                // Nothing to do here
            }
        }
    }

    // Report phase-separated metrics
    std::cerr << "\n========================================\n";
    std::cerr << "[Client " << client.id() << "] MU Watch Results\n";
    std::cerr << "========================================\n";

    // Phase-separated throughput reporting
    if (registration_timing_done) {
        const double reg_duration_s = std::chrono::duration<double>(
            registration_end_time - registration_start_time).count();
        const double reg_throughput = registration_ops / reg_duration_s;
        std::cerr << "\nPHASE THROUGHPUT:\n";
        std::cerr << "  Registration: " << static_cast<uint64_t>(reg_throughput) << " ops/s"
                  << " (" << registration_ops << " ops in " << std::fixed << std::setprecision(3)
                  << reg_duration_s << "s)\n";

        if (notification_timing_started) {
            const auto notification_end_time = std::chrono::steady_clock::now();
            const double notif_duration_s = std::chrono::duration<double>(
                notification_end_time - notification_start_time).count();
            const double notif_throughput = notification_ops / notif_duration_s;
            std::cerr << "  Notification: " << static_cast<uint64_t>(notif_throughput) << " ops/s"
                      << " (" << notification_ops << " ops in " << std::fixed << std::setprecision(3)
                      << notif_duration_s << "s)\n";
        }
    }

    // Phase-separated latency reporting
    auto calculate_percentile = [](uint64_t* lats, size_t start, size_t count, double percentile) -> double {
        if (count == 0) return 0.0;
        std::vector<uint64_t> sorted(lats + start, lats + start + count);
        std::sort(sorted.begin(), sorted.end());
        const size_t idx = static_cast<size_t>(percentile * count / 100.0);
        return sorted[std::min(idx, count - 1)] / 1000.0;  // Convert ns to μs
    };

    if (registration_ops > 0) {
        std::cerr << "\nREGISTRATION LATENCY (μs):\n";
        std::cerr << "  P50: " << std::fixed << std::setprecision(2)
                  << calculate_percentile(latencies, registration_latency_start, registration_ops, 50.0) << "\n";
        std::cerr << "  P90: " << calculate_percentile(latencies, registration_latency_start, registration_ops, 90.0) << "\n";
        std::cerr << "  P99: " << calculate_percentile(latencies, registration_latency_start, registration_ops, 99.0) << "\n";
        std::cerr << "  P99.9: " << calculate_percentile(latencies, registration_latency_start, registration_ops, 99.9) << "\n";
    }

    if (notification_ops > 0) {
        std::cerr << "\nNOTIFICATION LATENCY (μs):\n";
        std::cerr << "  P50: " << std::fixed << std::setprecision(2)
                  << calculate_percentile(latencies, notification_latency_start, notification_ops, 50.0) << "\n";
        std::cerr << "  P90: " << calculate_percentile(latencies, notification_latency_start, notification_ops, 90.0) << "\n";
        std::cerr << "  P99: " << calculate_percentile(latencies, notification_latency_start, notification_ops, 99.0) << "\n";
        std::cerr << "  P99.9: " << calculate_percentile(latencies, notification_latency_start, notification_ops, 99.9) << "\n";
    }

    std::cerr << "========================================\n" << std::flush;
}
