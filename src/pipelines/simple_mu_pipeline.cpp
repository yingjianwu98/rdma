#include "rdma/pipelines/simple_mu_pipeline.h"

// Client-side RPC pipeline for the simple_mu primitive.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/simple_mu_encoding.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

enum class SimpleMuPhase : uint8_t {
    idle = 0,
    wait_response = 1,
};

constexpr uint64_t SIMPLE_MU_RECV_WR_TAG = 0xD1ULL;
constexpr uint64_t SIMPLE_MU_SEND_WR_TAG = 0xD2ULL;
constexpr uint64_t SIMPLE_MU_WR_TAG_SHIFT = 56;
constexpr uint64_t SIMPLE_MU_WR_GEN_SHIFT = 24;
constexpr uint64_t SIMPLE_MU_WR_SLOT_SHIFT = 8;
constexpr uint64_t SIMPLE_MU_WR_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t SIMPLE_MU_WR_SLOT_MASK = 0xFFFFULL;
constexpr size_t SIMPLE_MU_CLIENT_RECV_RING_MIN = 32;

struct SimpleMuClientBuffers {
    SimpleMuResponse* responses = nullptr;
    size_t recv_ring = 0;
};

struct SimpleMuOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t req_id = 0;
    size_t latency_index = 0;
    SimpleMuPhase phase = SimpleMuPhase::idle;
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t make_recv_wr_id(const uint32_t recv_slot) {
    return (SIMPLE_MU_RECV_WR_TAG << SIMPLE_MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(recv_slot) & SIMPLE_MU_WR_SLOT_MASK) << SIMPLE_MU_WR_SLOT_SHIFT);
}

uint64_t make_send_wr_id(const SimpleMuOpCtx& op, const SimpleMuPhase phase) {
    return (SIMPLE_MU_SEND_WR_TAG << SIMPLE_MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(op.generation) & SIMPLE_MU_WR_GEN_MASK) << SIMPLE_MU_WR_GEN_SHIFT)
         | ((static_cast<uint64_t>(op.slot) & SIMPLE_MU_WR_SLOT_MASK) << SIMPLE_MU_WR_SLOT_SHIFT)
         | static_cast<uint64_t>(phase);
}

bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> SIMPLE_MU_WR_TAG_SHIFT) == SIMPLE_MU_RECV_WR_TAG;
}

bool is_send_wr_id(const uint64_t wr_id) {
    return (wr_id >> SIMPLE_MU_WR_TAG_SHIFT) == SIMPLE_MU_SEND_WR_TAG;
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> SIMPLE_MU_WR_SLOT_SHIFT) & SIMPLE_MU_WR_SLOT_MASK);
}

SimpleMuClientBuffers map_client_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t recv_ring
) {
    SimpleMuClientBuffers buffers{};
    auto* base = static_cast<uint8_t*>(raw_buffer);
    const size_t response_bytes = align_up(recv_ring * sizeof(SimpleMuResponse), 64);
    if (response_bytes > buffer_size) {
        throw std::runtime_error("simple_mu pipeline: client buffer too small");
    }
    buffers.responses = reinterpret_cast<SimpleMuResponse*>(base);
    buffers.recv_ring = recv_ring;
    return buffers;
}

void post_recv(Client& client, SimpleMuResponse* response, const uint32_t recv_slot) {
    auto* mr = client.mr();
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(response);
    sge.length = sizeof(SimpleMuResponse);
    sge.lkey = mr->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_recv_wr_id(recv_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(leader.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_mu pipeline: failed to post recv");
    }
}

void post_request(
    Client& client,
    const SimpleMuOpCtx& op,
    const SimpleMuRequest& request,
    uint32_t& signal_count,
    const uint32_t signal_every
) {
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&request);
    sge.length = sizeof(SimpleMuRequest);
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
        throw std::runtime_error("simple_mu pipeline: failed to post request send");
    }
}

} // namespace

SimpleMuPipelineConfig load_simple_mu_pipeline_config() {
    SimpleMuPipelineConfig config{};
    config.active_window = std::max<size_t>(1, SIMPLE_MU_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, SIMPLE_MU_CQ_BATCH);
    config.client_send_signal_every = std::max<uint32_t>(1, SIMPLE_MU_CLIENT_SEND_SIGNAL_EVERY);
    return config;
}

size_t simple_mu_pipeline_client_buffer_size(const SimpleMuPipelineConfig& config) {
    const size_t recv_ring = std::max(config.active_window * 2, SIMPLE_MU_CLIENT_RECV_RING_MIN);
    const size_t response_bytes = align_up(recv_ring * sizeof(SimpleMuResponse), 64);
    return align_up(response_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_simple_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const SimpleMuPipelineConfig& config
) {
    if (client.connections().empty()) {
        throw std::runtime_error("simple_mu pipeline: missing leader connection");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint32_t>::max() >> 8)) {
        throw std::runtime_error("simple_mu pipeline: active window too large");
    }
    if (config.active_window > SIMPLE_MU_WR_SLOT_MASK) {
        throw std::runtime_error("simple_mu pipeline: active window exceeds wr_id slot capacity");
    }

    const size_t recv_ring = std::max(config.active_window * 2, SIMPLE_MU_CLIENT_RECV_RING_MIN);
    if (recv_ring > SIMPLE_MU_WR_SLOT_MASK) {
        throw std::runtime_error("simple_mu pipeline: recv ring exceeds wr_id slot capacity");
    }

    auto buffers = map_client_buffers(client.buffer(), client.buffer_size(), recv_ring);
    std::vector<SimpleMuOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    std::unordered_map<uint32_t, uint32_t> req_to_slot;
    req_to_slot.reserve(config.active_window * 2);
    uint32_t leader_signal_count = 0;

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    for (uint32_t recv_slot = 0; recv_slot < recv_ring; ++recv_slot) {
        post_recv(client, &buffers.responses[recv_slot], recv_slot);
    }

    auto submit_request = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.req_id = next_req_id++;
        op.latency_index = submitted;
        op.phase = SimpleMuPhase::wait_response;
        op.started_at = std::chrono::steady_clock::now();
        req_to_slot[op.req_id] = op.slot;

        SimpleMuRequest req{};
        req.client_id = static_cast<uint16_t>(client.id());
        req.req_id = op.req_id;
        post_request(client, op, req, leader_signal_count, config.client_send_signal_every);

        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_request(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("simple_mu pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        std::vector<uint32_t> recv_reposts;
        recv_reposts.reserve(static_cast<size_t>(std::max(polled, 0)));

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "simple_mu pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            if (is_send_wr_id(wc.wr_id)) {
                continue;
            }
            if (!is_recv_wr_id(wc.wr_id)) {
                continue;
            }

            const uint32_t recv_slot = wr_slot(wc.wr_id);
            if (recv_slot >= buffers.recv_ring) {
                throw std::runtime_error("simple_mu pipeline: recv ring slot out of range");
            }

            const SimpleMuResponse resp = buffers.responses[recv_slot];
            recv_reposts.push_back(recv_slot);

            const auto it = req_to_slot.find(resp.req_id);
            if (it == req_to_slot.end()) {
                continue;
            }

            const uint32_t slot = it->second;
            auto& op = ops[slot];
            if (!op.active) {
                continue;
            }
            if (resp.client_id != client.id() || resp.req_id != op.req_id) {
                throw std::runtime_error("simple_mu pipeline: mismatched response payload");
            }
            if (resp.status != static_cast<uint8_t>(SimpleMuStatus::Ok)) {
                throw std::runtime_error(
                    "simple_mu pipeline: leader returned error status " + std::to_string(resp.status));
            }
            if (op.phase != SimpleMuPhase::wait_response) {
                throw std::runtime_error("simple_mu pipeline: response arrived in wrong phase");
            }

            latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - op.started_at).count();
            lock_counts[0]++;
            req_to_slot.erase(it);
            op.active = false;
            op.phase = SimpleMuPhase::idle;
            completed++;
            active--;

            if (submitted < NUM_OPS_PER_CLIENT) {
                submit_request(slot);
            }
        }

        for (const uint32_t recv_slot : recv_reposts) {
            post_recv(client, &buffers.responses[recv_slot], recv_slot);
        }
    }
}
