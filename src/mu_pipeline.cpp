#include "rdma/mu_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

enum class MuOpPhase : uint8_t {
    idle = 0,
    wait_lock_grant = 1,
    wait_unlock_ack = 2,
};

constexpr uint64_t MU_RECV_WR_TAG = 0x4D55000000000000ULL;
constexpr uint64_t MU_SEND_WR_TAG = 0x4D56000000000000ULL;

struct MuClientBuffers {
    MuResponse* responses = nullptr;
};

struct MuOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t req_id = 0;
    uint32_t granted_slot = 0;
    size_t latency_index = 0;
    MuOpPhase phase = MuOpPhase::idle;
    MuResponse* response = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t make_recv_wr_id(const MuOpCtx& op, const MuOpPhase phase) {
    return MU_RECV_WR_TAG
         | (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 8)
         | static_cast<uint64_t>(phase);
}

uint64_t make_send_wr_id(const MuOpCtx& op, const MuOpPhase phase) {
    return MU_SEND_WR_TAG
         | (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 8)
         | static_cast<uint64_t>(phase);
}

bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == MU_RECV_WR_TAG;
}

bool is_send_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == MU_SEND_WR_TAG;
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 8) & 0xFFFFFFu);
}

MuOpPhase wr_phase(const uint64_t wr_id) {
    return static_cast<MuOpPhase>(wr_id & 0xFFu);
}

MuClientBuffers map_client_buffers(void* raw_buffer, const size_t buffer_size, const size_t active_window) {
    MuClientBuffers buffers{};
    auto* base = static_cast<uint8_t*>(raw_buffer);
    const size_t response_bytes = align_up(active_window * sizeof(MuResponse), 64);
    if (response_bytes > buffer_size) {
        throw std::runtime_error("MU pipeline: client buffer too small");
    }
    buffers.responses = reinterpret_cast<MuResponse*>(base);
    return buffers;
}

void post_recv(Client& client, MuOpCtx& op) {
    auto* mr = client.mr();
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.response);
    sge.length = sizeof(MuResponse);
    sge.lkey = mr->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_recv_wr_id(op, op.phase);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(leader.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MU pipeline: failed to post recv");
    }
}

void post_request(
    Client& client,
    const MuOpCtx& op,
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
        throw std::runtime_error("MU pipeline: failed to post request send");
    }
}

}  // namespace

MuPipelineConfig load_mu_pipeline_config() {
    MuPipelineConfig config{};
    config.active_window = std::max<size_t>(1, get_uint_env_or("MU_ACTIVE_WINDOW", 1));
    config.cq_batch = std::max<size_t>(1, get_uint_env_or("MU_CQ_BATCH", MU_CLIENT_CQ_BATCH_DEFAULT));
    config.client_send_signal_every = std::max<uint32_t>(
        1,
        get_uint_env_or("MU_CLIENT_SEND_SIGNAL_EVERY", MU_CLIENT_SEND_SIGNAL_EVERY_DEFAULT));
    return config;
}

size_t mu_pipeline_client_buffer_size(const MuPipelineConfig& config) {
    const size_t response_bytes = align_up(config.active_window * sizeof(MuResponse), 64);
    return align_up(response_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MuPipelineConfig& config
) {
    if (client.connections().empty()) {
        throw std::runtime_error("MU pipeline: missing leader connection");
    }

    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint32_t>::max() >> 8)) {
        throw std::runtime_error("MU pipeline: active window too large");
    }

    auto buffers = map_client_buffers(client.buffer(), client.buffer_size(), config.active_window);
    std::vector<MuOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<uint32_t> lock_dist(0, MAX_LOCKS - 1);
    uint32_t leader_signal_count = 0;

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto submit_lock = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = lock_dist(rng);
        op.req_id = next_req_id++;
        op.granted_slot = 0;
        op.latency_index = submitted;
        op.phase = MuOpPhase::wait_lock_grant;
        op.response = &buffers.responses[slot];
        op.started_at = std::chrono::steady_clock::now();

        post_recv(client, op);

        MuRequest req{};
        req.op = static_cast<uint8_t>(MuRpcOp::Lock);
        req.client_id = static_cast<uint16_t>(client.id());
        req.lock_id = op.lock_id;
        req.req_id = op.req_id;
        req.granted_slot = 0;
        post_request(
            client,
            op,
            req,
            leader_signal_count,
            config.client_send_signal_every);

        submitted++;
        active++;
    };

    auto submit_unlock = [&](MuOpCtx& op) {
        op.phase = MuOpPhase::wait_unlock_ack;
        post_recv(client, op);

        MuRequest req{};
        req.op = static_cast<uint8_t>(MuRpcOp::Unlock);
        req.client_id = static_cast<uint16_t>(client.id());
        req.lock_id = op.lock_id;
        req.req_id = op.req_id;
        req.granted_slot = op.granted_slot;
        post_request(
            client,
            op,
            req,
            leader_signal_count,
            config.client_send_signal_every);
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_lock(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("MU pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "MU pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            if (is_send_wr_id(wc.wr_id)) {
                continue;
            }
            if (!is_recv_wr_id(wc.wr_id)) {
                continue;
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("MU pipeline: recv slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const MuOpPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            const MuResponse& resp = *op.response;
            if (resp.client_id != client.id() || resp.lock_id != op.lock_id || resp.req_id != op.req_id) {
                throw std::runtime_error("MU pipeline: mismatched response payload");
            }
            if (resp.status != static_cast<uint8_t>(MuRpcStatus::Ok)) {
                throw std::runtime_error("MU pipeline: leader returned error status " + std::to_string(resp.status));
            }

            if (phase == MuOpPhase::wait_lock_grant) {
                if (resp.op != static_cast<uint8_t>(MuRpcOp::Lock)) {
                    throw std::runtime_error("MU pipeline: expected lock grant response");
                }
                op.granted_slot = resp.granted_slot;
                latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - op.started_at).count();
                submit_unlock(op);
                continue;
            }

            if (resp.op != static_cast<uint8_t>(MuRpcOp::Unlock)) {
                throw std::runtime_error("MU pipeline: expected unlock ack response");
            }

            lock_counts[op.lock_id]++;
            op.active = false;
            op.phase = MuOpPhase::idle;
            completed++;
            active--;

            if (submitted < NUM_OPS_PER_CLIENT) {
                submit_lock(slot);
            }
        }
    }
}
