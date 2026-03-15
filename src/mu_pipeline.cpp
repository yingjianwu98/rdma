#include "rdma/mu_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

enum class MuOpPhase : uint8_t {
    idle = 0,
    wait_lock_grant = 1,
    wait_unlock_ack = 2,
};

constexpr uint64_t MU_RECV_WR_TAG = 0xA1ULL;
constexpr uint64_t MU_SEND_WR_TAG = 0xA2ULL;
constexpr uint64_t MU_WR_TAG_SHIFT = 56;
constexpr uint64_t MU_WR_GEN_SHIFT = 24;
constexpr uint64_t MU_WR_SLOT_SHIFT = 8;
constexpr uint64_t MU_WR_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t MU_WR_SLOT_MASK = 0xFFFFULL;
constexpr size_t MU_CLIENT_RECV_RING_MIN = 32;

struct MuClientBuffers {
    MuResponse* responses = nullptr;
    size_t recv_ring = 0;
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
    std::chrono::steady_clock::time_point started_at{};
};

uint64_t make_recv_wr_id(const uint32_t recv_slot) {
    return (MU_RECV_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(recv_slot) & MU_WR_SLOT_MASK) << MU_WR_SLOT_SHIFT);
}

uint64_t make_send_wr_id(const MuOpCtx& op, const MuOpPhase phase) {
    return (MU_SEND_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(op.generation) & MU_WR_GEN_MASK) << MU_WR_GEN_SHIFT)
         | ((static_cast<uint64_t>(op.slot) & MU_WR_SLOT_MASK) << MU_WR_SLOT_SHIFT)
         | static_cast<uint64_t>(phase);
}

bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_RECV_WR_TAG;
}

bool is_send_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_SEND_WR_TAG;
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_WR_GEN_SHIFT) & MU_WR_GEN_MASK);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_WR_SLOT_SHIFT) & MU_WR_SLOT_MASK);
}

MuOpPhase wr_phase(const uint64_t wr_id) {
    return static_cast<MuOpPhase>(wr_id & 0xFFu);
}

MuClientBuffers map_client_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t recv_ring
) {
    MuClientBuffers buffers{};
    auto* base = static_cast<uint8_t*>(raw_buffer);
    const size_t response_bytes = align_up(recv_ring * sizeof(MuResponse), 64);
    if (response_bytes > buffer_size) {
        throw std::runtime_error("MU pipeline: client buffer too small");
    }
    buffers.responses = reinterpret_cast<MuResponse*>(base);
    buffers.recv_ring = recv_ring;
    return buffers;
}

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
    config.active_window = std::max<size_t>(1, MU_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, get_uint_env_or("MU_CQ_BATCH", MU_CLIENT_CQ_BATCH_DEFAULT));
    config.client_send_signal_every = std::max<uint32_t>(
        1,
        get_uint_env_or("MU_CLIENT_SEND_SIGNAL_EVERY", MU_CLIENT_SEND_SIGNAL_EVERY_DEFAULT));
    return config;
}

size_t mu_pipeline_client_buffer_size(const MuPipelineConfig& config) {
    const size_t recv_ring = std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN);
    const size_t response_bytes = align_up(recv_ring * sizeof(MuResponse), 64);
    return align_up(response_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MuPipelineConfig& config
) {
    const bool mu_debug = get_uint_env_or("MU_DEBUG", 0) != 0;
    auto debug = [&](const std::string& msg) {
        if (mu_debug) {
            std::cout << "[MuClient " << client.id() << "] " << msg << "\n";
        }
    };

    if (client.connections().empty()) {
        throw std::runtime_error("MU pipeline: missing leader connection");
    }

    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint32_t>::max() >> 8)) {
        throw std::runtime_error("MU pipeline: active window too large");
    }
    if (config.active_window > MU_WR_SLOT_MASK) {
        throw std::runtime_error("MU pipeline: active window exceeds wr_id slot capacity");
    }
    const size_t recv_ring = std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN);
    if (recv_ring > MU_WR_SLOT_MASK) {
        throw std::runtime_error("MU pipeline: recv ring exceeds wr_id slot capacity");
    }

    auto buffers = map_client_buffers(client.buffer(), client.buffer_size(), recv_ring);
    std::vector<MuOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<uint32_t> lock_dist(0, MAX_LOCKS - 1);
    uint32_t leader_signal_count = 0;
    std::unordered_map<uint32_t, uint32_t> req_to_slot;
    req_to_slot.reserve(config.active_window * 2);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    for (uint32_t recv_slot = 0; recv_slot < recv_ring; ++recv_slot) {
        post_recv(client, &buffers.responses[recv_slot], recv_slot);
    }

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
        op.started_at = std::chrono::steady_clock::now();
        req_to_slot[op.req_id] = op.slot;

        MuRequest req{};
        req.op = static_cast<uint8_t>(MuRpcOp::Lock);
        req.client_id = static_cast<uint16_t>(client.id());
        req.lock_id = op.lock_id;
        req.req_id = op.req_id;
        req.granted_slot = 0;
        debug(
            "send lock lock=" + std::to_string(op.lock_id)
            + " req=" + std::to_string(op.req_id)
            + " slot=" + std::to_string(op.slot));
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

        MuRequest req{};
        req.op = static_cast<uint8_t>(MuRpcOp::Unlock);
        req.client_id = static_cast<uint16_t>(client.id());
        req.lock_id = op.lock_id;
        req.req_id = op.req_id;
        req.granted_slot = op.granted_slot;
        debug(
            "send unlock lock=" + std::to_string(op.lock_id)
            + " req=" + std::to_string(op.req_id)
            + " granted_slot=" + std::to_string(op.granted_slot)
            + " op_slot=" + std::to_string(op.slot));
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

        std::vector<uint32_t> recv_reposts;
        recv_reposts.reserve(static_cast<size_t>(std::max(polled, 0)));

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

            const uint32_t recv_slot = wr_slot(wc.wr_id);
            if (recv_slot >= buffers.recv_ring) {
                throw std::runtime_error("MU pipeline: recv ring slot out of range");
            }

            const MuResponse resp = buffers.responses[recv_slot];
            recv_reposts.push_back(recv_slot);

            const auto it = req_to_slot.find(resp.req_id);
            if (it == req_to_slot.end()) {
                if (mu_debug) {
                    debug(
                        "recv unknown response op=" + std::to_string(resp.op)
                        + " lock=" + std::to_string(resp.lock_id)
                        + " req=" + std::to_string(resp.req_id));
                }
                continue;
            }

            const uint32_t slot = it->second;

            auto& op = ops[slot];
            if (!op.active) {
                continue;
            }
            if (resp.client_id != client.id() || resp.lock_id != op.lock_id || resp.req_id != op.req_id) {
                throw std::runtime_error("MU pipeline: mismatched response payload");
            }
            if (resp.status != static_cast<uint8_t>(MuRpcStatus::Ok)) {
                throw std::runtime_error("MU pipeline: leader returned error status " + std::to_string(resp.status));
            }

            if (resp.op == static_cast<uint8_t>(MuRpcOp::Lock)) {
                if (op.phase != MuOpPhase::wait_lock_grant) {
                    throw std::runtime_error("MU pipeline: lock response arrived in wrong phase");
                }
                if (resp.op != static_cast<uint8_t>(MuRpcOp::Lock)) {
                    throw std::runtime_error("MU pipeline: expected lock grant response");
                }
                debug(
                    "recv grant lock=" + std::to_string(op.lock_id)
                    + " req=" + std::to_string(op.req_id)
                    + " granted_slot=" + std::to_string(resp.granted_slot));
                op.granted_slot = resp.granted_slot;
                latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - op.started_at).count();
                submit_unlock(op);
                continue;
            }

            if (resp.op != static_cast<uint8_t>(MuRpcOp::Unlock)) {
                throw std::runtime_error("MU pipeline: unexpected response opcode");
            }
            if (op.phase != MuOpPhase::wait_unlock_ack) {
                throw std::runtime_error("MU pipeline: unlock response arrived in wrong phase");
            }

            debug(
                "recv unlock ack lock=" + std::to_string(op.lock_id)
                + " req=" + std::to_string(op.req_id)
                + " granted_slot=" + std::to_string(op.granted_slot));

            lock_counts[op.lock_id]++;
            req_to_slot.erase(it);
            op.active = false;
            op.phase = MuOpPhase::idle;
            completed++;
            active--;

            if (submitted < NUM_OPS_PER_CLIENT) {
                submit_lock(slot);
            }
        }

        for (const uint32_t recv_slot : recv_reposts) {
            post_recv(client, &buffers.responses[recv_slot], recv_slot);
        }
    }
}
