#include "rdma/servers/simple_mu_leader.h"

// Simple MU leader: append the client id to one flat log slot and respond after
// follower replication reaches quorum.

#include "rdma/common.h"
#include "rdma/simple_mu_encoding.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct SimpleMuAppendCtx {
    bool in_use = false;
    uint32_t generation = 0;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t slot = 0;
    uint32_t ack_count = 0;
};

struct SimpleMuRuntime {
    uint32_t node_id = 0;
    uint8_t* local_buf = nullptr;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_mr* local_mr = nullptr;
    std::vector<RemoteConnection>& clients;
    std::vector<RemoteConnection>& peers;
    uint32_t num_clients = 0;
    std::vector<SimpleMuRequest> recv_buffers;
    ibv_mr* recv_mr = nullptr;
    std::deque<uint32_t> free_ctxs;
    std::vector<SimpleMuAppendCtx> append_ctxs;
    std::vector<uint32_t> client_send_signal_counts;
    std::vector<size_t> follower_indices;
};

constexpr uint64_t SIMPLE_MU_RECV_WR_TAG = 0xC1ULL;
constexpr uint64_t SIMPLE_MU_REPL_WR_TAG = 0xC2ULL;
constexpr uint64_t SIMPLE_MU_RESP_WR_TAG = 0x534D000000000000ULL;
constexpr uint64_t SIMPLE_MU_WR_TAG_SHIFT = 56;
constexpr uint64_t SIMPLE_MU_REPL_GEN_SHIFT = 24;
constexpr uint64_t SIMPLE_MU_REPL_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t SIMPLE_MU_REPL_ID_MASK = 0xFFFFFFULL;

uint64_t make_recv_wr_id(const uint16_t client_id, const uint16_t recv_slot) {
    return (SIMPLE_MU_RECV_WR_TAG << SIMPLE_MU_WR_TAG_SHIFT)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(recv_slot);
}

uint16_t recv_client_id(const uint64_t wr_id) {
    return static_cast<uint16_t>((wr_id >> 16) & 0xFFFFu);
}

uint16_t recv_slot_index(const uint64_t wr_id) {
    return static_cast<uint16_t>(wr_id & 0xFFFFu);
}

uint64_t make_repl_wr_id(const uint32_t ctx_id, const uint32_t generation) {
    return (SIMPLE_MU_REPL_WR_TAG << SIMPLE_MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(generation) & SIMPLE_MU_REPL_GEN_MASK) << SIMPLE_MU_REPL_GEN_SHIFT)
         | (static_cast<uint64_t>(ctx_id) & SIMPLE_MU_REPL_ID_MASK);
}

uint32_t repl_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> SIMPLE_MU_REPL_GEN_SHIFT) & SIMPLE_MU_REPL_GEN_MASK);
}

uint32_t repl_ctx_id(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & SIMPLE_MU_REPL_ID_MASK);
}

bool is_repl_wr_id(const uint64_t wr_id) {
    return (wr_id >> SIMPLE_MU_WR_TAG_SHIFT) == SIMPLE_MU_REPL_WR_TAG;
}

bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> SIMPLE_MU_WR_TAG_SHIFT) == SIMPLE_MU_RECV_WR_TAG;
}

bool is_resp_send_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == SIMPLE_MU_RESP_WR_TAG;
}

void send_response(SimpleMuRuntime& rt, const SimpleMuResponse& resp) {
    if (resp.client_id >= rt.clients.size() || rt.clients[resp.client_id].cm_id == nullptr) {
        throw std::runtime_error("SimpleMuLeader: invalid client response target");
    }

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&resp);
    sge.length = sizeof(SimpleMuResponse);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = SIMPLE_MU_RESP_WR_TAG | resp.client_id;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_INLINE;
    if (++rt.client_send_signal_counts[resp.client_id] % MU_SERVER_SEND_SIGNAL_EVERY == 0) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(rt.clients[resp.client_id].cm_id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("SimpleMuLeader: failed to post client response send");
    }
}

void post_recv(SimpleMuRuntime& rt, const uint16_t client_id, const uint16_t recv_slot) {
    auto& buffer = rt.recv_buffers[static_cast<size_t>(client_id) * SIMPLE_MU_SERVER_RECV_RING + recv_slot];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&buffer);
    sge.length = sizeof(SimpleMuRequest);
    sge.lkey = rt.recv_mr->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_recv_wr_id(client_id, recv_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(rt.clients[client_id].cm_id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("SimpleMuLeader: failed to post recv");
    }
}

std::optional<uint32_t> try_alloc_ctx(SimpleMuRuntime& rt) {
    if (rt.free_ctxs.empty()) {
        return std::nullopt;
    }

    const uint32_t ctx_id = rt.free_ctxs.front();
    rt.free_ctxs.pop_front();
    auto& ctx = rt.append_ctxs[ctx_id];
    ctx.in_use = true;
    ctx.generation++;
    ctx.ack_count = 0;
    return ctx_id;
}

void release_ctx(SimpleMuRuntime& rt, const uint32_t ctx_id) {
    auto& ctx = rt.append_ctxs[ctx_id];
    ctx.in_use = false;
    rt.free_ctxs.push_back(ctx_id);
}

void complete_ctx(SimpleMuRuntime& rt, const uint32_t ctx_id) {
    auto& ctx = rt.append_ctxs[ctx_id];
    if (!ctx.in_use) return;

    SimpleMuResponse resp{};
    resp.status = static_cast<uint8_t>(SimpleMuStatus::Ok);
    resp.client_id = ctx.client_id;
    resp.req_id = ctx.req_id;
    resp.slot = ctx.slot;
    send_response(rt, resp);
    release_ctx(rt, ctx_id);
}

void post_replication_writes(SimpleMuRuntime& rt, const uint32_t ctx_id) {
    auto& ctx = rt.append_ctxs[ctx_id];
    auto* entry_ptr = reinterpret_cast<uint64_t*>(rt.local_buf + simple_mu_log_slot_offset(ctx.slot));

    for (const size_t follower_idx : rt.follower_indices) {
        auto& follower = rt.peers[follower_idx];

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(entry_ptr);
        sge.length = sizeof(uint64_t);
        sge.lkey = rt.local_mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_repl_wr_id(ctx_id, ctx.generation);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = follower.remote_addr + simple_mu_log_slot_offset(ctx.slot);
        wr.wr.rdma.rkey = follower.rkey;

        if (ibv_post_send(follower.cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("SimpleMuLeader: failed to replicate entry");
        }
    }
}

void handle_request(SimpleMuRuntime& rt, const uint16_t connected_client_id, const SimpleMuRequest& req) {
    if (req.client_id != connected_client_id) {
        SimpleMuResponse resp{};
        resp.status = static_cast<uint8_t>(SimpleMuStatus::InternalError);
        resp.client_id = connected_client_id;
        resp.req_id = req.req_id;
        send_response(rt, resp);
        return;
    }

    const auto ctx_id_opt = try_alloc_ctx(rt);
    if (!ctx_id_opt.has_value()) {
        SimpleMuResponse resp{};
        resp.status = static_cast<uint8_t>(SimpleMuStatus::QueueFull);
        resp.client_id = req.client_id;
        resp.req_id = req.req_id;
        send_response(rt, resp);
        return;
    }

    auto* counter = reinterpret_cast<uint64_t*>(rt.local_buf + simple_mu_counter_offset());
    const uint64_t slot64 = *counter;
    if (slot64 >= SIMPLE_MU_LOG_CAPACITY) {
        release_ctx(rt, *ctx_id_opt);

        SimpleMuResponse resp{};
        resp.status = static_cast<uint8_t>(SimpleMuStatus::QueueFull);
        resp.client_id = req.client_id;
        resp.req_id = req.req_id;
        send_response(rt, resp);
        return;
    }

    *counter = slot64 + 1;
    const uint32_t ctx_id = *ctx_id_opt;
    auto& ctx = rt.append_ctxs[ctx_id];
    ctx.client_id = req.client_id;
    ctx.req_id = req.req_id;
    ctx.slot = static_cast<uint32_t>(slot64);
    ctx.ack_count = 1;

    *reinterpret_cast<uint64_t*>(rt.local_buf + simple_mu_log_slot_offset(ctx.slot)) =
        pack_simple_mu_entry(req.client_id);

    post_replication_writes(rt, ctx_id);
    if (ctx.in_use && ctx.ack_count >= QUORUM) {
        complete_ctx(rt, ctx_id);
    }
}

void handle_recv_cqe(SimpleMuRuntime& rt, const ibv_wc& comp) {
    const uint16_t client_id = recv_client_id(comp.wr_id);
    const uint16_t recv_slot = recv_slot_index(comp.wr_id);
    const SimpleMuRequest req = rt.recv_buffers[static_cast<size_t>(client_id) * SIMPLE_MU_SERVER_RECV_RING + recv_slot];
    post_recv(rt, client_id, recv_slot);
    handle_request(rt, client_id, req);
}

void handle_repl_cqe(SimpleMuRuntime& rt, const ibv_wc& comp) {
    const uint32_t ctx_id = repl_ctx_id(comp.wr_id);
    if (ctx_id >= rt.append_ctxs.size()) {
        throw std::runtime_error("SimpleMuLeader: append ctx id out of range");
    }

    auto& ctx = rt.append_ctxs[ctx_id];
    if (!ctx.in_use || ctx.generation != repl_generation(comp.wr_id)) {
        return;
    }

    ctx.ack_count++;
    if (ctx.ack_count >= QUORUM) {
        complete_ctx(rt, ctx_id);
    }
}

} // namespace

void SimpleMuLeader::run() {
    const uint32_t num_clients = expected_clients();
    const size_t ctx_pool_size = std::max<size_t>(
        TOTAL_CLIENTS * std::max<size_t>(1, SIMPLE_MU_ACTIVE_WINDOW),
        SIMPLE_MU_ACTIVE_WINDOW + 1);
    if (ctx_pool_size > SIMPLE_MU_REPL_ID_MASK) {
        throw std::runtime_error("SimpleMuLeader: ctx pool exceeds wr_id capacity");
    }

    std::cout << "[SimpleMuLeader " << node_id_ << "] flat log primitive\n";

    SimpleMuRuntime rt{
        .node_id = node_id_,
        .local_buf = static_cast<uint8_t*>(buf_),
        .pd = pd_,
        .cq = cq_,
        .local_mr = mr_,
        .clients = clients_,
        .peers = peers_,
        .num_clients = num_clients,
        .recv_buffers = std::vector<SimpleMuRequest>(num_clients * SIMPLE_MU_SERVER_RECV_RING),
        .append_ctxs = std::vector<SimpleMuAppendCtx>(ctx_pool_size),
        .client_send_signal_counts = std::vector<uint32_t>(num_clients, 0),
    };

    rt.free_ctxs.resize(ctx_pool_size);
    for (uint32_t i = 0; i < ctx_pool_size; ++i) {
        rt.free_ctxs[i] = i;
    }

    rt.follower_indices.reserve(rt.peers.size());
    for (size_t i = 0; i < rt.peers.size(); ++i) {
        if (i == rt.node_id || rt.peers[i].cm_id == nullptr) continue;
        rt.follower_indices.push_back(i);
    }

    rt.recv_mr = ibv_reg_mr(
        rt.pd,
        rt.recv_buffers.data(),
        rt.recv_buffers.size() * sizeof(SimpleMuRequest),
        IBV_ACCESS_LOCAL_WRITE);
    if (!rt.recv_mr) {
        throw std::runtime_error("SimpleMuLeader: failed to register recv buffers");
    }

    for (uint16_t client_id = 0; client_id < rt.num_clients; ++client_id) {
        for (uint16_t recv_slot = 0; recv_slot < SIMPLE_MU_SERVER_RECV_RING; ++recv_slot) {
            post_recv(rt, client_id, recv_slot);
        }
    }

    ibv_wc wc[64];
    while (true) {
        const int n = ibv_poll_cq(rt.cq, 64, wc);
        if (n < 0) {
            throw std::runtime_error("SimpleMuLeader: CQ poll failed");
        }

        for (int i = 0; i < n; ++i) {
            const ibv_wc& comp = wc[i];
            if (comp.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    std::string("SimpleMuLeader: completion error ") + ibv_wc_status_str(comp.status));
            }

            if ((comp.opcode & IBV_WC_RECV) != 0) {
                if (is_recv_wr_id(comp.wr_id)) {
                    handle_recv_cqe(rt, comp);
                }
                continue;
            }

            if (is_resp_send_wr_id(comp.wr_id)) {
                continue;
            }

            if (is_repl_wr_id(comp.wr_id)) {
                handle_repl_cqe(rt, comp);
            }
        }
    }
}
