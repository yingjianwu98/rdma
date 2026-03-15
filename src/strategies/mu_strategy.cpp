#include "rdma/strategies/mu_strategy.h"

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <stdexcept>
#include <string>

namespace {

enum class MuSyncPhase : uint8_t {
    wait_lock = 1,
    wait_unlock = 2,
};

constexpr uint64_t MU_SYNC_RECV_WR_TAG = 0x4D53000000000000ULL;

uint64_t make_recv_wr_id(const MuSyncPhase phase) {
    return MU_SYNC_RECV_WR_TAG | static_cast<uint64_t>(phase);
}

MuResponse send_and_wait(Client& client, const MuRequest& request, const MuSyncPhase phase) {
    auto* response = reinterpret_cast<MuResponse*>(client.buffer());
    auto& leader = client.connections().front();

    ibv_sge recv_sge{};
    recv_sge.addr = reinterpret_cast<uintptr_t>(response);
    recv_sge.length = sizeof(MuResponse);
    recv_sge.lkey = client.mr()->lkey;

    ibv_recv_wr recv_wr{}, *bad_recv = nullptr;
    recv_wr.wr_id = make_recv_wr_id(phase);
    recv_wr.sg_list = &recv_sge;
    recv_wr.num_sge = 1;
    if (ibv_post_recv(leader.id->qp, &recv_wr, &bad_recv)) {
        throw std::runtime_error("MuStrategy: failed to post recv");
    }

    ibv_sge send_sge{};
    send_sge.addr = reinterpret_cast<uintptr_t>(&request);
    send_sge.length = sizeof(MuRequest);
    send_sge.lkey = 0;

    ibv_send_wr send_wr{}, *bad_send = nullptr;
    send_wr.wr_id = 0;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.sg_list = &send_sge;
    send_wr.num_sge = 1;
    send_wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
    if (ibv_post_send(leader.id->qp, &send_wr, &bad_send)) {
        throw std::runtime_error("MuStrategy: failed to post send");
    }

    ibv_wc wc{};
    while (true) {
        const int n = ibv_poll_cq(client.cq(), 1, &wc);
        if (n <= 0) continue;
        if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error(
                "MuStrategy: WC error " + std::to_string(wc.status)
                + " opcode " + std::to_string(wc.opcode));
        }
        if ((wc.opcode & IBV_WC_RECV) == 0) {
            continue;
        }
        if (wc.wr_id != make_recv_wr_id(phase)) {
            continue;
        }
        return *response;
    }
}

}  // namespace

uint64_t MuStrategy::acquire(Client& client, int op_id, uint32_t lock_id) {
    MuRequest req{};
    req.op = static_cast<uint8_t>(MuRpcOp::Lock);
    req.client_id = static_cast<uint16_t>(client.id());
    req.lock_id = lock_id;
    req.req_id = static_cast<uint32_t>(op_id);

    const MuResponse resp = send_and_wait(client, req, MuSyncPhase::wait_lock);
    if (resp.status != static_cast<uint8_t>(MuRpcStatus::Ok)) {
        throw std::runtime_error("MuStrategy: lock request failed");
    }
    return resp.granted_slot;
}

void MuStrategy::release(Client& client, int op_id, uint32_t lock_id) {
    const auto* state = static_cast<const LocalState*>(client.buffer());

    MuRequest req{};
    req.op = static_cast<uint8_t>(MuRpcOp::Unlock);
    req.client_id = static_cast<uint16_t>(client.id());
    req.lock_id = lock_id;
    req.req_id = static_cast<uint32_t>(op_id);
    req.granted_slot = static_cast<uint32_t>(state->metadata);

    const MuResponse resp = send_and_wait(client, req, MuSyncPhase::wait_unlock);
    if (resp.status != static_cast<uint8_t>(MuRpcStatus::Ok)) {
        throw std::runtime_error("MuStrategy: unlock request failed");
    }
}
