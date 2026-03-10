#include "rdma/strategies/mu_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <iostream>
#include <stdexcept>
#include <string>

static constexpr uint64_t MU_SEND_TAG = 0xAA;

static void mu_send_and_wait(Client& client, uint32_t lock_id, uint32_t op) {
    auto* cq = client.cq();
    // client connects to leader only, so connections()[0] is the leader
    auto& leader = client.connections()[0];

    const uint32_t imm = mu_encode_imm(
        static_cast<uint16_t>(lock_id),
        static_cast<uint16_t>(client.id()),
        op
    );

    // ── pre-post recv for the ack ──
    ibv_recv_wr rr{}, *bad_rr = nullptr;
    rr.wr_id = 0;
    rr.sg_list = nullptr;
    rr.num_sge = 0;
    if (ibv_post_recv(leader.id->qp, &rr, &bad_rr)) {
        throw std::runtime_error("MuStrategy: Failed to pre-post recv");
    }

    // ── send zero-length WRITE_WITH_IMM to leader ──
    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = MU_SEND_TAG;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.num_sge = 0;
    wr.sg_list = nullptr;
    wr.imm_data = htonl(imm);
    wr.wr.rdma.remote_addr = leader.addr + client_staging_offset(client.id());
    wr.wr.rdma.rkey = leader.rkey;

    if (ibv_post_send(leader.id->qp, &wr, &bad)) {
        throw std::runtime_error("MuStrategy: Failed to post send to leader");
    }

    // ── drain CQ: wait for send completion + ack ──
    bool send_done = false;
    bool ack_done = false;
    ibv_wc wc{};

    while (!send_done || !ack_done) {
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n <= 0) continue;

        if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error(
                "MuStrategy: WC error " + std::to_string(wc.status)
                + " opcode " + std::to_string(wc.opcode));
        }

        if (wc.opcode == IBV_WC_RDMA_WRITE && wc.wr_id == MU_SEND_TAG) {
            send_done = true;
        } else if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            ack_done = true;
        }
    }
}

uint64_t MuStrategy::acquire(Client& client, int /*op_id*/, uint32_t lock_id) {
    mu_send_and_wait(client, lock_id, MU_OP_CLIENT_LOCK);
    return 0;
}

void MuStrategy::release(Client& client, int /*op_id*/, uint32_t lock_id) {
    mu_send_and_wait(client, lock_id, MU_OP_CLIENT_UNLOCK);
}