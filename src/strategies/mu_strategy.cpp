#include "rdma/strategies/mu_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <stdexcept>
#include <string>

static void send_and_wait(Client& client, uint32_t lock_id, uint32_t op) {
    auto* cq = client.cq();

    // route to the leader instance that owns this lock
    size_t inst = mu_instance_for_lock(static_cast<uint16_t>(lock_id));
    auto& leader = client.connections()[inst];

    const uint32_t imm = mu_encode_imm(
        static_cast<uint16_t>(lock_id),
        static_cast<uint16_t>(client.id()),
        op
    );

    ibv_recv_wr rr{}, *bad_rr = nullptr;
    rr.wr_id = 0;
    rr.sg_list = nullptr;
    rr.num_sge = 0;
    if (ibv_post_recv(leader.id->qp, &rr, &bad_rr)) {
        throw std::runtime_error("MuStrategy: Failed to pre-post recv");
    }

    // track unsignaled sends PER QP, not globally
    thread_local uint32_t send_counts[16] = {};  // up to 16 instances
    send_counts[inst]++;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = 0;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_INLINE;
    if ((send_counts[inst] & 1023) == 0) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }
    wr.num_sge = 0;
    wr.sg_list = nullptr;
    wr.imm_data = htonl(imm);

    if (ibv_post_send(leader.id->qp, &wr, &bad)) {
        throw std::runtime_error("MuStrategy: Failed to post send to leader");
    }

    ibv_wc wc{};
    while (true) {
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n <= 0) continue;

        if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error(
                "MuStrategy: WC error " + std::to_string(wc.status)
                + " opcode " + std::to_string(wc.opcode));
        }

        if (wc.opcode & IBV_WC_RECV) return;
    }
}

uint64_t MuStrategy::acquire(Client& client, int /*op_id*/, uint32_t lock_id) {
    send_and_wait(client, lock_id, MU_OP_CLIENT_LOCK);
    return 0;
}

void MuStrategy::release(Client& client, int /*op_id*/, uint32_t lock_id) {
    send_and_wait(client, lock_id, MU_OP_CLIENT_UNLOCK);
}