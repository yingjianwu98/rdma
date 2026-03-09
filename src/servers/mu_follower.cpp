#include "rdma/servers/mu_follower.h"
#include "rdma/common.h"

#include <arpa/inet.h>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>

void MuFollower::connect_to_leader(const std::string& leader_ip, uint16_t port) {
    ec_ = rdma_create_event_channel();
    if (!ec_) throw std::runtime_error("create_event_channel failed");

    buf_ = allocate_rdma_buffer();

    // Initialize control words to 0
    auto* base = static_cast<uint8_t*>(buf_);
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        *reinterpret_cast<uint64_t*>(base + lock_control_offset(i)) = 0;
    }

    rdma_cm_id* id = nullptr;
    if (rdma_create_id(ec_, &id, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("create_id failed");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, leader_ip.c_str(), &addr.sin_addr);

    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000))
        throw std::runtime_error("resolve_addr failed");

    rdma_cm_event* event = nullptr;
    rdma_get_cm_event(ec_, &event);
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(id, 2000))
        throw std::runtime_error("resolve_route failed");

    rdma_get_cm_event(ec_, &event);
    rdma_ack_cm_event(event);

    pd_ = ibv_alloc_pd(id->verbs);
    cq_ = ibv_create_cq(id->verbs, QP_DEPTH * 2, nullptr, nullptr, 0);

    mr_ = ibv_reg_mr(pd_, buf_, ALIGNED_SIZE,
                     IBV_ACCESS_LOCAL_WRITE |
                     IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_ATOMIC);
    if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

    ibv_qp_init_attr qp_attr{};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq_;
    qp_attr.recv_cq = cq_;
    qp_attr.cap.max_send_wr = QP_DEPTH;
    qp_attr.cap.max_recv_wr = QP_DEPTH;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

    rdma_create_qp(id, pd_, &qp_attr);

    ConnPrivateData priv{};
    priv.node_id = node_id_;
    priv.type = ConnType::FOLLOWER;
    priv.addr = reinterpret_cast<uintptr_t>(buf_);
    priv.rkey = mr_->rkey;

    rdma_conn_param param{};
    param.private_data = &priv;
    param.private_data_len = sizeof(priv);
    param.responder_resources = 1;
    param.initiator_depth = 1;

    rdma_connect(id, &param);

    rdma_get_cm_event(ec_, &event);
    if (event->event != RDMA_CM_EVENT_ESTABLISHED)
        throw std::runtime_error("connect failed");

    rdma_ack_cm_event(event);
    leader_id_ = id;

    std::cout << "[MuFollower " << node_id_ << "] Connected to leader\n";
    run();
}

void MuFollower::run() {
    std::cout << "[MuFollower " << node_id_ << "] Processing replicated writes\n";
    while (true) {}
    //
    // auto* local_buf = static_cast<uint8_t*>(buf_);
    // ibv_qp* qp = leader_id_->qp;
    //
    // constexpr int RECV_DEPTH = 512;
    // for (int i = 0; i < RECV_DEPTH; ++i) {
    //     ibv_recv_wr rr{}, *bad = nullptr;
    //     rr.wr_id = i;
    //     rr.sg_list = nullptr;
    //     rr.num_sge = 0;
    //     if (ibv_post_recv(qp, &rr, &bad))
    //         throw std::runtime_error("Failed to pre-post recv");
    // }
    //
    // uint64_t total_applied = 0;
    //
    // while (true) {
    //     ibv_wc wc{};
    //     const int n = ibv_poll_cq(cq_, 1, &wc);
    //     if (n <= 0) continue;
    //
    //     if (wc.status != IBV_WC_SUCCESS) {
    //         std::cerr << "[MuFollower] WC error: "
    //             << ibv_wc_status_str(wc.status) << "\n";
    //         throw std::runtime_error("Follower completion failure");
    //     }
    //
    //     if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
    //         const uint16_t lock_id = mu_decode_lock_id(wc.imm_data);
    //         const uint16_t slot = mu_decode_client_id(wc.imm_data);
    //
    //         uint64_t value = *reinterpret_cast<uint64_t*>(local_buf + lock_log_slot_offset(lock_id, slot));
    //
    //
    //         total_applied++;
    //
    //         ibv_recv_wr rr{}, *bad = nullptr;
    //         rr.wr_id = total_applied;
    //         rr.sg_list = nullptr;
    //         rr.num_sge = 0;
    //         if (ibv_post_recv(qp, &rr, &bad))
    //             throw std::runtime_error("Failed to re-post recv");
    //     }
    // }
}
