#include "rdma/server.h"

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>

Server::Server(const uint32_t node_id)
    : node_id_(node_id)
    , peers_(CLUSTER_NODES.size())
    , clients_(NUM_CLIENTS)
{
    server_creds_.node_id = node_id;
    server_creds_.type    = ConnType::LEADER;
}

Server::~Server() {
    for (auto& c : clients_) {
        if (c.cm_id && c.cm_id->qp) rdma_destroy_qp(c.cm_id);
        if (c.cm_id) rdma_destroy_id(c.cm_id);
    }
    for (auto& p : peers_) {
        if (p.cm_id && p.cm_id->qp) rdma_destroy_qp(p.cm_id);
        if (p.cm_id) rdma_destroy_id(p.cm_id);
    }
    if (mr_)       ibv_dereg_mr(mr_);
    if (cq_)       ibv_destroy_cq(cq_);
    if (pd_)       ibv_dealloc_pd(pd_);
    if (buf_)      munmap(buf_, ALIGNED_SIZE);
    if (listener_) rdma_destroy_id(listener_);
    if (ec_)       rdma_destroy_event_channel(ec_);
}

void Server::start(uint16_t port) {
    ec_ = rdma_create_event_channel();
    if (!ec_) throw std::runtime_error("rdma_create_event_channel failed");

    if (rdma_create_id(ec_, &listener_, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("rdma_create_id failed");

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (rdma_bind_addr(listener_, reinterpret_cast<sockaddr*>(&addr)))
        throw std::runtime_error("rdma_bind_addr failed");

    if (rdma_listen(listener_, 16))
        throw std::runtime_error("rdma_listen failed");

    buf_ = allocate_rdma_buffer();

    auto* base = static_cast<uint8_t*>(buf_);
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        *reinterpret_cast<uint64_t*>(base + lock_control_offset(i)) = 0;
    }

    const uint32_t num_followers = expected_followers();
    const uint32_t num_clients   = expected_clients();

    std::cout << "[Server " << node_id_ << "] Listening on port " << port
              << ", waiting for " << num_followers << " followers and "
              << num_clients << " clients\n";

    uint32_t followers_connected = 0;
    uint32_t clients_connected   = 0;

    while (followers_connected < num_followers ||
           clients_connected < num_clients) {

        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec_, &event))
            throw std::runtime_error("rdma_get_cm_event failed");

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_ack_cm_event(event);
            continue;
        }

        rdma_cm_id* new_id = event->id;
        auto* incoming = static_cast<const ConnPrivateData*>(
            event->param.conn.private_data);

        if (!incoming ||
            event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        if (!pd_) {
            pd_ = ibv_alloc_pd(new_id->verbs);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(new_id->verbs, QP_DEPTH * (NUM_CLIENTS + CLUSTER_NODES.size()), nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(pd_, buf_, ALIGNED_SIZE,
                             IBV_ACCESS_LOCAL_WRITE  |
                             IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ  |
                             IBV_ACCESS_REMOTE_ATOMIC);
            if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

            server_creds_.addr = reinterpret_cast<uintptr_t>(buf_);
            server_creds_.rkey = mr_->rkey;
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type            = IBV_QPT_RC;
        qp_attr.send_cq            = cq_;
        qp_attr.recv_cq            = cq_;
        qp_attr.cap.max_send_wr    = QP_DEPTH;
        qp_attr.cap.max_recv_wr    = QP_DEPTH;
        qp_attr.cap.max_send_sge   = 1;
        qp_attr.cap.max_recv_sge   = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;
        qp_attr.sq_sig_all         = 0;

        if (rdma_create_qp(new_id, pd_, &qp_attr)) {
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        // Everyone gets the same credentials — same buffer, same rkey
        rdma_conn_param accept_params{};
        accept_params.responder_resources = 1;
        accept_params.initiator_depth     = 1;

        if (incoming->type == ConnType::CLIENT) {
            accept_params.private_data     = &server_creds_;
            accept_params.private_data_len = sizeof(server_creds_);
        }

        if (rdma_accept(new_id, &accept_params)) {
            int err = errno;
            std::cerr << "[Server " << node_id_ << "] rdma_accept failed: "
                      << strerror(err) << " (errno " << err << ")"
                      << " type=" << static_cast<int>(incoming->type)
                      << " node=" << incoming->node_id << "\n";
            rdma_destroy_qp(new_id);
            rdma_ack_cm_event(event);
            continue;  // don't throw — skip this one and keep going
        }
        if (incoming->type == ConnType::FOLLOWER) {
            uint32_t nid = incoming->node_id;
            peers_[nid] = {nid, new_id, incoming->addr, incoming->rkey, incoming->type};
            followers_connected++;
            std::cout << "[Server " << node_id_ << "] Follower " << nid << "\n";
        } else if (incoming->type == ConnType::CLIENT) {
            uint32_t nid = incoming->node_id;
            clients_[nid] = {nid, new_id, incoming->addr, incoming->rkey, incoming->type};
            clients_connected++;
            std::cout << "[Server " << node_id_ << "] Client "
                      << clients_connected << "/" << num_clients << "\n";
        }

        rdma_ack_cm_event(event);
    }

    std::cout << "[Server " << node_id_ << "] Ready.\n";
    run();
}