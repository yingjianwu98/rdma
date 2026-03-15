#include "rdma/server.h"

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <thread>
#include <chrono>

Server::Server(const uint32_t node_id)
    : node_id_(node_id)
    , peers_(CLUSTER_NODES.size())
    , clients_(TOTAL_CLIENTS)
{
    server_creds_.node_id = node_id;
    server_creds_.type    = ConnType::FOLLOWER;  // node-to-node type
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
    if (buf_)      free(buf_);
    if (listener_) rdma_destroy_id(listener_);
    if (ec_)       rdma_destroy_event_channel(ec_);
}

// ─── Active connect to a peer node ───

RemoteConnection Server::connect_to_node(const std::string& ip, uint16_t port) {
    rdma_cm_id* cm_id = nullptr;
    if (rdma_create_id(ec_, &cm_id, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("rdma_create_id failed");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (rdma_resolve_addr(cm_id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000))
        throw std::runtime_error("resolve_addr failed for " + ip);

    rdma_cm_event* ev = nullptr;
    rdma_get_cm_event(ec_, &ev);
    rdma_ack_cm_event(ev);

    if (rdma_resolve_route(cm_id, 2000))
        throw std::runtime_error("resolve_route failed for " + ip);

    rdma_get_cm_event(ec_, &ev);
    rdma_ack_cm_event(ev);

    // init RDMA resources on first connection
    if (!pd_) {
        pd_ = ibv_alloc_pd(cm_id->verbs);
        if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

        cq_ = ibv_create_cq(cm_id->verbs,
                             QP_DEPTH * (TOTAL_CLIENTS + CLUSTER_NODES.size()),
                             nullptr, nullptr, 0);
        if (!cq_) throw std::runtime_error("ibv_create_cq failed");

        mr_ = ibv_reg_mr(pd_, buf_, SERVER_ALIGNED_SIZE,
                         IBV_ACCESS_LOCAL_WRITE |
                         IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_READ |
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

    if (rdma_create_qp(cm_id, pd_, &qp_attr))
        throw std::runtime_error("rdma_create_qp failed for " + ip);

    ConnPrivateData priv = server_creds_;

    rdma_conn_param param{};
    param.private_data = &priv;
    param.private_data_len = sizeof(priv);
    param.responder_resources = RDMA_RESPONDER_RESOURCES;
    param.initiator_depth = RDMA_INITIATOR_DEPTH;
    param.rnr_retry_count = 7;

    if (rdma_connect(cm_id, &param))
        throw std::runtime_error("rdma_connect failed for " + ip);

    rdma_get_cm_event(ec_, &ev);
    if (ev->event != RDMA_CM_EVENT_ESTABLISHED) {
        rdma_ack_cm_event(ev);
        throw std::runtime_error("connect to node failed for " + ip);
    }

    RemoteConnection conn{};
    if (ev->param.conn.private_data &&
        ev->param.conn.private_data_len >= sizeof(ConnPrivateData)) {
        auto* remote = static_cast<const ConnPrivateData*>(
            ev->param.conn.private_data);
        conn = {remote->node_id, cm_id, remote->addr, remote->rkey, remote->type};
    }
    rdma_ack_cm_event(ev);
    return conn;
}

// ─── Main startup: mesh nodes + accept clients ───

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
    if (rdma_listen(listener_, 32))
        throw std::runtime_error("rdma_listen failed");

    buf_ = allocate_server_buffer();

    auto* base = static_cast<uint8_t*>(buf_);
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + i * LOCK_REGION_SIZE) = 0;
    }

    const size_t num_nodes = CLUSTER_NODES.size();
    const uint32_t num_clients = expected_clients();

    std::cout << "[Server " << node_id_ << "] Listening on port " << port << "\n";

    // ── Phase 1: Connect to all lower-id nodes ──
    for (uint32_t target = 0; target < node_id_; ++target) {
        std::cout << "[Server " << node_id_ << "] Connecting to node " << target << "...\n";
        RemoteConnection conn{};
        bool connected = false;
        for (int attempt = 0; attempt < 60; ++attempt) {
            try {
                conn = connect_to_node(CLUSTER_NODES[target], port);
                connected = true;
                break;
            } catch (...) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        if (!connected)
            throw std::runtime_error("Failed to connect to node " + std::to_string(target));
        peers_[target] = conn;
        std::cout << "[Server " << node_id_ << "] Peer " << target << " connected\n";
    }

    // ── Phase 2: Accept from higher-id nodes + all clients ──
    const uint32_t expect_higher = num_nodes - 1 - node_id_;
    uint32_t higher_connected = 0;
    uint32_t clients_connected = 0;

    std::cout << "[Server " << node_id_ << "] Waiting for "
              << expect_higher << " higher nodes + "
              << num_clients << " clients\n";

    while (higher_connected < expect_higher ||
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

        // init RDMA resources if first accepted connection
        if (!pd_) {
            pd_ = ibv_alloc_pd(new_id->verbs);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(new_id->verbs,
                                 QP_DEPTH * (TOTAL_CLIENTS + num_nodes),
                                 nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(pd_, buf_, SERVER_ALIGNED_SIZE,
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

        rdma_conn_param accept_params{};
        accept_params.private_data     = &server_creds_;
        accept_params.private_data_len = sizeof(server_creds_);
        accept_params.responder_resources = RDMA_RESPONDER_RESOURCES;
        accept_params.initiator_depth     = RDMA_INITIATOR_DEPTH;
        accept_params.rnr_retry_count = 7;

        if (rdma_accept(new_id, &accept_params)) {
            rdma_destroy_qp(new_id);
            rdma_ack_cm_event(event);
            continue;
        }

        const uint32_t nid = incoming->node_id;

        if (incoming->type == ConnType::FOLLOWER) {
            peers_[nid] = {nid, new_id, incoming->addr, incoming->rkey, incoming->type};
            higher_connected++;
            std::cout << "[Server " << node_id_ << "] Peer " << nid << " accepted\n";
        } else if (incoming->type == ConnType::CLIENT) {
            clients_[nid] = {nid, new_id, incoming->addr, incoming->rkey, incoming->type};
            clients_connected++;
            std::cout << "[Server " << node_id_ << "] Client "
                      << clients_connected << "/" << num_clients << "\n";
        }

        rdma_ack_cm_event(event);
    }

    // Mark self in peers
    peers_[node_id_].id = node_id_;

    std::cout << "[Server " << node_id_ << "] Ready — "
              << (num_nodes - 1) << " peers + "
              << clients_connected << " clients\n";

    signal_clients_ready();
    run();
}

void Server::signal_clients_ready() {
    const uint32_t num_clients = expected_clients();
    if (num_clients == 0) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Post all GO sends at once (don't wait one-by-one)
    for (uint32_t i = 0; i < num_clients; ++i) {
        ibv_send_wr swr{}, *bad_wr = nullptr;
        swr.wr_id = 0xBEEF0000 | i;
        swr.opcode = IBV_WR_SEND_WITH_IMM;
        swr.num_sge = 0;
        swr.sg_list = nullptr;
        swr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
        swr.imm_data = htonl(0x60606060);

        if (ibv_post_send(clients_[i].cm_id->qp, &swr, &bad_wr)) {
            throw std::runtime_error("Failed to send GO signal to client " + std::to_string(i));
        }
    }

    // Wait for all completions
    uint32_t done = 0;
    while (done < num_clients) {
        ibv_wc wc{};
        int n = ibv_poll_cq(cq_, 1, &wc);
        if (n > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "GO signal failed for wr_id " + std::to_string(wc.wr_id)
                    + " status " + std::to_string(wc.status));
            }
            done++;
        }
    }

    std::cout << "[Server " << node_id_ << "] GO sent to " << num_clients << " clients\n";
}
