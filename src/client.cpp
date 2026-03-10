#include "rdma/client.h"
#include "rdma/common.h"

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>
#include <sys/mman.h>

static rdma_cm_event* wait_for_event(
    rdma_event_channel* ec,
    const rdma_cm_event_type expected,
    const std::string& step
) {
    rdma_cm_event* event = nullptr;
    if (rdma_get_cm_event(ec, &event)) {
        throw std::runtime_error("rdma_get_cm_event failed during " + step);
    }
    if (event->event != expected) {
        const int status = event->status;
        const auto actual = event->event;
        rdma_ack_cm_event(event);
        throw std::runtime_error(
            "Expected " + step + " but got event " + std::to_string(actual) + " status " + std::to_string(status));
    }
    return event;
}

Client::Client(const uint32_t id) : id_(id) {
}

Client::~Client() {
    for (const auto& conn : peers_) {
        if (conn.id && conn.id->qp) rdma_destroy_qp(conn.id);
        if (conn.id) rdma_destroy_id(conn.id);
    }
    for (const auto& conn : connections_) {
        if (conn.id && conn.id->qp) rdma_destroy_qp(conn.id);
        if (conn.id) rdma_destroy_id(conn.id);
    }
    if (peer_listener_) rdma_destroy_id(peer_listener_);
    if (peer_ec_) rdma_destroy_event_channel(peer_ec_);
    for (auto* pec : peer_conn_ecs_) {
        if (pec) rdma_destroy_event_channel(pec);
    }
    if (mr_) ibv_dereg_mr(mr_);
    if (cq_) ibv_destroy_cq(cq_);
    if (pd_) ibv_dealloc_pd(pd_);
    if (buf_) munmap(buf_, ALIGNED_SIZE);
    if (ec_) rdma_destroy_event_channel(ec_);
}

void Client::connect(const std::vector<std::string>& node_ips, const uint16_t port) {
    ec_ = rdma_create_event_channel();
    if (!ec_) throw std::runtime_error("rdma_create_event_channel failed");

    buf_ = allocate_rdma_buffer();

    for (size_t i = 0; i < node_ips.size(); ++i) {
        std::cout << "[Client " << id_ << "] Connecting to " << node_ips[i] << "...\n";

        rdma_cm_id* cm_id = nullptr;
        if (rdma_create_id(ec_, &cm_id, nullptr, RDMA_PS_TCP)) {
            throw std::runtime_error(
                "rdma_create_id failed for node " + std::to_string(i));
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (inet_pton(AF_INET, node_ips[i].c_str(), &addr.sin_addr) <= 0) {
            throw std::runtime_error("Invalid IP: " + node_ips[i]);
        }

        if (rdma_resolve_addr(cm_id, nullptr,
                              reinterpret_cast<sockaddr*>(&addr), 2000)) {
            throw std::runtime_error(
                "rdma_resolve_addr failed for node " + std::to_string(i));
        }

        auto* ev_addr = wait_for_event(ec_, RDMA_CM_EVENT_ADDR_RESOLVED,
                                       "ADDR_RESOLVE");
        rdma_ack_cm_event(ev_addr);

        if (rdma_resolve_route(cm_id, 2000)) {
            throw std::runtime_error(
                "rdma_resolve_route failed for node " + std::to_string(i));
        }

        auto* ev_route = wait_for_event(ec_, RDMA_CM_EVENT_ROUTE_RESOLVED,
                                        "ROUTE_RESOLVE");
        rdma_ack_cm_event(ev_route);

        if (!pd_) {
            pd_ = ibv_alloc_pd(cm_id->verbs);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(
                cm_id->verbs,
                QP_DEPTH * static_cast<int>(node_ips.size() + NUM_CLIENTS),
                nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(
                pd_, buf_, ALIGNED_SIZE,
                IBV_ACCESS_LOCAL_WRITE |
                    IBV_ACCESS_REMOTE_WRITE |
                    IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_ATOMIC);
            if (!mr_) throw std::runtime_error("ibv_reg_mr failed");
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.send_cq = cq_;
        qp_attr.recv_cq = cq_;
        qp_attr.cap.max_send_wr = QP_DEPTH;
        qp_attr.cap.max_recv_wr = QP_DEPTH;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

        if (rdma_create_qp(cm_id, pd_, &qp_attr)) {
            throw std::runtime_error(
                "rdma_create_qp failed for node " + std::to_string(i));
        }

        ConnPrivateData priv{};
        priv.node_id = id_;
        priv.type = ConnType::CLIENT;
        priv.addr = reinterpret_cast<uintptr_t>(buf_);
        priv.rkey = mr_->rkey;

        rdma_conn_param param{};
        param.private_data = &priv;
        param.private_data_len = sizeof(priv);
        param.responder_resources = 1;
        param.initiator_depth = 1;
        param.rnr_retry_count = 10;

        if (rdma_connect(cm_id, &param)) {
            throw std::runtime_error("rdma_connect failed for node " + std::to_string(i));
        }

        auto* ev_conn = wait_for_event(ec_, RDMA_CM_EVENT_ESTABLISHED, "ESTABLISHED");
        if (!ev_conn->param.conn.private_data ||
            ev_conn->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            rdma_ack_cm_event(ev_conn);
            throw std::runtime_error("No private data from node " + std::to_string(i));
        }

        auto* remote = static_cast<const ConnPrivateData*>(ev_conn->param.conn.private_data);

        connections_.push_back({
            .id = cm_id,
            .addr = remote->addr,
            .rkey = remote->rkey,
        });

        rdma_ack_cm_event(ev_conn);

        std::cout << "[Client " << id_ << "] Connected to node " << i << "\n";
    }

    std::cout << "[Client " << id_ << "] All " << node_ips.size() << " node connections established\n";
}

void Client::connect_peers(uint16_t peer_port) {
    peers_.resize(NUM_CLIENTS);

    const sockaddr* local_addr = rdma_get_local_addr(connections_[0].id);
    const auto* local_in = reinterpret_cast<const sockaddr_in*>(local_addr);

    char ib_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &local_in->sin_addr, ib_ip, sizeof(ib_ip));
    std::cout << "[Client " << id_ << "] IB address: " << ib_ip << "\n";

    peer_ec_ = rdma_create_event_channel();
    if (!peer_ec_) throw std::runtime_error("rdma_create_event_channel failed for peers");

    if (rdma_create_id(peer_ec_, &peer_listener_, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("rdma_create_id failed for peer listener");

    sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(peer_port + id_);
    bind_addr.sin_addr.s_addr = INADDR_ANY;

    if (rdma_bind_addr(peer_listener_, reinterpret_cast<sockaddr*>(&bind_addr)))
        throw std::runtime_error("rdma_bind_addr failed for peer port " + std::to_string(peer_port + id_));
    if (rdma_listen(peer_listener_, 16))
        throw std::runtime_error("rdma_listen failed for peer listener");

    std::cout << "[Client " << id_ << "] Peer listener on port " << (peer_port + id_) << "\n";

    // ── Phase 1: Connect to all lower-id clients ──
    for (uint32_t target = 0; target < id_; ++target) {
        const uint16_t target_port = peer_port + target;

        rdma_event_channel* conn_ec = rdma_create_event_channel();
        if (!conn_ec) throw std::runtime_error("create_event_channel failed for peer connect");

        rdma_cm_id* cm_id = nullptr;
        bool connected = false;
        std::string last_error;

        for (int attempt = 0; attempt < 60; ++attempt) {
            try {
                if (rdma_create_id(conn_ec, &cm_id, nullptr, RDMA_PS_TCP))
                    throw std::runtime_error("create_id failed");

                sockaddr_in src_addr{};
                src_addr.sin_family = AF_INET;
                src_addr.sin_addr = local_in->sin_addr;
                src_addr.sin_port = 0;

                sockaddr_in dst_addr{};
                dst_addr.sin_family = AF_INET;
                dst_addr.sin_port = htons(target_port);
                dst_addr.sin_addr = local_in->sin_addr;

                if (rdma_resolve_addr(cm_id,
                                      reinterpret_cast<sockaddr*>(&src_addr),
                                      reinterpret_cast<sockaddr*>(&dst_addr), 2000))
                    throw std::runtime_error("resolve_addr failed: " + std::string(strerror(errno)));

                auto* ev = wait_for_event(conn_ec, RDMA_CM_EVENT_ADDR_RESOLVED, "PEER_ADDR");
                rdma_ack_cm_event(ev);

                if (rdma_resolve_route(cm_id, 2000))
                    throw std::runtime_error("resolve_route failed: " + std::string(strerror(errno)));

                ev = wait_for_event(conn_ec, RDMA_CM_EVENT_ROUTE_RESOLVED, "PEER_ROUTE");
                rdma_ack_cm_event(ev);

                ibv_qp_init_attr qp_attr{};
                qp_attr.qp_type = IBV_QPT_RC;
                qp_attr.send_cq = cq_;
                qp_attr.recv_cq = cq_;
                qp_attr.cap.max_send_wr = QP_DEPTH;
                qp_attr.cap.max_recv_wr = QP_DEPTH;
                qp_attr.cap.max_send_sge = 1;
                qp_attr.cap.max_recv_sge = 1;
                qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

                if (rdma_create_qp(cm_id, pd_, &qp_attr))
                    throw std::runtime_error("create_qp failed: " + std::string(strerror(errno)));

                ConnPrivateData priv{};
                priv.node_id = id_;
                priv.type = ConnType::CLIENT;
                priv.addr = reinterpret_cast<uintptr_t>(buf_);
                priv.rkey = mr_->rkey;

                rdma_conn_param param{};
                param.private_data = &priv;
                param.private_data_len = sizeof(priv);
                param.responder_resources = 1;
                param.initiator_depth = 1;
                param.rnr_retry_count = 7;

                if (rdma_connect(cm_id, &param))
                    throw std::runtime_error("connect failed: " + std::string(strerror(errno)));

                ev = wait_for_event(conn_ec, RDMA_CM_EVENT_ESTABLISHED, "PEER_ESTABLISHED");
                auto* remote = static_cast<const ConnPrivateData*>(
                    ev->param.conn.private_data);

                peers_[target] = {
                    .id = cm_id,
                    .addr = remote ? remote->addr : 0,
                    .rkey = remote ? remote->rkey : 0,
                };

                rdma_ack_cm_event(ev);
                connected = true;
                peer_conn_ecs_.push_back(conn_ec);
                break;
            } catch (const std::exception& e) {
                last_error = e.what();
                if (attempt % 10 == 0) {
                    std::cerr << "[Client " << id_ << "] Peer " << target
                              << " attempt " << attempt << ": " << last_error << "\n";
                }
                if (cm_id) {
                    rdma_destroy_id(cm_id);
                    cm_id = nullptr;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        if (!connected) {
            rdma_destroy_event_channel(conn_ec);
            throw std::runtime_error("Failed to connect to peer client " + std::to_string(target) + ": " + last_error);
        }

        std::cout << "[Client " << id_ << "] Peer " << target << " connected\n";
    }

    // ── Phase 2: Accept from all higher-id clients ──
    const uint32_t expect_higher = NUM_CLIENTS - 1 - id_;

    for (uint32_t i = 0; i < expect_higher; ++i) {
        rdma_cm_event* event = nullptr;

        while (true) {
            if (rdma_get_cm_event(peer_ec_, &event))
                throw std::runtime_error("rdma_get_cm_event failed in peer accept");
            if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) break;
            rdma_ack_cm_event(event);
        }

        rdma_cm_id* new_id = event->id;
        auto* incoming = static_cast<const ConnPrivateData*>(
            event->param.conn.private_data);

        if (!incoming || event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            --i;
            continue;
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.send_cq = cq_;
        qp_attr.recv_cq = cq_;
        qp_attr.cap.max_send_wr = QP_DEPTH;
        qp_attr.cap.max_recv_wr = QP_DEPTH;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

        if (rdma_create_qp(new_id, pd_, &qp_attr)) {
            std::cerr << "[Client " << id_ << "] Peer accept create_qp failed: " << strerror(errno) << "\n";
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            --i;
            continue;
        }

        ConnPrivateData my_creds{};
        my_creds.node_id = id_;
        my_creds.type = ConnType::CLIENT;
        my_creds.addr = reinterpret_cast<uintptr_t>(buf_);
        my_creds.rkey = mr_->rkey;

        rdma_conn_param accept_params{};
        accept_params.private_data = &my_creds;
        accept_params.private_data_len = sizeof(my_creds);
        accept_params.responder_resources = 1;
        accept_params.initiator_depth = 1;
        accept_params.rnr_retry_count = 7;

        if (rdma_accept(new_id, &accept_params)) {
            std::cerr << "[Client " << id_ << "] Peer accept rdma_accept failed: " << strerror(errno) << "\n";
            rdma_destroy_qp(new_id);
            rdma_ack_cm_event(event);
            --i;
            continue;
        }

        const uint32_t remote_id = incoming->node_id;
        peers_[remote_id] = {
            .id = new_id,
            .addr = incoming->addr,
            .rkey = incoming->rkey,
        };

        rdma_ack_cm_event(event);
        std::cout << "[Client " << id_ << "] Peer " << remote_id << " accepted\n";
    }

    std::cout << "[Client " << id_ << "] All " << (NUM_CLIENTS - 1) << " peer connections ready\n";
}