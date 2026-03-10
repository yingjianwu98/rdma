#include "rdma/client.h"
#include "rdma/common.h"

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
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
    for (const auto& conn : connections_) {
        if (conn.id && conn.id->qp) rdma_destroy_qp(conn.id);
        if (conn.id) rdma_destroy_id(conn.id);
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
                QP_DEPTH * static_cast<int>(node_ips.size()),
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

    std::cout << "[Client " << id_ << "] All " << node_ips.size() << " connections established\n";
}
