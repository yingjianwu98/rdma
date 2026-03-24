#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/tcp_qp_exchange.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <sys/mman.h>

// Modified Client to use TCP-based QP exchange instead of rdma_cm

Client::Client(const uint32_t id, const size_t buffer_size)
    : id_(id)
    , buffer_size_(std::max(align_up(buffer_size, PAGE_SIZE), PAGE_SIZE)) {
}

Client::~Client() {
    for (const auto& conn : connections_) {
        if (conn.id) {
            if (conn.id->qp) ibv_destroy_qp(conn.id->qp);
            delete conn.id;  // We allocate RemoteNode.id as a placeholder now
        }
    }
    if (mr_) ibv_dereg_mr(mr_);
    if (cq_) ibv_destroy_cq(cq_);
    if (pd_) ibv_dealloc_pd(pd_);
    if (buf_) free_hugepage_buffer(buf_, buffer_size_);
}

void Client::connect(const std::vector<std::string>& node_ips, const uint16_t port) {
    if (!buf_) {
        buf_ = allocate_client_buffer(buffer_size_);
    }

    // Get first RDMA device to initialize resources
    ibv_device** dev_list = ibv_get_device_list(nullptr);
    if (!dev_list || !dev_list[0]) {
        throw std::runtime_error("No RDMA devices found");
    }

    ibv_context* ctx = ibv_open_device(dev_list[0]);
    if (!ctx) {
        ibv_free_device_list(dev_list);
        throw std::runtime_error("Failed to open RDMA device");
    }
    ibv_free_device_list(dev_list);

    const int ib_port = 1;
    const int gid_index = detect_gid_index(ctx, ib_port);

    std::cout << "[Client " << id_ << "] Using GID index: " << gid_index << "\n";

    // Initialize PD, CQ, MR once
    if (!pd_) {
        pd_ = ibv_alloc_pd(ctx);
        if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

        cq_ = ibv_create_cq(ctx, QP_DEPTH * std::max<size_t>(node_ips.size() * 2, 32), nullptr, nullptr, 0);
        if (!cq_) throw std::runtime_error("ibv_create_cq failed");

        mr_ = ibv_reg_mr(pd_, buf_, buffer_size_, IBV_ACCESS_LOCAL_WRITE);
        if (!mr_) throw std::runtime_error("ibv_reg_mr failed");
    }

    // Connect to each server node via TCP + manual QP setup
    for (size_t i = 0; i < node_ips.size(); ++i) {
        std::cout << "[Client " << id_ << "] Connecting to " << node_ips[i] << ":" << port << "...\n";

        // 1. TCP connect to server
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            throw std::runtime_error("socket() failed for node " + std::to_string(i));
        }

        // Disable Nagle for lower latency
        int flag = 1;
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (inet_pton(AF_INET, node_ips[i].c_str(), &addr.sin_addr) <= 0) {
            close(sock);
            throw std::runtime_error("Invalid IP: " + node_ips[i]);
        }

        if (::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(sock);
            throw std::runtime_error("TCP connect failed for " + node_ips[i]);
        }

        // 2. Create QP
        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.send_cq = cq_;
        qp_attr.recv_cq = cq_;
        qp_attr.cap.max_send_wr = QP_DEPTH;
        qp_attr.cap.max_recv_wr = QP_DEPTH;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

        ibv_qp* qp = ibv_create_qp(pd_, &qp_attr);
        if (!qp) {
            close(sock);
            throw std::runtime_error("ibv_create_qp failed for node " + std::to_string(i));
        }

        // 3. Transition QP to INIT
        qp_to_init(qp, ib_port);

        // 4. Exchange QP info over TCP
        QpExchangeInfo local_info = get_local_qp_info(
            qp, ctx, ib_port, gid_index,
            reinterpret_cast<uintptr_t>(buf_), mr_->rkey,
            id_, static_cast<uint8_t>(ConnType::CLIENT)
        );

        QpExchangeInfo remote_info{};
        if (!tcp_exchange_qp_info(sock, &local_info, &remote_info)) {
            close(sock);
            ibv_destroy_qp(qp);
            throw std::runtime_error("QP exchange failed for node " + std::to_string(i));
        }

        close(sock);  // TCP handshake done

        // 5. Transition QP to RTR and RTS
        qp_to_rtr(qp, ib_port, gid_index, &remote_info);
        qp_to_rts(qp);

        // 6. Pre-post recv for GO signal
        ibv_recv_wr rr{}, *bad_rr = nullptr;
        rr.wr_id = 0xBEEF0000 | connections_.size();
        rr.sg_list = nullptr;
        rr.num_sge = 0;
        if (ibv_post_recv(qp, &rr, &bad_rr)) {
            throw std::runtime_error("Failed to pre-post GO recv");
        }

        // 7. Store connection info
        // Create a minimal rdma_cm_id* wrapper just to hold the QP
        auto* fake_id = new rdma_cm_id{};
        fake_id->qp = qp;
        fake_id->verbs = ctx;

        connections_.push_back({
            .id = fake_id,
            .addr = remote_info.remote_addr,
            .rkey = remote_info.rkey
        });

        std::cout << "[Client " << id_ << "] Connected to node " << i
                  << " (QP " << qp->qp_num << " <-> " << remote_info.qp_num << ")\n";
    }

    std::cout << "[Client " << id_ << "] All " << node_ips.size() << " connections established\n";
}
