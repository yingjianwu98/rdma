#include "rdma/server.h"
#include "rdma/tcp_qp_exchange.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <poll.h>

Server::Server(const uint32_t node_id)
    : node_id_(node_id)
    , peers_(CLUSTER_NODES.size())
    , clients_(TOTAL_CLIENTS)
{
    server_creds_.node_id = node_id;
    server_creds_.type = ConnType::FOLLOWER;
}

Server::~Server() {
    for (auto& c : clients_) {
        if (c.cm_id) {
            if (c.cm_id->qp) ibv_destroy_qp(c.cm_id->qp);
            delete c.cm_id;
        }
    }
    for (auto& p : peers_) {
        if (p.cm_id) {
            if (p.cm_id->qp) ibv_destroy_qp(p.cm_id->qp);
            delete p.cm_id;
        }
    }
    if (mr_) ibv_dereg_mr(mr_);
    if (cq_) ibv_destroy_cq(cq_);
    if (pd_) ibv_dealloc_pd(pd_);
    if (buf_) free_hugepage_buffer(buf_, SERVER_ALIGNED_SIZE);
}

// TCP-based connection to peer node
RemoteConnection Server::connect_to_node(const std::string& ip, uint16_t port) {
    // 1. TCP connect
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        throw std::runtime_error("socket() failed for " + ip);
    }

    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sock);
        throw std::runtime_error("TCP connect failed for " + ip);
    }

    // Get RDMA device if not already initialized
    ibv_context* ctx = pd_ ? pd_->context : nullptr;
    if (!ctx) {
        ibv_device** dev_list = ibv_get_device_list(nullptr);
        if (!dev_list || !dev_list[0]) {
            close(sock);
            throw std::runtime_error("No RDMA devices found");
        }
        ctx = ibv_open_device(dev_list[0]);
        ibv_free_device_list(dev_list);
        if (!ctx) {
            close(sock);
            throw std::runtime_error("Failed to open RDMA device");
        }
    }

    const int ib_port = 1;
    const int gid_index = detect_gid_index(ctx, ib_port);

    // 2. Initialize RDMA resources if first connection
    if (!pd_) {
        pd_ = ibv_alloc_pd(ctx);
        if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

        cq_ = ibv_create_cq(ctx, QP_DEPTH * (TOTAL_CLIENTS + CLUSTER_NODES.size()), nullptr, nullptr, 0);
        if (!cq_) throw std::runtime_error("ibv_create_cq failed");

        mr_ = ibv_reg_mr(pd_, buf_, SERVER_ALIGNED_SIZE,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
        if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

        server_creds_.addr = reinterpret_cast<uintptr_t>(buf_);
        server_creds_.rkey = mr_->rkey;
    }

    // 3. Create QP
    ibv_qp_init_attr qp_attr{};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq_;
    qp_attr.recv_cq = cq_;
    qp_attr.cap.max_send_wr = QP_DEPTH;
    qp_attr.cap.max_recv_wr = QP_DEPTH;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;
    qp_attr.sq_sig_all = 0;

    ibv_qp* qp = ibv_create_qp(pd_, &qp_attr);
    if (!qp) {
        close(sock);
        throw std::runtime_error("ibv_create_qp failed for " + ip);
    }

    // 4. Transition QP to INIT
    qp_to_init(qp, ib_port);

    // 5. Exchange QP info over TCP
    QpExchangeInfo local_info = get_local_qp_info(
        qp, ctx, ib_port, gid_index,
        server_creds_.addr, server_creds_.rkey,
        node_id_, static_cast<uint8_t>(ConnType::FOLLOWER)
    );

    QpExchangeInfo remote_info{};
    if (!tcp_exchange_qp_info(sock, &local_info, &remote_info)) {
        close(sock);
        ibv_destroy_qp(qp);
        throw std::runtime_error("QP exchange failed for " + ip);
    }

    close(sock);

    // 6. Transition QP to RTR and RTS
    qp_to_rtr(qp, ib_port, gid_index, &remote_info);
    qp_to_rts(qp);

    // 7. Create fake rdma_cm_id wrapper
    auto* fake_id = new rdma_cm_id{};
    fake_id->qp = qp;
    fake_id->verbs = ctx;

    RemoteConnection conn{};
    conn.id = remote_info.node_id;
    conn.cm_id = fake_id;
    conn.remote_addr = remote_info.remote_addr;
    conn.rkey = remote_info.rkey;
    conn.type = static_cast<ConnType>(remote_info.conn_type);

    return conn;
}

void Server::start(uint16_t port) {
    std::cout << "[Server " << node_id_ << "] Starting (TCP-based QP exchange)...\n" << std::flush;

    // Allocate buffer first
    buf_ = allocate_server_buffer();
    std::cout << "[Server " << node_id_ << "] Buffer allocated\n";

    auto* base = static_cast<uint8_t*>(buf_);
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + lock_control_offset(i)) = 0;
        *reinterpret_cast<volatile uint64_t*>(base + lock_turn_offset(i)) = 0;
    }

    const size_t num_nodes = CLUSTER_NODES.size();
    const uint32_t num_clients = expected_clients();
    std::cout << "[Server " << node_id_ << "] Config: " << num_nodes << " nodes, " << num_clients << " clients\n";

    // Create TCP listener
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        throw std::runtime_error("socket() failed");
    }

    int reuse = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(listen_sock);
        throw std::runtime_error("bind() failed");
    }

    if (listen(listen_sock, 32) < 0) {
        close(listen_sock);
        throw std::runtime_error("listen() failed");
    }

    std::cout << "[Server " << node_id_ << "] Listening on TCP port " << port << "\n";

    // Phase 1: Connect to lower-ID nodes
    for (uint32_t target = 0; target < node_id_; ++target) {
        std::cout << "[Server " << node_id_ << "] Connecting to node " << target << "...\n";
        RemoteConnection conn{};
        bool connected = false;
        for (int attempt = 0; attempt < 60; ++attempt) {
            try {
                conn = connect_to_node(CLUSTER_NODES[target], port);
                connected = true;
                break;
            } catch (const std::exception& e) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        if (!connected) {
            throw std::runtime_error("Failed to connect to node " + std::to_string(target));
        }
        peers_[target] = conn;
        std::cout << "[Server " << node_id_ << "] Peer " << target << " connected\n";
    }

    // Phase 2: Accept higher-ID nodes + clients
    const uint32_t expect_higher = num_nodes - 1 - node_id_;
    uint32_t higher_connected = 0;
    uint32_t clients_connected = 0;

    std::cout << "[Server " << node_id_ << "] Waiting for "
              << expect_higher << " higher nodes + " << num_clients << " clients\n";

    // Get RDMA device
    ibv_context* ctx = pd_ ? pd_->context : nullptr;
    if (!ctx) {
        ibv_device** dev_list = ibv_get_device_list(nullptr);
        if (!dev_list || !dev_list[0]) throw std::runtime_error("No RDMA devices found");
        ctx = ibv_open_device(dev_list[0]);
        ibv_free_device_list(dev_list);
        if (!ctx) throw std::runtime_error("Failed to open RDMA device");
    }

    const int ib_port = 1;
    const int gid_index = detect_gid_index(ctx, ib_port);

    while (higher_connected < expect_higher || clients_connected < num_clients) {
        // Accept TCP connection
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int conn_sock = accept(listen_sock, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (conn_sock < 0) continue;

        int flag = 1;
        setsockopt(conn_sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // Initialize RDMA resources if first accept
        if (!pd_) {
            pd_ = ibv_alloc_pd(ctx);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(ctx, QP_DEPTH * (TOTAL_CLIENTS + num_nodes), nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(pd_, buf_, SERVER_ALIGNED_SIZE,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
            if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

            server_creds_.addr = reinterpret_cast<uintptr_t>(buf_);
            server_creds_.rkey = mr_->rkey;
        }

        // Create QP
        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.send_cq = cq_;
        qp_attr.recv_cq = cq_;
        qp_attr.cap.max_send_wr = QP_DEPTH;
        qp_attr.cap.max_recv_wr = QP_DEPTH;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;
        qp_attr.sq_sig_all = 0;

        ibv_qp* qp = ibv_create_qp(pd_, &qp_attr);
        if (!qp) {
            close(conn_sock);
            continue;
        }

        qp_to_init(qp, ib_port);

        // Exchange QP info
        QpExchangeInfo local_info = get_local_qp_info(
            qp, ctx, ib_port, gid_index,
            server_creds_.addr, server_creds_.rkey,
            node_id_, static_cast<uint8_t>(ConnType::FOLLOWER)
        );

        QpExchangeInfo remote_info{};
        if (!tcp_exchange_qp_info(conn_sock, &local_info, &remote_info)) {
            close(conn_sock);
            ibv_destroy_qp(qp);
            continue;
        }

        close(conn_sock);

        // Transition QP
        qp_to_rtr(qp, ib_port, gid_index, &remote_info);
        qp_to_rts(qp);

        // Create connection record
        auto* fake_id = new rdma_cm_id{};
        fake_id->qp = qp;
        fake_id->verbs = ctx;

        const uint32_t nid = remote_info.node_id;
        const ConnType ctype = static_cast<ConnType>(remote_info.conn_type);

        if (ctype == ConnType::FOLLOWER || ctype == ConnType::LEADER) {
            peers_[nid] = {nid, fake_id, remote_info.remote_addr, remote_info.rkey, ctype};
            higher_connected++;
            std::cout << "[Server " << node_id_ << "] Peer " << nid << " accepted\n";
        } else if (ctype == ConnType::CLIENT) {
            clients_[nid] = {nid, fake_id, remote_info.remote_addr, remote_info.rkey, ctype};
            clients_connected++;
            std::cout << "[Server " << node_id_ << "] Client " << clients_connected << "/" << num_clients << "\n";
        }
    }

    close(listen_sock);

    peers_[node_id_].id = node_id_;

    std::cout << "[Server " << node_id_ << "] Ready — "
              << (num_nodes - 1) << " peers + " << clients_connected << " clients\n";

    signal_clients_ready();
    run();
}

void Server::signal_clients_ready() {
    const uint32_t num_clients = expected_clients();
    if (num_clients == 0) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

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

    uint32_t done = 0;
    while (done < num_clients) {
        ibv_wc wc{};
        int n = ibv_poll_cq(cq_, 1, &wc);
        if (n > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("GO signal failed");
            }
            done++;
        }
    }

    std::cout << "[Server " << node_id_ << "] GO sent to " << num_clients << " clients\n";
}
