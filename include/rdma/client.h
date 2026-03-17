#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include "common.h"

struct RemoteNode;

class Client {
    friend class Lock;
    template <typename>
    friend class LockStrategyBase;

public:
    explicit Client(uint32_t id, size_t buffer_size = CLIENT_ALIGNED_SIZE);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    void connect(const std::vector<std::string>& node_ips, uint16_t port);
    void connect_peers(uint16_t peer_port);

    [[nodiscard]] uint32_t id() const { return id_; }
    [[nodiscard]] ibv_cq* cq() const { return cq_; }
    [[nodiscard]] ibv_mr* mr() const { return mr_; }
    [[nodiscard]] void* buffer() const { return buf_; }
    [[nodiscard]] size_t buffer_size() const { return buffer_size_; }
    [[nodiscard]] const std::vector<RemoteNode>& connections() const { return connections_; }
    [[nodiscard]] const std::vector<RemoteNode>& peers() const { return peers_; }

private:
    uint32_t id_;

    rdma_event_channel* ec_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_cq* cq_ = nullptr;
    ibv_mr* mr_ = nullptr;
    void* buf_ = nullptr;
    size_t buffer_size_;
    std::vector<RemoteNode> connections_;
    std::vector<RemoteNode> peers_;

    // peer mesh resources — kept alive for connection lifetime
    rdma_event_channel* peer_ec_ = nullptr;
    rdma_cm_id* peer_listener_ = nullptr;
    std::vector<rdma_event_channel*> peer_conn_ecs_;
    std::vector<rdma_cm_id*> peer_aux_ids_;
};
