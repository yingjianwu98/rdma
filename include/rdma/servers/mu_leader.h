#pragma once

#include "rdma/server.h"

class MuLeader final : public Server {
public:
    explicit MuLeader(const uint32_t node_id) : Server(node_id) {}

protected:
    [[nodiscard]] uint32_t expected_followers() const override {
        return CLUSTER_NODES.size() - 1;
    }
    [[nodiscard]] uint32_t expected_clients() const override {
        return NUM_CLIENTS;
    }

    void on_accept(
        rdma_cm_id* new_id,
        const ConnPrivateData& incoming,
        rdma_conn_param& accept_params
    ) override;

    void run() override;

private:
    void*   client_pool_ = nullptr;
    ibv_mr* client_mr_   = nullptr;
};