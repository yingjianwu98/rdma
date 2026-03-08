#pragma once

#include "rdma/server.h"

class MuFollower final : public Server {
public:
    explicit MuFollower(const uint32_t node_id) : Server(node_id) {}
    void connect_to_leader(const std::string& leader_ip, uint16_t port);

protected:
    [[nodiscard]] uint32_t expected_followers() const override { return 0; }
    [[nodiscard]] uint32_t expected_clients()   const override { return 0; }
    void run() override;
private:
    rdma_cm_id* leader_id_ = nullptr;
};