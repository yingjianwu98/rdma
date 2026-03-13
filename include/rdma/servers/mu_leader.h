#pragma once

#include "rdma/server.h"

class MuLeader final : public Server {
public:
    explicit MuLeader(const uint32_t node_id) : Server(node_id) {}

protected:
    [[nodiscard]] uint32_t expected_clients() const override {
        return TOTAL_CLIENTS;
    }
    void run() override;
};