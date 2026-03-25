#pragma once

#include "rdma/server.h"

class SimpleMuLeader final : public Server {
public:
    explicit SimpleMuLeader(const uint32_t node_id)
        : Server(node_id) {
    }

protected:
    [[nodiscard]] uint32_t expected_clients() const override {
        return TOTAL_CLIENTS;
    }

    void run() override;
};
