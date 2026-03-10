#pragma once

#include "rdma/server.h"

class SynraNode final : public Server {
public:
    explicit SynraNode(const uint32_t node_id) : Server(node_id) {
    }

protected:
    [[nodiscard]] uint32_t expected_clients() const override { return NUM_CLIENTS; }
    void run() override;
};
