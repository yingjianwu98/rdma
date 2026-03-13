#pragma once

#include "rdma/server.h"

class MuLeader final : public Server {
public:
    MuLeader(uint32_t node_id, uint32_t lock_start, uint32_t lock_end)
        : Server(node_id), lock_start_(lock_start), lock_end_(lock_end) {}

protected:
    [[nodiscard]] uint32_t expected_clients() const override {
        return TOTAL_CLIENTS;
    }
    void run() override;

private:
    uint32_t lock_start_;
    uint32_t lock_end_;
};