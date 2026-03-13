#pragma once

#include "rdma/lock_strategy.h"
#include <cstdint>

#include "rdma/common.h"

class Client;

class CasStrategy final : public LockStrategy {
public:
    uint64_t acquire(Client& client, int op_id, uint32_t lock_id) override;
    void release(Client& client, int op_id, uint32_t lock_id) override;
    void cleanup(Client& client, int op_id, uint32_t lock_id) override;
    [[nodiscard]] const char* name() const override { return "synra-cas"; }

private:
    uint64_t target_slot_ = 1;
    uint32_t unsignaled_counts_[MAX_REPLICAS] = {};
};
