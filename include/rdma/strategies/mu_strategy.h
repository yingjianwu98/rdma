#pragma once

#include "rdma/lock_strategy.h"
#include <cstdint>

class Client;

class MuStrategy final : public LockStrategy {
public:
    uint64_t acquire(Client& client, int op_id, uint32_t lock_id) override;
    void release(Client& client, int op_id, uint32_t lock_id) override;
    [[nodiscard]] const char* name() const override { return "Mu"; }
private:
    static void send_and_wait(Client& client, uint32_t lock_id, uint32_t op);
};
