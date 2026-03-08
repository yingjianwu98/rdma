#pragma once

#include <cstdint>

class Client;

class LockStrategy {
public:
    virtual ~LockStrategy() = default;
    virtual uint64_t acquire(Client& client, int op_id, uint32_t lock_id) = 0;
    virtual void release(Client& client, int op_id, uint32_t lock_id) = 0;
    virtual void cleanup(Client& client, int op_id, uint32_t lock_id) {}
    [[nodiscard]] virtual const char* name() const = 0;
};
