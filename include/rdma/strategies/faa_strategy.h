#pragma once

#include "rdma/lock_strategy.h"
#include <cstdint>

class Client;

class FaaStrategy final : public LockStrategy {
public:
    uint64_t acquire(Client& client, int op_id, uint32_t lock_id) override;
    void release(Client& client, int op_id, uint32_t lock_id) override;
    void cleanup(Client& client, int op_id, uint32_t lock_id) override;
    [[nodiscard]] const char* name() const override { return "synra-faa"; }

private:
    uint64_t my_ticket_ = 0;
    uint32_t next_client_id_ = UINT32_MAX;

    static constexpr uint64_t DONE_BIT = 1ULL << 63;
    // Notification signal value — written into the waiter's local buffer
    static constexpr uint64_t GO_SIGNAL = 0xBEEF;
    // Clear value — what the waiter resets their notify slot to before waiting
    static constexpr uint64_t NOTIFY_CLEAR = 0;

    static inline uint64_t encode_slot(uint32_t client_id, bool done) {
        return static_cast<uint64_t>(client_id) | (done ? DONE_BIT : 0);
    }

    static inline uint32_t decode_client(uint64_t slot_val) {
        return static_cast<uint32_t>(slot_val & ~DONE_BIT);
    }

    static inline bool is_done(uint64_t slot_val) {
        return (slot_val & DONE_BIT) != 0;
    }
};