#pragma once
#include <cstdint>

class Client;
class LockStrategy;

class Lock {
public:
    Lock(Client& client, LockStrategy& strategy, uint32_t lock_id);
    ~Lock();

    Lock(Lock&&) noexcept = default;
    Lock& operator=(Lock&&) noexcept = default;
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;

    void lock();
    void unlock();
    void cleanup() const;

    [[nodiscard]] bool     is_locked() const { return locked_; }
    [[nodiscard]] uint32_t lock_id()   const { return lock_id_; }

private:
    Client*       client_;
    LockStrategy* strategy_;
    uint32_t      lock_id_;
    int           op_id_;
    bool          locked_   = false;
};