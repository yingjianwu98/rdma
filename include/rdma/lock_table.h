#pragma once

#include <cstdint>
#include <random>
#include <stdexcept>
#include <vector>

#include "lock.h"
#include "lock_strategy.h"

class Client;

class LockTable {
public:
    struct Entry {
        uint32_t      lock_id;
        LockStrategy* strategy;
    };

    uint32_t add(LockStrategy& strategy) {
        const uint32_t id = static_cast<uint32_t>(entries_.size());
        entries_.push_back({id, &strategy});
        return id;
    }

    [[nodiscard]] Lock get(const uint32_t lock_id, Client& client) const {
        if (lock_id >= entries_.size())
            throw std::runtime_error("Invalid lock_id: " + std::to_string(lock_id));
        return Lock(client, *entries_[lock_id].strategy, lock_id);
    }

    struct Selection {
        uint32_t lock_id;
        Lock     lock;
    };

    [[nodiscard]] Selection random(Client& client) {
        thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<uint32_t> dist(0, entries_.size() - 1);
        uint32_t id = dist(rng);
        return { id, Lock(client, *entries_[id].strategy, id) };
    }

    [[nodiscard]] size_t size() const { return entries_.size(); }
    [[nodiscard]] const Entry& operator[](size_t i) const { return entries_[i]; }

private:
    std::vector<Entry> entries_;
};