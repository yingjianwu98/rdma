#pragma once

#include "rdma/common.h"

#include <cstddef>
#include <cstdint>

enum class MuRpcOp : uint8_t {
    Lock = 1,
    Unlock = 2,
};

enum class MuRpcStatus : uint8_t {
    Ok = 0,
    InvalidUnlock = 1,
    QueueFull = 2,
    InternalError = 3,
};

struct MuRequest {
    uint8_t op;
    uint8_t reserved0;
    uint16_t client_id;
    uint32_t lock_id;
    uint32_t req_id;
    uint32_t granted_slot;
};

struct MuResponse {
    uint8_t op;
    uint8_t status;
    uint16_t client_id;
    uint32_t lock_id;
    uint32_t req_id;
    uint32_t granted_slot;
};

static_assert(sizeof(MuRequest) == 16);
static_assert(sizeof(MuResponse) == 16);

constexpr uint64_t MU_ENTRY_LIVE_BIT = 1ULL << 63;
constexpr uint64_t MU_ENTRY_GENERATION_SHIFT = 48;
constexpr uint64_t MU_ENTRY_CLIENT_ID_SHIFT = 32;
constexpr uint64_t MU_ENTRY_REQ_ID_SHIFT = 0;
constexpr uint64_t MU_ENTRY_GENERATION_MASK = 0x7FFFULL;
constexpr uint64_t MU_ENTRY_CLIENT_ID_MASK = 0xFFFFULL;
constexpr uint64_t MU_ENTRY_REQ_ID_MASK = 0xFFFFFFFFULL;

inline uint16_t mu_logical_generation(const uint32_t logical_slot) {
    return static_cast<uint16_t>(logical_slot / MU_LOG_CAPACITY);
}

inline uint32_t mu_physical_slot(const uint32_t logical_slot) {
    return logical_slot % MU_LOG_CAPACITY;
}

inline uint64_t mu_make_free(const uint16_t generation) {
    return static_cast<uint64_t>(generation) << MU_ENTRY_GENERATION_SHIFT;
}

inline uint64_t mu_expected_free(const uint16_t generation) {
    return generation == 0 ? EMPTY_SLOT : mu_make_free(generation);
}

inline uint64_t mu_make_live(const uint16_t generation, const uint16_t client_id, const uint32_t req_id) {
    return MU_ENTRY_LIVE_BIT
         | (static_cast<uint64_t>(generation) << MU_ENTRY_GENERATION_SHIFT)
         | (static_cast<uint64_t>(client_id) << MU_ENTRY_CLIENT_ID_SHIFT)
         | static_cast<uint64_t>(req_id);
}

inline uint16_t mu_entry_client_id(const uint64_t entry) {
    return static_cast<uint16_t>((entry >> MU_ENTRY_CLIENT_ID_SHIFT) & MU_ENTRY_CLIENT_ID_MASK);
}

inline uint32_t mu_entry_req_id(const uint64_t entry) {
    return static_cast<uint32_t>((entry >> MU_ENTRY_REQ_ID_SHIFT) & MU_ENTRY_REQ_ID_MASK);
}

inline uint16_t mu_entry_generation(const uint64_t entry) {
    return static_cast<uint16_t>((entry >> MU_ENTRY_GENERATION_SHIFT) & MU_ENTRY_GENERATION_MASK);
}

inline bool mu_entry_is_live(const uint64_t entry) {
    return (entry & MU_ENTRY_LIVE_BIT) != 0;
}

inline bool mu_entry_is_unlocked(const uint64_t entry) {
    return !mu_entry_is_live(entry);
}

inline void mu_write_commit_index(uint8_t* lock_base, const uint64_t commit_index) {
    *reinterpret_cast<uint64_t*>(lock_base) = commit_index;
}

inline uint64_t mu_read_commit_index(const uint8_t* lock_base) {
    return *reinterpret_cast<const uint64_t*>(lock_base);
}

inline uint8_t* mu_lock_base(uint8_t* buf, const uint32_t lock_id) {
    return buf + lock_base_offset(lock_id);
}

inline const uint8_t* mu_lock_base(const uint8_t* buf, const uint32_t lock_id) {
    return buf + lock_base_offset(lock_id);
}

inline uint8_t* mu_entry_ptr(uint8_t* lock_base, const uint32_t slot) {
    return lock_base + LOCK_HEADER_SIZE + (static_cast<size_t>(mu_physical_slot(slot)) * ENTRY_SIZE);
}

inline const uint8_t* mu_entry_ptr(const uint8_t* lock_base, const uint32_t slot) {
    return lock_base + LOCK_HEADER_SIZE + (static_cast<size_t>(mu_physical_slot(slot)) * ENTRY_SIZE);
}

inline uint64_t mu_read_entry_word(const uint8_t* lock_base, const uint32_t slot) {
    return *reinterpret_cast<const uint64_t*>(mu_entry_ptr(lock_base, slot));
}

inline void mu_write_entry_word(uint8_t* lock_base, const uint32_t slot, const uint64_t word) {
    *reinterpret_cast<uint64_t*>(mu_entry_ptr(lock_base, slot)) = word;
}

constexpr size_t MU_SERVER_RECV_RING = 2048;
constexpr size_t MU_MAX_PENDING_PER_LOCK = 1024;
constexpr size_t MU_MAX_APPEND_INFLIGHT_PER_LOCK = 64;
