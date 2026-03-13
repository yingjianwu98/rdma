#pragma once
#include <cstdint>

// ── MU multi-instance config ──

constexpr size_t MU_NUM_INSTANCES = 2;
constexpr size_t MU_LOCKS_PER_INSTANCE = MAX_LOCKS / MU_NUM_INSTANCES;

static inline size_t mu_instance_for_lock(uint16_t lock_id) {
    size_t inst = lock_id / MU_LOCKS_PER_INSTANCE;
    if (inst >= MU_NUM_INSTANCES) inst = MU_NUM_INSTANCES - 1;
    return inst;
}

// ── Lock header (first 8 bytes of each lock region) ──
static inline void mu_write_commit_index(uint8_t* lock_base, uint64_t commit_index) {
    *reinterpret_cast<uint64_t*>(lock_base) = commit_index;
}

static inline uint64_t mu_read_commit_index(const uint8_t* lock_base) {
    return *reinterpret_cast<const uint64_t*>(lock_base);
}

static inline uint8_t* mu_lock_base(uint8_t* buf, uint32_t lock_id) {
    return buf + lock_id * LOCK_REGION_SIZE;
}

static inline uint8_t* mu_entry_ptr(uint8_t* lock_base, uint64_t slot) {
    return lock_base + LOCK_HEADER_SIZE + slot * ENTRY_SIZE;
}

static constexpr size_t MAX_INFLIGHT = 20;

// ── Client IMM encoding (32 bits) ──
static constexpr uint32_t MU_OP_CLIENT_LOCK   = 0;
static constexpr uint32_t MU_OP_CLIENT_UNLOCK  = 1;

static inline uint32_t mu_encode_imm(uint16_t lock_id, uint16_t client_id, uint32_t op) {
    return (static_cast<uint32_t>(lock_id) << 16)
        | (1u << 15)
        | ((static_cast<uint32_t>(client_id) & 0x3FFF) << 1)
        | (op & 0x1);
}

static inline uint16_t mu_decode_client_id(uint32_t imm) {
    return static_cast<uint16_t>((imm >> 1) & 0x3FFF);
}

static inline uint16_t mu_decode_lock_id(uint32_t imm) {
    return static_cast<uint16_t>((imm >> 16) & 0xFFFF);
}

static inline uint32_t mu_decode_op(uint32_t imm) {
    return (imm & 0x1) ? MU_OP_CLIENT_UNLOCK : MU_OP_CLIENT_LOCK;
}

// ── Entry: just the client imm (4 bytes used, 8 byte slot) ──
static inline void mu_write_entry(uint8_t* entry, uint32_t client_imm) {
    *reinterpret_cast<uint64_t*>(entry) = static_cast<uint64_t>(client_imm);
}

static inline uint32_t mu_read_client_imm(const uint8_t* entry) {
    return *reinterpret_cast<const uint32_t*>(entry);
}

static constexpr uint64_t ACK_TAG = 0xFFFF;

inline constexpr uint32_t mu_encode_ack(uint8_t lock_id, uint16_t slot, bool granted) {
    return (static_cast<uint32_t>(lock_id) << 24)
        | (static_cast<uint32_t>(slot) << 8)
        | (granted ? 1u : 0u);
}

static constexpr uint32_t MU_REPL_COMMIT_BIT = (1u << 31);

static inline uint32_t mu_encode_commit_notify(uint16_t lock_id, uint16_t commit_index) {
    return MU_REPL_COMMIT_BIT
        | (static_cast<uint32_t>(lock_id) << 16)
        | static_cast<uint32_t>(commit_index);
}

static inline bool mu_is_commit_notify(uint32_t imm) {
    return (imm & MU_REPL_COMMIT_BIT) != 0;
}

static inline uint16_t mu_decode_commit_lock_id(uint32_t imm) {
    return static_cast<uint16_t>((imm >> 16) & 0x7FFF);
}

static inline uint16_t mu_decode_commit_index(uint32_t imm) {
    return static_cast<uint16_t>(imm & 0xFFFF);
}