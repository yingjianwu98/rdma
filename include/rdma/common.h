#pragma once

#include <array>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

// ─── Cluster config ───

inline const std::vector<std::string> CLUSTER_NODES = {
    "192.168.1.1",
    "192.168.1.2",
    "192.168.1.3",
    //"192.168.1.4"
};

inline const std::vector<std::string> CLIENT_NODES = {
     "192.168.1.4",
   // "192.168.1.5",
    // "192.168.1.6",
    // "192.168.1.7",
    // "192.168.1.8",
    // "192.168.1.9",
    // "192.168.1.10",
};

inline const size_t QUORUM = (CLUSTER_NODES.size() / 2) + 1;
inline const size_t SUPER_QUORUM = (3 * CLUSTER_NODES.size() + 3) / 4;

// ─── RDMA constants ───

constexpr uint16_t RDMA_PORT = 6969;
constexpr size_t ENTRY_SIZE = 8;
constexpr size_t QP_DEPTH = 2048;
constexpr size_t MAX_INLINE_DEPTH = 64;
constexpr size_t CLIENT_SLOT_SIZE = 1024;
constexpr size_t MAX_REPLICAS = 10;
constexpr uint8_t RDMA_RESPONDER_RESOURCES = 16;
constexpr uint8_t RDMA_INITIATOR_DEPTH = 16;

// ─── Benchmark / workload config ───

constexpr size_t NUM_OPS = 20000000;
constexpr size_t NUM_CLIENTS_PER_MACHINE = 1;
constexpr size_t TOTAL_MACHINES = 1;
constexpr size_t TOTAL_CLIENTS = NUM_CLIENTS_PER_MACHINE * TOTAL_MACHINES;
constexpr size_t NUM_OPS_PER_CLIENT = NUM_OPS / TOTAL_CLIENTS;
constexpr size_t NUM_TOTAL_OPS = NUM_OPS_PER_CLIENT * TOTAL_CLIENTS;
constexpr size_t MAX_LOCKS = 1000;

// ─── CAS config ───

constexpr size_t CAS_ACTIVE_CLIENTS = 2;
constexpr size_t CAS_CQ_BATCH = 32;
constexpr double CAS_ZIPF_SKEW = 0.9;
constexpr bool CAS_SHARD_OWNER = true;
constexpr uint32_t CAS_RELEASE_SIGNAL_EVERY = 100;
constexpr bool CAS_RELEASE_USE_CAS = true;

// ─── Simple CAS config ───

constexpr size_t SIMPLE_CAS_ACTIVE_WINDOW = 32;
constexpr bool SIMPLE_CAS_SHARD_OWNER = true;
constexpr size_t SIMPLE_CAS_CQ_BATCH = 32;
constexpr double SIMPLE_CAS_ZIPF_SKEW = 0.0;

// ─── FAA config ───

constexpr size_t FAA_ACTIVE_WINDOW = 32;
constexpr bool FAA_REPLICATE_USE_CAS = false;

// ─── Ticket FAA config ───

constexpr size_t TICKET_FAA_ACTIVE_WINDOW = 32;
constexpr bool TICKET_FAA_REPLICATE_USE_CAS = false;
constexpr bool TICKET_FAA_SHARD_OWNER = true;
constexpr size_t TICKET_FAA_CQ_BATCH = 32;
constexpr double TICKET_FAA_ZIPF_SKEW = 0.0;
constexpr uint32_t TICKET_FAA_TURN_SPIN_VERY_NEAR = 0;
constexpr uint32_t TICKET_FAA_TURN_SPIN_NEAR = 128;
constexpr uint32_t TICKET_FAA_TURN_SPIN_MID = 512;
constexpr uint32_t TICKET_FAA_TURN_SPIN_FAR = 1024;

// ─── Local Ticket FAA config ───

constexpr size_t LOCAL_TICKET_FAA_ACTIVE_WINDOW = 16;
constexpr bool LOCAL_TICKET_FAA_REPLICATE_USE_CAS = false;
constexpr size_t LOCAL_TICKET_FAA_CQ_BATCH = 32;
constexpr double LOCAL_TICKET_FAA_ZIPF_SKEW = 0.0;

// ─── MU config ───

constexpr size_t MU_ACTIVE_WINDOW = 32;

// ─── TAS config ───

constexpr size_t TAS_ACTIVE_WINDOW = 32;

// ─── Lock table layout ───

constexpr size_t MAX_LOG_PER_LOCK = ((NUM_OPS + MAX_LOCKS - 1) / MAX_LOCKS) * 4;
constexpr size_t LOCK_HEADER_SIZE = 16;
constexpr size_t LOCK_LOG_SIZE = MAX_LOG_PER_LOCK * ENTRY_SIZE;
constexpr size_t LOCK_REGION_SIZE = LOCK_HEADER_SIZE + LOCK_LOG_SIZE;
constexpr size_t LOCK_TABLE_SIZE = LOCK_REGION_SIZE * MAX_LOCKS;

// ─── Client staging area ───

constexpr size_t CLIENT_STAGING_SIZE = CLIENT_SLOT_SIZE;
constexpr size_t CLIENT_STAGING_OFFSET = LOCK_TABLE_SIZE;
constexpr size_t CLIENT_STAGING_TOTAL = CLIENT_STAGING_SIZE * TOTAL_CLIENTS;

// ─── Buffer sizing ───

constexpr size_t METADATA_SIZE = 4096;
constexpr size_t PAGE_SIZE = 4096;

// Server: full lock table + staging + metadata
constexpr size_t SERVER_POOL_SIZE = LOCK_TABLE_SIZE + CLIENT_STAGING_TOTAL + METADATA_SIZE;
constexpr size_t SERVER_ALIGNED_SIZE = ((SERVER_POOL_SIZE + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

// Client: just LocalState + padding (a few KB)
constexpr size_t CLIENT_POOL_SIZE = 4096 * 2;  // 8KB — plenty for LocalState
constexpr size_t CLIENT_ALIGNED_SIZE = ((CLIENT_POOL_SIZE + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

static_assert(LOCK_TABLE_SIZE <= SERVER_ALIGNED_SIZE, "Lock table exceeds server buffer size");

// ─── Per-lock offset helpers ───

inline constexpr size_t lock_base_offset(const uint32_t lock_id) {
    return lock_id * LOCK_REGION_SIZE;
}

inline constexpr size_t lock_control_offset(const uint32_t lock_id) {
    return lock_base_offset(lock_id);
}

inline constexpr size_t lock_turn_offset(const uint32_t lock_id) {
    return lock_base_offset(lock_id) + 8;
}

inline constexpr size_t lock_log_slot_offset(const uint32_t lock_id, const uint64_t slot) {
    return lock_base_offset(lock_id) + LOCK_HEADER_SIZE + (slot * ENTRY_SIZE);
}

// ─── Client staging offset helper ───

inline constexpr size_t client_staging_offset(const uint32_t client_id) {
    return CLIENT_STAGING_OFFSET + (client_id * CLIENT_STAGING_SIZE);
}

inline constexpr size_t align_up(const size_t value, const size_t alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
}

// ─── Sentinel values ───

constexpr uint64_t EMPTY_SLOT = 0xFFFFFFFFFFFFFFFF;

// ─── Connection types ───

enum class ConnType : uint8_t { FOLLOWER, CLIENT, LEADER };

struct ConnPrivateData {
    uintptr_t addr;
    uint32_t rkey;
    uint32_t node_id;
    ConnType type;
} __attribute__((packed));

struct RemoteNode {
    rdma_cm_id* id;
    uintptr_t addr;
    uint32_t rkey;
};

struct RemoteConnection {
    uint32_t id;
    rdma_cm_id* cm_id;
    uintptr_t remote_addr;
    uint32_t rkey;
    ConnType type;
};

struct alignas(64) LocalState {
    uint64_t frontier_values[MAX_REPLICAS];
    uint64_t cas_results[MAX_REPLICAS];
    uint64_t learn_results[MAX_REPLICAS];
    uint64_t next_frontier;
    uint64_t metadata;
    uint64_t notify_signal;
};
template <typename T, size_t Size>
class Queue {
    std::array<T, Size> buffer{};
    size_t head = 0;
    size_t tail = 0;
    size_t count = 0;

public:
    void push(T item) {
        if (count == Size) throw std::runtime_error("Queue full");
        buffer[tail] = item;
        tail = (tail + 1) % Size;
        count++;
    }

    bool pop(T& item) {
        if (count == 0) return false;
        item = buffer[head];
        head = (head + 1) % Size;
        count--;
        return true;
    }

    [[nodiscard]] size_t size() const { return count; }
};

// ─── Helpers ───

inline unsigned int get_uint_env(const std::string& name) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) {
        throw std::runtime_error("Environment variable '" + name + "' is not set or empty");
    }
    return static_cast<unsigned int>(std::stoul(val));
}

inline unsigned int get_uint_env_or(const std::string& name, const unsigned int fallback) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) return fallback;
    return static_cast<unsigned int>(std::stoul(val));
}

inline double get_double_env_or(const std::string& name, const double fallback) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) return fallback;
    return std::stod(val);
}

inline void* allocate_server_buffer(size_t num_locks = MAX_LOCKS) {
    void* ptr = aligned_alloc(PAGE_SIZE, SERVER_ALIGNED_SIZE);
    if (!ptr) throw std::runtime_error("Could not allocate server RDMA buffer");
    std::memset(ptr, 0xFF, SERVER_ALIGNED_SIZE);

    // zero lock headers (FAA counters)
    auto* base = static_cast<char*>(ptr);
    for (size_t l = 0; l < num_locks; ++l) {
        *reinterpret_cast<uint64_t*>(base + lock_control_offset(l)) = 0;
        *reinterpret_cast<uint64_t*>(base + lock_turn_offset(l)) = 0;
    }

    return ptr;
}

inline void* allocate_client_buffer(size_t requested_size = CLIENT_ALIGNED_SIZE) {
    const size_t aligned_size = std::max(align_up(requested_size, PAGE_SIZE), PAGE_SIZE);
    void* ptr = aligned_alloc(PAGE_SIZE, aligned_size);
    if (!ptr) throw std::runtime_error("Could not allocate client RDMA buffer");
    std::memset(ptr, 0xFF, aligned_size);
    return ptr;
}

inline void pin_thread_to_cpu(const int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        throw std::runtime_error("pthread_setaffinity_np failed");
    }
}

inline int pick_cpu_for_client(const int client_id) {
    static const int CPU_ORDER[16] = {
        0, 2, 4, 6, 8, 10, 12, 14,
        1, 3, 5, 7, 9, 11, 13, 15
    };
    return CPU_ORDER[client_id % 16];
}
