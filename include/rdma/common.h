#pragma once

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <pthread.h>
#include <sys/mman.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

// ─── Cluster config ───

inline const std::vector<std::string> CLUSTER_NODES = {
    "192.168.1.1",
    "192.168.1.2",
    "192.168.1.3",
};

inline const size_t QUORUM = (CLUSTER_NODES.size() / 2) + 1;

// ─── RDMA constants ───

constexpr uint16_t RDMA_PORT = 6969;
constexpr size_t ENTRY_SIZE = 8;
constexpr size_t QP_DEPTH = 2048;
constexpr size_t MAX_INLINE_DEPTH = 64;
constexpr size_t CLIENT_SLOT_SIZE = 1024;
constexpr size_t MAX_REPLICAS = 10;
constexpr size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024;

// ─── Benchmark constants ───

constexpr size_t NUM_OPS = 1000;
constexpr size_t NUM_CLIENTS = 1;
constexpr size_t NUM_OPS_PER_CLIENT = NUM_OPS / NUM_CLIENTS;
constexpr size_t NUM_TOTAL_OPS = NUM_OPS_PER_CLIENT * NUM_CLIENTS;

// ─── Lock table layout ───
//
// Each lock region: [8-byte control word] [log of MAX_LOG_PER_LOCK × ENTRY_SIZE]
// All locks laid out contiguously in the RDMA buffer.

constexpr size_t MAX_LOCKS = 32;
constexpr size_t MAX_LOG_PER_LOCK = NUM_OPS * 2;
constexpr size_t LOCK_HEADER_SIZE = 8;
constexpr size_t LOCK_LOG_SIZE = MAX_LOG_PER_LOCK * ENTRY_SIZE;
constexpr size_t LOCK_REGION_SIZE = LOCK_HEADER_SIZE + LOCK_LOG_SIZE;
constexpr size_t LOCK_TABLE_SIZE = LOCK_REGION_SIZE * MAX_LOCKS;

// ─── Client staging area ───
//
// Each client gets a fixed region in the server buffer where it RDMA WRITEs
// its request payload. The leader reads from here after receiving the IMM.
// For now each staging slot is one ENTRY_SIZE. Increase if payloads grow.

constexpr size_t CLIENT_STAGING_SIZE = CLIENT_SLOT_SIZE;
constexpr size_t CLIENT_STAGING_OFFSET = LOCK_TABLE_SIZE;
constexpr size_t CLIENT_STAGING_TOTAL = CLIENT_STAGING_SIZE * NUM_CLIENTS;

// ─── Buffer sizing — derived from lock table + staging ───

constexpr size_t METADATA_SIZE = 4096;
constexpr size_t FINAL_POOL_SIZE = LOCK_TABLE_SIZE + CLIENT_STAGING_TOTAL + METADATA_SIZE;
constexpr size_t ALIGNED_SIZE = ((FINAL_POOL_SIZE + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE) * HUGE_PAGE_SIZE;

static_assert(LOCK_TABLE_SIZE <= ALIGNED_SIZE, "Lock table exceeds RDMA buffer size");

// ─── Per-lock offset helpers ───

inline constexpr size_t lock_base_offset(const uint32_t lock_id) {
    return lock_id * LOCK_REGION_SIZE;
}

inline constexpr size_t lock_control_offset(const uint32_t lock_id) {
    return lock_base_offset(lock_id);
}

inline constexpr size_t lock_log_slot_offset(const uint32_t lock_id, const uint64_t slot) {
    return lock_base_offset(lock_id) + LOCK_HEADER_SIZE + (slot * ENTRY_SIZE);
}

// ─── Client staging offset helper ───

inline constexpr size_t client_staging_offset(const uint32_t client_id) {
    return CLIENT_STAGING_OFFSET + (client_id * CLIENT_STAGING_SIZE);
}

// ─── Mu protocol IMM encoding ───

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

inline void* allocate_rdma_buffer() {
    void* ptr = mmap(nullptr, ALIGNED_SIZE,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                     -1, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "[memory] HugePage alloc failed, falling back to aligned_alloc\n";
        ptr = aligned_alloc(4096, ALIGNED_SIZE);
        if (!ptr) throw std::runtime_error("Could not allocate RDMA buffer");
    }
    std::fill_n(static_cast<uint64_t*>(ptr), ALIGNED_SIZE / sizeof(uint64_t),
                EMPTY_SLOT);
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
