#pragma once

// Shared RPC structs and flat-log packing helpers for the simple_mu primitive.

#include <cstddef>
#include <cstdint>

enum class SimpleMuStatus : uint8_t {
    Ok = 0,
    QueueFull = 1,
    InternalError = 2,
};

struct SimpleMuRequest {
    uint16_t client_id = 0;
    uint16_t reserved0 = 0;
    uint32_t req_id = 0;
};

struct SimpleMuResponse {
    uint8_t status = 0;
    uint8_t reserved0 = 0;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t slot = 0;
    uint32_t reserved1 = 0;
};

static_assert(sizeof(SimpleMuRequest) == 8);
static_assert(sizeof(SimpleMuResponse) == 16);

constexpr size_t SIMPLE_MU_SERVER_RECV_RING = 2048;

inline uint64_t pack_simple_mu_entry(const uint16_t client_id) {
    return static_cast<uint64_t>(client_id);
}
