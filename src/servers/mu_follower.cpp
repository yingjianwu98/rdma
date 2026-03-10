#include "rdma/servers/mu_follower.h"
#include "rdma/common.h"

#include <arpa/inet.h>
#include <iostream>

void MuFollower::run() {
    std::cout << "[MuFollower " << node_id_ << "] Processing replicated writes\n";

    uint64_t applied[MAX_LOCKS] = {};
    auto* local_buf = static_cast<uint8_t*>(buf_);

    volatile uint64_t* commit_ptrs[MAX_LOCKS];
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        commit_ptrs[i] = reinterpret_cast<volatile uint64_t*>(
            local_buf + i * LOCK_REGION_SIZE);
    }

    while (true) {
        for (uint32_t lock_id = 0; lock_id < MAX_LOCKS; ++lock_id) {
            const uint64_t committed = *commit_ptrs[lock_id];

            while (applied[lock_id] < committed) {
                applied[lock_id]++;
            }
        }
    }
}