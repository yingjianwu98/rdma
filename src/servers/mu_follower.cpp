#include "rdma/servers/mu_follower.h"

#include <iostream>
#include <stdexcept>
#include <string>

void MuFollower::run() {
    std::cout << "[MuFollower " << node_id_ << "] Passive mode for locks ["
              << lock_start_ << ", " << lock_end_ << ")\n";

    ibv_wc wc[32];
    while (true) {
        const int n = ibv_poll_cq(cq_, 32, wc);
        if (n < 0) {
            throw std::runtime_error("MuFollower: ibv_poll_cq failed");
        }

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    std::string("MuFollower: WC error ") + ibv_wc_status_str(wc[i].status));
            }
        }
    }
}
