#include "rdma/servers/synra_node.h"

#include <iostream>
#include <stdexcept>

// Generic one-sided node: expose the registered lock-table MR and report CQ
// errors, but otherwise remain passive.
void SynraNode::run() {
    std::cout << "[SynraNode " << node_id_ << "] Passive mode — waiting for RDMA operations\n";

    ibv_wc wc[32];
    while (true) {
        const int n = ibv_poll_cq(cq_, 32, wc);
        if (n < 0) throw std::runtime_error("ibv_poll_cq failed");

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[SynraNode] WC error: "
                          << ibv_wc_status_str(wc[i].status)
                          << " opcode: " << wc[i].opcode << "\n";
            }
        }
    }
}
