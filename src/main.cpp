#include "rdma/common.h"
#include "rdma/server.h"
#include "rdma/servers/synra_node.h"
#include "rdma/servers/mu_leader.h"
#include "rdma/servers/mu_follower.h"
#include "rdma/client.h"

#include <iostream>
#include <thread>

int main() {
    try {
        if (get_uint_env("IS_CLIENT") != 0) {
            std::vector<std::thread> workers;
            for (uint32_t i = 0; i < NUM_CLIENTS; ++i) {
                workers.emplace_back([i]() {
                    Client client(i);
                    client.connect({CLUSTER_NODES[0]}, RDMA_PORT);
                    std::cout << "[Client " << i << "] Connected. Ready.\n";
                    while (true) {
                        std::this_thread::sleep_for(std::chrono::seconds(60));
                    }
                });
            }
            for (auto& w : workers) w.join();
        } else {
            pin_thread_to_cpu(1);
            const uint32_t node_id = get_uint_env("NODE_ID");
            // SynraNode node(node_id);
            // node.start(RDMA_PORT);
            if (node_id == 0) {
                std::cout << "Starting mu leader" << std::endl;
                MuLeader node(node_id);
                node.start(RDMA_PORT);
            } else {
                MuFollower node(node_id);
                node.connect_to_leader(CLUSTER_NODES[0], RDMA_PORT);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}