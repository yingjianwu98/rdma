#include "rdma/common.h"
#include "rdma/server.h"
#include "rdma/servers/mu_leader.h"
#include "rdma/servers/mu_follower.h"
#include "rdma/client.h"
#include "rdma/lock_table.h"
#include "rdma/strategies/mu_strategy.h"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <thread>

constexpr size_t NUM_LOCKS = 16;

int main() {
    try {
        if (get_uint_env("IS_CLIENT") != 0) {

            std::latch start_latch(NUM_CLIENTS + 1);
            std::vector<std::thread> workers;

            for (uint32_t i = 0; i < NUM_CLIENTS; ++i) {
                workers.emplace_back([i, &start_latch]() {
                    try {
                        pin_thread_to_cpu(pick_cpu_for_client(i));

                        Client client(i);
                        client.connect(CLUSTER_NODES, RDMA_PORT);

                        std::cout << "[Client " << i << "] Fully connected: "
                          << client.connections().size() << " nodes,\n";

                        client.connect_peers(7000);

                        std::cout << "[Client " << i << "] Fully connected: "
                                  << (NUM_CLIENTS - 1) << " peers\n";

                        start_latch.arrive_and_wait();

                        // just spin for now to prove everything is up
                        std::this_thread::sleep_for(std::chrono::seconds(3));

                        std::cout << "[Client " << i << "] Done.\n";
                    } catch (const std::exception& e) {
                        std::cerr << "[Client " << i << " error] " << e.what() << "\n";
                    }
                });
            }

            start_latch.arrive_and_wait();
            std::cout << "All clients connected. All-to-all mesh ready.\n";

            for (auto& w : workers) w.join();

        } else {
            pin_thread_to_cpu(1);
            const uint32_t node_id = get_uint_env("NODE_ID");

            if (node_id == 0) {
                MuLeader leader(node_id);
                leader.start(RDMA_PORT);
            } else {
                MuFollower follower(node_id);
                follower.start(RDMA_PORT);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}