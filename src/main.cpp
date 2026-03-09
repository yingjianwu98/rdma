#include "rdma/common.h"
#include "rdma/server.h"
#include "rdma/servers/synra_node.h"
#include "rdma/servers/mu_leader.h"
#include "rdma/servers/mu_follower.h"
#include "rdma/client.h"
#include "rdma/lock_table.h"
#include "rdma/strategies/cas_strategy.h"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <latch>
#include <thread>

#include "rdma/strategies/cas_strategy.h"

int main() {
    try {
        if (get_uint_env("IS_CLIENT") != 0) {
            CasStrategy strategy;
            LockTable table;
            table.add(strategy);

            auto all_latencies = std::make_unique<std::array<uint64_t, NUM_TOTAL_OPS>>();
            std::latch start_latch(NUM_CLIENTS + 1);
            std::vector<std::thread> workers;

            for (uint32_t i = 0; i < NUM_CLIENTS; ++i) {
                workers.emplace_back([i, &start_latch, &table, &all_latencies]() {
                    try {
                        pin_thread_to_cpu(pick_cpu_for_client(i));

                        Client client(i);
                        client.connect(CLUSTER_NODES, RDMA_PORT);

                        std::cout << "[Client " << i << "] Connected.\n";
                        start_latch.arrive_and_wait();

                        uint64_t* latencies = &((*all_latencies)[i * NUM_OPS_PER_CLIENT]);

                        for (size_t op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
                            auto t0 = std::chrono::steady_clock::now();

                            auto lock = table.get(0, client);
                            lock.lock();
                            lock.unlock();

                            auto t1 = std::chrono::steady_clock::now();
                            latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
                        }

                        std::cout << "[Client " << i << "] Done.\n";
                    } catch (const std::exception& e) {
                        std::cerr << "[Client " << i << " error] " << e.what() << "\n";
                    }
                });
            }

            start_latch.arrive_and_wait();
            std::cout << "All clients connected. Starting benchmark...\n";
            auto wall_start = std::chrono::steady_clock::now();

            for (auto& w : workers) w.join();
            auto wall_end = std::chrono::steady_clock::now();

            // ─── Compute stats ───

            std::sort(all_latencies->begin(), all_latencies->end());

            // Per-client active time → aggregate throughput
            std::vector<double> client_durations_s(NUM_CLIENTS, 0.0);
            for (size_t i = 0; i < NUM_CLIENTS; ++i) {
                uint64_t sum_ns = 0;
                for (size_t op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
                    sum_ns += (*all_latencies)[i * NUM_OPS_PER_CLIENT + op];
                }
                client_durations_s[i] = sum_ns / 1'000'000'000.0;
            }

            double total_throughput = 0;
            for (size_t i = 0; i < NUM_CLIENTS; ++i) {
                total_throughput += (NUM_OPS_PER_CLIENT / client_durations_s[i]);
            }
            double effective_total_time = NUM_TOTAL_OPS / total_throughput;

            // Latency stats
            auto get_p = [&](double p) {
                size_t idx = static_cast<size_t>(p * (NUM_TOTAL_OPS - 1));
                return (*all_latencies)[idx] / 1000.0;  // ns → us
            };

            double sum = 0;
            for (const auto& lat : *all_latencies) sum += (lat / 1000.0);
            double mean = sum / NUM_TOTAL_OPS;

            double sq_sum = 0;
            for (const auto& lat : *all_latencies) {
                double diff = (lat / 1000.0) - mean;
                sq_sum += diff * diff;
            }
            double std_dev = std::sqrt(sq_sum / NUM_TOTAL_OPS);

            std::cout << "\n" << std::string(42, '=') << "\n";
            std::cout << " RDMA BENCHMARK RESULTS\n";
            std::cout << std::string(42, '=') << "\n";
            std::cout << "Strategy:     " << std::setw(10) << "SYNRA_CAS" << "\n";
            std::cout << "Clients:      " << std::setw(10) << NUM_CLIENTS << "\n";
            std::cout << "Ops/Client:   " << std::setw(10) << NUM_OPS_PER_CLIENT << "\n";
            std::cout << "Total Ops:    " << std::setw(10) << NUM_TOTAL_OPS << "\n";
            std::cout << "Total Time:   " << std::setw(10) << std::fixed << std::setprecision(3) << effective_total_time << " s\n";
            std::cout << "Throughput:   " << std::setw(10) << std::fixed << std::setprecision(0) << total_throughput << " ops/s\n";
            std::cout << std::string(42, '-') << "\n";
            std::cout << "LATENCY (Microseconds)\n";
            std::cout << "Mean:         " << std::setw(10) << std::setprecision(2) << mean << " us\n";
            std::cout << "StdDev:       " << std::setw(10) << std::setprecision(2) << std_dev << " us\n";
            std::cout << "P0 (Min):     " << std::setw(10) << get_p(0.0) << " us\n";
            std::cout << "P50 (Med):    " << std::setw(10) << get_p(0.5) << " us\n";
            std::cout << "P90:          " << std::setw(10) << get_p(0.9) << " us\n";
            std::cout << "P99:          " << std::setw(10) << get_p(0.99) << " us\n";
            std::cout << "P99.9:        " << std::setw(10) << get_p(0.999) << " us\n";
            std::cout << "P100 (Max):   " << std::setw(10) << get_p(1.0) << " us\n";
            std::cout << std::string(42, '=') << std::endl;

        } else {
            pin_thread_to_cpu(1);
            const uint32_t node_id = get_uint_env("NODE_ID");
            SynraNode node(node_id);
            node.start(RDMA_PORT);
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
