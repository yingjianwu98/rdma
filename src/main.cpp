#include "rdma/common.h"
#include "rdma/server.h"
#include "rdma/servers/synra_node.h"
#include "rdma/servers/mu_leader.h"
#include "rdma/servers/mu_follower.h"
#include "rdma/client.h"
#include "rdma/lock_table.h"
#include "rdma/strategies/cas_strategy.h"
#include "rdma/strategies/tas_strategy.h"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <thread>

constexpr size_t NUM_LOCKS = 4;

int main() {
    try {
        if (get_uint_env("IS_CLIENT") != 0) {

            auto all_latencies = std::make_unique<std::array<uint64_t, NUM_TOTAL_OPS>>();
            std::latch start_latch(NUM_CLIENTS + 1);
            std::vector<std::thread> workers;

            // Track per-client, per-lock commit counts for verification
            auto lock_counts = std::make_unique<std::array<std::array<uint64_t, NUM_LOCKS>, NUM_CLIENTS>>();
            for (auto& client_counts : *lock_counts) client_counts.fill(0);

            for (uint32_t i = 0; i < NUM_CLIENTS; ++i) {
                workers.emplace_back([i, &start_latch, &all_latencies, &lock_counts]() {
                    try {
                        pin_thread_to_cpu(pick_cpu_for_client(i));

                        std::vector<std::unique_ptr<CasStrategy>> strategies;
                        LockTable table;
                        for (size_t l = 0; l < NUM_LOCKS; ++l) {
                            strategies.push_back(std::make_unique<CasStrategy>());
                            table.add(*strategies.back());
                        }

                        Client client(i);
                        client.connect(CLUSTER_NODES, RDMA_PORT);

                        std::cout << "[Client " << i << "] Connected.\n";
                        start_latch.arrive_and_wait();

                        uint64_t* latencies = &((*all_latencies)[i * NUM_OPS_PER_CLIENT]);

                        for (size_t op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
                            auto t0 = std::chrono::steady_clock::now();

                            auto [lock_id, lock] = table.random(client);
                            lock.lock();
                            lock.unlock();

                            auto t1 = std::chrono::steady_clock::now();
                            latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
                            (*lock_counts)[i][lock_id]++;
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

            // ─── Verify correctness ───

            std::cout << "\n" << std::string(42, '-') << "\n";
            std::cout << " VERIFICATION\n";
            std::cout << std::string(42, '-') << "\n";

            uint64_t total_committed = 0;
            bool correct = true;

            for (size_t l = 0; l < NUM_LOCKS; ++l) {
                uint64_t lock_total = 0;
                for (size_t c = 0; c < NUM_CLIENTS; ++c) {
                    lock_total += (*lock_counts)[c][l];
                }
                total_committed += lock_total;

                std::cout << "[VERIFY] Lock " << l << " | ops=" << lock_total << " | per-client: [";
                for (size_t c = 0; c < NUM_CLIENTS; ++c) {
                    if (c > 0) std::cout << ", ";
                    std::cout << (*lock_counts)[c][l];
                }
                std::cout << "]\n";
            }

            std::cout << "[VERIFY] Total committed: " << total_committed
                      << " / " << NUM_TOTAL_OPS;

            if (total_committed == NUM_TOTAL_OPS) {
                std::cout << " ✓ PASS\n";
            } else {
                std::cout << " ✗ FAIL\n";
                correct = false;
            }

            // ─── Remote verification: read back logs from server ───

            {
                // Reuse client 0's connection pattern — create a fresh verifier
                Client verifier(NUM_CLIENTS); // id beyond normal clients
                verifier.connect(CLUSTER_NODES, RDMA_PORT);

                auto* cq = verifier.cq();
                auto* mr = verifier.mr();
                const auto& conns = verifier.connections();

                for (size_t l = 0; l < NUM_LOCKS; ++l) {
                    // Read control word
                    uint64_t control_val = EMPTY_SLOT;

                    ibv_sge sge{
                        .addr   = reinterpret_cast<uintptr_t>(&control_val),
                        .length = 8,
                        .lkey   = mr->lkey
                    };
                    ibv_send_wr wr{}, *bad;
                    wr.wr_id      = l;
                    wr.opcode     = IBV_WR_RDMA_READ;
                    wr.send_flags = IBV_SEND_SIGNALED;
                    wr.sg_list    = &sge;
                    wr.num_sge    = 1;
                    wr.wr.rdma.remote_addr = conns[0].addr + lock_control_offset(l);
                    wr.wr.rdma.rkey        = conns[0].rkey;

                    if (ibv_post_send(conns[0].id->qp, &wr, &bad))
                        throw std::runtime_error("Verify: read control failed");

                    ibv_wc wc{};
                    while (ibv_poll_cq(cq, 1, &wc) == 0) {}
                    if (wc.status != IBV_WC_SUCCESS)
                        throw std::runtime_error("Verify: read control WC error");

                    // Count non-empty log slots
                    uint64_t committed = 0;
                    uint64_t bad_slots = 0;

                    for (uint64_t slot = 0; slot < MAX_LOG_PER_LOCK; ++slot) {
                        uint64_t val = EMPTY_SLOT;

                        ibv_sge slot_sge{
                            .addr   = reinterpret_cast<uintptr_t>(&val),
                            .length = 8,
                            .lkey   = mr->lkey
                        };
                        ibv_send_wr slot_wr{}, *slot_bad;
                        slot_wr.wr_id      = slot;
                        slot_wr.opcode     = IBV_WR_RDMA_READ;
                        slot_wr.send_flags = IBV_SEND_SIGNALED;
                        slot_wr.sg_list    = &slot_sge;
                        slot_wr.num_sge    = 1;
                        slot_wr.wr.rdma.remote_addr = conns[0].addr + lock_log_slot_offset(l, slot);
                        slot_wr.wr.rdma.rkey        = conns[0].rkey;

                        if (ibv_post_send(conns[0].id->qp, &slot_wr, &slot_bad))
                            throw std::runtime_error("Verify: read slot failed");

                        ibv_wc slot_wc{};
                        while (ibv_poll_cq(cq, 1, &slot_wc) == 0) {}

                        if (val == EMPTY_SLOT) break; // end of log
                        if (val >= NUM_CLIENTS) {
                            bad_slots++;
                        }
                        committed++;
                    }

                    std::cout << "[REMOTE] Lock " << l
                              << " | control=" << control_val
                              << " | log_entries=" << committed;
                    if (bad_slots > 0) {
                        std::cout << " | BAD_SLOTS=" << bad_slots << " ✗";
                        correct = false;
                    } else {
                        std::cout << " ✓";
                    }
                    std::cout << "\n";
                }

                std::cout << "[VERIFY] Overall: " << (correct ? "✓ PASS" : "✗ FAIL") << "\n";
            }

            // ─── Compute stats ───

            std::sort(all_latencies->begin(), all_latencies->end());

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

            auto get_p = [&](double p) {
                size_t idx = static_cast<size_t>(p * (NUM_TOTAL_OPS - 1));
                return (*all_latencies)[idx] / 1000.0;
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
            std::cout << "Locks:        " << std::setw(10) << NUM_LOCKS << "\n";
            std::cout << "Clients:      " << std::setw(10) << NUM_CLIENTS << "\n";
            std::cout << "Ops/Client:   " << std::setw(10) << NUM_OPS_PER_CLIENT << "\n";
            std::cout << "Total Ops:    " << std::setw(10) << NUM_TOTAL_OPS << "\n";
            const auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(wall_end - wall_start);
            std::cout << "Wall Clock:   " << std::setw(10) << std::fixed << std::setprecision(3) << (wall_ms.count() / 1000.0) << " s\n";
            std::cout << "Active Time:  " << std::setw(10) << std::fixed << std::setprecision(3) << effective_total_time << " s\n";
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