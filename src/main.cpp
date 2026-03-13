#include "rdma/common.h"
#include "rdma/server.h"
#include "rdma/servers/mu_leader.h"
#include "rdma/servers/mu_follower.h"
#include "rdma/servers/synra_node.h"
#include "rdma/client.h"
#include "rdma/lock_table.h"
#include "rdma/strategies/cas_strategy.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <thread>

#include "rdma/strategies/faa_strategy.h"
#include "rdma/strategies/mu_strategy.h"
#include "rdma/strategies/tas_strategy.h"
#include "rdma/mu_encoding.h"

// set to "mu", "faa", "cas", or "tas"
constexpr const char* STRATEGY = "mu";

int main() {
    try {
        const bool is_mu = (std::string(STRATEGY) == "mu");

        if (get_uint_env("IS_CLIENT") != 0) {
            const uint32_t machine_id = get_uint_env("MACHINE_ID");

            auto all_latencies = std::make_unique<std::array<uint64_t, NUM_TOTAL_OPS>>();
            std::latch start_latch(NUM_CLIENTS_PER_MACHINE + 1);
            std::vector<std::thread> workers;

            auto lock_counts = std::make_unique<
                std::array<std::array<uint64_t, MAX_LOCKS>, TOTAL_CLIENTS>>();
            for (auto& client_counts : *lock_counts) client_counts.fill(0);

            std::atomic<Client*> verify_client{nullptr};

            for (uint32_t i = 0; i < NUM_CLIENTS_PER_MACHINE; ++i) {
                const uint32_t global_id = machine_id * NUM_CLIENTS_PER_MACHINE + i;

                workers.emplace_back([i, global_id, is_mu, &start_latch, &all_latencies, &lock_counts, &verify_client]() {
                    try {
                        pin_thread_to_cpu(pick_cpu_for_client(i));

                        std::vector<std::unique_ptr<LockStrategy>> strategies;
                        LockTable table;
                        for (size_t l = 0; l < MAX_LOCKS; ++l) {
                            if (is_mu) {
                                strategies.push_back(std::make_unique<MuStrategy>());
                            } else {
                                strategies.push_back(std::make_unique<FaaStrategy>());
                            }
                            table.add(*strategies.back());
                        }

                        auto client = std::make_unique<Client>(global_id);

                        if (is_mu) {
                            // connect to each MU leader instance on its own port
                            // leader is node 0 only
                            std::vector<std::string> leader_only = { CLUSTER_NODES[0] };
                            for (size_t inst = 0; inst < MU_NUM_INSTANCES; ++inst) {
                                client->connect(leader_only,
                                    RDMA_PORT + static_cast<uint16_t>(inst));
                            }
                        } else {
                            client->connect(CLUSTER_NODES, RDMA_PORT);
                            client->connect_peers(7000);
                        }

                        std::cout << "[Client " << global_id << "] Connected ("
                                  << client->connections().size() << " conns)\n";

                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                        start_latch.arrive_and_wait();

                        uint64_t* latencies = &((*all_latencies)[i * NUM_OPS_PER_CLIENT]);

                        for (size_t op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
                            auto t0 = std::chrono::steady_clock::now();

                            auto [lock_id, lock] = table.random(*client);
                            lock.lock();
                            auto t1 = std::chrono::steady_clock::now();
                            lock.unlock();
                            lock.cleanup();

                            latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
                            (*lock_counts)[global_id][lock_id]++;
                        }

                        std::cout << "[Client " << global_id << "] Done.\n";

                        Client* expected = nullptr;
                        if (verify_client.compare_exchange_strong(expected, client.get())) {
                            client.release();
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "[Client " << global_id << " error] " << e.what() << "\n";
                    }
                });
            }

            start_latch.arrive_and_wait();
            std::cout << "All clients connected. Starting benchmark...\n";
            auto wall_start = std::chrono::steady_clock::now();

            for (auto& w : workers) w.join();
            auto wall_end = std::chrono::steady_clock::now();

            // ─── Verify (skip frontier read for MU — leader tracks it) ───

            std::array<uint64_t, MAX_LOCKS> global_frontiers{};
            Client* vc = verify_client.load();

            if (vc && !is_mu) {
                auto* state = static_cast<LocalState*>(vc->buffer());
                auto* cq = vc->cq();
                auto* mr = vc->mr();
                const auto& conns = vc->connections();

                for (size_t l = 0; l < MAX_LOCKS; ++l) {
                    state->metadata = 0xDEAD;

                    ibv_sge sge{
                        .addr = reinterpret_cast<uintptr_t>(&state->metadata),
                        .length = 8,
                        .lkey = mr->lkey
                    };
                    ibv_send_wr wr{}, *bad;
                    wr.wr_id = 0xF00D00 | l;
                    wr.opcode = IBV_WR_RDMA_READ;
                    wr.send_flags = IBV_SEND_SIGNALED;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;
                    wr.wr.rdma.remote_addr = conns[0].addr + lock_control_offset(l);
                    wr.wr.rdma.rkey = conns[0].rkey;

                    if (ibv_post_send(conns[0].id->qp, &wr, &bad)) {
                        std::cerr << "[VERIFY] Failed to read lock " << l << " frontier\n";
                        continue;
                    }

                    ibv_wc wc{};
                    while (true) {
                        int n = ibv_poll_cq(cq, 1, &wc);
                        if (n > 0 && wc.wr_id == (0xF00D00 | l)) break;
                    }

                    global_frontiers[l] = state->metadata;
                }
            }

            if (vc) delete vc;

            // ─── Print verification ───

            std::cout << "\n" << std::string(70, '-') << "\n";
            std::cout << " VERIFICATION (Machine " << machine_id << ")\n";
            std::cout << std::string(70, '-') << "\n";

            uint64_t total_committed = 0;
            uint64_t total_global = 0;
            const size_t local_ops_expected = NUM_CLIENTS_PER_MACHINE * NUM_OPS_PER_CLIENT;

            for (size_t l = 0; l < MAX_LOCKS; ++l) {
                uint64_t lock_total = 0;
                for (size_t i = 0; i < NUM_CLIENTS_PER_MACHINE; ++i) {
                    const uint32_t gid = machine_id * NUM_CLIENTS_PER_MACHINE + i;
                    lock_total += (*lock_counts)[gid][l];
                }
                total_committed += lock_total;

                const uint64_t global_acquires = global_frontiers[l];
                total_global += global_acquires;

                std::cout << "[VERIFY] Lock " << std::setw(2) << l
                          << " | local=" << std::setw(6) << lock_total
                          << " | global=" << std::setw(7) << global_acquires
                          << " | per-client: [";
                for (size_t i = 0; i < NUM_CLIENTS_PER_MACHINE; ++i) {
                    const uint32_t gid = machine_id * NUM_CLIENTS_PER_MACHINE + i;
                    if (i > 0) std::cout << ", ";
                    std::cout << (*lock_counts)[gid][l];
                }
                std::cout << "]\n";
            }

            std::cout << "[VERIFY] Local committed:  " << total_committed
                      << " / " << local_ops_expected;
            if (total_committed == local_ops_expected) {
                std::cout << " ✓ PASS\n";
            } else {
                std::cout << " ✗ FAIL\n";
            }

            std::cout << "[VERIFY] Global acquires:  " << total_global
                      << " (from server frontier)\n";

            // ─── Stats ───

            const size_t local_total_ops = NUM_CLIENTS_PER_MACHINE * NUM_OPS_PER_CLIENT;

            std::sort(all_latencies->begin(), all_latencies->begin() + local_total_ops);

            std::vector<double> client_durations_s(NUM_CLIENTS_PER_MACHINE, 0.0);
            for (size_t i = 0; i < NUM_CLIENTS_PER_MACHINE; ++i) {
                uint64_t sum_ns = 0;
                for (size_t op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
                    sum_ns += (*all_latencies)[i * NUM_OPS_PER_CLIENT + op];
                }
                client_durations_s[i] = sum_ns / 1'000'000'000.0;
            }

            double total_throughput = 0;
            for (size_t i = 0; i < NUM_CLIENTS_PER_MACHINE; ++i) {
                total_throughput += (NUM_OPS_PER_CLIENT / client_durations_s[i]);
            }

            auto get_p = [&](double p) {
                size_t idx = static_cast<size_t>(p * (local_total_ops - 1));
                return (*all_latencies)[idx] / 1000.0;
            };

            double sum = 0;
            for (size_t i = 0; i < local_total_ops; ++i) sum += ((*all_latencies)[i] / 1000.0);
            double mean = sum / local_total_ops;

            double sq_sum = 0;
            for (size_t i = 0; i < local_total_ops; ++i) {
                double diff = ((*all_latencies)[i] / 1000.0) - mean;
                sq_sum += diff * diff;
            }
            double std_dev = std::sqrt(sq_sum / local_total_ops);

            const auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(wall_end - wall_start);

            std::cout << "\n" << std::string(42, '=') << "\n";
            std::cout << " RDMA BENCHMARK RESULTS\n";
            std::cout << std::string(42, '=') << "\n";
            std::cout << "Strategy:     " << std::setw(10) << STRATEGY << "\n";
            std::cout << "Locks:        " << std::setw(10) << MAX_LOCKS << "\n";
            std::cout << "Clients:      " << std::setw(10) << TOTAL_CLIENTS << " (" << NUM_CLIENTS_PER_MACHINE << " on this machine)\n";
            std::cout << "Ops/Client:   " << std::setw(10) << NUM_OPS_PER_CLIENT << "\n";
            std::cout << "Total Ops:    " << std::setw(10) << NUM_TOTAL_OPS << "\n";
            std::cout << "Wall Clock:   " << std::setw(10) << std::fixed << std::setprecision(3) << (wall_ms.count() / 1000.0) << " s\n";
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
            const uint32_t node_id = get_uint_env("NODE_ID");

            if (is_mu) {
                // MU: spawn N independent server instances on different ports
                std::vector<std::thread> threads;

                for (size_t inst = 0; inst < MU_NUM_INSTANCES; ++inst) {
                    threads.emplace_back([node_id, inst]() {
                        pin_thread_to_cpu(static_cast<int>(inst));
                        uint16_t port = RDMA_PORT + static_cast<uint16_t>(inst);
                        uint32_t lock_start = inst * MU_LOCKS_PER_INSTANCE;
                        uint32_t lock_end = (inst + 1) * MU_LOCKS_PER_INSTANCE;

                        if (node_id == 0) {
                            MuLeader leader(node_id, lock_start, lock_end);
                            leader.start(port);
                        } else {
                            MuFollower follower(node_id, lock_start, lock_end);
                            follower.start(port);
                        }
                    });
                }

                for (auto& t : threads) t.join();
            } else {
                pin_thread_to_cpu(1);
                SynraNode node(node_id);
                node.start(RDMA_PORT);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}