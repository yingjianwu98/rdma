#include "rdma/strategies/mu_strategy.h"
#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>

// ── Per-thread timing stats ──
struct MuClientStats {
    uint64_t count = 0;
    uint64_t total_acquires = 0;

    uint64_t t_route_ns = 0;
    uint64_t t_post_recv_ns = 0;
    uint64_t t_post_send_ns = 0;
    uint64_t t_poll_ns = 0;
    uint64_t t_total_ns = 0;

    uint64_t poll_spins = 0;
    uint64_t poll_send_wc = 0;

    std::chrono::steady_clock::time_point thread_start{};
    bool started = false;

    void mark_start() {
        if (!started) {
            thread_start = std::chrono::steady_clock::now();
            started = true;
        }
    }

    void mark_end(uint32_t client_id) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - thread_start).count();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            now - thread_start).count();

        fprintf(stderr,
            "\n[MuClient %u] ── THREAD FINISHED ──\n"
            "  total_acquires: %lu\n"
            "  wall_time:      %ld ms (%.3f s)\n"
            "  avg_per_acq:    %ld us\n"
            "  throughput:     %.0f acq/s\n",
            client_id,
            total_acquires,
            elapsed_ms, elapsed_ms / 1000.0,
            total_acquires > 0 ? elapsed_us / (long)total_acquires : 0,
            total_acquires > 0 ? (double)total_acquires / (elapsed_ms / 1000.0) : 0.0
        );
    }

    void dump_and_reset(uint32_t client_id) {
        if (count == 0) return;
        auto now = std::chrono::steady_clock::now();
        auto since_start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - thread_start).count();

        fprintf(stderr,
            "\n[MuClient %u] ── TIMING DUMP (%lu ops, %lu acquires, %.1fs since start) ──\n"
            "  t_route:       %7.2f ms (%5.2f%%)  avg %lu ns\n"
            "  t_post_recv:   %7.2f ms (%5.2f%%)  avg %lu ns\n"
            "  t_post_send:   %7.2f ms (%5.2f%%)  avg %lu ns\n"
            "  t_poll:        %7.2f ms (%5.2f%%)  avg %lu ns\n"
            "  t_total:       %7.2f ms            avg %lu ns\n"
            "  poll_spins:    %lu (avg %lu per op)\n"
            "  poll_send_wc:  %lu (avg %lu per op)\n",
            client_id, count, total_acquires, since_start_ms / 1000.0,
            t_route_ns / 1e6,     100.0 * t_route_ns / t_total_ns,     t_route_ns / count,
            t_post_recv_ns / 1e6, 100.0 * t_post_recv_ns / t_total_ns, t_post_recv_ns / count,
            t_post_send_ns / 1e6, 100.0 * t_post_send_ns / t_total_ns, t_post_send_ns / count,
            t_poll_ns / 1e6,      100.0 * t_poll_ns / t_total_ns,      t_poll_ns / count,
            t_total_ns / 1e6,                                           t_total_ns / count,
            poll_spins,  poll_spins / count,
            poll_send_wc, poll_send_wc / count
        );
        count = 0;
        t_route_ns = t_post_recv_ns = t_post_send_ns = t_poll_ns = t_total_ns = 0;
        poll_spins = poll_send_wc = 0;
    }
};

static thread_local MuClientStats stats;

static void mu_send_and_wait(Client& client, uint32_t lock_id, uint32_t op) {
    using clock = std::chrono::steady_clock;
    using ns = std::chrono::nanoseconds;

    stats.mark_start();

    auto t_start = clock::now();

    // ── Route + encode ──
    auto t0 = clock::now();
    auto* cq = client.cq();
    size_t inst = mu_instance_for_lock(static_cast<uint16_t>(lock_id));
    auto& leader = client.connections()[inst];

    const uint32_t imm = mu_encode_imm(
        static_cast<uint16_t>(lock_id),
        static_cast<uint16_t>(client.id()),
        op
    );
    auto t1 = clock::now();
    stats.t_route_ns += std::chrono::duration_cast<ns>(t1 - t0).count();

    // ── Post recv ──
    auto t2 = clock::now();
    ibv_recv_wr rr{}, *bad_rr = nullptr;
    rr.wr_id = 0;
    rr.sg_list = nullptr;
    rr.num_sge = 0;
    if (ibv_post_recv(leader.id->qp, &rr, &bad_rr)) {
        throw std::runtime_error("MuStrategy: Failed to pre-post recv");
    }
    auto t3 = clock::now();
    stats.t_post_recv_ns += std::chrono::duration_cast<ns>(t3 - t2).count();

    // ── Post send ──
    auto t4 = clock::now();
    thread_local uint32_t send_counts[16] = {};
    send_counts[inst]++;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = 0;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_INLINE;
    if ((send_counts[inst] & 1023) == 0) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }
    wr.num_sge = 0;
    wr.sg_list = nullptr;
    wr.imm_data = htonl(imm);

    if (ibv_post_send(leader.id->qp, &wr, &bad)) {
        throw std::runtime_error("MuStrategy: Failed to post send to leader");
    }
    auto t5 = clock::now();
    stats.t_post_send_ns += std::chrono::duration_cast<ns>(t5 - t4).count();

    // ── Poll for recv ──
    auto t6 = clock::now();
    ibv_wc wc{};
    while (true) {
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n <= 0) {
            stats.poll_spins++;
            continue;
        }

        if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error(
                "MuStrategy: WC error " + std::to_string(wc.status)
                + " opcode " + std::to_string(wc.opcode));
        }

        if (wc.opcode & IBV_WC_RECV) {
            auto t7 = clock::now();
            stats.t_poll_ns += std::chrono::duration_cast<ns>(t7 - t6).count();

            auto t_end = clock::now();
            stats.t_total_ns += std::chrono::duration_cast<ns>(t_end - t_start).count();
            stats.count++;

            if (stats.count % 50000 == 0) {
                stats.dump_and_reset(client.id());
            }
            return;
        }

        stats.poll_send_wc++;
    }
}

uint64_t MuStrategy::acquire(Client& client, int /*op_id*/, uint32_t lock_id) {
    mu_send_and_wait(client, lock_id, MU_OP_CLIENT_LOCK);
    stats.total_acquires++;

    // Print when this thread finishes all its ops
    if (stats.total_acquires >= NUM_OPS_PER_CLIENT) {
        stats.mark_end(client.id());
    }

    return 0;
}

void MuStrategy::release(Client& client, int /*op_id*/, uint32_t lock_id) {
    mu_send_and_wait(client, lock_id, MU_OP_CLIENT_UNLOCK);
}