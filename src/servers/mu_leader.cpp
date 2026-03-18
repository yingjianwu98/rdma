#include "rdma/servers/mu_leader.h"

#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

enum class MutationKind : uint8_t {
    append_lock = 1,
    append_unlock = 2,
};

struct CommittedWaiter {
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t granted_slot = 0;
};

struct MutationCtx {
    bool in_use = false;
    bool quorum_done = false;
    bool applied = false;
    uint32_t generation = 0;
    MutationKind kind = MutationKind::append_lock;
    uint32_t global_slot = 0;
    uint32_t lock_id = 0;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t granted_slot = 0;
    uint32_t ack_count = 0;
    uint32_t pending_followers = 0;
};

struct LockState {
    std::deque<MuRequest> pending_locks;
    std::optional<MuRequest> pending_unlock;
    std::deque<CommittedWaiter> committed_waiters;
    bool holder_active = false;
    uint32_t holder_slot = 0;
    uint16_t holder_client_id = 0;
    uint32_t holder_req_id = 0;
    uint32_t append_inflight = 0;
    int unlock_mutation = -1;
};

struct MuLeaderStats {
    uint64_t lock_reqs_recv = 0;
    uint64_t unlock_reqs_recv = 0;
    uint64_t grants_sent = 0;
    uint64_t unlock_acks_sent = 0;
    uint64_t recv_cqes = 0;
    uint64_t resp_send_cqes = 0;
    uint64_t replication_writes_posted = 0;
    uint64_t replication_writes_signaled = 0;
    uint64_t replication_cqes = 0;
    uint64_t append_quorums = 0;
    uint64_t unlock_quorums = 0;
    uint64_t empty_cq_polls = 0;
    uint64_t nonempty_cq_polls = 0;
    uint64_t cqes_polled = 0;
    uint64_t ready_lock_queue_high_watermark = 0;
    uint64_t mutation_pool_high_watermark = 0;
    uint64_t append_inflight_high_watermark = 0;
    uint64_t pending_lock_queue_high_watermark = 0;
};

constexpr uint64_t MU_RECV_WR_TAG = 0xB1ULL;
constexpr uint64_t MU_REPL_WR_TAG = 0xB2ULL;
constexpr uint64_t MU_RESP_WR_TAG = 0x4C53000000000000ULL;
constexpr uint64_t MU_WR_TAG_SHIFT = 56;
constexpr uint64_t MU_REPL_GEN_SHIFT = 24;
constexpr uint64_t MU_REPL_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t MU_REPL_ID_MASK = 0xFFFFFFULL;

uint64_t make_recv_wr_id(const uint16_t client_id, const uint16_t recv_slot) {
    return (MU_RECV_WR_TAG << MU_WR_TAG_SHIFT)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(recv_slot);
}

uint16_t recv_client_id(const uint64_t wr_id) {
    return static_cast<uint16_t>((wr_id >> 16) & 0xFFFFu);
}

uint16_t recv_slot_index(const uint64_t wr_id) {
    return static_cast<uint16_t>(wr_id & 0xFFFFu);
}

uint64_t make_repl_wr_id(const uint32_t mutation_id, const uint32_t generation) {
    return (MU_REPL_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(generation) & MU_REPL_GEN_MASK) << MU_REPL_GEN_SHIFT)
         | (static_cast<uint64_t>(mutation_id) & MU_REPL_ID_MASK);
}

uint32_t repl_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_REPL_GEN_SHIFT) & MU_REPL_GEN_MASK);
}

uint32_t repl_mutation_id(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & MU_REPL_ID_MASK);
}

bool is_repl_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_REPL_WR_TAG;
}

bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_RECV_WR_TAG;
}

bool is_resp_send_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == MU_RESP_WR_TAG;
}

} // namespace

void MuLeader::run() {
    const bool mu_debug = MU_DEBUG;
    const bool mu_stats_enabled = MU_STATS;
    const bool mu_stats_print_idle = MU_STATS_PRINT_IDLE;
    const bool mu_quorum_only_signal = MU_REPL_SIGNAL_QUORUM_ONLY;
    auto debug = [&](const std::string& msg) {
        if (mu_debug) {
            std::cout << "[MuLeader " << node_id_ << "] " << msg << "\n";
        }
    };

    MuLeaderStats stats{};
    MuLeaderStats prev_stats{};
    auto last_stats_at = std::chrono::steady_clock::now();
    uint64_t current_ready_q = 0;
    uint64_t current_mutations = 0;
    uint64_t current_append_inflight = 0;
    uint64_t interval_ready_q_hwm = 0;
    uint64_t interval_mutation_hwm = 0;
    uint64_t interval_append_inflight_hwm = 0;
    uint64_t interval_pending_lock_hwm = 0;

    auto print_stats = [&](const MuLeaderStats& delta, const double interval_s) {
        const uint64_t total_polls = delta.empty_cq_polls + delta.nonempty_cq_polls;
        const double nonempty_poll_pct = total_polls == 0
            ? 0.0
            : 100.0 * static_cast<double>(delta.nonempty_cq_polls) / static_cast<double>(total_polls);

        std::ostringstream line1;
        line1 << std::fixed << std::setprecision(2)
              << "[MuLeaderStats " << node_id_ << "] interval=" << interval_s << "s"
              << " | rates: lock=" << delta.lock_reqs_recv
              << " unlock=" << delta.unlock_reqs_recv
              << " grant=" << delta.grants_sent
              << " ack=" << delta.unlock_acks_sent
              << " | ops/s: grant=" << std::setprecision(0) << (interval_s > 0.0 ? delta.grants_sent / interval_s : 0.0)
              << " ack=" << (interval_s > 0.0 ? delta.unlock_acks_sent / interval_s : 0.0);

        std::ostringstream line2;
        line2 << std::fixed << std::setprecision(1)
              << "[MuLeaderStats " << node_id_ << "] cq"
              << " | recv=" << delta.recv_cqes
              << " resp_send=" << delta.resp_send_cqes
              << " repl=" << delta.replication_cqes
              << " total=" << delta.cqes_polled
              << " | polls: empty=" << delta.empty_cq_polls
              << " nonempty=" << delta.nonempty_cq_polls
              << " nonempty%=" << nonempty_poll_pct;

        std::ostringstream line3;
        line3 << "[MuLeaderStats " << node_id_ << "] replication"
              << " | writes=" << delta.replication_writes_posted
              << " signaled=" << delta.replication_writes_signaled
              << " append_quorums=" << delta.append_quorums
              << " unlock_quorums=" << delta.unlock_quorums;

        std::ostringstream line4;
        line4 << "[MuLeaderStats " << node_id_ << "] queues"
              << " | current: ready=" << current_ready_q
              << " mutations=" << current_mutations
              << " append_total=" << current_append_inflight
              << " | interval_hwm: ready=" << interval_ready_q_hwm
              << " mutations=" << interval_mutation_hwm
              << " append_total=" << interval_append_inflight_hwm
              << " pending_lock=" << interval_pending_lock_hwm
              << " | lifetime_hwm: ready=" << stats.ready_lock_queue_high_watermark
              << " mutations=" << stats.mutation_pool_high_watermark
              << " append_per_lock=" << stats.append_inflight_high_watermark
              << " pending_lock=" << stats.pending_lock_queue_high_watermark;

        std::cout << line1.str() << "\n"
                  << line2.str() << "\n"
                  << line3.str() << "\n"
                  << line4.str() << "\n";
    };

    std::cout << "[MuLeader " << node_id_ << "] locks ["
              << lock_start_ << ", " << lock_end_ << ")\n";

    auto* local_buf = static_cast<uint8_t*>(buf_);
    const uint32_t num_clients = expected_clients();
    const size_t handled_locks = static_cast<size_t>(lock_end_ - lock_start_);
    size_t repl_signal_cursor = 0;
    uint32_t global_next_append_slot = 0;
    uint32_t global_commit_tail = 0;

    const size_t mutation_pool_size = std::max<size_t>(
        handled_locks * (MU_MAX_APPEND_INFLIGHT_PER_LOCK + 1),
        MU_MAX_APPEND_INFLIGHT_PER_LOCK + 1);
    if (mutation_pool_size > MU_REPL_ID_MASK) {
        throw std::runtime_error("MuLeader: mutation pool exceeds wr_id capacity");
    }

    std::vector<MuRequest> recv_buffers(num_clients * MU_SERVER_RECV_RING);
    ibv_mr* recv_mr = ibv_reg_mr(
        pd_,
        recv_buffers.data(),
        recv_buffers.size() * sizeof(MuRequest),
        IBV_ACCESS_LOCAL_WRITE);
    if (!recv_mr) {
        throw std::runtime_error("MuLeader: failed to register recv buffers");
    }

    std::vector<LockState> locks(MAX_LOCKS);
    std::vector<MutationCtx> mutations(mutation_pool_size);
    std::deque<uint32_t> free_mutations;
    free_mutations.resize(mutation_pool_size);
    for (uint32_t i = 0; i < mutation_pool_size; ++i) {
        free_mutations[i] = i;
    }

    std::unordered_map<uint32_t, uint32_t> slot_to_mutation;
    slot_to_mutation.reserve(mutation_pool_size * 2);

    std::deque<uint32_t> ready_locks;
    std::vector<uint8_t> ready_flags(MAX_LOCKS, 0);
    std::vector<uint32_t> client_send_signal_counts(num_clients, 0);
    std::vector<size_t> follower_indices;
    follower_indices.reserve(peers_.size());
    for (size_t i = 0; i < peers_.size(); ++i) {
        if (i == node_id_ || peers_[i].cm_id == nullptr) continue;
        follower_indices.push_back(i);
    }

    auto enqueue_ready = [&](const uint32_t lock_id) {
        if (lock_id < lock_start_ || lock_id >= lock_end_) return;
        if (ready_flags[lock_id] != 0) return;
        ready_flags[lock_id] = 1;
        ready_locks.push_back(lock_id);
        current_ready_q = ready_locks.size();
        stats.ready_lock_queue_high_watermark = std::max<uint64_t>(stats.ready_lock_queue_high_watermark, ready_locks.size());
        interval_ready_q_hwm = std::max<uint64_t>(interval_ready_q_hwm, ready_locks.size());
    };

    auto send_response = [&](const MuResponse& resp) {
        if (resp.client_id >= clients_.size() || clients_[resp.client_id].cm_id == nullptr) {
            throw std::runtime_error("MuLeader: invalid client response target");
        }

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&resp);
        sge.length = sizeof(MuResponse);
        sge.lkey = 0;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = MU_RESP_WR_TAG | resp.client_id;
        wr.opcode = IBV_WR_SEND;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_INLINE;
        if (++client_send_signal_counts[resp.client_id] % MU_SERVER_SEND_SIGNAL_EVERY == 0) {
            wr.send_flags |= IBV_SEND_SIGNALED;
        }

        if (ibv_post_send(clients_[resp.client_id].cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("MuLeader: failed to post client response send");
        }

        if (resp.op == static_cast<uint8_t>(MuRpcOp::Lock)
            && resp.status == static_cast<uint8_t>(MuRpcStatus::Ok)) {
            stats.grants_sent++;
        } else if (resp.op == static_cast<uint8_t>(MuRpcOp::Unlock)
                   && resp.status == static_cast<uint8_t>(MuRpcStatus::Ok)) {
            stats.unlock_acks_sent++;
        }
    };

    auto post_recv = [&](const uint16_t client_id, const uint16_t recv_slot) {
        auto& buffer = recv_buffers[static_cast<size_t>(client_id) * MU_SERVER_RECV_RING + recv_slot];

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&buffer);
        sge.length = sizeof(MuRequest);
        sge.lkey = recv_mr->lkey;

        ibv_recv_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_recv_wr_id(client_id, recv_slot);
        wr.sg_list = &sge;
        wr.num_sge = 1;

        if (ibv_post_recv(clients_[client_id].cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("MuLeader: failed to post recv");
        }
    };

    auto try_alloc_mutation = [&]() -> std::optional<uint32_t> {
        if (free_mutations.empty()) {
            return std::nullopt;
        }
        const uint32_t id = free_mutations.front();
        free_mutations.pop_front();
        auto& ctx = mutations[id];
        ctx.in_use = true;
        ctx.quorum_done = false;
        ctx.applied = false;
        ctx.generation++;
        ctx.ack_count = 1;
        ctx.pending_followers = 0;
        stats.mutation_pool_high_watermark = std::max<uint64_t>(
            stats.mutation_pool_high_watermark,
            mutations.size() - free_mutations.size());
        current_mutations = mutations.size() - free_mutations.size();
        interval_mutation_hwm = std::max<uint64_t>(interval_mutation_hwm, mutations.size() - free_mutations.size());
        return id;
    };

    auto release_mutation = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        if (ctx.kind == MutationKind::append_lock) {
            auto& lock = locks[ctx.lock_id];
            if (lock.append_inflight > 0) {
                lock.append_inflight--;
            }
            if (current_append_inflight > 0) {
                current_append_inflight--;
            }
        }
        ctx.in_use = false;
        free_mutations.push_back(mutation_id);
        current_mutations = mutations.size() - free_mutations.size();
        enqueue_ready(ctx.lock_id);
    };

    auto maybe_release_mutation = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        if (!ctx.in_use) return;
        if (ctx.pending_followers != 0) return;
        if (!ctx.applied) return;
        release_mutation(mutation_id);
    };

    std::function<void(uint32_t)> try_grant;
    std::function<void()> advance_commit_tail;

    try_grant = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (lock.holder_active || lock.committed_waiters.empty()) return;

        const CommittedWaiter waiter = lock.committed_waiters.front();
        lock.committed_waiters.pop_front();

        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
        resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
        resp.client_id = waiter.client_id;
        resp.lock_id = lock_id;
        resp.req_id = waiter.req_id;
        resp.granted_slot = waiter.granted_slot;
        send_response(resp);

        lock.holder_active = true;
        lock.holder_slot = waiter.granted_slot;
        lock.holder_client_id = waiter.client_id;
        lock.holder_req_id = waiter.req_id;
    };

    auto apply_mutation = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        auto& lock = locks[ctx.lock_id];

        if (ctx.kind == MutationKind::append_lock) {
            stats.append_quorums++;
            if (!lock.holder_active && lock.committed_waiters.empty()) {
                MuResponse resp{};
                resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
                resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
                resp.client_id = ctx.client_id;
                resp.lock_id = ctx.lock_id;
                resp.req_id = ctx.req_id;
                resp.granted_slot = ctx.global_slot;
                send_response(resp);

                lock.holder_active = true;
                lock.holder_slot = ctx.global_slot;
                lock.holder_client_id = ctx.client_id;
                lock.holder_req_id = ctx.req_id;
            } else {
                lock.committed_waiters.push_back({ctx.client_id, ctx.req_id, ctx.global_slot});
            }
        } else {
            stats.unlock_quorums++;
            if (lock.unlock_mutation == static_cast<int>(mutation_id)) {
                lock.unlock_mutation = -1;
            }

            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
            resp.client_id = ctx.client_id;
            resp.lock_id = ctx.lock_id;
            resp.req_id = ctx.req_id;
            resp.granted_slot = ctx.granted_slot;

            if (!lock.holder_active
                || lock.holder_client_id != ctx.client_id
                || lock.holder_req_id != ctx.req_id
                || lock.holder_slot != ctx.granted_slot) {
                resp.status = static_cast<uint8_t>(MuRpcStatus::InvalidUnlock);
                send_response(resp);
            } else {
                resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
                send_response(resp);
                lock.holder_active = false;
                lock.holder_slot = 0;
                lock.holder_client_id = 0;
                lock.holder_req_id = 0;
                try_grant(ctx.lock_id);
            }
        }

        ctx.applied = true;
        maybe_release_mutation(mutation_id);
        enqueue_ready(ctx.lock_id);
    };

    advance_commit_tail = [&]() {
        while (true) {
            const auto it = slot_to_mutation.find(global_commit_tail);
            if (it == slot_to_mutation.end()) {
                break;
            }

            const uint32_t mutation_id = it->second;
            auto& ctx = mutations[mutation_id];
            if (!ctx.in_use || !ctx.quorum_done || ctx.applied) {
                break;
            }

            slot_to_mutation.erase(it);
            apply_mutation(mutation_id);
            global_commit_tail++;
        }
    };

    auto post_mutation_writes = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        auto* entry_ptr = local_buf + mu_global_log_slot_offset(ctx.global_slot);
        const size_t followers = follower_indices.size();
        const size_t quorum_needed = (QUORUM > 0) ? std::min<size_t>(QUORUM - 1, followers) : 0;
        const size_t signaled_to_track = mu_quorum_only_signal ? quorum_needed : followers;
        const size_t signal_start = followers == 0 ? 0 : (repl_signal_cursor % followers);
        if (followers != 0 && signaled_to_track != 0) {
            repl_signal_cursor = (repl_signal_cursor + signaled_to_track) % followers;
        }

        for (size_t follower_pos = 0; follower_pos < follower_indices.size(); ++follower_pos) {
            const size_t follower_idx = follower_indices[follower_pos];
            auto& follower = peers_[follower_idx];
            bool should_signal = !mu_quorum_only_signal;
            if (mu_quorum_only_signal && followers != 0) {
                should_signal = false;
                for (size_t i = 0; i < signaled_to_track; ++i) {
                    if (follower_pos == ((signal_start + i) % followers)) {
                        should_signal = true;
                        break;
                    }
                }
            }

            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(entry_ptr);
            sge.length = ENTRY_SIZE;
            sge.lkey = mr_->lkey;

            ibv_send_wr wr{}, *bad_wr = nullptr;
            wr.wr_id = make_repl_wr_id(mutation_id, ctx.generation);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.send_flags = IBV_SEND_INLINE | (should_signal ? IBV_SEND_SIGNALED : 0);
            wr.wr.rdma.remote_addr = follower.remote_addr + mu_global_log_slot_offset(ctx.global_slot);
            wr.wr.rdma.rkey = follower.rkey;

            if (ibv_post_send(follower.cm_id->qp, &wr, &bad_wr)) {
                throw std::runtime_error("MuLeader: failed to replicate mutation");
            }

            stats.replication_writes_posted++;
            if (should_signal) {
                ctx.pending_followers++;
                stats.replication_writes_signaled++;
            }
        }

        if (ctx.pending_followers == 0) {
            ctx.quorum_done = true;
            advance_commit_tail();
            maybe_release_mutation(mutation_id);
        }
    };

    auto start_append_mutation = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (lock.pending_locks.empty() || lock.append_inflight >= MU_MAX_APPEND_INFLIGHT_PER_LOCK) return;

        const auto mutation_id_opt = try_alloc_mutation();
        if (!mutation_id_opt.has_value()) {
            enqueue_ready(lock_id);
            return;
        }

        MuRequest req = lock.pending_locks.front();
        lock.pending_locks.pop_front();

        if (global_next_append_slot >= MU_GLOBAL_LOG_CAPACITY) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::QueueFull);
            resp.client_id = req.client_id;
            resp.lock_id = req.lock_id;
            resp.req_id = req.req_id;
            send_response(resp);
            enqueue_ready(lock_id);
            return;
        }

        const uint32_t global_slot = global_next_append_slot++;
        *reinterpret_cast<uint64_t*>(local_buf + mu_global_log_slot_offset(global_slot)) =
            mu_make_log_entry(MuRpcOp::Lock, req.lock_id, req.client_id, req.req_id);

        const uint32_t mutation_id = *mutation_id_opt;
        auto& ctx = mutations[mutation_id];
        ctx.kind = MutationKind::append_lock;
        ctx.global_slot = global_slot;
        ctx.lock_id = lock_id;
        ctx.client_id = req.client_id;
        ctx.req_id = req.req_id;
        ctx.granted_slot = 0;
        slot_to_mutation[global_slot] = mutation_id;

        lock.append_inflight++;
        current_append_inflight++;
        stats.append_inflight_high_watermark = std::max<uint64_t>(stats.append_inflight_high_watermark, lock.append_inflight);
        interval_append_inflight_hwm = std::max<uint64_t>(interval_append_inflight_hwm, current_append_inflight);

        post_mutation_writes(mutation_id);
    };

    auto start_unlock_mutation = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (!lock.pending_unlock.has_value() || lock.unlock_mutation != -1) return;

        const MuRequest req = *lock.pending_unlock;
        if (!lock.holder_active
            || req.client_id != lock.holder_client_id
            || req.req_id != lock.holder_req_id
            || req.granted_slot != lock.holder_slot) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::InvalidUnlock);
            resp.client_id = req.client_id;
            resp.lock_id = req.lock_id;
            resp.req_id = req.req_id;
            resp.granted_slot = req.granted_slot;
            send_response(resp);
            lock.pending_unlock.reset();
            enqueue_ready(lock_id);
            return;
        }

        const auto mutation_id_opt = try_alloc_mutation();
        if (!mutation_id_opt.has_value()) {
            enqueue_ready(lock_id);
            return;
        }

        if (global_next_append_slot >= MU_GLOBAL_LOG_CAPACITY) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
            resp.client_id = req.client_id;
            resp.lock_id = req.lock_id;
            resp.req_id = req.req_id;
            resp.granted_slot = req.granted_slot;
            send_response(resp);
            lock.pending_unlock.reset();
            enqueue_ready(lock_id);
            return;
        }

        const uint32_t global_slot = global_next_append_slot++;
        *reinterpret_cast<uint64_t*>(local_buf + mu_global_log_slot_offset(global_slot)) =
            mu_make_log_entry(MuRpcOp::Unlock, req.lock_id, req.client_id, req.req_id);

        const uint32_t mutation_id = *mutation_id_opt;
        auto& ctx = mutations[mutation_id];
        ctx.kind = MutationKind::append_unlock;
        ctx.global_slot = global_slot;
        ctx.lock_id = lock_id;
        ctx.client_id = req.client_id;
        ctx.req_id = req.req_id;
        ctx.granted_slot = req.granted_slot;
        slot_to_mutation[global_slot] = mutation_id;

        lock.pending_unlock.reset();
        lock.unlock_mutation = static_cast<int>(mutation_id);
        post_mutation_writes(mutation_id);
    };

    auto service_lock = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (lock.pending_unlock.has_value() && lock.unlock_mutation == -1) {
            start_unlock_mutation(lock_id);
        }
        while (!lock.pending_locks.empty() && lock.append_inflight < MU_MAX_APPEND_INFLIGHT_PER_LOCK) {
            start_append_mutation(lock_id);
        }
    };

    for (uint16_t client_id = 0; client_id < num_clients; ++client_id) {
        for (uint16_t recv_slot = 0; recv_slot < MU_SERVER_RECV_RING; ++recv_slot) {
            post_recv(client_id, recv_slot);
        }
    }

    ibv_wc wc[64];
    while (true) {
        const int n = ibv_poll_cq(cq_, 64, wc);
        if (n < 0) {
            throw std::runtime_error("MuLeader: CQ poll failed");
        }
        if (n == 0) {
            stats.empty_cq_polls++;
        } else {
            stats.nonempty_cq_polls++;
            stats.cqes_polled += static_cast<uint64_t>(n);
        }

        for (int i = 0; i < n; ++i) {
            const ibv_wc& comp = wc[i];
            if (comp.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(std::string("MuLeader: completion error ") + ibv_wc_status_str(comp.status));
            }

            if ((comp.opcode & IBV_WC_RECV) != 0) {
                if (!is_recv_wr_id(comp.wr_id)) {
                    continue;
                }
                stats.recv_cqes++;

                const uint16_t client_id = recv_client_id(comp.wr_id);
                const uint16_t recv_slot = recv_slot_index(comp.wr_id);
                const MuRequest req = recv_buffers[static_cast<size_t>(client_id) * MU_SERVER_RECV_RING + recv_slot];
                post_recv(client_id, recv_slot);

                if (req.client_id != client_id) {
                    MuResponse resp{};
                    resp.op = req.op;
                    resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
                    resp.client_id = client_id;
                    resp.lock_id = req.lock_id;
                    resp.req_id = req.req_id;
                    resp.granted_slot = req.granted_slot;
                    send_response(resp);
                    continue;
                }

                if (req.lock_id < lock_start_ || req.lock_id >= lock_end_) {
                    MuResponse resp{};
                    resp.op = req.op;
                    resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
                    resp.client_id = req.client_id;
                    resp.lock_id = req.lock_id;
                    resp.req_id = req.req_id;
                    resp.granted_slot = req.granted_slot;
                    send_response(resp);
                    continue;
                }

                auto& lock = locks[req.lock_id];
                if (req.op == static_cast<uint8_t>(MuRpcOp::Lock)) {
                    if (lock.pending_locks.size() >= MU_MAX_PENDING_PER_LOCK) {
                        MuResponse resp{};
                        resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
                        resp.status = static_cast<uint8_t>(MuRpcStatus::QueueFull);
                        resp.client_id = req.client_id;
                        resp.lock_id = req.lock_id;
                        resp.req_id = req.req_id;
                        send_response(resp);
                        continue;
                    }

                    lock.pending_locks.push_back(req);
                    stats.lock_reqs_recv++;
                    stats.pending_lock_queue_high_watermark = std::max<uint64_t>(stats.pending_lock_queue_high_watermark, lock.pending_locks.size());
                    interval_pending_lock_hwm = std::max<uint64_t>(interval_pending_lock_hwm, lock.pending_locks.size());
                    enqueue_ready(req.lock_id);
                } else if (req.op == static_cast<uint8_t>(MuRpcOp::Unlock)) {
                    stats.unlock_reqs_recv++;
                    if (lock.pending_unlock.has_value()) {
                        MuResponse resp{};
                        resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
                        resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
                        resp.client_id = req.client_id;
                        resp.lock_id = req.lock_id;
                        resp.req_id = req.req_id;
                        resp.granted_slot = req.granted_slot;
                        send_response(resp);
                        continue;
                    }

                    lock.pending_unlock = req;
                    enqueue_ready(req.lock_id);
                } else {
                    MuResponse resp{};
                    resp.op = req.op;
                    resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
                    resp.client_id = req.client_id;
                    resp.lock_id = req.lock_id;
                    resp.req_id = req.req_id;
                    resp.granted_slot = req.granted_slot;
                    send_response(resp);
                }

                continue;
            }

            if (is_resp_send_wr_id(comp.wr_id)) {
                stats.resp_send_cqes++;
                continue;
            }

            if (is_repl_wr_id(comp.wr_id)) {
                const uint32_t mutation_id = repl_mutation_id(comp.wr_id);
                if (mutation_id >= mutations.size()) {
                    throw std::runtime_error("MuLeader: mutation id out of range");
                }

                auto& ctx = mutations[mutation_id];
                if (!ctx.in_use || ctx.generation != repl_generation(comp.wr_id)) {
                    continue;
                }

                if (ctx.pending_followers == 0) {
                    continue;
                }

                ctx.pending_followers--;
                ctx.ack_count++;
                stats.replication_cqes++;

                if (!ctx.quorum_done && ctx.ack_count >= QUORUM) {
                    ctx.quorum_done = true;
                    if (ctx.kind == MutationKind::append_lock) {
                        stats.append_quorums++;
                    } else {
                        stats.unlock_quorums++;
                    }
                    advance_commit_tail();
                }

                maybe_release_mutation(mutation_id);
                continue;
            }
        }

        while (!ready_locks.empty()) {
            const uint32_t lock_id = ready_locks.front();
            ready_locks.pop_front();
            ready_flags[lock_id] = 0;
            current_ready_q = ready_locks.size();
            service_lock(lock_id);
        }

        if (mu_stats_enabled) {
            const auto now = std::chrono::steady_clock::now();
            if (now - last_stats_at >= std::chrono::seconds(1)) {
                const double interval_s = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_stats_at).count();
                MuLeaderStats delta{};
                delta.lock_reqs_recv = stats.lock_reqs_recv - prev_stats.lock_reqs_recv;
                delta.unlock_reqs_recv = stats.unlock_reqs_recv - prev_stats.unlock_reqs_recv;
                delta.grants_sent = stats.grants_sent - prev_stats.grants_sent;
                delta.unlock_acks_sent = stats.unlock_acks_sent - prev_stats.unlock_acks_sent;
                delta.recv_cqes = stats.recv_cqes - prev_stats.recv_cqes;
                delta.resp_send_cqes = stats.resp_send_cqes - prev_stats.resp_send_cqes;
                delta.replication_writes_posted = stats.replication_writes_posted - prev_stats.replication_writes_posted;
                delta.replication_writes_signaled = stats.replication_writes_signaled - prev_stats.replication_writes_signaled;
                delta.replication_cqes = stats.replication_cqes - prev_stats.replication_cqes;
                delta.append_quorums = stats.append_quorums - prev_stats.append_quorums;
                delta.unlock_quorums = stats.unlock_quorums - prev_stats.unlock_quorums;
                delta.empty_cq_polls = stats.empty_cq_polls - prev_stats.empty_cq_polls;
                delta.nonempty_cq_polls = stats.nonempty_cq_polls - prev_stats.nonempty_cq_polls;
                delta.cqes_polled = stats.cqes_polled - prev_stats.cqes_polled;
                const bool interval_active = delta.lock_reqs_recv != 0
                    || delta.unlock_reqs_recv != 0
                    || delta.grants_sent != 0
                    || delta.unlock_acks_sent != 0
                    || delta.recv_cqes != 0
                    || delta.replication_cqes != 0;
                if (interval_active || mu_stats_print_idle) {
                    print_stats(delta, interval_s);
                }
                prev_stats = stats;
                last_stats_at = now;
                current_ready_q = ready_locks.size();
                current_mutations = mutations.size() - free_mutations.size();
                interval_ready_q_hwm = current_ready_q;
                interval_mutation_hwm = current_mutations;
                interval_append_inflight_hwm = current_append_inflight;
                interval_pending_lock_hwm = 0;
            }
        }
    }
}
