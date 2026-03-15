#include "rdma/servers/mu_leader.h"

#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

enum class MutationKind : uint8_t {
    append_lock = 1,
    unlock_flip = 2,
};

constexpr uint64_t MU_RECV_WR_TAG = 0xB1ULL;
constexpr uint64_t MU_REPL_WR_TAG = 0xB2ULL;
constexpr uint64_t MU_RESP_WR_TAG = 0x4C53000000000000ULL;
constexpr uint64_t MU_WR_TAG_SHIFT = 56;
constexpr uint64_t MU_REPL_GEN_SHIFT = 24;
constexpr uint64_t MU_REPL_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t MU_REPL_ID_MASK = 0xFFFFFFULL;

struct MutationCtx {
    bool in_use = false;
    uint32_t generation = 0;
    MutationKind kind = MutationKind::append_lock;
    uint32_t lock_id = 0;
    uint32_t slot = 0;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t ack_count = 0;
    uint32_t pending_followers = 0;
    bool quorum_done = false;
};

struct LockState {
    std::deque<MuRequest> pending_locks;
    std::optional<MuRequest> pending_unlock;
    uint32_t next_append_slot = 0;
    uint32_t committed_tail = 0;
    uint32_t next_grant_slot = 0;
    bool holder_active = false;
    uint32_t holder_slot = 0;
    uint16_t holder_client_id = 0;
    uint32_t holder_req_id = 0;
    uint32_t append_inflight = 0;
    int unlock_mutation = -1;
};

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

}  // namespace

void MuLeader::run() {
    const bool mu_debug = get_uint_env_or("MU_DEBUG", 0) != 0;
    auto debug = [&](const std::string& msg) {
        if (mu_debug) {
            std::cout << "[MuLeader " << node_id_ << "] " << msg << "\n";
        }
    };

    std::cout << "[MuLeader " << node_id_ << "] locks ["
              << lock_start_ << ", " << lock_end_ << ")\n";

    auto* local_buf = static_cast<uint8_t*>(buf_);
    const uint32_t num_clients = expected_clients();
    const size_t handled_locks = static_cast<size_t>(lock_end_ - lock_start_);
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
        if (++client_send_signal_counts[resp.client_id] % MU_SERVER_SEND_SIGNAL_EVERY_DEFAULT == 0) {
            wr.send_flags |= IBV_SEND_SIGNALED;
        }

        if (ibv_post_send(clients_[resp.client_id].cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("MuLeader: failed to post client response send");
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
        ctx.generation++;
        ctx.ack_count = 1;
        ctx.pending_followers = 0;
        ctx.quorum_done = false;
        return id;
    };

    auto release_mutation = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        ctx.in_use = false;
        free_mutations.push_back(mutation_id);
    };

    std::function<void(uint32_t)> try_grant;
    std::function<void(uint32_t)> handle_quorum;

    auto post_mutation_writes = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        auto* lock_base = mu_lock_base(local_buf, ctx.lock_id);
        auto* entry_ptr = mu_entry_ptr(lock_base, ctx.slot);

        for (const size_t follower_idx : follower_indices) {
            auto& follower = peers_[follower_idx];

            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(entry_ptr);
            sge.length = ENTRY_SIZE;
            sge.lkey = mr_->lkey;

            ibv_send_wr wr{}, *bad_wr = nullptr;
            wr.wr_id = make_repl_wr_id(mutation_id, ctx.generation);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = follower.remote_addr + lock_log_slot_offset(ctx.lock_id, ctx.slot);
            wr.wr.rdma.rkey = follower.rkey;

            if (ibv_post_send(follower.cm_id->qp, &wr, &bad_wr)) {
                throw std::runtime_error("MuLeader: failed to replicate mutation");
            }

            ctx.pending_followers++;
        }

        if (ctx.pending_followers == 0) {
            debug(
                "mutation immediate quorum kind=" + std::to_string(static_cast<int>(ctx.kind))
                + " lock=" + std::to_string(ctx.lock_id)
                + " slot=" + std::to_string(ctx.slot));
            handle_quorum(mutation_id);
            release_mutation(mutation_id);
        }
    };

    try_grant = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (lock.holder_active) return;

        const auto* lock_base = mu_lock_base(local_buf, lock_id);
        while (lock.next_grant_slot < lock.committed_tail) {
            const uint64_t entry = mu_read_entry_word(lock_base, lock.next_grant_slot);
            if (mu_entry_is_unlocked(entry)) {
                lock.next_grant_slot++;
                continue;
            }

            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
            resp.client_id = mu_entry_client_id(entry);
            resp.lock_id = lock_id;
            resp.req_id = mu_entry_req_id(entry);
            resp.granted_slot = lock.next_grant_slot;
            debug(
                "grant sent lock=" + std::to_string(lock_id)
                + " slot=" + std::to_string(lock.next_grant_slot)
                + " client=" + std::to_string(resp.client_id)
                + " req=" + std::to_string(resp.req_id));
            send_response(resp);

            lock.holder_active = true;
            lock.holder_slot = lock.next_grant_slot;
            lock.holder_client_id = resp.client_id;
            lock.holder_req_id = resp.req_id;
            lock.next_grant_slot++;
            return;
        }
    };

    handle_quorum = [&](const uint32_t mutation_id) {
        auto& ctx = mutations[mutation_id];
        if (!ctx.in_use || ctx.quorum_done) return;
        ctx.quorum_done = true;

        auto& lock = locks[ctx.lock_id];

        if (ctx.kind == MutationKind::append_lock) {
            lock.committed_tail = std::max(lock.committed_tail, ctx.slot + 1);
            mu_write_commit_index(mu_lock_base(local_buf, ctx.lock_id), lock.committed_tail);
            debug(
                "append quorum lock=" + std::to_string(ctx.lock_id)
                + " slot=" + std::to_string(ctx.slot)
                + " committed_tail=" + std::to_string(lock.committed_tail));
            try_grant(ctx.lock_id);
        } else {
            if (lock.unlock_mutation == static_cast<int>(mutation_id)) {
                lock.unlock_mutation = -1;
            }
            debug(
                "unlock quorum lock=" + std::to_string(ctx.lock_id)
                + " slot=" + std::to_string(ctx.slot)
                + " client=" + std::to_string(ctx.client_id)
                + " req=" + std::to_string(ctx.req_id));
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
            resp.client_id = ctx.client_id;
            resp.lock_id = ctx.lock_id;
            resp.req_id = ctx.req_id;
            resp.granted_slot = ctx.slot;
            send_response(resp);

            lock.holder_active = false;
            lock.holder_slot = 0;
            lock.holder_client_id = 0;
            lock.holder_req_id = 0;
            try_grant(ctx.lock_id);
        }

        enqueue_ready(ctx.lock_id);
    };

    auto start_append_mutation = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (lock.pending_locks.empty() || lock.append_inflight >= MU_MAX_APPEND_INFLIGHT_PER_LOCK) return;

        const auto mutation_id_opt = try_alloc_mutation();
        if (!mutation_id_opt.has_value()) {
            debug(
                "append backpressure lock=" + std::to_string(lock_id)
                + " pending=" + std::to_string(lock.pending_locks.size())
                + " append_inflight=" + std::to_string(lock.append_inflight));
            enqueue_ready(lock_id);
            return;
        }

        MuRequest req = lock.pending_locks.front();
        lock.pending_locks.pop_front();

        if (lock.next_append_slot >= MAX_LOG_PER_LOCK) {
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

        const uint32_t slot = lock.next_append_slot++;
        auto* lock_base = mu_lock_base(local_buf, lock_id);
        mu_write_entry_word(lock_base, slot, mu_make_entry(req.client_id, req.req_id, false));
        debug(
            "append posted lock=" + std::to_string(lock_id)
            + " slot=" + std::to_string(slot)
            + " client=" + std::to_string(req.client_id)
            + " req=" + std::to_string(req.req_id)
            + " append_inflight=" + std::to_string(lock.append_inflight + 1));

        const uint32_t mutation_id = *mutation_id_opt;
        auto& ctx = mutations[mutation_id];
        ctx.kind = MutationKind::append_lock;
        ctx.lock_id = lock_id;
        ctx.slot = slot;
        ctx.client_id = req.client_id;
        ctx.req_id = req.req_id;
        lock.append_inflight++;

        post_mutation_writes(mutation_id);
    };

    auto start_unlock_mutation = [&](const uint32_t lock_id) {
        auto& lock = locks[lock_id];
        if (!lock.pending_unlock.has_value() || lock.unlock_mutation != -1) return;

        const auto mutation_id_opt = try_alloc_mutation();
        if (!mutation_id_opt.has_value()) {
            debug(
                "unlock backpressure lock=" + std::to_string(lock_id)
                + " holder_slot=" + std::to_string(lock.holder_slot));
            enqueue_ready(lock_id);
            return;
        }

        const MuRequest req = *lock.pending_unlock;
        lock.pending_unlock.reset();

        if (!lock.holder_active
            || req.client_id != lock.holder_client_id
            || req.req_id != lock.holder_req_id
            || req.granted_slot != lock.holder_slot) {
            debug(
                "unlock rejected lock=" + std::to_string(lock_id)
                + " req_client=" + std::to_string(req.client_id)
                + " holder_client=" + std::to_string(lock.holder_client_id)
                + " req_slot=" + std::to_string(req.granted_slot)
                + " holder_slot=" + std::to_string(lock.holder_slot)
                + " req_id=" + std::to_string(req.req_id)
                + " holder_req=" + std::to_string(lock.holder_req_id));
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::InvalidUnlock);
            resp.client_id = req.client_id;
            resp.lock_id = req.lock_id;
            resp.req_id = req.req_id;
            resp.granted_slot = req.granted_slot;
            send_response(resp);
            enqueue_ready(lock_id);
            return;
        }

        auto* lock_base = mu_lock_base(local_buf, lock_id);
        const uint64_t current_entry = mu_read_entry_word(lock_base, req.granted_slot);
        mu_write_entry_word(lock_base, req.granted_slot, mu_entry_mark_unlocked(current_entry));
        debug(
            "unlock posted lock=" + std::to_string(lock_id)
            + " slot=" + std::to_string(req.granted_slot)
            + " client=" + std::to_string(req.client_id)
            + " req=" + std::to_string(req.req_id));

        const uint32_t mutation_id = *mutation_id_opt;
        auto& ctx = mutations[mutation_id];
        ctx.kind = MutationKind::unlock_flip;
        ctx.lock_id = lock_id;
        ctx.slot = req.granted_slot;
        ctx.client_id = req.client_id;
        ctx.req_id = req.req_id;
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

        for (int i = 0; i < n; ++i) {
            const ibv_wc& comp = wc[i];
            if (comp.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    std::string("MuLeader: completion error ") + ibv_wc_status_str(comp.status));
            }

            debug(
                "cq opcode=" + std::to_string(comp.opcode)
                + " wr_id=" + std::to_string(comp.wr_id));

            if ((comp.opcode & IBV_WC_RECV) != 0) {
                if (!is_recv_wr_id(comp.wr_id)) {
                    continue;
                }

                const uint16_t client_id = recv_client_id(comp.wr_id);
                const uint16_t recv_slot = recv_slot_index(comp.wr_id);
                const MuRequest req = recv_buffers[static_cast<size_t>(client_id) * MU_SERVER_RECV_RING + recv_slot];
                post_recv(client_id, recv_slot);
                debug(
                    "recv op=" + std::to_string(req.op)
                    + " lock=" + std::to_string(req.lock_id)
                    + " client=" + std::to_string(req.client_id)
                    + " req=" + std::to_string(req.req_id)
                    + " slot=" + std::to_string(req.granted_slot));

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
                    enqueue_ready(req.lock_id);
                } else if (req.op == static_cast<uint8_t>(MuRpcOp::Unlock)) {
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
                        continue;
                    }

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
                debug(
                    "replication cqe kind=" + std::to_string(static_cast<int>(ctx.kind))
                    + " lock=" + std::to_string(ctx.lock_id)
                    + " slot=" + std::to_string(ctx.slot)
                    + " ack_count=" + std::to_string(ctx.ack_count)
                    + " pending_followers=" + std::to_string(ctx.pending_followers));

                if (!ctx.quorum_done && ctx.ack_count >= QUORUM) {
                    handle_quorum(mutation_id);
                }

                if (ctx.pending_followers == 0) {
                    auto& lock = locks[ctx.lock_id];
                    if (ctx.kind == MutationKind::append_lock && lock.append_inflight > 0) {
                        lock.append_inflight--;
                    }
                    release_mutation(mutation_id);
                    enqueue_ready(ctx.lock_id);
                }

                continue;
            }
        }

        while (!ready_locks.empty()) {
            const uint32_t lock_id = ready_locks.front();
            ready_locks.pop_front();
            ready_flags[lock_id] = 0;
            service_lock(lock_id);
        }
    }
}
