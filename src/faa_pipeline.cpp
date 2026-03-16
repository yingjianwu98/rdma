#include "rdma/faa_pipeline.h"

#include "rdma/client.h"
#include "rdma/common.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

namespace {

constexpr uint64_t FAA_DONE_BIT = 1ULL << 63;
constexpr uint64_t FAA_NOTIFY_CLEAR = 0;
constexpr uint64_t FAA_CONN_BITS = 8;
constexpr uint64_t FAA_ROUND_BITS = 8;
constexpr uint64_t FAA_PHASE_BITS = 8;
constexpr uint64_t FAA_SLOT_BITS = 16;
constexpr uint64_t FAA_CONN_SHIFT = 0;
constexpr uint64_t FAA_ROUND_SHIFT = FAA_CONN_SHIFT + FAA_CONN_BITS;
constexpr uint64_t FAA_PHASE_SHIFT = FAA_ROUND_SHIFT + FAA_ROUND_BITS;
constexpr uint64_t FAA_SLOT_SHIFT = FAA_PHASE_SHIFT + FAA_PHASE_BITS;
constexpr uint64_t FAA_GENERATION_SHIFT = FAA_SLOT_SHIFT + FAA_SLOT_BITS;
constexpr uint64_t FAA_CONN_MASK = (1ULL << FAA_CONN_BITS) - 1;
constexpr uint64_t FAA_ROUND_MASK = (1ULL << FAA_ROUND_BITS) - 1;
constexpr uint64_t FAA_PHASE_MASK = (1ULL << FAA_PHASE_BITS) - 1;
constexpr uint64_t FAA_SLOT_MASK = (1ULL << FAA_SLOT_BITS) - 1;
constexpr uint64_t FAA_GENERATION_MASK = (1ULL << FAA_SLOT_SHIFT) - 1;

enum class FaaPhase : uint8_t {
    idle = 0,
    faa_ticket = 1,
    replicate_ticket = 2,
    wait_predecessor_read = 3,
    wait_notify_spin = 4,
    mark_done = 5,
    successor_read = 6,
    notify_successor = 7,
};

struct RegisteredFaaBuffers {
    uint64_t* faa_results = nullptr;
    uint64_t* replicate_results = nullptr;
    uint64_t* prev_reads = nullptr;
    uint64_t* next_reads = nullptr;
    uint64_t* release_values = nullptr;
    uint64_t* notify_values = nullptr;
    uint64_t* notify_slots = nullptr;
};

struct FaaOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t req_id = 0;
    uint8_t round = 0;
    uint64_t ticket = 0;
    uint64_t waiter_id = 0;
    uint64_t next_waiter_id = EMPTY_SLOT;
    bool successor_known = false;
    uint32_t notify_spin_remaining = 0;
    bool local_handoff_ready = false;
    FaaPhase phase = FaaPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;
    uint32_t quorum_hits = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

class ZipfLockPicker {
public:
    explicit ZipfLockPicker(const double skew)
        : skew_(std::max(skew, 0.0))
        , uniform_(0, MAX_LOCKS - 1) {
        if (skew_ > 0.0) {
            std::vector<double> weights(MAX_LOCKS);
            for (size_t i = 0; i < MAX_LOCKS; ++i) {
                weights[i] = 1.0 / std::pow(static_cast<double>(i + 1), skew_);
            }
            zipf_ = std::discrete_distribution<uint32_t>(weights.begin(), weights.end());
            use_zipf_ = true;
        }
    }

    uint32_t next() {
        return use_zipf_ ? zipf_(rng_) : uniform_(rng_);
    }

private:
    double skew_;
    bool use_zipf_ = false;
    std::mt19937 rng_{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> uniform_;
    std::discrete_distribution<uint32_t> zipf_;
};

uint64_t encode_waiter(const uint16_t client_id, const uint16_t op_slot, const uint32_t req_id, const bool done) {
    return (done ? FAA_DONE_BIT : 0ULL)
         | (static_cast<uint64_t>(client_id) << 47)
         | (static_cast<uint64_t>(op_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

uint16_t decode_waiter_client(const uint64_t waiter) {
    return static_cast<uint16_t>((waiter >> 47) & 0xFFFFu);
}

uint16_t decode_waiter_slot(const uint64_t waiter) {
    return static_cast<uint16_t>((waiter >> 32) & 0x7FFFu);
}

uint32_t decode_waiter_req(const uint64_t waiter) {
    return static_cast<uint32_t>(waiter & 0xFFFFFFFFu);
}

bool waiter_done(const uint64_t waiter) {
    return (waiter & FAA_DONE_BIT) != 0;
}

uint64_t waiter_mark_done(const uint64_t waiter) {
    return waiter | FAA_DONE_BIT;
}

uint64_t encode_wr_id(const FaaOpCtx& op, const FaaPhase phase, const uint8_t conn_index) {
    return ((static_cast<uint64_t>(op.generation) & FAA_GENERATION_MASK) << FAA_GENERATION_SHIFT)
         | ((static_cast<uint64_t>(op.slot) & FAA_SLOT_MASK) << FAA_SLOT_SHIFT)
         | ((static_cast<uint64_t>(phase) & FAA_PHASE_MASK) << FAA_PHASE_SHIFT)
         | ((static_cast<uint64_t>(op.round) & FAA_ROUND_MASK) << FAA_ROUND_SHIFT)
         | ((static_cast<uint64_t>(conn_index) & FAA_CONN_MASK) << FAA_CONN_SHIFT);
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> FAA_GENERATION_SHIFT);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> FAA_SLOT_SHIFT) & FAA_SLOT_MASK);
}

FaaPhase wr_phase(const uint64_t wr_id) {
    return static_cast<FaaPhase>((wr_id >> FAA_PHASE_SHIFT) & FAA_PHASE_MASK);
}

uint8_t wr_round(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> FAA_ROUND_SHIFT) & FAA_ROUND_MASK);
}

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> FAA_CONN_SHIFT) & FAA_CONN_MASK);
}

size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

uint64_t* row_ptr(uint64_t* base, const uint32_t slot, const size_t replica_count) {
    return base + row_offset(slot, replica_count);
}

size_t faa_notify_slots_offset(const size_t active_window, const size_t replica_count) {
    const size_t matrix_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    size_t offset = 0;
    offset += align_up(active_window * sizeof(uint64_t), 64); // faa results
    offset += matrix_bytes; // replicate results
    offset += matrix_bytes; // prev reads
    offset += matrix_bytes; // next reads
    offset += align_up(active_window * sizeof(uint64_t), 64); // release values
    offset += align_up(active_window * sizeof(uint64_t), 64); // notify values
    return offset;
}

size_t faa_notify_slot_offset(const size_t active_window, const size_t replica_count, const uint16_t op_slot) {
    return faa_notify_slots_offset(active_window, replica_count) + (static_cast<size_t>(op_slot) * sizeof(uint64_t));
}

RegisteredFaaBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    RegisteredFaaBuffers buffers{};
    const size_t matrix_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    const size_t scalar_bytes = align_up(active_window * sizeof(uint64_t), 64);

    buffers.faa_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.prev_reads = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.next_reads = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.release_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.notify_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.notify_slots = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("FAA pipeline: registered client buffer too small");
    }
    return buffers;
}

void post_faa_ticket(Client& client, FaaOpCtx& op, const RegisteredFaaBuffers& buffers, FaaPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.faa_results[op.slot];
    *result = 0;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    op.round++;
    op.phase = FaaPhase::faa_ticket;
    op.responses = 0;
    op.response_target = 1;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, FaaPhase::faa_ticket, 0);
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.atomic.remote_addr = conns[0].addr + lock_control_offset(op.lock_id);
    wr.wr.atomic.rkey = conns[0].rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(conns[0].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("FAA pipeline: FAA ticket post failed");
    }

    stats.faa_ticket_posts++;

}

void post_replicate_ticket(Client& client, FaaOpCtx& op, const RegisteredFaaBuffers& buffers, FaaPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    op.round++;
    op.phase = FaaPhase::replicate_ticket;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());
    op.quorum_hits = 0;

    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = EMPTY_SLOT - 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, FaaPhase::replicate_ticket, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.atomic.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.ticket);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = EMPTY_SLOT;
        wr.wr.atomic.swap = op.waiter_id;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("FAA pipeline: replicate ticket post failed");
        }
    }

    stats.replicate_posts += conns.size();

}

void post_wait_predecessor_read(Client& client, FaaOpCtx& op, const RegisteredFaaBuffers& buffers, FaaPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* prev_values = row_ptr(buffers.prev_reads, op.slot, conns.size());
    auto* next_values = row_ptr(buffers.next_reads, op.slot, conns.size());
    const uint64_t prev_slot = op.ticket - 1;
    const uint64_t next_slot = op.ticket + 1;

    op.round++;
    op.phase = FaaPhase::wait_predecessor_read;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        prev_values[i] = EMPTY_SLOT;

        ibv_sge prev_sge{};
        prev_sge.addr = reinterpret_cast<uintptr_t>(&prev_values[i]);
        prev_sge.length = sizeof(uint64_t);
        prev_sge.lkey = mr->lkey;

        ibv_send_wr prev_wr{}, *bad_wr = nullptr;
        prev_wr.wr_id = encode_wr_id(op, FaaPhase::wait_predecessor_read, static_cast<uint8_t>(i));
        prev_wr.opcode = IBV_WR_RDMA_READ;
        prev_wr.send_flags = IBV_SEND_SIGNALED;
        prev_wr.sg_list = &prev_sge;
        prev_wr.num_sge = 1;
        prev_wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, prev_slot);
        prev_wr.wr.rdma.rkey = conns[i].rkey;

        if (!op.successor_known) {
            next_values[i] = EMPTY_SLOT;

            ibv_sge next_sge{};
            next_sge.addr = reinterpret_cast<uintptr_t>(&next_values[i]);
            next_sge.length = sizeof(uint64_t);
            next_sge.lkey = mr->lkey;

            ibv_send_wr next_wr{};
            next_wr.wr_id = encode_wr_id(op, FaaPhase::wait_predecessor_read, static_cast<uint8_t>(i));
            next_wr.opcode = IBV_WR_RDMA_READ;
            next_wr.send_flags = 0;
            next_wr.sg_list = &next_sge;
            next_wr.num_sge = 1;
            next_wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, next_slot);
            next_wr.wr.rdma.rkey = conns[i].rkey;
            next_wr.next = &prev_wr;

            if (ibv_post_send(conns[i].id->qp, &next_wr, &bad_wr)) {
                throw std::runtime_error("FAA pipeline: wait round post failed");
            }
        } else if (ibv_post_send(conns[i].id->qp, &prev_wr, &bad_wr)) {
            throw std::runtime_error("FAA pipeline: wait round post failed");
        }
    }

    stats.wait_round_posts += op.successor_known ? conns.size() : conns.size() * 2;

}

void post_mark_done(Client& client, FaaOpCtx& op, const RegisteredFaaBuffers& buffers, FaaPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* value = &buffers.release_values[op.slot];
    *value = waiter_mark_done(op.waiter_id);

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(value);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    op.round++;
    op.phase = FaaPhase::mark_done;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(QUORUM);

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, FaaPhase::mark_done, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, op.ticket);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("FAA pipeline: mark done post failed");
        }
    }

    stats.mark_done_posts += conns.size();

}

void post_successor_read(Client& client, FaaOpCtx& op, const RegisteredFaaBuffers& buffers, FaaPipelineStats& stats) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* next_values = row_ptr(buffers.next_reads, op.slot, conns.size());
    const uint64_t next_slot = op.ticket + 1;

    op.round++;
    op.phase = FaaPhase::successor_read;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());

    for (size_t i = 0; i < conns.size(); ++i) {
        next_values[i] = EMPTY_SLOT;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&next_values[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, FaaPhase::successor_read, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + lock_log_slot_offset(op.lock_id, next_slot);
        wr.wr.rdma.rkey = conns[i].rkey;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("FAA pipeline: successor read post failed");
        }
    }

    stats.successor_read_posts += conns.size();

}

bool post_notify_successor(
    Client& client,
    FaaOpCtx& op,
    std::vector<FaaOpCtx>& ops,
    const RegisteredFaaBuffers& buffers,
    const size_t active_window,
    FaaPipelineStats& stats
) {
    if (!op.successor_known) {
        return false;
    }

    const auto& peers = client.peers();
    const uint16_t next_client = decode_waiter_client(op.next_waiter_id);
    const uint16_t next_slot = decode_waiter_slot(op.next_waiter_id);
    auto* value = &buffers.notify_values[op.slot];
    *value = op.next_waiter_id;

    if (next_client == client.id()) {
        if (next_slot < ops.size()) {
            auto& target = ops[next_slot];
            if (target.active
                && target.waiter_id == op.next_waiter_id
                && target.req_id == decode_waiter_req(op.next_waiter_id)) {
                target.local_handoff_ready = true;
                stats.local_direct_handoffs++;
            }
        }
        return false;
    }

    if (next_client >= peers.size() || peers[next_client].id == nullptr) {
        return false;
    }

    op.round++;
    op.phase = FaaPhase::notify_successor;
    op.responses = 0;
    op.response_target = 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(value);
    sge.length = sizeof(uint64_t);
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, FaaPhase::notify_successor, 0);
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = peers[next_client].addr + faa_notify_slot_offset(active_window, client.connections().size(), next_slot);
    wr.wr.rdma.rkey = peers[next_client].rkey;

    if (ibv_post_send(peers[next_client].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("FAA pipeline: notify post failed");
    }

    stats.notify_posts++;

    return true;
}

bool quorum_waiter_done(const uint64_t* values, const size_t replica_count) {
    size_t done_count = 0;
    for (size_t i = 0; i < replica_count; ++i) {
        if (values[i] != EMPTY_SLOT && waiter_done(values[i])) {
            done_count++;
        }
    }
    return done_count >= QUORUM;
}

uint64_t learn_waiter_quorum(const uint64_t* values, const size_t replica_count) {
    for (size_t i = 0; i < replica_count; ++i) {
        const uint64_t value = values[i];
        if (value == EMPTY_SLOT) continue;
        size_t count = 0;
        for (size_t j = 0; j < replica_count; ++j) {
            if (values[j] == value) {
                count++;
            }
        }
        if (count >= QUORUM) {
            return value;
        }
    }
    return EMPTY_SLOT;
}

} // namespace

FaaPipelineConfig load_faa_pipeline_config() {
    FaaPipelineConfig config{};
    config.active_window = std::max<size_t>(1, FAA_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, FAA_CQ_BATCH);
    config.zipf_skew = FAA_ZIPF_SKEW;
    return config;
}

size_t faa_pipeline_client_buffer_size(const FaaPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t matrix_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    const size_t scalar_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    return align_up(scalar_bytes * 4 + matrix_bytes * 3 + PAGE_SIZE, PAGE_SIZE);
}

void run_faa_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const FaaPipelineConfig& config,
    FaaPipelineStats* out_stats
) {
    const bool faa_debug = FAA_DEBUG;
    auto debug = [&](const std::string& msg) {
        if (faa_debug) {
            std::cout << "[FaaClient " << client.id() << "] " << msg << "\n";
        }
    };
    FaaPipelineStats stats{};

    const auto& conns = client.connections();
    if (conns.empty()) throw std::runtime_error("FAA pipeline: no server connections");
    if (config.active_window > 0x7FFFu) throw std::runtime_error("FAA pipeline: active window too large");

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<FaaOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);
    std::fill_n(buffers.notify_slots, config.active_window, FAA_NOTIFY_CLEAR);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto begin_release = [&](FaaOpCtx& op) {
        debug(
            "begin release lock=" + std::to_string(op.lock_id)
            + " ticket=" + std::to_string(op.ticket)
            + " slot=" + std::to_string(op.slot)
            + " successor_known=" + std::to_string(op.successor_known));
        latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - op.started_at).count();
        post_mark_done(client, op, buffers, stats);
    };

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = picker.next();
        op.req_id = next_req_id++;
        op.ticket = 0;
        op.waiter_id = encode_waiter(static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot), op.req_id, false);
        op.next_waiter_id = EMPTY_SLOT;
        op.successor_known = false;
        op.notify_spin_remaining = 0;
        op.local_handoff_ready = false;
        op.responses = 0;
        op.response_target = 0;
        op.quorum_hits = 0;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        buffers.notify_slots[slot] = FAA_NOTIFY_CLEAR;
        debug(
            "submit lock=" + std::to_string(op.lock_id)
            + " req=" + std::to_string(op.req_id)
            + " slot=" + std::to_string(op.slot));
        stats.active_ops_hwm = std::max<uint64_t>(stats.active_ops_hwm, active + 1);
        post_faa_ticket(client, op, buffers, stats);
        submitted++;
        active++;
        stats.active_ops_hwm = std::max<uint64_t>(stats.active_ops_hwm, active);
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        for (auto& op : ops) {
            if (!op.active) continue;
            if (op.local_handoff_ready
                && (op.phase == FaaPhase::wait_predecessor_read || op.phase == FaaPhase::wait_notify_spin)) {
                op.local_handoff_ready = false;
                begin_release(op);
                continue;
            }

            if (op.phase != FaaPhase::wait_notify_spin) continue;
            if (buffers.notify_slots[op.slot] == op.waiter_id) {
                buffers.notify_slots[op.slot] = FAA_NOTIFY_CLEAR;
                stats.notify_spin_hits++;
                begin_release(op);
                continue;
            }

            stats.notify_spin_iterations++;
            if (op.notify_spin_remaining > 0) {
                --op.notify_spin_remaining;
            }
            if (op.notify_spin_remaining == 0) {
                stats.notify_spin_exhausted++;
                stats.wait_round_retries++;
                debug(
                    "wait round retry lock=" + std::to_string(op.lock_id)
                    + " req=" + std::to_string(op.req_id)
                    + " ticket=" + std::to_string(op.ticket));
                post_wait_predecessor_read(client, op, buffers, stats);
            }
        }

        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) throw std::runtime_error("FAA pipeline: CQ poll failed");
        if (polled == 0) {
            stats.empty_polls++;
            continue;
        }
        stats.nonempty_polls++;
        stats.cqes_polled += static_cast<uint64_t>(polled);

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "FAA pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) throw std::runtime_error("FAA pipeline: completion slot out of range");
            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) continue;
            const FaaPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) continue;
            if (wr_round(wc.wr_id) != op.round) continue;

            if (phase == FaaPhase::faa_ticket) {
                stats.faa_ticket_cqes++;
                op.ticket = buffers.faa_results[op.slot];
                debug(
                    "faa ticket lock=" + std::to_string(op.lock_id)
                    + " req=" + std::to_string(op.req_id)
                    + " ticket=" + std::to_string(op.ticket));
                post_replicate_ticket(client, op, buffers, stats);
                continue;
            }

            op.responses++;

            if (phase == FaaPhase::replicate_ticket) {
                stats.replicate_cqes++;
                auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (results[idx] == EMPTY_SLOT) {
                    op.quorum_hits++;
                }
                const uint32_t remaining = op.response_target - op.responses;
                if (op.quorum_hits >= QUORUM) {
                    stats.replicate_quorum_wins++;
                    debug(
                        "replicate quorum lock=" + std::to_string(op.lock_id)
                        + " req=" + std::to_string(op.req_id)
                        + " ticket=" + std::to_string(op.ticket));
                    if (op.ticket == 0) {
                        begin_release(op);
                    } else if (op.local_handoff_ready) {
                        op.local_handoff_ready = false;
                        begin_release(op);
                    } else {
                        post_wait_predecessor_read(client, op, buffers, stats);
                    }
                    continue;
                }
                if (op.quorum_hits + remaining < QUORUM) {
                    throw std::runtime_error("FAA pipeline: ticket replication cannot reach quorum");
                }
                if (op.responses < op.response_target) continue;
                if (op.quorum_hits < QUORUM) {
                    throw std::runtime_error("FAA pipeline: ticket replication failed to reach quorum");
                }
                if (op.ticket == 0) {
                    begin_release(op);
                } else if (op.local_handoff_ready) {
                    op.local_handoff_ready = false;
                    begin_release(op);
                } else {
                    post_wait_predecessor_read(client, op, buffers, stats);
                }
                continue;
            }

            if (phase == FaaPhase::wait_predecessor_read) {
                stats.wait_round_cqes++;
                auto* prev_values = row_ptr(buffers.prev_reads, op.slot, conns.size());
                auto* next_values = row_ptr(buffers.next_reads, op.slot, conns.size());

                if (op.local_handoff_ready) {
                    op.local_handoff_ready = false;
                    begin_release(op);
                    continue;
                }

                if (!op.successor_known) {
                    const uint64_t next_waiter = learn_waiter_quorum(next_values, conns.size());
                    if (next_waiter != EMPTY_SLOT) {
                        stats.successor_learn_quorum++;
                        stats.successor_learned_while_waiting++;
                        op.next_waiter_id = next_waiter;
                        op.successor_known = true;
                    }
                }

                if (quorum_waiter_done(prev_values, conns.size())) {
                    stats.predecessor_quorum_done++;
                    debug(
                        "predecessor cleared lock=" + std::to_string(op.lock_id)
                        + " req=" + std::to_string(op.req_id)
                        + " ticket=" + std::to_string(op.ticket)
                        + " successor_known=" + std::to_string(op.successor_known));
                    begin_release(op);
                } else if (op.responses >= op.response_target) {
                    debug(
                        "wait notify spin lock=" + std::to_string(op.lock_id)
                        + " req=" + std::to_string(op.req_id)
                        + " ticket=" + std::to_string(op.ticket));
                    op.notify_spin_remaining = FAA_NOTIFY_SPIN_ROUNDS;
                    op.phase = FaaPhase::wait_notify_spin;
                    stats.notify_spin_entries++;
                } else {
                    continue;
                }
                continue;
            }

            if (phase == FaaPhase::mark_done) {
                stats.mark_done_cqes++;
                if (op.responses < QUORUM) continue;
                debug(
                    "mark done quorum lock=" + std::to_string(op.lock_id)
                    + " req=" + std::to_string(op.req_id)
                    + " successor_known=" + std::to_string(op.successor_known));
                if (op.successor_known) {
                    if (!post_notify_successor(client, op, ops, buffers, config.active_window, stats)) {
                        debug(
                            "retire after local/no notify lock=" + std::to_string(op.lock_id)
                            + " req=" + std::to_string(op.req_id));
                        lock_counts[op.lock_id]++;
                        op.active = false;
                        op.phase = FaaPhase::idle;
                        completed++;
                        active--;
                        if (submitted < NUM_OPS_PER_CLIENT) submit_op(slot);
                    }
                } else {
                    post_successor_read(client, op, buffers, stats);
                }
                continue;
            }

            if (phase == FaaPhase::successor_read) {
                stats.successor_read_cqes++;
                auto* next_values = row_ptr(buffers.next_reads, op.slot, conns.size());
                const uint64_t next_waiter = learn_waiter_quorum(next_values, conns.size());
                if (next_waiter != EMPTY_SLOT) {
                    stats.successor_learn_quorum++;
                    stats.successor_learned_on_unlock++;
                    op.next_waiter_id = next_waiter;
                    op.successor_known = true;
                    debug(
                        "successor learned lock=" + std::to_string(op.lock_id)
                        + " req=" + std::to_string(op.req_id)
                        + " next_client=" + std::to_string(decode_waiter_client(next_waiter))
                        + " next_slot=" + std::to_string(decode_waiter_slot(next_waiter)));
                    if (!post_notify_successor(client, op, ops, buffers, config.active_window, stats)) {
                        debug(
                            "retire after successor local/no notify lock=" + std::to_string(op.lock_id)
                            + " req=" + std::to_string(op.req_id));
                        lock_counts[op.lock_id]++;
                        op.active = false;
                        op.phase = FaaPhase::idle;
                        completed++;
                        active--;
                        if (submitted < NUM_OPS_PER_CLIENT) submit_op(slot);
                    }
                } else if (op.responses >= op.response_target) {
                    stats.retire_no_successor++;
                    debug(
                        "retire no successor lock=" + std::to_string(op.lock_id)
                        + " req=" + std::to_string(op.req_id));
                    lock_counts[op.lock_id]++;
                    op.active = false;
                    op.phase = FaaPhase::idle;
                    completed++;
                    active--;
                    if (submitted < NUM_OPS_PER_CLIENT) submit_op(slot);
                } else {
                    continue;
                }
                continue;
            }

            if (phase == FaaPhase::notify_successor) {
                stats.notify_cqes++;
                if (op.responses < 1) continue;
                debug(
                    "notify successor completed lock=" + std::to_string(op.lock_id)
                    + " req=" + std::to_string(op.req_id));
                lock_counts[op.lock_id]++;
                op.active = false;
                op.phase = FaaPhase::idle;
                completed++;
                active--;
                if (submitted < NUM_OPS_PER_CLIENT) submit_op(slot);
            }
        }
    }

    if (out_stats) {
        *out_stats = stats;
    }
}
