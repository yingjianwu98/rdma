#pragma once

// TCP-based RDMA QP exchange - bypasses rdma_cm
// Based on mpmc_rdma approach that works on Emulab nodes

#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>

struct QpExchangeInfo {
    uint32_t qp_num;
    uint16_t lid;
    uint8_t gid[16];
    uintptr_t remote_addr;
    uint32_t rkey;
    uint32_t node_id;
    uint8_t conn_type;
} __attribute__((packed));

// Detect GID index: 0 for RoCE, -1 for native InfiniBand
static inline int detect_gid_index(ibv_context* ctx, int port) {
    ibv_port_attr pa;
    if (ibv_query_port(ctx, port, &pa)) return -1;
    return (pa.link_layer == IBV_LINK_LAYER_ETHERNET) ? 0 : -1;
}

// Exchange QP info over TCP
static inline bool tcp_exchange_qp_info(int sock, const QpExchangeInfo* local, QpExchangeInfo* remote) {
    if (write(sock, local, sizeof(*local)) != static_cast<ssize_t>(sizeof(*local))) {
        return false;
    }
    if (read(sock, remote, sizeof(*remote)) != static_cast<ssize_t>(sizeof(*remote))) {
        return false;
    }
    return true;
}

// QP state transitions
static inline void qp_to_init(ibv_qp* qp, int ib_port) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(qp, &attr, flags)) {
        throw std::runtime_error("Failed to transition QP to INIT");
    }
}

static inline void qp_to_rtr(ibv_qp* qp, int ib_port, int gid_index, const QpExchangeInfo* remote) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remote->qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;

    attr.ah_attr.dlid = remote->lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    if (gid_index >= 0) {
        std::memcpy(&attr.ah_attr.grh.dgid, remote->gid, 16);
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 64;
        attr.ah_attr.grh.sgid_index = gid_index;
    } else {
        attr.ah_attr.is_global = 0;
    }

    if (ibv_modify_qp(qp, &attr, flags)) {
        throw std::runtime_error("Failed to transition QP to RTR");
    }
}

static inline void qp_to_rts(ibv_qp* qp) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;

    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    if (ibv_modify_qp(qp, &attr, flags)) {
        throw std::runtime_error("Failed to transition QP to RTS");
    }
}

// Get local QP info for exchange
static inline QpExchangeInfo get_local_qp_info(
    ibv_qp* qp, ibv_context* ctx, int ib_port, int gid_index,
    uintptr_t addr, uint32_t rkey, uint32_t node_id, uint8_t conn_type
) {
    QpExchangeInfo info{};
    info.qp_num = qp->qp_num;
    info.remote_addr = addr;
    info.rkey = rkey;
    info.node_id = node_id;
    info.conn_type = conn_type;

    ibv_port_attr port_attr;
    if (ibv_query_port(ctx, ib_port, &port_attr)) {
        throw std::runtime_error("Failed to query port");
    }
    info.lid = port_attr.lid;

    if (gid_index >= 0) {
        ibv_gid gid;
        if (ibv_query_gid(ctx, ib_port, gid_index, &gid)) {
            throw std::runtime_error("Failed to query GID");
        }
        std::memcpy(info.gid, &gid, 16);
    }

    return info;
}
