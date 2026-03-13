#include "rdma/lock.h"
#include "rdma/client.h"
#include "rdma/lock_strategy.h"

#include <stdexcept>

static int next_op_id() {
    thread_local int counter = 0;
    return counter++;
}

Lock::Lock(Client& client, LockStrategy& strategy, const uint32_t lock_id)
    : client_(&client)
    , strategy_(&strategy)
    , lock_id_(lock_id)
    , op_id_(next_op_id())
{}

Lock::~Lock() {
    if (locked_) {
        strategy_->release(*client_, op_id_, lock_id_);
    }
}

void Lock::lock() {
    if (locked_) throw std::runtime_error("Lock already held");
    const uint64_t held = strategy_->acquire(*client_, op_id_, lock_id_);
    auto* state = static_cast<LocalState*>(client_->buffer());
    state->metadata = held;
    locked_ = true;
}

void Lock::unlock() {
    if (!locked_) throw std::runtime_error("Not locked");
    strategy_->release(*client_, op_id_, lock_id_);
    locked_ = false;
}

void Lock::cleanup() const {
    if (locked_) throw std::runtime_error("Cannot cleanup while locked");
    strategy_->cleanup(*client_, op_id_, lock_id_);
}
