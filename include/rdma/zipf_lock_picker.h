#pragma once

#include "rdma/common.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <vector>

class ZipfLockPicker {
public:
    explicit ZipfLockPicker(const double skew, const uint32_t start = 0, const uint32_t count = MAX_LOCKS)
        : skew_(std::max(skew, 0.0))
        , start_(start)
        , count_(count)
        , uniform_(0, count_ - 1) {
        if (count_ == 0 || start_ + count_ > MAX_LOCKS) {
            throw std::runtime_error("ZipfLockPicker: invalid lock range");
        }

        if (skew_ > 0.0) {
            std::vector<double> weights(count_);
            for (size_t i = 0; i < count_; ++i) {
                weights[i] = 1.0 / std::pow(static_cast<double>(i + 1), skew_);
            }
            zipf_ = std::discrete_distribution<uint32_t>(weights.begin(), weights.end());
            use_zipf_ = true;
        }
    }

    uint32_t next() {
        const uint32_t offset = use_zipf_ ? zipf_(rng_) : uniform_(rng_);
        return start_ + offset;
    }

private:
    double skew_;
    uint32_t start_ = 0;
    uint32_t count_ = 0;
    bool use_zipf_ = false;
    std::mt19937 rng_{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> uniform_;
    std::discrete_distribution<uint32_t> zipf_;
};
