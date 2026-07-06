//
// SPDX-FileCopyrightText: 2026 Stephen F. Booth <contact@sbooth.dev>
// SPDX-License-Identifier: MIT
//
// Part of https://github.com/sbooth/CXXMessageQueue
//

#include "mpsc/MessageQueue.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <numeric>
#include <random>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace {

constexpr std::size_t KB = 1024;
constexpr std::size_t MB = 1024 * KB;
constexpr std::size_t GB = 1024 * MB;

class MessageQueueTest : public ::testing::Test {
  protected:
    mpsc::MessageQueue<1024> q;
};

} // namespace

TEST_F(MessageQueueTest, Empty) {
    EXPECT_EQ(q.slotCount(), 0);
    EXPECT_EQ(q.emptySlots(), 0);
    EXPECT_EQ(q.occupiedSlots(), 0);

    std::array<unsigned char, 1024> a;
    std::size_t sz;
    EXPECT_EQ(q.read(a, sz), false);
    EXPECT_EQ(sz, 0);
    EXPECT_EQ(q.write(a), false);
}
