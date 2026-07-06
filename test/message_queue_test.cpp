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

constexpr std::size_t slotCount = 32;
constexpr std::size_t slotCapacity = 32;

class MessageQueueTest : public ::testing::Test {
  protected:
    mpsc::MessageQueue<slotCount, slotCapacity> q;
};

} // namespace

TEST_F(MessageQueueTest, Basic) {
    EXPECT_EQ(q.slotCount(), slotCount);
    EXPECT_EQ(q.emptySlots(), slotCount);
    EXPECT_EQ(q.occupiedSlots(), 0);

    std::array<unsigned char, slotCapacity> a;
    std::size_t sz;
    EXPECT_EQ(q.dequeue(a, sz), false);
    EXPECT_EQ(sz, 0);
    EXPECT_EQ(q.enqueue(a), true);
}
