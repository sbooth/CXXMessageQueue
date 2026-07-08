//
// SPDX-FileCopyrightText: 2026 Stephen F. Booth <contact@sbooth.dev>
// SPDX-License-Identifier: MIT
//
// Part of https://github.com/sbooth/CXXMessageQueue
//

#include "mpsc/MessageQueue.hpp"

#include <numeric>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

// Test structural types for Concept validation
struct ValidStruct {
    int a;
    double b;
};

struct InvalidNonStandardLayout {
  public:
    int x;

  private:
    int y; // Mixing access specifiers breaches standard layout rules in older standards
};

// ============================================================================
// 1. Concept & Compile-Time Constraint Verification
// ============================================================================
TEST(MessageQueueStaticTest, ConceptConstraints) {
    // Test ValidPowerOfTwo
    static_assert(mpsc::ValidPowerOfTwo<2>);
    static_assert(mpsc::ValidPowerOfTwo<4>);
    static_assert(mpsc::ValidPowerOfTwo<1024>);
    static_assert(!mpsc::ValidPowerOfTwo<0>);
    static_assert(!mpsc::ValidPowerOfTwo<1>);
    static_assert(!mpsc::ValidPowerOfTwo<3>);
    static_assert(!mpsc::ValidPowerOfTwo<1000>);

    // Test ValueLike
    static_assert(mpsc::ValueLike<int>);
    static_assert(mpsc::ValueLike<ValidStruct>);
    static_assert(!mpsc::ValueLike<int *>);             // Pointers rejected
    static_assert(!mpsc::ValueLike<std::vector<char>>); // Ranges rejected
}

// ============================================================================
// 2. Initial State & Configuration Tests
// ============================================================================
TEST(MessageQueueTest, InitialStateAndAttributes) {
    constexpr std::size_t Slots = 4;
    constexpr std::size_t Capacity = 16;
    mpsc::MessageQueue<Slots, Capacity> queue;

    EXPECT_EQ(queue.slotCount(), Slots);
    EXPECT_EQ(queue.slotCapacity(), Capacity);

    // Consumer-safe checks on empty queue
    EXPECT_TRUE(queue.isEmpty());
    EXPECT_EQ(queue.occupiedSlots(), 0);

    // Producer-safe checks on empty queue
    EXPECT_FALSE(queue.isFull());
    EXPECT_EQ(queue.emptySlots(), Slots);
}

// ============================================================================
// 3. Raw Byte Enqueue / Dequeue / Peek Operations
// ============================================================================
TEST(MessageQueueTest, RawByteLifecycle) {
    mpsc::MessageQueue<4, 8> queue;
    std::vector<unsigned char> sendData = {0xDE, 0xAD, 0xBE, 0xEF};
    std::vector<unsigned char> recvBuffer(8, 0);
    std::size_t bytesWritten = 0;

    // 1. Enqueue
    ASSERT_TRUE(queue.enqueue(sendData));
    EXPECT_FALSE(queue.isEmpty());
    EXPECT_EQ(queue.occupiedSlots(), 1);

    // 2. Peek Verification
    ASSERT_TRUE(queue.peek(recvBuffer, bytesWritten));
    EXPECT_EQ(bytesWritten, sendData.size());
    EXPECT_EQ(std::memcmp(recvBuffer.data(), sendData.data(), bytesWritten), 0);

    // Ensure peek didn't consume anything
    EXPECT_EQ(queue.occupiedSlots(), 1);

    // 3. Dequeue Verification
    std::fill(recvBuffer.begin(), recvBuffer.end(), 0);
    ASSERT_TRUE(queue.dequeue(recvBuffer, bytesWritten));
    EXPECT_EQ(bytesWritten, sendData.size());
    EXPECT_EQ(std::memcmp(recvBuffer.data(), sendData.data(), bytesWritten), 0);

    // Ensure it is empty again
    EXPECT_TRUE(queue.isEmpty());
}

// ============================================================================
// 4. Boundary and Failure Conditions
// ============================================================================
TEST(MessageQueueTest, FailureAndBoundaryCases) {
    mpsc::MessageQueue<2, 4> queue;
    std::size_t written = 0;
    std::array<unsigned char, 8> largeBuffer{};
    std::array<unsigned char, 2> smallBuffer{};
    std::array<unsigned char, 4> validBuffer = {1, 2, 3, 4};

    // Reject messaging exceeding single slot capacity
    EXPECT_FALSE(queue.enqueue(largeBuffer));

    // Reject empty payload spans
    EXPECT_FALSE(queue.enqueue(std::span<const unsigned char>{}));

    // Fill queue to limits
    EXPECT_TRUE(queue.enqueue(std::span<const unsigned char>(validBuffer.data(), 2)));
    EXPECT_TRUE(queue.enqueue(std::span<const unsigned char>(validBuffer.data(), 4)));

    // Queue should now report completely full
    EXPECT_TRUE(queue.isFull());
    EXPECT_EQ(queue.emptySlots(), 0);

    // Reject enqueue on full state
    EXPECT_FALSE(queue.enqueue(std::span<const unsigned char>(validBuffer.data(), 1)));

    // Try dequeuing into an insufficiently sized target buffer
    // API states: false if target buffer size < C (Slot capacity)
    EXPECT_FALSE(queue.dequeue(smallBuffer, written));

    // Dequeue safely with properly sized buffer
    std::array<unsigned char, 4> outputBuffer{};
    EXPECT_TRUE(queue.dequeue(outputBuffer, written));
    EXPECT_EQ(written, 2);
}

// ============================================================================
// 5. Value-Like Serialization (Variadic Arguments)
// ============================================================================
TEST(MessageQueueTest, ValueLikeVariadicAPI) {
    mpsc::MessageQueue<4, 32> queue;

    int inputInt = 42;
    double inputDouble = 3.14159;
    ValidStruct inputStruct{100, 2.718};

    // Serialization
    ASSERT_TRUE(queue.enqueueValues(inputInt, inputDouble, inputStruct));

    // Peek matching values
    int peekInt = 0;
    double peekDouble = 0.0;
    ValidStruct peekStruct{0, 0.0};
    ASSERT_TRUE(queue.peekValues(peekInt, peekDouble, peekStruct));

    EXPECT_EQ(peekInt, inputInt);
    EXPECT_DOUBLE_EQ(peekDouble, inputDouble);
    EXPECT_EQ(peekStruct.a, inputStruct.a);
    EXPECT_DOUBLE_EQ(peekStruct.b, inputStruct.b);

    // Deserialization Dequeue
    int outInt = 0;
    double outDouble = 0.0;
    ValidStruct outStruct{0, 0.0};
    ASSERT_TRUE(queue.dequeueValues(outInt, outDouble, outStruct));

    EXPECT_EQ(outInt, inputInt);
    EXPECT_DOUBLE_EQ(outDouble, inputDouble);
    EXPECT_EQ(outStruct.a, inputStruct.a);
    EXPECT_DOUBLE_EQ(outStruct.b, inputStruct.b);
}

// ============================================================================
// 6. Ring-Buffer Generation Overwrap Test
// ============================================================================
TEST(MessageQueueTest, GenerationWrapAround) {
    // Tests that slot generations advance properly and work beyond multiple rotations
    constexpr std::size_t Slots = 2;
    mpsc::MessageQueue<Slots, 4> queue;
    std::array<unsigned char, 4> token = {7, 7, 7, 7};
    std::array<unsigned char, 4> outToken{};
    std::size_t written = 0;

    // Cycle through slots repeatedly to guarantee generation additions tick upward
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(queue.enqueue(token));
        ASSERT_TRUE(queue.dequeue(outToken, written));
        EXPECT_EQ(written, 4);
        EXPECT_EQ(outToken[0], 7);
    }
}

// ============================================================================
// 7. Multi-Producer, Single-Consumer Concurrency Stress Test
// ============================================================================
TEST(MessageQueueStressTest, ConcurrencyMPSC) {
    constexpr std::size_t Slots = 128;
    constexpr std::size_t Capacity = 8;
    mpsc::MessageQueue<Slots, Capacity> queue;

    constexpr int NumProducers = 4;
    constexpr int MessagesPerProducer = 5000;
    constexpr int TotalMessages = NumProducers * MessagesPerProducer;

    std::atomic<bool> startSignal{false};
    std::atomic<int> activeProducers{NumProducers};

    // Tracks validation on the Consumer side
    std::atomic<int> itemsSuccessfullyRead{0};
    std::uint64_t totalSumReceived = 0;

    // Payload configuration: We store a producer ID alongside a monotonic sequence value
    struct Packet {
        int producerId;
        int sequence;
    };
    static_assert(mpsc::ValueLike<Packet>);

    // 1. Launch Consumer Thread
    std::thread consumerThread([&]() {
        int emptySpins = 0;
        while (activeProducers.load(std::memory_order_relaxed) > 0 || !queue.isEmpty()) {
            Packet pkt{};
            if (queue.dequeueValues(pkt)) {
                totalSumReceived += pkt.sequence;
                itemsSuccessfullyRead.fetch_add(1, std::memory_order_relaxed);
                emptySpins = 0;
            } else {
                // Yield briefly if queue yields a transient empty state
                std::this_thread::yield();
            }
        }
    });

    // 2. Launch Producer Threads
    std::vector<std::thread> producers;
    producers.reserve(NumProducers);
    for (int id = 0; id < NumProducers; ++id) {
        producers.emplace_back([&queue, &startSignal, &activeProducers, id]() {
            while (!startSignal.load()) {
                std::this_thread::yield(); // Synchronized kickoff
            }

            for (int seq = 1; seq <= MessagesPerProducer; ++seq) {
                Packet pkt{id, seq};
                // Keep pushing until the thread successfully claims a slot
                while (!queue.enqueueValues(pkt)) {
                    std::this_thread::yield();
                }
            }
            activeProducers.fetch_sub(1, std::memory_order_release);
        });
    }

    // Fire the starting gun
    startSignal.store(true);

    // Join all production activities
    for (auto &th : producers) {
        if (th.joinable()) {
            th.join();
        }
    }
    // Join consumption activities
    if (consumerThread.joinable()) {
        consumerThread.join();
    }

    // 3. Data Invariant Verification
    EXPECT_EQ(itemsSuccessfullyRead.load(), TotalMessages);

    // Deduce what total mathematical sum of all messages should sequence to:
    // NumProducers * Sum(1 to MessagesPerProducer)
    std::uint64_t expectedSumPerProducer =
            (static_cast<std::uint64_t>(MessagesPerProducer) * (MessagesPerProducer + 1)) / 2;
    std::uint64_t totalExpectedSum = expectedSumPerProducer * NumProducers;

    EXPECT_EQ(totalSumReceived, totalExpectedSum);
}
