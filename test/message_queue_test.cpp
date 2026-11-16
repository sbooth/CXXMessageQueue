//
// SPDX-FileCopyrightText: 2026 Stephen F. Booth <contact@sbooth.dev>
// SPDX-License-Identifier: MIT
//
// Part of https://github.com/sbooth/CXXMessageQueue
//

#include "mpsc/MessageQueue.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mpsc::test {

// Test structural types for Concept validation
struct ValidStruct {
    int a;
    double b;
};

struct InvalidNonStandardLayout {
  public:
    int x;

  private:
    [[maybe_unused]] int y; // Mixing access specifiers breaks standard layout
};

// ============================================================================
// 1. Concept & Compile-Time Constraint Verification
// ============================================================================
TEST(MessageQueueStaticTest, ConceptConstraints) {
    // Test ValidPowerOfTwo
    static_assert(ValidPowerOfTwo<2>);
    static_assert(ValidPowerOfTwo<4>);
    static_assert(ValidPowerOfTwo<1024>);
    static_assert(!ValidPowerOfTwo<0>);
    static_assert(!ValidPowerOfTwo<1>);
    static_assert(!ValidPowerOfTwo<3>);
    static_assert(!ValidPowerOfTwo<1000>);

    // Test ValueLike
    static_assert(ValueLike<int>);
    static_assert(ValueLike<ValidStruct>);
    static_assert(!ValueLike<int *>);             // Pointers rejected
    static_assert(!ValueLike<InvalidNonStandardLayout>);
    static_assert(!ValueLike<std::vector<char>>); // Ranges rejected
}

// ============================================================================
// 2. Initial State & Configuration Tests
// ============================================================================
TEST(MessageQueueTest, InitialStateAndAttributes) {
    constexpr std::size_t Slots = 4;
    constexpr std::size_t Capacity = 16;
    MessageQueue<Slots, Capacity> queue;

    EXPECT_EQ(decltype(queue)::slotCount, Slots);
    EXPECT_EQ(decltype(queue)::slotCapacity, Capacity);

    // Consumer-safe checks on empty queue
    EXPECT_TRUE(queue.isEmpty());
    EXPECT_EQ(queue.occupiedSlots(), 0U);

    // Producer-safe checks on empty queue
    EXPECT_FALSE(queue.isFull());
    EXPECT_EQ(queue.emptySlots(), Slots);
}

// ============================================================================
// 3. Raw Byte Enqueue / Dequeue / Peek Operations
// ============================================================================
TEST(MessageQueueTest, RawByteLifecycle) {
    MessageQueue<4, 8> queue;
    std::vector<unsigned char> sendData = {0xDE, 0xAD, 0xBE, 0xEF};
    std::vector<unsigned char> recvBuffer(8, 0);
    std::size_t bytesWritten = 0;

    // 1. Enqueue
    ASSERT_TRUE(queue.enqueue(sendData));
    EXPECT_FALSE(queue.isEmpty());
    EXPECT_EQ(queue.occupiedSlots(), 1U);

    // 2. Peek Verification
    ASSERT_TRUE(queue.peek(recvBuffer, bytesWritten));
    EXPECT_EQ(bytesWritten, sendData.size());
    EXPECT_EQ(std::memcmp(recvBuffer.data(), sendData.data(), bytesWritten), 0);

    // Ensure peek didn't consume anything
    EXPECT_EQ(queue.occupiedSlots(), 1U);

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
    MessageQueue<2, 4> queue;
    std::size_t written = 0;
    std::array<unsigned char, 8> largeBuffer{};
    std::array<unsigned char, 1> smallBuffer{};
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
    EXPECT_EQ(queue.emptySlots(), 0U);

    // Reject enqueue on full state
    EXPECT_FALSE(queue.enqueue(std::span<const unsigned char>(validBuffer.data(), 1)));

    // Try dequeuing into an insufficiently sized target buffer
    EXPECT_FALSE(queue.dequeue(smallBuffer, written));

    // Dequeue safely with properly sized buffer
    std::array<unsigned char, 4> outputBuffer{};
    EXPECT_TRUE(queue.dequeue(outputBuffer, written));
    EXPECT_EQ(written, 2U);
}

// ============================================================================
// 5. Value-Like Serialization (Variadic Arguments)
// ============================================================================
TEST(MessageQueueTest, ValueLikeVariadicAPI) {
    MessageQueue<4, 32> queue;

    int inputInt = 42;
    double inputDouble = 3.14159;
    ValidStruct inputStruct{100, 2.718};

    // Serialization
    ASSERT_TRUE(queue.enqueue(inputInt, inputDouble, inputStruct));

    // Peek matching values
    int peekInt = 0;
    double peekDouble = 0.0;
    ValidStruct peekStruct{0, 0.0};
    ASSERT_TRUE(queue.peek(peekInt, peekDouble, peekStruct));

    EXPECT_EQ(peekInt, inputInt);
    EXPECT_DOUBLE_EQ(peekDouble, inputDouble);
    EXPECT_EQ(peekStruct.a, inputStruct.a);
    EXPECT_DOUBLE_EQ(peekStruct.b, inputStruct.b);

    // Deserialization Dequeue
    int outInt = 0;
    double outDouble = 0.0;
    ValidStruct outStruct{0, 0.0};
    ASSERT_TRUE(queue.dequeue(outInt, outDouble, outStruct));

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
    MessageQueue<Slots, 4> queue;
    std::array<unsigned char, 4> token = {7, 7, 7, 7};
    std::array<unsigned char, 4> outToken{};
    std::size_t written = 0;

    // Cycle through slots repeatedly to guarantee generation additions tick upward
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(queue.enqueue(token));
        ASSERT_TRUE(queue.dequeue(outToken, written));
        EXPECT_EQ(written, 4U);
        EXPECT_EQ(outToken[0], 7);
    }
}

// ============================================================================
// 7. Multi-Producer, Single-Consumer Concurrency Stress Test
// ============================================================================
TEST(MessageQueueStressTest, ConcurrencyMPSC) {
    constexpr std::size_t Slots = 128;
    constexpr std::size_t Capacity = 8;
    MessageQueue<Slots, Capacity> queue;

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
    static_assert(ValueLike<Packet>);

    // 1. Launch Consumer Thread
    // Messages from one producer must arrive in the order that producer enqueued them
    std::array<int, NumProducers> lastSequence{};
    bool orderingViolated = false;

    std::thread consumerThread([&]() {
        while (activeProducers.load(std::memory_order_relaxed) > 0 || !queue.isEmpty()) {
            Packet pkt{};
            if (queue.dequeue(pkt)) {
                totalSumReceived += pkt.sequence;
                itemsSuccessfullyRead.fetch_add(1, std::memory_order_relaxed);
                if (pkt.producerId < 0 || pkt.producerId >= NumProducers ||
                    pkt.sequence != lastSequence[pkt.producerId] + 1) {
                    orderingViolated = true;
                } else {
                    lastSequence[pkt.producerId] = pkt.sequence;
                }
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
                while (!queue.enqueue(pkt)) {
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
    EXPECT_FALSE(orderingViolated);

    // Deduce what total mathematical sum of all messages should sequence to:
    // NumProducers * Sum(1 to MessagesPerProducer)
    std::uint64_t expectedSumPerProducer =
            (static_cast<std::uint64_t>(MessagesPerProducer) * (MessagesPerProducer + 1)) / 2;
    std::uint64_t totalExpectedSum = expectedSumPerProducer * NumProducers;

    EXPECT_EQ(totalSumReceived, totalExpectedSum);
}

// ============================================================================
// 8. Partial Peek and Size Mismatches
// ============================================================================
TEST(MessageQueueTest, PeekPrefixAndSizeMismatch) {
    MessageQueue<4, 16> queue;
    const std::uint32_t header = 0xCAFEF00D;
    const std::uint64_t body = 0x0123456789ABCDEF;
    ASSERT_TRUE(queue.enqueue(header, body));

    // A raw peek into a small buffer copies a prefix
    std::array<unsigned char, 2> small{};
    std::size_t written = 0;
    ASSERT_TRUE(queue.peek(small, written));
    EXPECT_EQ(written, small.size());
    EXPECT_EQ(std::memcmp(small.data(), &header, small.size()), 0);

    // A value peek may read a prefix of the message
    std::uint32_t peekedHeader = 0;
    ASSERT_TRUE(queue.peek(peekedHeader));
    EXPECT_EQ(peekedHeader, header);

    // A value dequeue requires an exact size match and leaves the message in place otherwise
    std::uint32_t wrong = 0;
    EXPECT_FALSE(queue.dequeue(wrong));
    EXPECT_EQ(queue.occupiedSlots(), 1U);

    std::uint32_t outHeader = 0;
    std::uint64_t outBody = 0;
    ASSERT_TRUE(queue.dequeue(outHeader, outBody));
    EXPECT_EQ(outHeader, header);
    EXPECT_EQ(outBody, body);

    // Nothing left to peek
    EXPECT_FALSE(queue.peek(peekedHeader));
    EXPECT_FALSE(queue.peek(small, written));
    EXPECT_EQ(written, 0U);
}

// ============================================================================
// 9. Discard and Drain
// ============================================================================
TEST(MessageQueueTest, DiscardAndDrain) {
    MessageQueue<8, 4> queue;

    EXPECT_EQ(queue.discard(), 0U);
    EXPECT_EQ(queue.drain(), 0U);

    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(queue.enqueue(i));
    }

    EXPECT_EQ(queue.discard(), 1U);
    EXPECT_EQ(queue.discard(2), 2U);
    EXPECT_EQ(queue.discard(0), 0U);

    int value = -1;
    ASSERT_TRUE(queue.dequeue(value));
    EXPECT_EQ(value, 3);

    // Asking for more than is available discards what is there
    EXPECT_EQ(queue.discard(100), 2U);
    EXPECT_TRUE(queue.isEmpty());

    // Drain a full queue that straddles the end of the slot array
    for (int i = 0; i < 8; ++i) {
        ASSERT_TRUE(queue.enqueue(i));
    }
    EXPECT_TRUE(queue.isFull());
    EXPECT_EQ(queue.drain(), 8U);
    EXPECT_TRUE(queue.isEmpty());
    EXPECT_EQ(queue.emptySlots(), 8U);

    // The queue remains usable
    ASSERT_TRUE(queue.enqueue(42));
    ASSERT_TRUE(queue.dequeue(value));
    EXPECT_EQ(value, 42);
}

// ============================================================================
// 10. Drain Terminates While Producers Are Active
// ============================================================================
TEST(MessageQueueStressTest, DrainIsBounded) {
    constexpr std::size_t Slots = 64;
    auto queue = std::make_unique<MessageQueue<Slots, 8>>();
    std::atomic<bool> stop{false};

    std::vector<std::thread> producers;
    for (int id = 0; id < 2; ++id) {
        producers.emplace_back([&] {
            std::uint64_t value = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                if (!queue->enqueue(value)) {
                    std::this_thread::yield();
                }
                ++value;
            }
        });
    }

    // Each call must return after visiting at most one queue's worth of slots
    // even though the producers never stop refilling the queue.
    for (int i = 0; i < 2000; ++i) {
        EXPECT_LE(queue->drain(), Slots);
    }

    stop.store(true);
    for (auto &th : producers) {
        th.join();
    }
    EXPECT_LE(queue->drain(), Slots);
    EXPECT_TRUE(queue->isEmpty());
}

// ============================================================================
// 11. Variable-Size Payload Integrity Under Contention
// ============================================================================
TEST(MessageQueueStressTest, VariableSizePayloadIntegrity) {
    constexpr std::size_t Capacity = 48;
    // A small queue keeps it full most of the time, exercising the full and wrap-around paths
    auto queue = std::make_unique<MessageQueue<8, Capacity>>();

    constexpr int NumProducers = 3;
    constexpr int MessagesPerProducer = 20000;

    std::vector<std::thread> producers;
    for (int id = 0; id < NumProducers; ++id) {
        producers.emplace_back([&queue, id] {
            std::array<unsigned char, Capacity> message{};
            for (int seq = 0; seq < MessagesPerProducer; ++seq) {
                // Layout: [producer id][sequence (4 bytes)][fill bytes derived from the sequence]
                const std::size_t size = 5 + static_cast<std::size_t>(seq % (Capacity - 4));
                message[0] = static_cast<unsigned char>(id);
                std::memcpy(&message[1], &seq, sizeof seq);
                for (std::size_t i = 5; i < size; ++i) {
                    message[i] = static_cast<unsigned char>(seq + static_cast<int>(i));
                }
                while (!queue->enqueue(std::span<const unsigned char>{message.data(), size})) {
                    std::this_thread::yield();
                }
            }
        });
    }

    std::array<int, NumProducers> nextSequence{};
    std::array<unsigned char, Capacity> buffer{};
    int received = 0;
    int corrupted = 0;
    while (received < NumProducers * MessagesPerProducer) {
        std::size_t size = 0;
        if (!queue->dequeue(buffer, size)) {
            std::this_thread::yield();
            continue;
        }
        ++received;

        const int id = buffer[0];
        int seq = -1;
        std::memcpy(&seq, &buffer[1], sizeof seq);
        bool ok = id < NumProducers && seq == nextSequence[static_cast<std::size_t>(id < NumProducers ? id : 0)] &&
                  size == 5 + static_cast<std::size_t>(seq % (Capacity - 4));
        for (std::size_t i = 5; ok && i < size; ++i) {
            ok = buffer[i] == static_cast<unsigned char>(seq + static_cast<int>(i));
        }
        if (ok) {
            ++nextSequence[static_cast<std::size_t>(id)];
        } else {
            ++corrupted;
        }
    }

    for (auto &th : producers) {
        th.join();
    }

    EXPECT_EQ(corrupted, 0);
    EXPECT_TRUE(queue->isEmpty());
}

// ============================================================================
// 12. Statistics Stay In Range Under Concurrency
// ============================================================================
TEST(MessageQueueStressTest, StatisticsFromConsumerAreExact) {
    // Only the consumer advances the read position, so statistics sampled on the consumer's
    // thread can never report more messages than have been enqueued but not yet dequeued.
    auto queue = std::make_unique<MessageQueue<1024, 8>>();
    constexpr std::uint64_t Messages = 200000;
    std::atomic<std::uint64_t> sent{0};

    std::thread producer([&] {
        for (std::uint64_t i = 0; i < Messages; ++i) {
            // Count the message before enqueuing so `sent` never lags the queue
            sent.fetch_add(1, std::memory_order_seq_cst);
            while (!queue->enqueue(i)) {
                std::this_thread::yield();
            }
        }
    });

    std::uint64_t received = 0;
    bool outOfRange = false;
    while (received < Messages) {
        const auto occupied = queue->occupiedSlots();
        const auto empty = queue->emptySlots();
        // Load `sent` after sampling: it can only have grown since
        const auto upperBound = sent.load(std::memory_order_seq_cst) - received;
        if (occupied > upperBound || occupied > 1024 || empty > 1024) {
            outOfRange = true;
        }
        std::uint64_t value = 0;
        if (queue->dequeue(value)) {
            if (value != received) {
                outOfRange = true;
            }
            ++received;
        }
    }
    producer.join();

    EXPECT_FALSE(outOfRange);
    EXPECT_TRUE(queue->isEmpty());
    EXPECT_FALSE(queue->isFull());
    EXPECT_EQ(queue->emptySlots(), 1024U);
}

// ============================================================================
// 13. Statistics Sampled From A Third Thread While Workers Run
// ============================================================================
TEST(MessageQueueStressTest, StatisticsFromObserverStayWithinCapacity) {
    // The producer waits for each message to be consumed before sending the next, so the
    // queue never holds more than one message. An observer racing with both sides still
    // can't require that tighter bound: the statistics are estimates, and if the observer
    // is paused between its position loads after exhausting its retries the estimate also
    // counts the message cycles completed in the meantime. Only the documented capacity
    // bounds hold under every schedule.
    constexpr std::size_t Slots = 1024;
    auto queue = std::make_unique<MessageQueue<Slots, 8>>();
    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> acknowledged{0};

    std::thread consumer([&] {
        std::uint64_t value = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            if (queue->dequeue(value)) {
                acknowledged.fetch_add(1);
            }
        }
    });
    std::thread producer([&] {
        std::uint64_t sent = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            if (acknowledged.load() == sent && queue->enqueue(sent)) {
                ++sent;
            }
        }
    });

    std::size_t maxOccupied = 0;
    std::size_t maxEmpty = 0;
    for (int i = 0; i < 2000000; ++i) {
        maxOccupied = std::max(maxOccupied, queue->occupiedSlots());
        maxEmpty = std::max(maxEmpty, queue->emptySlots());
    }

    stop.store(true);
    consumer.join();
    producer.join();

    EXPECT_LE(maxOccupied, Slots);
    EXPECT_LE(maxEmpty, Slots);

    // With both workers joined the statistics are exact: the producer may have
    // enqueued one final message that the consumer did not get to.
    const auto remaining = queue->occupiedSlots();
    EXPECT_LE(remaining, 1U);
    EXPECT_EQ(queue->emptySlots(), Slots - remaining);
    EXPECT_EQ(queue->isEmpty(), remaining == 0);
    EXPECT_FALSE(queue->isFull());
}

// ============================================================================
// 14. Statistics Sampled From A Third Thread While Workers Are Parked
// ============================================================================
TEST(MessageQueueStressTest, StatisticsFromObserverAreExactWhenQuiescent) {
    // The observer drives the workers in lockstep and samples only while both are parked.
    // Each worker acknowledges its work through a sequentially consistent counter after
    // touching the queue, so the observer is guaranteed to see the resulting positions and
    // the statistics must be exact.
    constexpr std::size_t Slots = 8;
    constexpr std::uint64_t Rounds = 4000;
    auto queue = std::make_unique<MessageQueue<Slots, 8>>();

    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> produceRequested{0};
    std::atomic<std::uint64_t> produced{0};
    std::atomic<std::uint64_t> consumeRequested{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<bool> dataMismatch{false};

    std::thread producer([&] {
        std::uint64_t sent = 0;
        while (!stop.load()) {
            if (sent == produceRequested.load()) {
                std::this_thread::yield();
                continue;
            }
            // The observer never requests more messages than there are empty slots
            while (!queue->enqueue(sent)) {
                std::this_thread::yield();
            }
            ++sent;
            produced.store(sent);
        }
    });
    std::thread consumer([&] {
        std::uint64_t received = 0;
        while (!stop.load()) {
            if (received == consumeRequested.load()) {
                std::this_thread::yield();
                continue;
            }
            std::uint64_t value = 0;
            while (!queue->dequeue(value)) {
                std::this_thread::yield();
            }
            if (value != received) {
                dataMismatch.store(true);
            }
            ++received;
            consumed.store(received);
        }
    });

    const auto waitFor = [](const std::atomic<std::uint64_t> &counter, std::uint64_t target) {
        while (counter.load() != target) {
            std::this_thread::yield();
        }
    };

    std::uint64_t totalProduced = 0;
    std::uint64_t totalConsumed = 0;
    int mismatches = 0;
    const auto check = [&](std::size_t expected) {
        if (queue->occupiedSlots() != expected || queue->emptySlots() != Slots - expected ||
            queue->isEmpty() != (expected == 0) || queue->isFull() != (expected == Slots)) {
            ++mismatches;
        }
    };

    for (std::uint64_t round = 0; round < Rounds; ++round) {
        // Cycle through every fill level, including full; the positions
        // advance by a different amount each round and wrap the slot array
        const std::size_t batch = 1 + static_cast<std::size_t>(round % Slots);

        totalProduced += batch;
        produceRequested.store(totalProduced);
        waitFor(produced, totalProduced);
        check(batch);

        // Consume part of the batch, then the rest
        const std::size_t firstHalf = batch / 2;
        totalConsumed += firstHalf;
        consumeRequested.store(totalConsumed);
        waitFor(consumed, totalConsumed);
        check(batch - firstHalf);

        totalConsumed += batch - firstHalf;
        consumeRequested.store(totalConsumed);
        waitFor(consumed, totalConsumed);
        check(0);
    }

    stop.store(true);
    producer.join();
    consumer.join();

    EXPECT_EQ(mismatches, 0);
    EXPECT_FALSE(dataMismatch.load());
    EXPECT_TRUE(queue->isEmpty());
}

} // namespace mpsc::test
