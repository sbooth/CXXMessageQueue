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

#include <benchmark/benchmark.h>

namespace mpsc::bench {

// Dummy POD type for value serialization benchmarks
struct SampleEvent {
    std::uint64_t timestamp;
    std::uint32_t componentId;
    std::uint32_t errorCode;
};
static_assert(ValueLike<SampleEvent>);

// ============================================================================
// 1. Single-Threaded Raw Performance (Baseline)
// ============================================================================
// Measures the pure overhead of the enqueue/dequeue logic without contention.
static void BM_SingleThreaded_RawBytes(benchmark::State &state) {
    // 1024 slots, 64-byte slot capacity
    mpsc::MessageQueue<1024, 64> queue;

    std::array<unsigned char, 16> writeBuffer;
    writeBuffer.fill(0xAA);
    std::array<unsigned char, 64> readBuffer;
    std::size_t bytesWritten = 0;

    for (auto _ : state) {
        // Enqueue and immediately dequeue to prevent filling up the queue
        queue.enqueue(writeBuffer);
        queue.dequeue(readBuffer, bytesWritten);
    }

    // Track total processed items per second
    state.SetItemsProcessed(state.iterations() * 2);
}
BENCHMARK(BM_SingleThreaded_RawBytes);

static void BM_SingleThreaded_Values(benchmark::State &state) {
    mpsc::MessageQueue<1024, 64> queue;
    SampleEvent event{123456789ULL, 42, 0};

    for (auto _ : state) {
        queue.enqueue(event);
        queue.dequeue(event);
    }
    state.SetItemsProcessed(state.iterations() * 2);
}
BENCHMARK(BM_SingleThreaded_Values);

// ============================================================================
// 2. Multi-Threaded MPSC Contention & Throughput
// ============================================================================
// This test evaluates scalability by varying the number of producer threads.
// Google Benchmark handles the thread lifetime and barriers via state.thread_index().

template <std::size_t Slots, std::size_t Capacity> class MpscFixture : public benchmark::Fixture {
  public:
    // Make these static or carefully managed so they don't race during concurrent SetUp/TearDown
    std::unique_ptr<mpsc::MessageQueue<Slots, Capacity>> queue;
    std::unique_ptr<std::thread> consumerThread;
    std::atomic<bool> running{false};

    void SetUp(const ::benchmark::State &state) override {
        // Only the first thread performs the initialization
        if (state.thread_index() == 0) {
            queue = std::make_unique<mpsc::MessageQueue<Slots, Capacity>>();
            running.store(true, std::memory_order_relaxed);

            consumerThread = std::make_unique<std::thread>([this]() {
                SampleEvent ev{};
                while (running.load(std::memory_order_relaxed)) {
                    if (!queue->dequeue(ev)) {
                        std::this_thread::yield();
                    }
                }
            });
        }
    }

    void TearDown(const ::benchmark::State &state) override {
        // Only the first thread performs the cleanup
        if (state.thread_index() == 0) {
            running.store(false, std::memory_order_relaxed);
            if (consumerThread && consumerThread->joinable()) {
                consumerThread->join();
            }
            consumerThread.reset();
            queue.reset();
        }
    }
};

// Define the benchmark utilizing our MPSC Fixture
BENCHMARK_TEMPLATE_DEFINE_F(MpscFixture, BM_MpscThroughput, 4096, 64)(benchmark::State &state) {
    SampleEvent event{987654321ULL, 7, 1};

    // Every thread defined in the `.Threads()` argument below executes this loop concurrently
    for (auto _ : state) {
        // Spin-retry if the queue gets full under heavy thread pressure
        while (!queue->enqueue(event)) {
            std::this_thread::yield();
        }
    }

    // Report items processed per individual producer thread
    state.SetItemsProcessed(state.iterations());
}

// Register the benchmark to run with 1, 2, 4, and 8 producer threads
BENCHMARK_REGISTER_F(MpscFixture, BM_MpscThroughput)
        ->Threads(1)
        ->Threads(2)
        ->Threads(4)
        ->Threads(8)
        ->Unit(benchmark::kMicrosecond);

} // namespace mpsc::bench

// Standard Google Benchmark main macro
BENCHMARK_MAIN();
