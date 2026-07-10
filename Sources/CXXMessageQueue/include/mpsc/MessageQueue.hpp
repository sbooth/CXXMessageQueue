//
// SPDX-FileCopyrightText: 2026 Stephen F. Booth <contact@sbooth.dev>
// SPDX-License-Identifier: MIT
//
// Part of https://github.com/sbooth/CXXMessageQueue
//

#ifndef MPSC_MESSAGE_QUEUE_HPP
#define MPSC_MESSAGE_QUEUE_HPP

#include <algorithm>
#include <atomic>
#include <bit>
#include <concepts>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

namespace mpsc {

template <std::size_t N>
concept ValidPowerOfTwo = (N >= 2) && std::has_single_bit(N);

template <typename T>
concept ByteCopyable =
        std::is_object_v<std::remove_cvref_t<T>> && std::is_trivially_copyable_v<std::remove_cvref_t<T>> &&
        std::is_standard_layout_v<std::remove_cvref_t<T>> && !std::is_pointer_v<std::remove_cvref_t<T>>;

template <typename T>
concept ValueLike = ByteCopyable<T> && !std::ranges::range<std::remove_cvref_t<T>>;

template <typename F>
concept Writer =
        std::invocable<F, std::span<unsigned char>> && std::is_nothrow_invocable_v<F, std::span<unsigned char>> &&
        requires(F &&func, std::span<unsigned char> data) {
            { std::invoke(std::forward<F>(func), data) } -> std::same_as<std::size_t>;
        };

template <typename F>
concept Reader = std::invocable<F, std::span<const unsigned char>> &&
                 std::is_nothrow_invocable_v<F, std::span<const unsigned char>> &&
                 requires(F &&func, std::span<const unsigned char> data) {
                     { std::invoke(std::forward<F>(func), data) } -> std::same_as<bool>;
                 };

/// A lock-free MPSC message queue.
///
/// This class is thread safe when used with multiple producers and a single consumer.
///
/// The message queue contains a fixed number of slots (N), each with a fixed byte capacity (C).
/// The amount of data that can be written or read in one operation is limited by the size of a single slot.
/// Each write occupies a single slot regardless of size, and similarly reads occur from a single slot only.
///
/// This message queue performs raw byte copies; it does not provide serialization.
template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
class MessageQueue final {
  public:
    /// Unsigned integer type.
    using SizeType = std::size_t;
    /// Atomic unsigned integer type.
    using AtomicSizeType = std::atomic<SizeType>;

    // MARK: Construction and Destruction

    /// Creates an empty message queue.
    constexpr MessageQueue() noexcept;

    MessageQueue(const MessageQueue &) = delete;
    MessageQueue &operator=(const MessageQueue &) = delete;
    MessageQueue(MessageQueue &&) noexcept = delete;
    MessageQueue &operator=(MessageQueue &&) noexcept = delete;

    /// Destroys the message queue and releases all associated resources.
    ~MessageQueue() noexcept = default;

    // MARK: Buffer Information

    /// The number of slots in the message queue.
    static constexpr auto slotCount = N;

    /// The capacity of a single slot in the message queue in bytes.
    static constexpr auto slotCapacity = C;

    // MARK: Buffer Usage

    /// Returns the number of empty slots in the message queue.
    /// @note The result of this method is only valid when called from a producer.
    /// @note The returned value is a transient snapshot and may become stale immediately after return.
    /// @return The number of empty slots available for messages.
    [[nodiscard]] SizeType emptySlots() const noexcept [[clang::nonblocking]];

    /// Returns true if the message queue is full.
    /// @note The result of this method is only valid when called from a producer.
    /// @note The returned value is a transient snapshot and may become stale immediately after return.
    /// @return true if the all slots in the queue are occupied.
    [[nodiscard]] bool isFull() const noexcept [[clang::nonblocking]];

    /// Returns the number of occupied slots in the message queue.
    /// @note The result of this method is only accurate when called from the consumer.
    /// @return The number of occupied slots containing messages.
    [[nodiscard]] SizeType occupiedSlots() const noexcept [[clang::nonblocking]];

    /// Returns true if the message queue is empty.
    /// @note The result of this method is only accurate when called from the consumer.
    /// @return true if all slots in the queue are empty.
    [[nodiscard]] bool isEmpty() const noexcept [[clang::nonblocking]];

    // MARK: Enqueuing Messages

    /// Enqueues a message in the next available slot and advances the write position.
    /// @note This method is only safe to call from a producer.
    /// @param message A span containing the message data to copy.
    /// @return true if the message was successfully enqueued, false if the queue is full or the slot capacity is
    /// insufficient.
    bool enqueue(std::span<const unsigned char> message) noexcept [[clang::nonblocking]];

    /// Enqueues message values in the next available slot and advances the write position.
    /// @note This method is only safe to call from a producer.
    /// @tparam Args The types to enqueue.
    /// @param args The message values to enqueue.
    /// @return true if the message values were successfully enqueued, false if the queue is full or the slot capacity
    /// is insufficient.
    template <ValueLike... Args>
        requires(sizeof...(Args) > 0)
    bool enqueue(const Args &...args) noexcept [[clang::nonblocking]];

    // MARK: Dequeuing Messages

    /// Dequeues a message from the first occupied slot and advances the read position.
    /// @note This method is only safe to call from the consumer.
    /// @param buffer A span to receive the copied message data.
    /// @param written On return, the number of bytes copied from the queue to the buffer.
    /// @return true if a message was successfully dequeued, false if the queue is empty or the buffer capacity is
    /// insufficient.
    bool dequeue(std::span<unsigned char> buffer, SizeType &written) noexcept [[clang::nonblocking]];

    /// Dequeues message values from the first occupied slot and advances the read position.
    /// @note This method is only safe to call from the consumer.
    /// @tparam Args The types to dequeue.
    /// @param args On return, the message values copied from the queue.
    /// @return true if the message values were successfully copied, false if the queue is empty or the slot contains
    /// insufficient data.
    template <ValueLike... Args>
        requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
    bool dequeue(Args &...args) noexcept [[clang::nonblocking]];

    // MARK: Peeking

    /// Copies a message or portion of a message from the first occupied slot without advancing the read position.
    /// @note This method is only safe to call from the consumer.
    /// @param buffer A span to receive the copied message data.
    /// @param written On return, the number of bytes copied from the queue to the buffer.
    /// @return true if any message data was successfully copied, false if the message queue is empty.
    [[nodiscard]] bool peek(std::span<unsigned char> buffer, SizeType &written) const noexcept [[clang::nonblocking]];

    /// Copies message values from the first occupied slot without advancing the read position.
    /// @note This method is only safe to call from the consumer.
    /// @tparam Args The types to copy.
    /// @param args On return, the message values copied from the queue.
    /// @return true if the message values were successfully copied, false if the message queue is empty or the slot
    /// contains insufficient data.
    template <ValueLike... Args>
        requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
    [[nodiscard]] bool peek(Args &...args) const noexcept [[clang::nonblocking]];

    // MARK: Discarding Messages

    /// Discards messages without copying and advances the read position.
    /// @note This method is only safe to call from the consumer.
    /// @param count The maximum number of messages to discard.
    /// @return The number of messages actually discarded.
    [[nodiscard]] SizeType discard(SizeType count = 1) noexcept [[clang::nonblocking]];

    /// Discards all messages without copying and advances the read position.
    /// @note This method is only safe to call from the consumer.
    /// @return The number of messages discarded.
    [[nodiscard]] SizeType drain() noexcept [[clang::nonblocking]];

  private:
    /// A message queue slot.
    struct Slot {
        /// The slot's generation.
        AtomicSizeType generation_{0};
        /// The number of valid bytes in data_
        SizeType dataSize_{0};
        /// The slot data.
        unsigned char data_[C];
    };

    /// The message queue slots.
    Slot slots_[N];

    /// The number of slots in slots_ minus one.
    static constexpr auto slotCountMask_ = N - 1;

    /// The free-running write location.
    AtomicSizeType writePosition_{0};
    /// The free-running read location.
    AtomicSizeType readPosition_{0};

    static_assert(AtomicSizeType::is_always_lock_free, "Lock-free AtomicSizeType required");

    // MARK: Helpers

    /// Claims a writable slot if available and invokes a callable to write data.
    /// @tparam W The type of the callable object.
    /// @param writer A callable performing the write.
    /// @return true if a writable slot was claimed.
    template <Writer W> bool withWritableSlot(W &&writer) noexcept;

    /// Invokes a callable with data from the readable slot.
    /// @tparam R The type of the callable object.
    /// @param reader A callable performing the read.
    /// @param readPos The read position of the slot providing the data.
    /// @return true if data was provided and the callable returned true.
    template <Reader R> bool withReadableSlot(R &&reader, SizeType &readPos) const noexcept;

    /// Invokes a callable with data from the readable slot and advances the read position.
    /// @tparam R The type of the callable object.
    /// @param reader A callable performing the read.
    /// @return true if data was provided and the callable returned true.
    template <Reader R> bool consumeReadableSlot(R &&reader) noexcept;

    /// Invokes a callable with data from the readable slot without advancing the read position.
    /// @tparam R The type of the callable object.
    /// @param reader A callable performing the read.
    /// @return true if data was provided and the callable returned true.
    template <Reader R> bool peekReadableSlot(R &&reader) const noexcept;
};

// MARK: - Implementation -

namespace detail {

template <ValueLike... Args>
    requires(sizeof...(Args) > 0)
inline void serialize(std::span<unsigned char> buffer, const Args &...args) noexcept {
    std::size_t cursor = 0;
    const auto writeArg = [&cursor, buffer](const auto &arg) noexcept {
        constexpr auto size = sizeof(arg);
        std::memcpy(buffer.data() + cursor, std::addressof(arg), size);
        cursor += size;
    };
    (writeArg(args), ...);
}

template <ValueLike... Args>
    requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
inline void deserialize(std::span<const unsigned char> data, Args &...args) noexcept {
    std::size_t cursor = 0;
    const auto readArg = [&cursor, data](auto &arg) noexcept {
        constexpr auto size = sizeof(arg);
        std::memcpy(std::addressof(arg), data.data() + cursor, size);
        cursor += size;
    };
    (readArg(args), ...);
}

} /* namespace detail */

// MARK: Construction and Destruction

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
constexpr MessageQueue<N, C>::MessageQueue() noexcept {
    for (SizeType i = 0; i < N; ++i) {
        slots_[i].generation_ = i;
    }
}

// MARK: Buffer Usage

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline auto MessageQueue<N, C>::emptySlots() const noexcept -> SizeType {
    const auto writePos = writePosition_.load(std::memory_order_relaxed);
    const auto readPos = readPosition_.load(std::memory_order_acquire);
    return N - (writePos - readPos);
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline bool MessageQueue<N, C>::isFull() const noexcept {
    const auto writePos = writePosition_.load(std::memory_order_relaxed);
    const auto readPos = readPosition_.load(std::memory_order_acquire);
    return (writePos - readPos) == N;
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline auto MessageQueue<N, C>::occupiedSlots() const noexcept -> SizeType {
    const auto writePos = writePosition_.load(std::memory_order_acquire);
    const auto readPos = readPosition_.load(std::memory_order_relaxed);
    return writePos - readPos;
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline bool MessageQueue<N, C>::isEmpty() const noexcept {
    const auto writePos = writePosition_.load(std::memory_order_acquire);
    const auto readPos = readPosition_.load(std::memory_order_relaxed);
    return writePos == readPos;
}

// MARK: Enqueuing Messages

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline bool MessageQueue<N, C>::enqueue(std::span<const unsigned char> message) noexcept {
    if (message.empty() || message.size() > C) [[unlikely]] {
        return false;
    }

    return withWritableSlot([message](std::span<unsigned char> buffer) noexcept {
        std::memcpy(buffer.data(), message.data(), message.size());
        return message.size();
    });
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
                               template <ValueLike... Args>
                 requires(sizeof...(Args) > 0)
inline bool MessageQueue<N, C>::enqueue(const Args &...args) noexcept {
    constexpr auto totalSize = (sizeof(Args) + ...);
    if (totalSize > C) [[unlikely]] {
        return false;
    }

    return withWritableSlot([&](std::span<unsigned char> buffer) noexcept {
        detail::serialize(buffer, args...);
        return totalSize;
    });
}

// MARK: Dequeuing Messages

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline bool MessageQueue<N, C>::dequeue(std::span<unsigned char> buffer, SizeType &written) noexcept {
    written = 0;
    if (buffer.empty()) [[unlikely]] {
        return false;
    }

    return consumeReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        if (buffer.size() < data.size()) [[unlikely]] {
            return false;
        }
        std::memcpy(buffer.data(), data.data(), data.size());
        written = data.size();
        return true;
    });
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
                               template <ValueLike... Args>
                 requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
inline bool MessageQueue<N, C>::dequeue(Args &...args) noexcept {
    constexpr auto totalSize = (sizeof(Args) + ...);
    if (totalSize > C) [[unlikely]] {
        return false;
    }

    return consumeReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        if (data.size() < totalSize) [[unlikely]] {
            return false;
        }
        detail::deserialize(data, args...);
        return true;
    });
}

// MARK: Peeking

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline bool MessageQueue<N, C>::peek(std::span<unsigned char> buffer, SizeType &written) const noexcept {
    written = 0;
    if (buffer.empty()) [[unlikely]] {
        return false;
    }

    return peekReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        const auto count = std::min(data.size(), buffer.size());
        std::memcpy(buffer.data(), data.data(), count);
        written = count;
        return true;
    });
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
                               template <ValueLike... Args>
                 requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
inline bool MessageQueue<N, C>::peek(Args &...args) const noexcept {
    constexpr auto totalSize = (sizeof(Args) + ...);
    if (totalSize > C) [[unlikely]] {
        return false;
    }

    return peekReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        if (data.size() < totalSize) [[unlikely]] {
            return false;
        }
        detail::deserialize(data, args...);
        return true;
    });
}

// MARK: Discarding Messages

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline auto MessageQueue<N, C>::discard(SizeType count) noexcept -> SizeType {
    SizeType discarded = 0;
    while (discarded < count) {
        if (!consumeReadableSlot([&](std::span<const unsigned char>) noexcept -> bool { return true; })) {
            break;
        }
        ++discarded;
    }
    return discarded;
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
inline auto MessageQueue<N, C>::drain() noexcept -> SizeType {
    return discard(std::numeric_limits<SizeType>::max());
}

// MARK: Helpers

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
template <Writer W>
inline bool MessageQueue<N, C>::withWritableSlot(W &&writer) noexcept {
    auto writePos = writePosition_.load(std::memory_order_relaxed);

    while (true) {
        auto &slot = slots_[writePos & slotCountMask_];
        const auto generation = slot.generation_.load(std::memory_order_acquire);
        const auto udiff = generation - writePos;
        const auto diff = static_cast<std::make_signed_t<SizeType>>(udiff);

        if (diff == 0) {
            // Attempt to claim the slot
            if (writePosition_.compare_exchange_weak(writePos, writePos + 1, std::memory_order_relaxed,
                                                     std::memory_order_relaxed)) {

                std::span<unsigned char> buf{slot.data_, C};
                const auto bytesWritten = std::invoke(std::forward<W>(writer), buf);
                slot.dataSize_ = bytesWritten;

                slot.generation_.store(writePos + 1, std::memory_order_release);
                return true;
            }
        } else if (diff < 0) {
            // All slots are full
            return false;
        } else {
            // Another producer claimed this slot
            writePos = writePosition_.load(std::memory_order_relaxed);
        }
    }
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
template <Reader R>
inline bool MessageQueue<N, C>::withReadableSlot(R &&reader, SizeType &readPos) const noexcept {
    readPos = readPosition_.load(std::memory_order_relaxed);

    auto &slot = slots_[readPos & slotCountMask_];
    const auto generation = slot.generation_.load(std::memory_order_acquire);
    const auto udiff = generation - (readPos + 1);
    const auto diff = static_cast<std::make_signed_t<SizeType>>(udiff);

    if (diff != 0) {
        return false;
    }

    const auto data = std::span<const unsigned char>{slot.data_, slot.dataSize_};
    return std::invoke(std::forward<R>(reader), data);
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
template <Reader R>
inline bool MessageQueue<N, C>::consumeReadableSlot(R &&reader) noexcept {
    SizeType readPos;
    if (!withReadableSlot(std::forward<R>(reader), readPos)) {
        return false;
    }

    auto &slot = slots_[readPos & slotCountMask_];

    slot.generation_.store(readPos + N, std::memory_order_release);
    readPosition_.store(readPos + 1, std::memory_order_relaxed);

    return true;
}

template <std::size_t N, std::size_t C>
    requires ValidPowerOfTwo<N> && ValidPowerOfTwo<C>
template <Reader R>
inline bool MessageQueue<N, C>::peekReadableSlot(R &&reader) const noexcept {
    SizeType unused;
    return withReadableSlot(std::forward<R>(reader), unused);
}

} /* namespace mpsc */

#endif
