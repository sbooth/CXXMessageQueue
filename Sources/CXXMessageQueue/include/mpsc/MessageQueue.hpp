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

/// A lock-free MPSC message queue.
///
/// This class is thread safe when used with multiple producers and a single consumer.
///
/// The message queue contains a fixed number of slots, each with a fixed byte capacity (C).
/// The amount of data that can be written or read in one operation is limited by the size of a single slot.
/// Each write occupies a single slot regardless of size, and similarly reads occur from a single slot only.
///
/// This message queue performs raw byte copies; it does not provide serialization.
template <std::size_t C>
    requires ValidPowerOfTwo<C>
class MessageQueue final {
  public:
    /// Unsigned integer type.
    using SizeType = std::size_t;
    /// Atomic unsigned integer type.
    using AtomicSizeType = std::atomic<SizeType>;

    /// The minimum supported slot count.
    static constexpr auto minSlots = SizeType{2};
    /// The maximum supported slot count.
    static constexpr auto maxSlots = SizeType{1} << (std::numeric_limits<SizeType>::digits - 1);

    // MARK: Construction and Destruction

    /// Creates an empty message queue.
    /// @note ``allocate`` must be called before the object may be used.
    MessageQueue() noexcept = default;

    /// Creates a message queue with the specified minimum slot count.
    ///
    /// The actual slot count will be the smallest integral power of two that is not less than the specified
    /// minimum slot count.
    /// @param minSlots The desired minimum slot count.
    /// @throw std::bad_alloc if memory could not be allocated or std::invalid_argument if the slot count is not
    /// supported.
    explicit MessageQueue(SizeType minSlots);

    MessageQueue(const MessageQueue &) = delete;
    MessageQueue &operator=(const MessageQueue &) = delete;

    /// Creates a message queue by moving the contents of another message queue.
    /// @note This method is not thread safe for the message queue being moved.
    /// @param other The message queue to move.
    MessageQueue(MessageQueue &&other) noexcept;

    /// Moves the contents of another message queue into this message queue.
    /// @note This method is not thread safe.
    /// @param other The message queue to move.
    MessageQueue &operator=(MessageQueue &&other) noexcept;

    /// Destroys the message queue and releases all associated resources.
    ~MessageQueue() noexcept = default;

    // MARK: Buffer Management

    /// Allocates space for data.
    ///
    /// The actual slot count will be the smallest integral power of two that is not less than the specified
    /// minimum slot count.
    /// @note This method is not thread safe.
    /// @param minSlots The desired minimum slot count.
    /// @return true on success, false if memory could not be allocated or the slot count is not supported.
    bool allocate(SizeType minSlots) noexcept [[clang::allocating]];

    /// Frees any space allocated for data.
    /// @note This method is not thread safe.
    void deallocate() noexcept;

    /// Returns true if the message queue has allocated space for data.
    [[nodiscard]] explicit operator bool() const noexcept [[clang::nonblocking]];

    // MARK: Buffer Information

    /// Returns the slot count of the message queue.
    /// @note This method is safe to call from both producer and consumer.
    /// @return The message queue slot count.
    [[nodiscard]] SizeType slotCount() const noexcept [[clang::nonblocking]];

    /// Returns the capacity of a single slot in the message queue.
    /// @note This method is safe to call from both producer and consumer.
    /// @return The capacity of a single slot in the message queue in bytes.
    [[nodiscard]] std::size_t slotCapacity() const noexcept [[clang::nonblocking]];

    // MARK: Buffer Usage

    /// Returns the number of empty slots in the message queue.
    /// @note The result of this method is only valid when called from a producer.
    /// @note The returned value is a transient snapshot and may become stale immediately after return.
    /// @return The number of empty slots available for writing.
    [[nodiscard]] SizeType emptySlots() const noexcept [[clang::nonblocking]];

    /// Returns true if the message queue is full.
    /// @note The result of this method is only valid when called from a producer.
    /// @note The returned value is a transient snapshot and may become stale immediately after return.
    /// @return true if the all slots in the buffer are occupied.
    [[nodiscard]] bool isFull() const noexcept [[clang::nonblocking]];

    /// Returns the number of occupied slots in the message queue.
    /// @note The result of this method is only accurate when called from the consumer.
    /// @return The number of occupied slots available for reading.
    [[nodiscard]] SizeType occupiedSlots() const noexcept [[clang::nonblocking]];

    /// Returns true if the message queue is empty.
    /// @note The result of this method is only accurate when called from the consumer.
    /// @return true if all slots in the buffer are empty.
    [[nodiscard]] bool isEmpty() const noexcept [[clang::nonblocking]];

    // MARK: Writing

    /// Writes data to the next available slot and advances the write position.
    /// @note This method is only safe to call from a producer.
    /// @param data A span containing the data to copy.
    /// @return true if the data was successfully written, false if the message queue is full or the slot capacity is
    /// insufficient.
    bool write(std::span<const unsigned char> data) noexcept [[clang::nonblocking]];

    /// Writes values to the next available slot and advances the write position.
    /// @note This method is only safe to call from a producer.
    /// @tparam Args The types to write.
    /// @param args The values to write.
    /// @return true if the values were successfully written, false if the message queue is full or the slot capacity is
    /// insufficient.
    template <ValueLike... Args>
        requires(sizeof...(Args) > 0)
    bool writeValues(const Args &...args) noexcept [[clang::nonblocking]];

    // MARK: Reading

    /// Reads data from the first occupied slot and advances the read position.
    /// @note This method is only safe to call from the consumer.
    /// @param buffer A span to receive the data.
    /// @param written On return, the number of bytes read.
    /// @return true if data was successfully read, false if the message queue is empty or the buffer capacity is
    /// insufficient.
    bool read(std::span<unsigned char> buffer, SizeType &written) noexcept [[clang::nonblocking]];

    /// Reads values from the first occupied slot and advances the read position.
    /// @note This method is only safe to call from the consumer.
    /// @tparam Args The types to read.
    /// @param args The destination values.
    /// @return true if the values were successfully read, false if the message queue is empty or the slot contains
    /// fewer bytes than requested.
    template <ValueLike... Args>
        requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
    bool readValues(Args &...args) noexcept [[clang::nonblocking]];

    // MARK: Peeking

    /// Reads data from the first occupied slot without advancing the read position.
    /// @note This method is only safe to call from the consumer.
    /// @param buffer A span to receive the data.
    /// @param written On return, the number of bytes read.
    /// @return true if data was successfully read, false if the message queue is empty.
    [[nodiscard]] bool peek(std::span<unsigned char> buffer, SizeType &written) const noexcept [[clang::nonblocking]];

    /// Reads values from the first occupied slot without advancing the read position.
    /// @note This method is only safe to call from the consumer.
    /// @tparam Args The types to read.
    /// @param args The destination values.
    /// @return true if the values were successfully read, false if the message queue is empty or the slot contains
    /// insufficient data.
    template <ValueLike... Args>
        requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
    [[nodiscard]] bool peekValues(Args &...args) const noexcept [[clang::nonblocking]];

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
    std::unique_ptr<Slot[]> slots_;

    /// The number of slots in slots_.
    SizeType slotCount_{0};
    /// The number of slots in slots_ minus one.
    SizeType slotCountMask_{0};

    /// The free-running write location.
    AtomicSizeType writePosition_{0};
    /// The free-running read location.
    AtomicSizeType readPosition_{0};

    static_assert(AtomicSizeType::is_always_lock_free, "Lock-free AtomicSizeType required");

    // MARK: Helpers

    /// Claims a writable slot if available and invokes a callable to write data.
    /// @tparam Writer The type of the callable object.
    /// @param writer A callable performing the write.
    /// @return true if a writable slot was claimed.
    template <typename Writer>
        requires std::invocable<Writer, std::span<unsigned char>> &&
                 std::is_nothrow_invocable_v<Writer, std::span<unsigned char>>
    bool withWritableSlot(Writer &&writer) noexcept;

    /// Invokes a callable with data from the readable slot.
    /// @tparam Reader The type of the callable object.
    /// @param reader A callable performing the read.
    /// @param readPos The read position of the slot providing the data.
    /// @return true if data was provided and the callable returned true.
    template <typename Reader>
        requires std::invocable<Reader, std::span<const unsigned char>> &&
                 std::is_nothrow_invocable_v<Reader, std::span<const unsigned char>>
    bool withReadableSlot(Reader &&reader, SizeType &readPos) const noexcept;

    /// Invokes a callable with data from the readable slot and advances the read position.
    /// @tparam Reader The type of the callable object.
    /// @param reader A callable performing the read.
    /// @return true if data was provided and the callable returned true.
    template <typename Reader>
        requires std::invocable<Reader, std::span<const unsigned char>> &&
                 std::is_nothrow_invocable_v<Reader, std::span<const unsigned char>>
    bool consumeReadableSlot(Reader &&reader) noexcept;

    /// Invokes a callable with data from the readable slot without advancing the read position.
    /// @tparam Reader The type of the callable object.
    /// @param reader A callable performing the read.
    /// @return true if data was provided and the callable returned true.
    template <typename Reader>
        requires std::invocable<Reader, std::span<const unsigned char>> &&
                 std::is_nothrow_invocable_v<Reader, std::span<const unsigned char>>
    bool peekReadableSlot(Reader &&reader) const noexcept;
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

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline MessageQueue<C>::MessageQueue(SizeType minSlots) {
    if (minSlots < MessageQueue::minSlots || minSlots > MessageQueue::maxSlots) [[unlikely]] {
        throw std::invalid_argument("slot count out of range");
    }
    if (!allocate(minSlots)) [[unlikely]] {
        throw std::bad_alloc();
    }
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline MessageQueue<C>::MessageQueue(MessageQueue &&other) noexcept
    : slots_{std::exchange(other.slots_, nullptr)}, slotCount_{std::exchange(other.slotCount_, 0)},
      slotCountMask_{std::exchange(other.slotCountMask_, 0)},
      writePosition_{other.writePosition_.exchange(0, std::memory_order_relaxed)},
      readPosition_{other.readPosition_.exchange(0, std::memory_order_relaxed)} {}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline auto MessageQueue<C>::operator=(MessageQueue &&other) noexcept -> MessageQueue & {
    if (this != &other) [[likely]] {
        slots_ = std::exchange(other.slots_, nullptr);
        slotCount_ = std::exchange(other.slotCount_, 0);
        slotCountMask_ = std::exchange(other.slotCountMask_, 0);

        writePosition_.store(other.writePosition_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
        readPosition_.store(other.readPosition_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
    }
    return *this;
}

// MARK: Buffer Management

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline bool MessageQueue<C>::allocate(SizeType minSlots) noexcept {
    if (minSlots < MessageQueue::minSlots || minSlots > MessageQueue::maxSlots) [[unlikely]] {
        return false;
    }

    deallocate();

    const auto slotCount = std::bit_ceil(minSlots);

    try {
        slots_ = std::make_unique_for_overwrite<Slot[]>(slotCount);
    } catch (...) {
        return false;
    }

    for (SizeType i = 0; i < slotCount; ++i) {
        slots_[i].generation_ = i;
        slots_[i].dataSize_ = 0;
    }

    slotCount_ = slotCount;
    slotCountMask_ = slotCount - 1;

    writePosition_.store(0, std::memory_order_relaxed);
    readPosition_.store(0, std::memory_order_relaxed);

    return true;
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline void MessageQueue<C>::deallocate() noexcept {
    if (slots_) [[likely]] {
        slots_.reset();

        slotCount_ = 0;
        slotCountMask_ = 0;

        writePosition_.store(0, std::memory_order_relaxed);
        readPosition_.store(0, std::memory_order_relaxed);
    }
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline MessageQueue<C>::operator bool() const noexcept {
    return static_cast<bool>(slots_);
}

// MARK: Buffer Information

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline auto MessageQueue<C>::slotCount() const noexcept -> SizeType {
    return slotCount_;
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline std::size_t MessageQueue<C>::slotCapacity() const noexcept {
    return C;
}

// MARK: Buffer Usage

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline auto MessageQueue<C>::emptySlots() const noexcept -> SizeType {
    const auto writePos = writePosition_.load(std::memory_order_relaxed);
    const auto readPos = readPosition_.load(std::memory_order_acquire);
    return slotCount_ - (writePos - readPos);
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline bool MessageQueue<C>::isFull() const noexcept {
    const auto writePos = writePosition_.load(std::memory_order_relaxed);
    const auto readPos = readPosition_.load(std::memory_order_acquire);
    return (writePos - readPos) == slotCount_;
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline auto MessageQueue<C>::occupiedSlots() const noexcept -> SizeType {
    const auto writePos = writePosition_.load(std::memory_order_acquire);
    const auto readPos = readPosition_.load(std::memory_order_relaxed);
    return writePos - readPos;
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline bool MessageQueue<C>::isEmpty() const noexcept {
    const auto writePos = writePosition_.load(std::memory_order_acquire);
    const auto readPos = readPosition_.load(std::memory_order_relaxed);
    return writePos == readPos;
}

// MARK: Writing

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline bool MessageQueue<C>::write(std::span<const unsigned char> data) noexcept {
    if (data.empty() || data.size() > C || slotCount_ == 0) [[unlikely]] {
        return false;
    }

    return withWritableSlot([data](std::span<unsigned char> buffer) noexcept {
        std::memcpy(buffer.data(), data.data(), data.size());
        return data.size();
    });
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <ValueLike... Args>
    requires(sizeof...(Args) > 0)
inline bool MessageQueue<C>::writeValues(const Args &...args) noexcept {
    constexpr auto totalSize = (sizeof(Args) + ...);
    if (totalSize > C || slotCount_ == 0) [[unlikely]] {
        return false;
    }

    return withWritableSlot([&](std::span<unsigned char> buffer) noexcept {
        detail::serialize(buffer, args...);
        return totalSize;
    });
}

// MARK: Reading

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline bool MessageQueue<C>::read(std::span<unsigned char> buffer, SizeType &written) noexcept {
    written = 0;
    if (buffer.empty() || buffer.size() < C || slotCount_ == 0) [[unlikely]] {
        return false;
    }

    return consumeReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        std::memcpy(buffer.data(), data.data(), data.size());
        written = data.size();
        return true;
    });
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <ValueLike... Args>
    requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
inline bool MessageQueue<C>::readValues(Args &...args) noexcept {
    constexpr auto totalSize = (sizeof(Args) + ...);
    if (totalSize > C || slotCount_ == 0) [[unlikely]] {
        return false;
    }

    return consumeReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        if (data.size() < totalSize) {
            return false;
        }
        detail::deserialize(data, args...);
        return true;
    });
}

// MARK: Peeking

template <std::size_t C>
    requires ValidPowerOfTwo<C>
inline bool MessageQueue<C>::peek(std::span<unsigned char> buffer, SizeType &written) const noexcept {
    written = 0;
    if (buffer.empty() || slotCount_ == 0) [[unlikely]] {
        return false;
    }

    return peekReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        const auto count = std::min(data.size(), buffer.size());
        std::memcpy(buffer.data(), data.data(), count);
        written = count;
        return true;
    });
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <ValueLike... Args>
    requires(sizeof...(Args) > 0) && (std::assignable_from<Args &, const Args &> && ...)
inline bool MessageQueue<C>::peekValues(Args &...args) const noexcept {
    constexpr auto totalSize = (sizeof(Args) + ...);
    if (totalSize > C || slotCount_ == 0) [[unlikely]] {
        return false;
    }

    return peekReadableSlot([&](std::span<const unsigned char> data) noexcept -> bool {
        if (data.size() < totalSize) {
            return false;
        }
        detail::deserialize(data, args...);
        return true;
    });
}

// MARK: Helpers

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <typename Writer>
    requires std::invocable<Writer, std::span<unsigned char>> &&
             std::is_nothrow_invocable_v<Writer, std::span<unsigned char>>
inline bool MessageQueue<C>::withWritableSlot(Writer &&writer) noexcept {
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
                const auto bytesWritten = std::invoke(std::forward<Writer>(writer), buf);
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

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <typename Reader>
    requires std::invocable<Reader, std::span<const unsigned char>> &&
             std::is_nothrow_invocable_v<Reader, std::span<const unsigned char>>
inline bool MessageQueue<C>::withReadableSlot(Reader &&reader, SizeType &readPos) const noexcept {
    readPos = readPosition_.load(std::memory_order_relaxed);
    auto &slot = slots_[readPos & slotCountMask_];

    const auto generation = slot.generation_.load(std::memory_order_acquire);
    const auto udiff = generation - (readPos + 1);
    const auto diff = static_cast<std::make_signed_t<SizeType>>(udiff);

    if (diff != 0) {
        return false;
    }

    const auto data = std::span<const unsigned char>{slot.data_, slot.dataSize_};
    return std::invoke(std::forward<Reader>(reader), data);
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <typename Reader>
    requires std::invocable<Reader, std::span<const unsigned char>> &&
             std::is_nothrow_invocable_v<Reader, std::span<const unsigned char>>
inline bool MessageQueue<C>::consumeReadableSlot(Reader &&reader) noexcept {
    SizeType readPos;
    if (!withReadableSlot(std::forward<Reader>(reader), readPos)) {
        return false;
    }

    auto &slot = slots_[readPos & slotCountMask_];

    slot.generation_.store(readPos + slotCount_, std::memory_order_release);
    readPosition_.store(readPos + 1, std::memory_order_relaxed);

    return true;
}

template <std::size_t C>
    requires ValidPowerOfTwo<C>
template <typename Reader>
    requires std::invocable<Reader, std::span<const unsigned char>> &&
             std::is_nothrow_invocable_v<Reader, std::span<const unsigned char>>
inline bool MessageQueue<C>::peekReadableSlot(Reader &&reader) const noexcept {
    SizeType unused;
    return withReadableSlot(std::forward<Reader>(reader), unused);
}

} /* namespace mpsc */

#endif
