#pragma once
#include <atomic>
#include <thread>

// Reader-writer spinlock optimized for high-throughput point queries
class RWSpinLock {
public:
    void lock_shared() {
        while (true) {
            uint32_t current = state_.load(std::memory_order_acquire);
            if ((current & WRITE_MASK) == 0) {
                if (state_.compare_exchange_weak(current, current + 1, std::memory_order_acquire))
                    return;
            }
            // backoff
            for(int i = 0; i < 4; ++i) {
#if defined(__x86_64__)
                __builtin_ia32_pause();
#elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
#endif
            }
        }
    }

    void unlock_shared() {
        state_.fetch_sub(1, std::memory_order_release);
    }

    void lock() {
        while (true) {
            uint32_t current = state_.load(std::memory_order_acquire);
            if ((current & WRITE_MASK) == 0) {
                if (state_.compare_exchange_weak(current, current | WRITE_MASK, std::memory_order_acquire)) {
                    // Wait for readers to finish
                    while ((state_.load(std::memory_order_acquire) & ~WRITE_MASK) != 0) {
                        for(int i = 0; i < 4; ++i) {
#if defined(__x86_64__)
                            __builtin_ia32_pause();
#elif defined(__aarch64__)
                            asm volatile("yield" ::: "memory");
#endif
                        }
                    }
                    return;
                }
            }
            // backoff
            for(int i = 0; i < 4; ++i) {
#if defined(__x86_64__)
                __builtin_ia32_pause();
#elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
#endif
            }
        }
    }

    void unlock() {
        state_.fetch_and(~WRITE_MASK, std::memory_order_release);
    }

private:
    static constexpr uint32_t WRITE_MASK = 0x80000000;
    std::atomic<uint32_t> state_{0};
};
