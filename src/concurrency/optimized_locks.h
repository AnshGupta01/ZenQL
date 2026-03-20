#pragma once
#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>

// High-performance spinlock with exponential backoff
class OptimizedSpinLock
{
private:
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
    static constexpr unsigned max_spins = 1024;

public:
    void lock()
    {
        unsigned spins = 0;
        while (flag.test_and_set(std::memory_order_acquire))
        {
            if (++spins < max_spins)
            {
                // Busy wait with CPU hints
                if (spins < 16)
                {
#if defined(__x86_64__) || defined(__i386__)
                    __builtin_ia32_pause(); // x86 pause instruction
#elif defined(__aarch64__) || defined(__arm__)
                    __asm__ __volatile__("yield" ::: "memory"); // ARM yield
#else
                    std::this_thread::yield();
#endif
                }
                else if (spins < 64)
                {
                    std::this_thread::yield();
                }
                else
                {
                    // Exponential backoff
                    std::this_thread::sleep_for(std::chrono::nanoseconds(spins));
                }
            }
            else
            {
                // Fall back to yielding after too many spins
                std::this_thread::yield();
                spins = 0;
            }
        }
    }

    bool try_lock()
    {
        return !flag.test_and_set(std::memory_order_acquire);
    }

    void unlock()
    {
        flag.clear(std::memory_order_release);
    }
};

// Reader-Writer SpinLock for better read concurrency
// Fixed: Readers no longer acquire write lock, allowing true concurrent reads
class OptimizedRWSpinLock
{
private:
    alignas(64) mutable std::atomic<int> readers{0};
    alignas(64) mutable std::atomic<bool> write_lock_held{false};
    mutable OptimizedSpinLock write_lock;

public:
    void lock_shared() const
    {
        // Wait until no writer is active
        while (write_lock_held.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }

        // Increment reader count
        readers.fetch_add(1, std::memory_order_acquire);

        // Double-check no writer started after we incremented
        if (write_lock_held.load(std::memory_order_acquire))
        {
            // A writer started, back off and retry
            readers.fetch_sub(1, std::memory_order_release);
            lock_shared(); // Recursive retry
            return;
        }
        // Successfully acquired shared lock
    }

    void unlock_shared() const
    {
        readers.fetch_sub(1, std::memory_order_release);
    }

    void lock() const
    {
        write_lock.lock();
        write_lock_held.store(true, std::memory_order_release);

        // Wait for all readers to finish
        while (readers.load(std::memory_order_acquire) > 0)
        {
            std::this_thread::yield();
        }
    }

    void unlock() const
    {
        write_lock_held.store(false, std::memory_order_release);
        write_lock.unlock();
    }
};

// Lock-free queue for high-throughput scenarios
template <typename T>
class LockFreeQueue
{
private:
    struct Node
    {
        std::atomic<T *> data{nullptr};
        std::atomic<Node *> next{nullptr};
    };

    std::atomic<Node *> head;
    std::atomic<Node *> tail;

public:
    LockFreeQueue()
    {
        Node *dummy = new Node;
        head.store(dummy);
        tail.store(dummy);
    }

    ~LockFreeQueue()
    {
        while (Node *oldHead = head.load())
        {
            head.store(oldHead->next);
            delete oldHead;
        }
    }

    void enqueue(T item)
    {
        Node *newNode = new Node;
        T *data = new T(std::move(item));
        newNode->data.store(data);

        while (true)
        {
            Node *last = tail.load();
            Node *next = last->next.load();

            if (last == tail.load())
            {
                if (next == nullptr)
                {
                    if (last->next.compare_exchange_weak(next, newNode))
                    {
                        break;
                    }
                }
                else
                {
                    tail.compare_exchange_weak(last, next);
                }
            }
        }
        tail.compare_exchange_weak(tail.load(), newNode);
    }

    bool dequeue(T &result)
    {
        while (true)
        {
            Node *first = head.load();
            Node *last = tail.load();
            Node *next = first->next.load();

            if (first == head.load())
            {
                if (first == last)
                {
                    if (next == nullptr)
                    {
                        return false; // Queue is empty
                    }
                    tail.compare_exchange_weak(last, next);
                }
                else
                {
                    if (next == nullptr)
                    {
                        continue;
                    }

                    T *data = next->data.load();
                    if (head.compare_exchange_weak(first, next))
                    {
                        if (data)
                        {
                            result = *data;
                            delete data;
                            delete first;
                            return true;
                        }
                    }
                }
            }
        }
    }
};

// Adaptive lock that switches between SpinLock and Mutex based on contention
class AdaptiveLock
{
private:
    std::atomic<unsigned> contention_counter{0};
    std::atomic<bool> use_mutex{false};
    mutable OptimizedSpinLock spin_lock;
    mutable std::mutex mutex_lock;
    static constexpr unsigned contention_threshold = 100;

public:
    void lock()
    {
        if (use_mutex.load(std::memory_order_relaxed))
        {
            auto start = std::chrono::high_resolution_clock::now();
            mutex_lock.lock();
            auto end = std::chrono::high_resolution_clock::now();

            // If we got the mutex quickly, maybe we can switch back to spinning
            auto wait_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            if (wait_time.count() < 10)
            { // Less than 10 microseconds
                unsigned current = contention_counter.load();
                if (current > 0)
                {
                    contention_counter.store(current - 1);
                }
            }
        }
        else
        {
            unsigned spins = 0;
            while (!spin_lock.try_lock())
            {
                ++spins;
                if (spins > 100)
                {
                    // High contention, switch to mutex
                    contention_counter.fetch_add(1);
                    if (contention_counter.load() > contention_threshold)
                    {
                        use_mutex.store(true);
                        mutex_lock.lock();
                        return;
                    }
                }
#if defined(__x86_64__) || defined(__i386__)
                __builtin_ia32_pause(); // x86 pause instruction
#elif defined(__aarch64__) || defined(__arm__)
                __asm__ __volatile__("yield" ::: "memory"); // ARM yield
#else
                std::this_thread::yield();
#endif
            }
        }
    }

    void unlock()
    {
        if (use_mutex.load(std::memory_order_relaxed))
        {
            mutex_lock.unlock();
        }
        else
        {
            spin_lock.unlock();
        }
    }

    bool try_lock()
    {
        if (use_mutex.load(std::memory_order_relaxed))
        {
            return mutex_lock.try_lock();
        }
        else
        {
            return spin_lock.try_lock();
        }
    }
};

// Cache-aligned false sharing prevention
template <typename T>
struct CacheAligned
{
    alignas(64) T value; // 64-byte cache line alignment
};

// Lock-free atomic counter for statistics
class AtomicCounter
{
private:
    alignas(64) std::atomic<uint64_t> count{0};

public:
    uint64_t increment()
    {
        return count.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t get() const
    {
        return count.load(std::memory_order_relaxed);
    }

    void reset()
    {
        count.store(0, std::memory_order_relaxed);
    }
};