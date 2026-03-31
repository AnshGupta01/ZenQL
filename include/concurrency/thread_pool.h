#pragma once
#include <vector>
#include <functional>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

class ThreadPool {
public:
    explicit ThreadPool(int n_threads);
    ~ThreadPool();

    static void set_global(ThreadPool* pool) { global_ = pool; }
    static ThreadPool* get_global() { return global_; }

    void submit(std::function<void()> task);
    void wait_all();   // Wait until all tasks are done

private:
    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> queue_;
    std::mutex                        mu_;
    std::condition_variable           cv_;
    std::condition_variable           done_cv_;
    std::atomic<int>                  active_{0};
    bool                              stop_{false};
    static inline ThreadPool*         global_{nullptr};
};
