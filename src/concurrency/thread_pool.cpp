#include "concurrency/thread_pool.h"

ThreadPool::ThreadPool(int n) {
    for (int i = 0; i < n; i++) {
        workers_.emplace_back([this] {
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lk(mu_);
                    cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                    if (stop_ && queue_.empty()) return;
                    task = std::move(queue_.front());
                    queue_.pop();
                    active_++;
                }
                task();
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    active_--;
                }
                done_cv_.notify_all();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    { std::lock_guard<std::mutex> lk(mu_); stop_ = true; }
    cv_.notify_all();
    for (auto& t : workers_) t.join();
}

void ThreadPool::submit(std::function<void()> task) {
    { std::lock_guard<std::mutex> lk(mu_); queue_.push(std::move(task)); }
    cv_.notify_one();
}

void ThreadPool::wait_all() {
    std::unique_lock<std::mutex> lk(mu_);
    done_cv_.wait(lk, [this] { return queue_.empty() && active_ == 0; });
}
