#pragma once
#include <unordered_map>
#include <string>
#include <optional>
#include <mutex>
#include "../concurrency/spinlock.h"

class HashIndex {
    std::unordered_map<std::string, size_t> index; 
    SpinLock spin_lock;
public:
    void insert(const std::string& key, size_t row_id) {
        std::lock_guard<SpinLock> lock(spin_lock);
        index[key] = row_id;
    }

    std::optional<size_t> lookup(const std::string& key) {
        std::lock_guard<SpinLock> lock(spin_lock);
        auto it = index.find(key);
        if (it != index.end()) {
            return it->second;
        }
        return std::nullopt;
    }
};
