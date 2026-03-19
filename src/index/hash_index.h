#pragma once
#include <unordered_map>
#include <string>
#include <optional>
#include <mutex>
#include "../concurrency/spinlock.h"

class HashIndex
{
    std::unordered_map<std::string, size_t> index;
    SpinLock spin_lock;

public:
    void insert(const std::string &key, size_t row_id)
    {
        std::lock_guard<SpinLock> lock(spin_lock);
        index[key] = row_id;
    }

    std::optional<size_t> lookup(const std::string &key)
    {
        std::lock_guard<SpinLock> lock(spin_lock);
        auto it = index.find(key);
        if (it != index.end())
        {
            return it->second;
        }
        return std::nullopt;
    }

    // Persistence support methods
    std::unordered_map<std::string, size_t> export_data()
    {
        std::lock_guard<SpinLock> lock(spin_lock);
        return index; // Copy the entire map
    }

    void import_data(const std::unordered_map<std::string, size_t> &data)
    {
        std::lock_guard<SpinLock> lock(spin_lock);
        index.clear();
        index = data; // Copy data
    }

    void clear()
    {
        std::lock_guard<SpinLock> lock(spin_lock);
        index.clear();
    }
};
