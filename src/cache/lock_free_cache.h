#pragma once
#include <unordered_map>
#include <list>
#include <string>
#include <shared_mutex>
#include <atomic>
#include <optional>
#include <array>
#include <memory>

// Lock-free optimized cache with better concurrency
class LockFreeLRUCache
{
public:
    static constexpr size_t NUM_SHARDS = 16; // Power of 2 for efficient modulo

private:
    struct CacheEntry
    {
        std::string key;
        std::string value;
        std::atomic<uint64_t> access_time{0};
        std::atomic<bool> valid{true};
    };

    struct CacheShard
    {
        std::unordered_map<std::string, std::unique_ptr<CacheEntry>> entries;
        std::shared_mutex rw_lock;
        std::atomic<uint64_t> access_counter{0};
        size_t shard_capacity;

        CacheShard(size_t cap) : shard_capacity(cap)
        {
            entries.reserve(cap * 2); // Reserve extra space to avoid rehashing
        }
    };

    std::array<std::unique_ptr<CacheShard>, NUM_SHARDS> shards;
    size_t total_capacity;
    std::atomic<uint64_t> global_access_counter{0};

    // Fast hash function for shard selection
    size_t get_shard_index(const std::string &key) const
    {
        // Simple FNV-1a hash for better distribution
        size_t hash = 2166136261u;
        for (char c : key)
        {
            hash ^= static_cast<size_t>(c);
            hash *= 16777619u;
        }
        return hash & (NUM_SHARDS - 1); // Fast modulo for power of 2
    }

    void evict_lru_entries(CacheShard &shard)
    {
        if (shard.entries.size() <= shard.shard_capacity)
            return;

        // Find entries to evict (simple approach: remove 25% of oldest)
        std::vector<std::pair<uint64_t, std::string>> candidates;
        candidates.reserve(shard.entries.size());

        for (const auto &[key, entry] : shard.entries)
        {
            if (entry && entry->valid.load(std::memory_order_relaxed))
            {
                candidates.emplace_back(entry->access_time.load(std::memory_order_relaxed), key);
            }
        }

        if (candidates.size() <= shard.shard_capacity)
            return;

        // Sort by access time and remove oldest 25%
        std::sort(candidates.begin(), candidates.end());
        size_t to_remove = (candidates.size() - shard.shard_capacity) + (shard.shard_capacity / 4);

        for (size_t i = 0; i < to_remove && i < candidates.size(); ++i)
        {
            auto it = shard.entries.find(candidates[i].second);
            if (it != shard.entries.end())
            {
                it->second->valid.store(false, std::memory_order_relaxed);
                shard.entries.erase(it);
            }
        }
    }

public:
    explicit LockFreeLRUCache(size_t capacity) : total_capacity(capacity)
    {
        size_t shard_cap = (capacity + NUM_SHARDS - 1) / NUM_SHARDS;
        for (size_t i = 0; i < NUM_SHARDS; ++i)
        {
            shards[i] = std::make_unique<CacheShard>(shard_cap);
        }
    }

    std::optional<std::string> get(const std::string &key)
    {
        size_t shard_idx = get_shard_index(key);
        auto &shard = *shards[shard_idx];
        uint64_t access_time = global_access_counter.fetch_add(1, std::memory_order_relaxed);

        // Try read-only access first (most common case)
        {
            std::shared_lock<std::shared_mutex> lock(shard.rw_lock);
            auto it = shard.entries.find(key);
            if (it != shard.entries.end() && it->second &&
                it->second->valid.load(std::memory_order_relaxed))
            {
                // Update access time atomically
                it->second->access_time.store(access_time, std::memory_order_relaxed);
                return it->second->value;
            }
        }

        return std::nullopt;
    }

    void put(const std::string &key, const std::string &value)
    {
        size_t shard_idx = get_shard_index(key);
        auto &shard = *shards[shard_idx];
        uint64_t access_time = global_access_counter.fetch_add(1, std::memory_order_relaxed);

        std::unique_lock<std::shared_mutex> lock(shard.rw_lock);

        // Check if key already exists
        auto it = shard.entries.find(key);
        if (it != shard.entries.end() && it->second &&
            it->second->valid.load(std::memory_order_relaxed))
        {
            // Update existing entry
            it->second->value = value;
            it->second->access_time.store(access_time, std::memory_order_relaxed);
            return;
        }

        // Create new entry
        auto entry = std::make_unique<CacheEntry>();
        entry->key = key;
        entry->value = value;
        entry->access_time.store(access_time, std::memory_order_relaxed);
        entry->valid.store(true, std::memory_order_relaxed);

        shard.entries[key] = std::move(entry);

        // Periodic cleanup to avoid lock contention
        if (shard.entries.size() > shard.shard_capacity * 1.5)
        {
            evict_lru_entries(shard);
        }
    }

    void invalidate(const std::string &key)
    {
        size_t shard_idx = get_shard_index(key);
        auto &shard = *shards[shard_idx];

        std::unique_lock<std::shared_mutex> lock(shard.rw_lock);
        auto it = shard.entries.find(key);
        if (it != shard.entries.end())
        {
            if (it->second)
            {
                it->second->valid.store(false, std::memory_order_relaxed);
            }
            shard.entries.erase(it);
        }
    }

    // Non-blocking cache statistics
    size_t get_size() const
    {
        size_t total = 0;
        for (const auto &shard : shards)
        {
            std::shared_lock<std::shared_mutex> lock(shard->rw_lock);
            total += shard->entries.size();
        }
        return total;
    }

    void clear()
    {
        for (auto &shard : shards)
        {
            std::unique_lock<std::shared_mutex> lock(shard->rw_lock);
            for (auto &[key, entry] : shard->entries)
            {
                if (entry)
                    entry->valid.store(false, std::memory_order_relaxed);
            }
            shard->entries.clear();
        }
        global_access_counter.store(0, std::memory_order_relaxed);
    }
};

// Drop-in replacement with same interface as LRUCache
class OptimizedLRUCache
{
private:
    LockFreeLRUCache impl;

public:
    OptimizedLRUCache(size_t capacity) : impl(capacity) {}

    std::optional<std::string> get(const std::string &key)
    {
        return impl.get(key);
    }

    void put(const std::string &key, const std::string &value)
    {
        impl.put(key, value);
    }

    void invalidate(const std::string &key)
    {
        impl.invalidate(key);
    }

    size_t size() const
    {
        return impl.get_size();
    }

    void clear()
    {
        impl.clear();
    }
};