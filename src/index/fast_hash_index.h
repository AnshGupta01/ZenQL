#pragma once
#include <unordered_map>
#include <string>
#include <optional>
#include <atomic>
#include <vector>
#include <memory>
#include "../concurrency/optimized_locks.h"

// Lock-free hash index with fine-grained locking
class FastHashIndex
{
private:
    static constexpr size_t NUM_BUCKETS = 1024; // Power of 2 for fast modulo

    struct Bucket
    {
        OptimizedRWSpinLock lock;
        std::unordered_map<std::string, size_t> data;
    };

    std::array<Bucket, NUM_BUCKETS> buckets;
    AtomicCounter insert_count;
    AtomicCounter lookup_count;

    size_t hash_to_bucket(const std::string &key) const
    {
        // Use built-in hash function with bitwise AND for fast modulo
        return std::hash<std::string>{}(key) & (NUM_BUCKETS - 1);
    }

public:
    void insert(const std::string &key, size_t row_id)
    {
        size_t bucket_idx = hash_to_bucket(key);
        auto &bucket = buckets[bucket_idx];

        std::lock_guard<OptimizedRWSpinLock> lock(bucket.lock);
        bucket.data[key] = row_id;
        insert_count.increment();
    }

    std::optional<size_t> lookup(const std::string &key)
    {
        size_t bucket_idx = hash_to_bucket(key);
        auto &bucket = buckets[bucket_idx];

        bucket.lock.lock_shared();
        auto it = bucket.data.find(key);
        std::optional<size_t> result = (it != bucket.data.end()) ? std::optional<size_t>(it->second) : std::nullopt;
        bucket.lock.unlock_shared();

        lookup_count.increment();
        return result;
    }

    // Bulk insert for better performance
    void insert_bulk(const std::vector<std::pair<std::string, size_t>> &entries)
    {
        // Group by bucket to minimize lock contention
        std::array<std::vector<std::pair<std::string, size_t>>, NUM_BUCKETS> bucket_entries;

        for (const auto &[key, row_id] : entries)
        {
            size_t bucket_idx = hash_to_bucket(key);
            bucket_entries[bucket_idx].emplace_back(key, row_id);
        }

        // Insert into each bucket
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (!bucket_entries[i].empty())
            {
                auto &bucket = buckets[i];
                std::lock_guard<OptimizedRWSpinLock> lock(bucket.lock);

                for (const auto &[key, row_id] : bucket_entries[i])
                {
                    bucket.data[key] = row_id;
                }
                insert_count.increment();
            }
        }
    }

    // Remove entry
    bool remove(const std::string &key)
    {
        size_t bucket_idx = hash_to_bucket(key);
        auto &bucket = buckets[bucket_idx];

        std::lock_guard<OptimizedRWSpinLock> lock(bucket.lock);
        return bucket.data.erase(key) > 0;
    }

    // Get statistics
    uint64_t get_insert_count() const
    {
        return insert_count.get();
    }

    uint64_t get_lookup_count() const
    {
        return lookup_count.get();
    }

    size_t size() const
    {
        size_t total_size = 0;
        for (const auto &bucket : buckets)
        {
            bucket.lock.lock_shared();
            total_size += bucket.data.size();
            bucket.lock.unlock_shared();
        }
        return total_size;
    }

    // Clear all entries
    void clear()
    {
        for (auto &bucket : buckets)
        {
            std::lock_guard<OptimizedRWSpinLock> lock(bucket.lock);
            bucket.data.clear();
        }
    }

    // Export data for persistence
    std::unordered_map<std::string, size_t> export_data() const
    {
        std::unordered_map<std::string, size_t> result;

        for (const auto &bucket : buckets)
        {
            bucket.lock.lock_shared();
            for (const auto &[key, value] : bucket.data)
            {
                result[key] = value;
            }
            bucket.lock.unlock_shared();
        }

        return result;
    }

    // Import data for recovery
    void import_data(const std::unordered_map<std::string, size_t> &data)
    {
        clear(); // Clear existing data first

        std::vector<std::pair<std::string, size_t>> entries;
        entries.reserve(data.size());

        for (const auto &[key, value] : data)
        {
            entries.emplace_back(key, value);
        }

        insert_bulk(entries);
    }
};

// Concurrent HashMap with even better performance for read-heavy workloads
template <typename K, typename V>
class ConcurrentHashMap
{
private:
    static constexpr size_t NUM_SEGMENTS = 64; // Reduce lock contention

    struct Segment
    {
        mutable OptimizedRWSpinLock lock;
        std::unordered_map<K, V> data;
        CacheAligned<AtomicCounter> access_count;
    };

    std::array<Segment, NUM_SEGMENTS> segments;

    size_t hash_to_segment(const K &key) const
    {
        return std::hash<K>{}(key) & (NUM_SEGMENTS - 1);
    }

public:
    void insert(const K &key, const V &value)
    {
        size_t segment_idx = hash_to_segment(key);
        auto &segment = segments[segment_idx];

        std::lock_guard<OptimizedRWSpinLock> lock(segment.lock);
        segment.data[key] = value;
        segment.access_count.value.increment();
    }

    std::optional<V> find(const K &key) const
    {
        size_t segment_idx = hash_to_segment(key);
        auto &segment = segments[segment_idx];

        segment.lock.lock_shared();
        auto it = segment.data.find(key);
        std::optional<V> result = (it != segment.data.end()) ? std::optional<V>(it->second) : std::nullopt;
        segment.lock.unlock_shared();

        segment.access_count.value.increment();
        return result;
    }

    bool remove(const K &key)
    {
        size_t segment_idx = hash_to_segment(key);
        auto &segment = segments[segment_idx];

        std::lock_guard<OptimizedRWSpinLock> lock(segment.lock);
        return segment.data.erase(key) > 0;
    }

    size_t size() const
    {
        size_t total_size = 0;
        for (const auto &segment : segments)
        {
            segment.lock.lock_shared();
            total_size += segment.data.size();
            segment.lock.unlock_shared();
        }
        return total_size;
    }

    // Get access statistics per segment
    std::vector<uint64_t> get_segment_stats() const
    {
        std::vector<uint64_t> stats;
        stats.reserve(NUM_SEGMENTS);

        for (const auto &segment : segments)
        {
            stats.push_back(segment.access_count.value.get());
        }

        return stats;
    }
};