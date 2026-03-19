#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <iostream>
#include <chrono>
#include <atomic>
#include "../parser/parser.h"

// Cache-aligned memory pool for better performance
template <typename T>
class CacheAlignedVector
{
private:
    static constexpr size_t CACHE_LINE_SIZE = 64;
    alignas(CACHE_LINE_SIZE) std::vector<T> data;
    size_t capacity_hint = 0;

public:
    CacheAlignedVector()
    {
        data.reserve(1024); // Start with reasonable size
    }

    void push_back(const T &item)
    {
        data.push_back(item);
    }

    const T &operator[](size_t index) const
    {
        return data[index];
    }

    T &operator[](size_t index)
    {
        return data[index];
    }

    size_t size() const
    {
        return data.size();
    }

    void reserve(size_t new_capacity)
    {
        if (new_capacity > capacity_hint)
        {
            // Reserve extra space to avoid frequent reallocations
            size_t growth_capacity = new_capacity * 1.5;
            data.reserve(growth_capacity);
            capacity_hint = growth_capacity;
        }
    }

    void clear()
    {
        data.clear();
        capacity_hint = 0;
    }

    // Direct access to underlying vector for bulk operations
    std::vector<T> &get_vector()
    {
        return data;
    }

    const std::vector<T> &get_vector() const
    {
        return data;
    }
};

// Optimized column class with better memory layout
class OptimizedColumn
{
public:
    virtual ~OptimizedColumn() = default;
    virtual void push_back(const std::string &val) = 0;
    virtual std::string get(size_t index) = 0;
    virtual size_t size() const = 0;
    virtual void reserve(size_t capacity) = 0;
    virtual void load_data(const std::vector<std::string> &data) = 0;
    virtual std::vector<std::string> export_data() const = 0;
    virtual void clear() = 0;

    // New: Bulk operations for better performance
    virtual void insert_bulk(const std::vector<std::string> &values) = 0;
    virtual std::vector<std::string> get_range(size_t start, size_t count) = 0;
};

// Cache-optimized string column
class CacheOptimizedStringColumn : public OptimizedColumn
{
private:
    CacheAlignedVector<std::string> data;
    mutable std::shared_mutex rw_lock;

public:
    void push_back(const std::string &val) override
    {
        std::unique_lock lock(rw_lock);
        data.push_back(val);
    }

    std::string get(size_t index) override
    {
        std::shared_lock lock(rw_lock);
        if (index < data.size())
        {
            return data[index];
        }
        return "NULL";
    }

    size_t size() const override
    {
        std::shared_lock lock(rw_lock);
        return data.size();
    }

    void reserve(size_t capacity) override
    {
        std::unique_lock lock(rw_lock);
        data.reserve(capacity);
    }

    void load_data(const std::vector<std::string> &input) override
    {
        std::unique_lock lock(rw_lock);
        data.clear();
        data.reserve(input.size());
        for (const auto &str : input)
        {
            data.push_back(str);
        }
    }

    std::vector<std::string> export_data() const override
    {
        std::shared_lock lock(rw_lock);
        return data.get_vector();
    }

    void clear() override
    {
        std::unique_lock lock(rw_lock);
        data.clear();
    }

    // Bulk operations for better cache efficiency
    void insert_bulk(const std::vector<std::string> &values) override
    {
        std::unique_lock lock(rw_lock);
        data.reserve(data.size() + values.size());
        for (const auto &val : values)
        {
            data.push_back(val);
        }
    }

    std::vector<std::string> get_range(size_t start, size_t count) override
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> result;
        result.reserve(count);

        size_t end = std::min(start + count, data.size());
        for (size_t i = start; i < end; ++i)
        {
            result.push_back(data[i]);
        }

        return result;
    }
};

// Cache-optimized integer column with SIMD support prep
class CacheOptimizedIntColumn : public OptimizedColumn
{
private:
    alignas(32) CacheAlignedVector<int32_t> data; // 32-byte aligned for AVX
    mutable std::shared_mutex rw_lock;

public:
    void push_back(const std::string &val) override
    {
        std::unique_lock lock(rw_lock);
        try
        {
            data.push_back(std::stoi(val));
        }
        catch (const std::exception &)
        {
            data.push_back(0);
        }
    }

    std::string get(size_t index) override
    {
        std::shared_lock lock(rw_lock);
        if (index < data.size())
        {
            return std::to_string(data[index]);
        }
        return "NULL";
    }

    size_t size() const override
    {
        std::shared_lock lock(rw_lock);
        return data.size();
    }

    void reserve(size_t capacity) override
    {
        std::unique_lock lock(rw_lock);
        data.reserve(capacity);
    }

    void load_data(const std::vector<std::string> &input) override
    {
        std::unique_lock lock(rw_lock);
        data.clear();
        data.reserve(input.size());
        for (const auto &str : input)
        {
            try
            {
                data.push_back(std::stoi(str));
            }
            catch (const std::exception &)
            {
                data.push_back(0);
            }
        }
    }

    std::vector<std::string> export_data() const override
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> result;
        result.reserve(data.size());
        for (const auto &val : data.get_vector())
        {
            result.push_back(std::to_string(val));
        }
        return result;
    }

    void clear() override
    {
        std::unique_lock lock(rw_lock);
        data.clear();
    }

    void insert_bulk(const std::vector<std::string> &values) override
    {
        std::unique_lock lock(rw_lock);
        data.reserve(data.size() + values.size());
        for (const auto &val : values)
        {
            try
            {
                data.push_back(std::stoi(val));
            }
            catch (const std::exception &)
            {
                data.push_back(0);
            }
        }
    }

    std::vector<std::string> get_range(size_t start, size_t count) override
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> result;
        result.reserve(count);

        size_t end = std::min(start + count, data.size());
        for (size_t i = start; i < end; ++i)
        {
            result.push_back(std::to_string(data[i]));
        }

        return result;
    }

    // Direct integer access for numeric operations
    int32_t get_int(size_t index)
    {
        std::shared_lock lock(rw_lock);
        if (index < data.size())
        {
            return data[index];
        }
        return 0;
    }
};

// Memory-optimized table class
class MemoryOptimizedTable
{
    std::string name;
    std::vector<ColumnDef> schema;
    std::vector<std::unique_ptr<OptimizedColumn>> columns;
    CacheAlignedVector<uint64_t> expires_at;
    std::shared_mutex rw_lock;
    std::atomic<size_t> row_count{0};

    // Memory optimization: pre-allocate common sizes
    static constexpr size_t INITIAL_CAPACITY = 1000;
    static constexpr size_t GROWTH_FACTOR = 2;

public:
    MemoryOptimizedTable(const std::string &name, const std::vector<ColumnDef> &schema)
        : name(name), schema(schema)
    {

        // Create optimized columns based on type
        for (const auto &col_def : schema)
        {
            if (col_def.get_type_string() == "INT")
            {
                columns.push_back(std::make_unique<CacheOptimizedIntColumn>());
            }
            else
            {
                columns.push_back(std::make_unique<CacheOptimizedStringColumn>());
            }
        }

        // Pre-allocate memory for better performance
        for (auto &col : columns)
        {
            col->reserve(INITIAL_CAPACITY);
        }
        expires_at.reserve(INITIAL_CAPACITY);
    }

    size_t insert_row(const std::vector<std::string> &values, uint64_t expiry = 0)
    {
        std::unique_lock lock(rw_lock);

        size_t current_size = row_count.load();

        for (size_t i = 0; i < columns.size() && i < values.size(); i++)
        {
            columns[i]->push_back(values[i]);
        }
        expires_at.push_back(expiry);

        return row_count++;
    }

    // Optimized bulk insert with better memory management
    size_t insert_bulk(const std::vector<std::vector<std::string>> &rows_data,
                       const std::vector<uint64_t> &expiries = {})
    {
        std::unique_lock lock(rw_lock);

        size_t new_rows = rows_data.size();
        size_t current_size = row_count.load();

        // Pre-allocate memory for all columns
        for (auto &col : columns)
        {
            col->reserve(current_size + new_rows);
        }
        expires_at.reserve(current_size + new_rows);

        // Process in batches for better cache performance
        constexpr size_t BATCH_SIZE = 64; // Process 64 rows at a time

        for (size_t batch_start = 0; batch_start < new_rows; batch_start += BATCH_SIZE)
        {
            size_t batch_end = std::min(batch_start + BATCH_SIZE, new_rows);

            for (size_t i = batch_start; i < batch_end; ++i)
            {
                const auto &values = rows_data[i];
                uint64_t expiry = (i < expiries.size()) ? expiries[i] : 0;

                for (size_t j = 0; j < columns.size() && j < values.size(); j++)
                {
                    columns[j]->push_back(values[j]);
                }
                expires_at.push_back(expiry);
            }
        }

        row_count.store(current_size + new_rows);
        return new_rows;
    }

    std::vector<std::string> get_row(size_t row_index)
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> row;
        row.reserve(columns.size());

        for (auto &col : columns)
        {
            row.push_back(col->get(row_index));
        }
        return row;
    }

    // Cache-friendly batch row retrieval
    std::vector<std::vector<std::string>> get_rows_range(size_t start, size_t count)
    {
        std::shared_lock lock(rw_lock);

        std::vector<std::vector<std::string>> rows;
        rows.reserve(count);

        size_t end = std::min(start + count, static_cast<size_t>(row_count.load()));

        for (size_t i = start; i < end; ++i)
        {
            std::vector<std::string> row;
            row.reserve(columns.size());

            for (auto &col : columns)
            {
                row.push_back(col->get(i));
            }
            rows.push_back(std::move(row));
        }

        return rows;
    }

    size_t get_row_count()
    {
        return row_count.load(std::memory_order_relaxed);
    }

    bool is_expired(size_t row_index, uint64_t current_time)
    {
        if (row_index >= static_cast<size_t>(row_count.load()))
            return false;

        std::shared_lock lock(rw_lock);
        if (row_index >= expires_at.size())
            return false;
        return (expires_at[row_index] > 0 && expires_at[row_index] < current_time);
    }

    std::shared_mutex &get_rw_lock()
    {
        return rw_lock;
    }

    const std::vector<ColumnDef> &get_schema() const
    {
        return schema;
    }

    const std::string &get_name() const
    {
        return name;
    }

    // Memory compaction to reduce fragmentation
    void compact()
    {
        std::unique_lock lock(rw_lock);

        // This would remove expired rows and defragment memory
        // For now, just trigger any internal cleanup
        uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

        // Could implement actual compaction here
        // For safety, we'll just ensure memory is properly aligned
    }
};