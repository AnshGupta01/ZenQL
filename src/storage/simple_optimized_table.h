#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <iostream>
#include <algorithm>
#include "../parser/parser.h"

// Simple cache-friendly improvements to the existing table structure
class OptimizedStringColumn
{
private:
    std::vector<std::string> data;

public:
    void push_back(const std::string &val)
    {
        data.push_back(val);
    }

    std::string get(size_t index)
    {
        if (index < data.size())
            return data[index];
        return "NULL";
    }

    size_t size() const
    {
        return data.size();
    }

    void reserve(size_t capacity)
    {
        data.reserve(capacity);
    }

    // Bulk operations for better performance
    void insert_bulk(const std::vector<std::string> &values)
    {
        data.reserve(data.size() + values.size());
        for (const auto &val : values)
        {
            data.push_back(val);
        }
    }

    void clear()
    {
        data.clear();
    }

    const std::vector<std::string> &get_data() const
    {
        return data;
    }
};

class FastTable
{
    std::string name;
    std::vector<ColumnDef> schema;
    std::vector<std::unique_ptr<OptimizedStringColumn>> columns;
    std::vector<uint64_t> expires_at;
    std::shared_mutex rw_lock;
    size_t row_count = 0;

public:
    FastTable(const std::string &name, const std::vector<ColumnDef> &schema)
        : name(name), schema(schema)
    {

        // Create optimized columns
        for (size_t i = 0; i < schema.size(); i++)
        {
            columns.push_back(std::make_unique<OptimizedStringColumn>());
        }

        // Pre-allocate some space
        for (auto &col : columns)
        {
            col->reserve(1000);
        }
        expires_at.reserve(1000);
    }

    size_t insert_row(const std::vector<std::string> &values, uint64_t expiry = 0)
    {
        std::unique_lock lock(rw_lock);

        for (size_t i = 0; i < columns.size() && i < values.size(); i++)
        {
            columns[i]->push_back(values[i]);
        }
        expires_at.push_back(expiry);
        return row_count++;
    }

    // Optimized bulk insert
    size_t insert_bulk(const std::vector<std::vector<std::string>> &rows_data)
    {
        std::unique_lock lock(rw_lock);

        size_t new_rows = rows_data.size();

        // Reserve space for better performance
        for (auto &col : columns)
        {
            col->reserve(row_count + new_rows);
        }
        expires_at.reserve(row_count + new_rows);

        for (const auto &values : rows_data)
        {
            for (size_t i = 0; i < columns.size() && i < values.size(); i++)
            {
                columns[i]->push_back(values[i]);
            }
            expires_at.push_back(0);
            row_count++;
        }

        return new_rows;
    }

    std::vector<std::string> get_row(size_t row_index)
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> row;
        row.reserve(columns.size()); // Pre-allocate

        for (auto &col : columns)
        {
            row.push_back(col->get(row_index));
        }
        return row;
    }

    // Cache-friendly batch row access
    std::vector<std::vector<std::string>> get_rows_batch(size_t start, size_t count)
    {
        std::shared_lock lock(rw_lock);

        std::vector<std::vector<std::string>> rows;
        rows.reserve(count);

        size_t end = std::min(start + count, row_count);
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
        std::shared_lock lock(rw_lock);
        return row_count;
    }

    bool is_expired(size_t row_index, uint64_t current_time)
    {
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
};