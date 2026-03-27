#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <iostream>
#include "../parser/parser.h"

class Column
{
public:
    virtual ~Column() = default;
    virtual void push_back(const std::string &val) = 0;
    virtual std::string get(size_t index) = 0;
    virtual void clear() = 0; // For persistence support
};

// Simplified typed column for storing data
class StringColumn : public Column
{
    std::vector<std::string> data;

public:
    void push_back(const std::string &val) override
    {
        data.push_back(val);
    }
    std::string get(size_t index) override
    {
        if (index < data.size())
            return data[index];
        return "NULL";
    }
    void clear() override
    {
        data.clear();
    }
};

class Table
{
    std::string name;
    std::vector<ColumnDef> schema;
    std::vector<std::unique_ptr<Column>> columns;
    std::vector<uint64_t> expires_at;
    std::shared_mutex rw_lock;
    size_t row_count = 0;
    size_t version = 0;

public:
    Table(const std::string &name, const std::vector<ColumnDef> &schema)
        : name(name), schema(schema)
    {
        for (size_t i = 0; i < schema.size(); i++)
        {
            // For now, mapping everything to string columns to get skeletal functionality
            columns.push_back(std::make_unique<StringColumn>());
        }
    }

    size_t insert_row(const std::vector<std::string> &values, uint64_t expiry = 0)
    {
        std::unique_lock lock(rw_lock);
        for (size_t i = 0; i < columns.size() && i < values.size(); i++)
        {
            columns[i]->push_back(values[i]);
        }
        expires_at.push_back(expiry);
        version++;
        return row_count++;
    }

    // Thread-safe single-row access (acquires lock internally)
    std::vector<std::string> get_row(size_t row_index)
    {
        std::shared_lock lock(rw_lock);
        return get_row_nolock(row_index);
    }

    // Lock-free variant — caller MUST hold at least a shared_lock on get_rw_lock()
    std::vector<std::string> get_row_nolock(size_t row_index) const
    {
        std::vector<std::string> row;
        row.reserve(columns.size());
        for (const auto &col : columns)
        {
            row.push_back(col->get(row_index));
        }
        return row;
    }

    size_t get_row_count()
    {
        std::shared_lock lock(rw_lock);
        return row_count;
    }

    // Lock-free row count — caller MUST hold at least a shared_lock on get_rw_lock()
    size_t get_row_count_nolock() const { return row_count; }

    // Optimized row retrieval that appends directly to a buffer to avoid copies
    void serialize_row_to_buffer(size_t row_index, std::string &buffer) const
    {
        for (const auto &col : columns)
        {
            buffer.append(col->get(row_index));
            buffer.push_back('\t');
        }
    }

    size_t get_version()
    {
        std::shared_lock lock(rw_lock);
        return version;
    }

    size_t get_version_nolock() const { return version; }

    bool is_expired(size_t row_index, uint64_t current_time)
    {
        std::shared_lock lock(rw_lock);
        return is_expired_nolock(row_index, current_time);
    }

    // Lock-free variant — caller MUST hold at least a shared_lock on get_rw_lock()
    bool is_expired_nolock(size_t row_index, uint64_t current_time) const
    {
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

    // Persistence support: Export methods for checkpointing
    std::vector<std::vector<std::string>> export_all_rows()
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::vector<std::string>> all_rows;
        all_rows.reserve(row_count);

        for (size_t i = 0; i < row_count; ++i)
        {
            std::vector<std::string> row;
            row.reserve(columns.size());
            for (auto &col : columns)
            {
                row.push_back(col->get(i));
            }
            all_rows.push_back(std::move(row));
        }
        return all_rows;
    }

    std::vector<uint64_t> export_expiry_data()
    {
        std::shared_lock lock(rw_lock);
        return expires_at; // Copy the vector
    }

    // Persistence support: Import methods for recovery
    bool import_rows(const std::vector<std::vector<std::string>> &rows,
                     const std::vector<uint64_t> &expiry_data)
    {
        std::unique_lock lock(rw_lock);

        // Clear existing data
        for (auto &col : columns)
        {
            col->clear();
        }
        expires_at.clear();
        row_count = 0;

        // Import new data
        for (size_t row_idx = 0; row_idx < rows.size(); ++row_idx)
        {
            const auto &row = rows[row_idx];
            for (size_t col_idx = 0; col_idx < columns.size() && col_idx < row.size(); ++col_idx)
            {
                columns[col_idx]->push_back(row[col_idx]);
            }
            uint64_t expiry = (row_idx < expiry_data.size()) ? expiry_data[row_idx] : 0;
            expires_at.push_back(expiry);
            row_count++;
        }
        version++;
        return true;
    }
    void clear()
    {
        std::unique_lock lock(rw_lock);
        for (auto &col : columns) { col->clear(); }
        expires_at.clear();
        row_count = 0;
        version++;
    }
};
