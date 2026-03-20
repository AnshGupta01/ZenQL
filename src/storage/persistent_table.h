#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <iostream>
#include "../parser/parser.h"
#include "wal.h"
#include "disk_storage.h"

class Column
{
public:
    virtual ~Column() = default;
    virtual void push_back(const std::string &val) = 0;
    virtual std::string get(size_t index) = 0;
    virtual size_t size() const = 0;
    virtual void reserve(size_t capacity) = 0;
    virtual void load_data(const std::vector<std::string> &data) = 0;
    virtual std::vector<std::string> export_data() const = 0;
};

// Optimized string column with better memory management
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
    size_t size() const override
    {
        return data.size();
    }
    void reserve(size_t capacity) override
    {
        data.reserve(capacity);
    }
    void load_data(const std::vector<std::string> &loaded_data) override
    {
        data = loaded_data;
    }
    std::vector<std::string> export_data() const override
    {
        return data;
    }
};

class PersistentTable
{
    std::string name;
    std::vector<ColumnDef> schema;
    std::vector<std::unique_ptr<Column>> columns;
    std::vector<uint64_t> expires_at;
    std::shared_mutex rw_lock;
    size_t row_count = 0;

    // Persistence components
    std::shared_ptr<WAL> wal;
    std::shared_ptr<DiskStorage> disk_storage;
    bool persistence_enabled;

public:
    PersistentTable(const std::string &name, const std::vector<ColumnDef> &schema,
                    std::shared_ptr<WAL> wal_ptr = nullptr,
                    std::shared_ptr<DiskStorage> storage_ptr = nullptr)
        : name(name), schema(schema), wal(wal_ptr), disk_storage(storage_ptr),
          persistence_enabled(wal_ptr != nullptr && storage_ptr != nullptr)
    {
        for (size_t i = 0; i < schema.size(); i++)
        {
            columns.push_back(std::make_unique<StringColumn>());
        }

        // Try to recover from disk if persistence is enabled
        if (persistence_enabled)
        {
            recover_from_disk();
        }
    }

    size_t insert_row(const std::vector<std::string> &values, uint64_t expiry = 0)
    {
        std::unique_lock lock(rw_lock);

        // Log to WAL first if persistence is enabled
        if (persistence_enabled && wal)
        {
            wal->log_insert(name, values, expiry);
        }

        for (size_t i = 0; i < columns.size() && i < values.size(); i++)
        {
            columns[i]->push_back(values[i]);
        }
        expires_at.push_back(expiry);
        return row_count++;
    }

    size_t insert_bulk(const std::vector<std::vector<std::string>> &rows_data,
                       const std::vector<uint64_t> &expiries = {})
    {
        std::unique_lock lock(rw_lock);

        // Reserve space for better performance
        size_t new_rows = rows_data.size();
        for (auto &col : columns)
        {
            col->reserve(row_count + new_rows);
        }
        expires_at.reserve(row_count + new_rows);

        // Batch WAL logging - CRITICAL OPTIMIZATION
        if (persistence_enabled && wal)
        {
            wal->log_insert_batch(name, rows_data, expiries);
        }

        // Insert all rows into memory
        size_t inserted = 0;
        for (size_t i = 0; i < rows_data.size(); ++i)
        {
            const auto &values = rows_data[i];
            uint64_t expiry = (i < expiries.size()) ? expiries[i] : 0;

            for (size_t j = 0; j < columns.size() && j < values.size(); j++)
            {
                columns[j]->push_back(values[j]);
            }
            expires_at.push_back(expiry);
            ++inserted;
        }

        row_count += inserted;
        return inserted;
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

    // Persistence methods
    bool checkpoint_to_disk()
    {
        if (!persistence_enabled)
            return false;

        std::shared_lock lock(rw_lock);

        // Save schema
        if (!disk_storage->save_table_schema(name, schema))
        {
            return false;
        }

        // Convert column data to row format
        std::vector<std::vector<std::string>> rows;
        rows.reserve(row_count);

        for (size_t i = 0; i < row_count; ++i)
        {
            std::vector<std::string> row;
            row.reserve(columns.size());
            for (auto &col : columns)
            {
                row.push_back(col->get(i));
            }
            rows.push_back(std::move(row));
        }

        return disk_storage->save_table_data(name, rows, expires_at);
    }

    bool recover_from_disk()
    {
        if (!persistence_enabled)
            return false;

        // Load schema
        auto loaded_schema = disk_storage->load_table_schema(name);
        if (loaded_schema.empty())
        {
            return true; // No existing data, fresh table
        }

        // Verify schema compatibility
        if (loaded_schema.size() != schema.size())
        {
            std::cerr << "Schema mismatch for table: " << name << std::endl;
            return false;
        }

        // Load data
        std::vector<std::vector<std::string>> rows;
        std::vector<uint64_t> loaded_expires;

        if (disk_storage->load_table_data(name, rows, loaded_expires))
        {
            // Restore data to columns
            for (size_t i = 0; i < columns.size(); ++i)
            {
                std::vector<std::string> col_data;
                col_data.reserve(rows.size());
                for (const auto &row : rows)
                {
                    if (i < row.size())
                    {
                        col_data.push_back(row[i]);
                    }
                    else
                    {
                        col_data.push_back("NULL");
                    }
                }
                columns[i]->load_data(col_data);
            }

            expires_at = std::move(loaded_expires);
            row_count = rows.size();

            std::cout << "Recovered " << row_count << " rows for table: " << name << std::endl;
            return true;
        }

        return false;
    }

    const std::string &get_name() const
    {
        return name;
    }
};