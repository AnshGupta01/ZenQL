#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <iostream>
#include "../parser/parser.h"
#include "typed_columns.h"
#include "wal.h"
#include "disk_storage.h"

class OptimizedTable
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
    OptimizedTable(const std::string &name, const std::vector<ColumnDef> &schema,
                   std::shared_ptr<WAL> wal_ptr = nullptr,
                   std::shared_ptr<DiskStorage> storage_ptr = nullptr)
        : name(name), schema(schema), wal(wal_ptr), disk_storage(storage_ptr),
          persistence_enabled(wal_ptr != nullptr && storage_ptr != nullptr)
    {
        // Create properly typed columns
        for (const auto &col_def : schema)
        {
            columns.push_back(ColumnFactory::create_column(col_def.get_type_string()));
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

    // Highly optimized bulk insert with batch WAL logging
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

        size_t inserted = 0;
        for (size_t i = 0; i < rows_data.size(); ++i)
        {
            const auto &values = rows_data[i];
            uint64_t expiry = (i < expiries.size()) ? expiries[i] : 0;

            // Log to WAL if persistence is enabled (could be batched for even better performance)
            if (persistence_enabled && wal)
            {
                wal->log_insert(name, values, expiry);
            }

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

    // Optimized row retrieval with reduced memory allocations
    std::vector<std::string> get_row(size_t row_index)
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> row;
        row.reserve(columns.size());
        for (auto &col : columns)
        {
            row.push_back(col->get_as_string(row_index));
        }
        return row;
    }

    // Zero-copy row access for specific columns (performance optimization)
    std::string get_cell(size_t row_index, size_t col_index)
    {
        std::shared_lock lock(rw_lock);
        if (col_index < columns.size())
        {
            return columns[col_index]->get_as_string(row_index);
        }
        return "NULL";
    }

    size_t get_row_count()
    {
        std::shared_lock lock(rw_lock);
        return row_count;
    }

    bool is_expired(size_t row_index, uint64_t current_time)
    {
        // No lock needed for atomic access
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

    // Enhanced persistence methods
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

        // Convert column data to row format more efficiently
        std::vector<std::vector<std::string>> rows;
        rows.reserve(row_count);

        for (size_t i = 0; i < row_count; ++i)
        {
            std::vector<std::string> row;
            row.reserve(columns.size());
            for (auto &col : columns)
            {
                row.push_back(col->get_as_string(i));
            }
            rows.push_back(std::move(row));
        }

        bool success = disk_storage->save_table_data(name, rows, expires_at);
        if (success)
        {
            std::cout << "Checkpointed " << row_count << " rows for table: " << name << std::endl;
        }
        return success;
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
            // Clear existing data first
            for (auto &col : columns)
            {
                col->clear();
            }

            // Restore data to typed columns
            if (!rows.empty())
            {
                // Reserve space for better performance
                for (auto &col : columns)
                {
                    col->reserve(rows.size());
                }

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
            }

            expires_at = std::move(loaded_expires);
            row_count = rows.size();

            std::cout << "Recovered " << row_count << " rows for table: " << name
                      << " with typed columns" << std::endl;
            return true;
        }

        return false;
    }

    const std::string &get_name() const
    {
        return name;
    }

    // Get column type for optimization decisions
    ColumnType get_column_type(size_t col_index) const
    {
        if (col_index < columns.size())
        {
            return columns[col_index]->get_type();
        }
        return ColumnType::VARCHAR; // Default
    }

    // Perform compaction to reduce memory usage
    void compact()
    {
        std::unique_lock lock(rw_lock);
        // For now, just trigger a checkpoint
        // In a more advanced implementation, this could remove expired rows
        if (persistence_enabled)
        {
            checkpoint_to_disk();
        }
    }
};