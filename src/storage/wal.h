#pragma once
#include <string>
#include <fstream>
#include <mutex>
#include <vector>
#include <memory>
#include <chrono>

// Write-Ahead Log for data persistence
class WAL
{
private:
    std::string wal_file_path;
    std::ofstream wal_file;
    std::mutex wal_mutex;
    size_t log_sequence_number = 0;
    size_t checkpoint_threshold = 10000; // Force checkpoint after 10K operations

public:
    explicit WAL(const std::string &path) : wal_file_path(path)
    {
        wal_file.open(wal_file_path, std::ios::app | std::ios::binary);
        if (!wal_file.is_open())
        {
            throw std::runtime_error("Failed to open WAL file: " + wal_file_path);
        }
    }

    ~WAL()
    {
        if (wal_file.is_open())
        {
            wal_file.close();
        }
    }

    // Log a CREATE TABLE operation
    size_t log_create_table(const std::string &table_name, const std::string &schema_def)
    {
        std::lock_guard<std::mutex> lock(wal_mutex);
        size_t lsn = ++log_sequence_number;

        uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        uint8_t op_type = 1; // CREATE_TABLE
        uint32_t table_name_len = table_name.length();
        uint32_t schema_len = schema_def.length();

        wal_file.write(reinterpret_cast<const char *>(&lsn), sizeof(lsn));
        wal_file.write(reinterpret_cast<const char *>(&timestamp), sizeof(timestamp));
        wal_file.write(reinterpret_cast<const char *>(&op_type), sizeof(op_type));
        wal_file.write(reinterpret_cast<const char *>(&table_name_len), sizeof(table_name_len));
        wal_file.write(table_name.c_str(), table_name_len);
        wal_file.write(reinterpret_cast<const char *>(&schema_len), sizeof(schema_len));
        wal_file.write(schema_def.c_str(), schema_len);
        wal_file.flush();

        return lsn;
    }

    // Log an INSERT operation
    size_t log_insert(const std::string &table_name, const std::vector<std::string> &values, uint64_t expiry = 0)
    {
        std::lock_guard<std::mutex> lock(wal_mutex);
        size_t lsn = ++log_sequence_number;

        uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        uint8_t op_type = 2; // INSERT
        uint32_t table_name_len = table_name.length();
        uint32_t value_count = values.size();

        wal_file.write(reinterpret_cast<const char *>(&lsn), sizeof(lsn));
        wal_file.write(reinterpret_cast<const char *>(&timestamp), sizeof(timestamp));
        wal_file.write(reinterpret_cast<const char *>(&op_type), sizeof(op_type));
        wal_file.write(reinterpret_cast<const char *>(&table_name_len), sizeof(table_name_len));
        wal_file.write(table_name.c_str(), table_name_len);
        wal_file.write(reinterpret_cast<const char *>(&value_count), sizeof(value_count));
        wal_file.write(reinterpret_cast<const char *>(&expiry), sizeof(expiry));

        for (const auto &value : values)
        {
            uint32_t val_len = value.length();
            wal_file.write(reinterpret_cast<const char *>(&val_len), sizeof(val_len));
            wal_file.write(value.c_str(), val_len);
        }
        wal_file.flush();

        check_checkpoint();
        return lsn;
    }

    // Log a checkpoint operation
    size_t log_checkpoint()
    {
        std::lock_guard<std::mutex> lock(wal_mutex);
        size_t lsn = ++log_sequence_number;

        uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        uint8_t op_type = 3; // CHECKPOINT

        wal_file.write(reinterpret_cast<const char *>(&lsn), sizeof(lsn));
        wal_file.write(reinterpret_cast<const char *>(&timestamp), sizeof(timestamp));
        wal_file.write(reinterpret_cast<const char *>(&op_type), sizeof(op_type));
        wal_file.flush();

        return lsn;
    }

    size_t get_current_lsn()
    {
        std::lock_guard<std::mutex> lock(wal_mutex);
        return log_sequence_number;
    }

private:
    void check_checkpoint()
    {
        if (log_sequence_number % checkpoint_threshold == 0)
        {
            // Trigger background checkpoint
            log_checkpoint();
        }
    }
};