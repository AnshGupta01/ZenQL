#pragma once
#include <string>
#include <fstream>
#include <vector>
#include <memory>
#include <filesystem>
#include <unordered_map>
#include "../parser/parser.h"

// Disk storage system for tables
class DiskStorage
{
private:
    std::string storage_path;
    std::string tables_dir;
    std::string indexes_dir;

public:
    explicit DiskStorage(const std::string &path = "data") : storage_path(path)
    {
        tables_dir = storage_path + "/tables";
        indexes_dir = storage_path + "/indexes";

        // Create directories if they don't exist
        std::filesystem::create_directories(tables_dir);
        std::filesystem::create_directories(indexes_dir);
    }

    // Save table schema to disk
    bool save_table_schema(const std::string &table_name, const std::vector<ColumnDef> &schema)
    {
        std::string schema_file = tables_dir + "/" + table_name + ".schema";
        std::ofstream file(schema_file, std::ios::binary);
        if (!file.is_open())
            return false;

        uint32_t col_count = schema.size();
        file.write(reinterpret_cast<const char *>(&col_count), sizeof(col_count));

        for (const auto &col : schema)
        {
            uint32_t name_len = col.name.length();
            std::string type_str = col.get_type_string();
            uint32_t type_len = type_str.length();
            uint8_t is_primary_key = col.is_primary_key ? 1 : 0;

            file.write(reinterpret_cast<const char *>(&name_len), sizeof(name_len));
            file.write(col.name.c_str(), name_len);
            file.write(reinterpret_cast<const char *>(&type_len), sizeof(type_len));
            file.write(type_str.c_str(), type_len);
            file.write(reinterpret_cast<const char *>(&is_primary_key), sizeof(is_primary_key));
        }

        return true;
    }

    // Load table schema from disk
    std::vector<ColumnDef> load_table_schema(const std::string &table_name)
    {
        std::vector<ColumnDef> schema;
        std::string schema_file = tables_dir + "/" + table_name + ".schema";
        std::ifstream file(schema_file, std::ios::binary);
        if (!file.is_open())
            return schema;

        uint32_t col_count;
        file.read(reinterpret_cast<char *>(&col_count), sizeof(col_count));

        for (uint32_t i = 0; i < col_count; ++i)
        {
            ColumnDef col;
            uint32_t name_len, type_len;
            uint8_t is_primary_key;

            file.read(reinterpret_cast<char *>(&name_len), sizeof(name_len));
            col.name.resize(name_len);
            file.read(&col.name[0], name_len);

            file.read(reinterpret_cast<char *>(&type_len), sizeof(type_len));
            std::string type_str;
            type_str.resize(type_len);
            file.read(&type_str[0], type_len);
            col.type = ColumnDef::parse_type_string(type_str);

            file.read(reinterpret_cast<char *>(&is_primary_key), sizeof(is_primary_key));
            col.is_primary_key = (is_primary_key == 1);

            schema.push_back(col);
        }

        return schema;
    }

    // Save table data to disk (checkpoint)
    bool save_table_data(const std::string &table_name,
                         const std::vector<std::vector<std::string>> &rows,
                         const std::vector<uint64_t> &expires_at)
    {
        std::string data_file = tables_dir + "/" + table_name + ".data";
        std::ofstream file(data_file, std::ios::binary);
        if (!file.is_open())
            return false;

        uint64_t row_count = rows.size();
        file.write(reinterpret_cast<const char *>(&row_count), sizeof(row_count));

        for (uint64_t i = 0; i < row_count; ++i)
        {
            const auto &row = rows[i];
            uint64_t expiry = (i < expires_at.size()) ? expires_at[i] : 0;

            file.write(reinterpret_cast<const char *>(&expiry), sizeof(expiry));

            uint32_t col_count = row.size();
            file.write(reinterpret_cast<const char *>(&col_count), sizeof(col_count));

            for (const auto &val : row)
            {
                uint32_t val_len = val.length();
                file.write(reinterpret_cast<const char *>(&val_len), sizeof(val_len));
                file.write(val.c_str(), val_len);
            }
        }

        return true;
    }

    // Load table data from disk
    bool load_table_data(const std::string &table_name,
                         std::vector<std::vector<std::string>> &rows,
                         std::vector<uint64_t> &expires_at)
    {
        std::string data_file = tables_dir + "/" + table_name + ".data";
        std::ifstream file(data_file, std::ios::binary);
        if (!file.is_open())
            return false;

        uint64_t row_count;
        file.read(reinterpret_cast<char *>(&row_count), sizeof(row_count));

        rows.clear();
        expires_at.clear();
        rows.reserve(row_count);
        expires_at.reserve(row_count);

        for (uint64_t i = 0; i < row_count; ++i)
        {
            uint64_t expiry;
            file.read(reinterpret_cast<char *>(&expiry), sizeof(expiry));
            expires_at.push_back(expiry);

            uint32_t col_count;
            file.read(reinterpret_cast<char *>(&col_count), sizeof(col_count));

            std::vector<std::string> row;
            row.reserve(col_count);

            for (uint32_t j = 0; j < col_count; ++j)
            {
                uint32_t val_len;
                file.read(reinterpret_cast<char *>(&val_len), sizeof(val_len));

                std::string val;
                val.resize(val_len);
                file.read(&val[0], val_len);
                row.push_back(val);
            }
            rows.push_back(row);
        }

        return true;
    }

    // Get list of existing tables
    std::vector<std::string> get_table_names()
    {
        std::vector<std::string> table_names;

        for (const auto &entry : std::filesystem::directory_iterator(tables_dir))
        {
            if (entry.is_regular_file() && entry.path().extension() == ".schema")
            {
                std::string table_name = entry.path().stem().string();
                table_names.push_back(table_name);
            }
        }

        return table_names;
    }

    // Save index to disk
    bool save_hash_index(const std::string &table_name,
                         const std::unordered_map<std::string, size_t> &index)
    {
        std::string index_file = indexes_dir + "/" + table_name + ".idx";
        std::ofstream file(index_file, std::ios::binary);
        if (!file.is_open())
            return false;

        uint64_t entry_count = index.size();
        file.write(reinterpret_cast<const char *>(&entry_count), sizeof(entry_count));

        for (const auto &[key, value] : index)
        {
            uint32_t key_len = key.length();
            file.write(reinterpret_cast<const char *>(&key_len), sizeof(key_len));
            file.write(key.c_str(), key_len);
            file.write(reinterpret_cast<const char *>(&value), sizeof(value));
        }

        return true;
    }

    // Load index from disk
    bool load_hash_index(const std::string &table_name,
                         std::unordered_map<std::string, size_t> &index)
    {
        std::string index_file = indexes_dir + "/" + table_name + ".idx";
        std::ifstream file(index_file, std::ios::binary);
        if (!file.is_open())
            return false;

        uint64_t entry_count;
        file.read(reinterpret_cast<char *>(&entry_count), sizeof(entry_count));

        index.clear();
        index.reserve(entry_count);

        for (uint64_t i = 0; i < entry_count; ++i)
        {
            uint32_t key_len;
            file.read(reinterpret_cast<char *>(&key_len), sizeof(key_len));

            std::string key;
            key.resize(key_len);
            file.read(&key[0], key_len);

            size_t value;
            file.read(reinterpret_cast<char *>(&value), sizeof(value));

            index[key] = value;
        }

        return true;
    }
};