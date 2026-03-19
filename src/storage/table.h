#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <iostream>
#include "../parser/parser.h"

class Column {
public:
    virtual ~Column() = default;
    virtual void push_back(const std::string& val) = 0;
    virtual std::string get(size_t index) = 0;
};

// Simplified typed column for storing data
class StringColumn : public Column {
    std::vector<std::string> data;
public:
    void push_back(const std::string& val) override {
        data.push_back(val);
    }
    std::string get(size_t index) override {
        if (index < data.size()) return data[index];
        return "NULL";
    }
};

class Table {
    std::string name;
    std::vector<ColumnDef> schema;
    std::vector<std::unique_ptr<Column>> columns;
    std::vector<uint64_t> expires_at; 
    std::shared_mutex rw_lock;
    size_t row_count = 0;

public:
    Table(const std::string& name, const std::vector<ColumnDef>& schema) 
        : name(name), schema(schema) {
        for (size_t i = 0; i < schema.size(); i++) {
            // For now, mapping everything to string columns to get skeletal functionality
            columns.push_back(std::make_unique<StringColumn>());
        }
    }

    size_t insert_row(const std::vector<std::string>& values, uint64_t expiry = 0) {
        std::unique_lock lock(rw_lock);
        for (size_t i = 0; i < columns.size() && i < values.size(); i++) {
            columns[i]->push_back(values[i]);
        }
        expires_at.push_back(expiry);
        return row_count++;
    }

    std::vector<std::string> get_row(size_t row_index) {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> row;
        for (auto& col : columns) {
            row.push_back(col->get(row_index));
        }
        return row;
    }

    size_t get_row_count() {
        std::shared_lock lock(rw_lock);
        return row_count;
    }

    bool is_expired(size_t row_index, uint64_t current_time) {
        std::shared_lock lock(rw_lock);
        if (row_index >= expires_at.size()) return false;
        return (expires_at[row_index] > 0 && expires_at[row_index] < current_time);
    }

    const std::vector<ColumnDef>& get_schema() const {
        return schema;
    }
};
