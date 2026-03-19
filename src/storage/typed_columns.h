#pragma once
#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <cstring>
#include <stdexcept>

// Type-safe value storage
using ColumnValue = std::variant<int32_t, double, std::string, uint64_t>;

enum class ColumnType
{
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

class Column
{
public:
    virtual ~Column() = default;
    virtual void push_back(const std::string &val) = 0;
    virtual void push_back(const ColumnValue &val) = 0;
    virtual std::string get_as_string(size_t index) const = 0;
    virtual ColumnValue get_as_value(size_t index) const = 0;
    virtual size_t size() const = 0;
    virtual void reserve(size_t capacity) = 0;
    virtual void load_data(const std::vector<std::string> &data) = 0;
    virtual std::vector<std::string> export_data() const = 0;
    virtual ColumnType get_type() const = 0;
    virtual void clear() = 0;
};

// Optimized integer column using contiguous memory
class IntColumn : public Column
{
private:
    std::vector<int32_t> data;

public:
    void push_back(const std::string &val) override
    {
        try
        {
            data.push_back(std::stoi(val));
        }
        catch (const std::exception &)
        {
            data.push_back(0); // Default value for invalid input
        }
    }

    void push_back(const ColumnValue &val) override
    {
        if (std::holds_alternative<int32_t>(val))
        {
            data.push_back(std::get<int32_t>(val));
        }
        else
        {
            push_back(get_string_value(val));
        }
    }

    std::string get_as_string(size_t index) const override
    {
        if (index < data.size())
        {
            return std::to_string(data[index]);
        }
        return "NULL";
    }

    ColumnValue get_as_value(size_t index) const override
    {
        if (index < data.size())
        {
            return data[index];
        }
        return int32_t(0);
    }

    size_t size() const override
    {
        return data.size();
    }

    void reserve(size_t capacity) override
    {
        data.reserve(capacity);
    }

    void load_data(const std::vector<std::string> &input) override
    {
        data.clear();
        data.reserve(input.size());
        for (const auto &str : input)
        {
            push_back(str);
        }
    }

    std::vector<std::string> export_data() const override
    {
        std::vector<std::string> result;
        result.reserve(data.size());
        for (const auto &val : data)
        {
            result.push_back(std::to_string(val));
        }
        return result;
    }

    ColumnType get_type() const override
    {
        return ColumnType::INT;
    }

    void clear() override
    {
        data.clear();
    }

private:
    std::string get_string_value(const ColumnValue &val)
    {
        return std::visit([](const auto &v) -> std::string
                          {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, std::string>) {
                return v;
            } else {
                return std::to_string(v);
            } }, val);
    }
};

// Optimized decimal column
class DecimalColumn : public Column
{
private:
    std::vector<double> data;

public:
    void push_back(const std::string &val) override
    {
        try
        {
            data.push_back(std::stod(val));
        }
        catch (const std::exception &)
        {
            data.push_back(0.0);
        }
    }

    void push_back(const ColumnValue &val) override
    {
        if (std::holds_alternative<double>(val))
        {
            data.push_back(std::get<double>(val));
        }
        else
        {
            push_back(get_string_value(val));
        }
    }

    std::string get_as_string(size_t index) const override
    {
        if (index < data.size())
        {
            return std::to_string(data[index]);
        }
        return "NULL";
    }

    ColumnValue get_as_value(size_t index) const override
    {
        if (index < data.size())
        {
            return data[index];
        }
        return double(0.0);
    }

    size_t size() const override
    {
        return data.size();
    }

    void reserve(size_t capacity) override
    {
        data.reserve(capacity);
    }

    void load_data(const std::vector<std::string> &input) override
    {
        data.clear();
        data.reserve(input.size());
        for (const auto &str : input)
        {
            push_back(str);
        }
    }

    std::vector<std::string> export_data() const override
    {
        std::vector<std::string> result;
        result.reserve(data.size());
        for (const auto &val : data)
        {
            result.push_back(std::to_string(val));
        }
        return result;
    }

    ColumnType get_type() const override
    {
        return ColumnType::DECIMAL;
    }

    void clear() override
    {
        data.clear();
    }

private:
    std::string get_string_value(const ColumnValue &val)
    {
        return std::visit([](const auto &v) -> std::string
                          {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, std::string>) {
                return v;
            } else {
                return std::to_string(v);
            } }, val);
    }
};

// Optimized varchar column with small string optimization
class VarcharColumn : public Column
{
private:
    std::vector<std::string> data;

public:
    void push_back(const std::string &val) override
    {
        data.push_back(val);
    }

    void push_back(const ColumnValue &val) override
    {
        data.push_back(get_string_value(val));
    }

    std::string get_as_string(size_t index) const override
    {
        if (index < data.size())
        {
            return data[index];
        }
        return "NULL";
    }

    ColumnValue get_as_value(size_t index) const override
    {
        if (index < data.size())
        {
            return data[index];
        }
        return std::string("NULL");
    }

    size_t size() const override
    {
        return data.size();
    }

    void reserve(size_t capacity) override
    {
        data.reserve(capacity);
    }

    void load_data(const std::vector<std::string> &input) override
    {
        data = input;
    }

    std::vector<std::string> export_data() const override
    {
        return data;
    }

    ColumnType get_type() const override
    {
        return ColumnType::VARCHAR;
    }

    void clear() override
    {
        data.clear();
    }

private:
    std::string get_string_value(const ColumnValue &val)
    {
        return std::visit([](const auto &v) -> std::string
                          {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, std::string>) {
                return v;
            } else {
                return std::to_string(v);
            } }, val);
    }
};

// DateTime column storing as timestamp
class DateTimeColumn : public Column
{
private:
    std::vector<uint64_t> data; // Store as Unix timestamps

public:
    void push_back(const std::string &val) override
    {
        try
        {
            // Simple timestamp parsing - could be enhanced
            data.push_back(std::stoull(val));
        }
        catch (const std::exception &)
        {
            data.push_back(0);
        }
    }

    void push_back(const ColumnValue &val) override
    {
        if (std::holds_alternative<uint64_t>(val))
        {
            data.push_back(std::get<uint64_t>(val));
        }
        else
        {
            push_back(get_string_value(val));
        }
    }

    std::string get_as_string(size_t index) const override
    {
        if (index < data.size())
        {
            return std::to_string(data[index]);
        }
        return "NULL";
    }

    ColumnValue get_as_value(size_t index) const override
    {
        if (index < data.size())
        {
            return data[index];
        }
        return uint64_t(0);
    }

    size_t size() const override
    {
        return data.size();
    }

    void reserve(size_t capacity) override
    {
        data.reserve(capacity);
    }

    void load_data(const std::vector<std::string> &input) override
    {
        data.clear();
        data.reserve(input.size());
        for (const auto &str : input)
        {
            push_back(str);
        }
    }

    std::vector<std::string> export_data() const override
    {
        std::vector<std::string> result;
        result.reserve(data.size());
        for (const auto &val : data)
        {
            result.push_back(std::to_string(val));
        }
        return result;
    }

    ColumnType get_type() const override
    {
        return ColumnType::DATETIME;
    }

    void clear() override
    {
        data.clear();
    }

private:
    std::string get_string_value(const ColumnValue &val)
    {
        return std::visit([](const auto &v) -> std::string
                          {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, std::string>) {
                return v;
            } else {
                return std::to_string(v);
            } }, val);
    }
};

// Factory for creating typed columns
class ColumnFactory
{
public:
    static std::unique_ptr<Column> create_column(const std::string &type)
    {
        std::string upper_type = type;
        std::transform(upper_type.begin(), upper_type.end(), upper_type.begin(), ::toupper);

        if (upper_type == "INT")
        {
            return std::make_unique<IntColumn>();
        }
        else if (upper_type == "DECIMAL")
        {
            return std::make_unique<DecimalColumn>();
        }
        else if (upper_type == "VARCHAR" || upper_type == "TEXT")
        {
            return std::make_unique<VarcharColumn>();
        }
        else if (upper_type == "DATETIME")
        {
            return std::make_unique<DateTimeColumn>();
        }
        else
        {
            // Default to VARCHAR for unknown types
            return std::make_unique<VarcharColumn>();
        }
    }
};