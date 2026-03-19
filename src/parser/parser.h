#pragma once
#include <string>
#include <vector>
#include <variant>
#include <algorithm>

enum class QueryType
{
    CREATE_TABLE,
    INSERT,
    SELECT,
    CHECKPOINT,
    UNKNOWN
};

enum class DataType
{
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

struct ColumnDef
{
    std::string name;
    DataType type;
    bool is_primary_key;

    // Helper method to convert DataType to string
    std::string get_type_string() const
    {
        switch (type)
        {
        case DataType::INT:
            return "INT";
        case DataType::DECIMAL:
            return "DECIMAL";
        case DataType::VARCHAR:
            return "VARCHAR";
        case DataType::DATETIME:
            return "DATETIME";
        default:
            return "VARCHAR";
        }
    }

    // Helper method to parse string to DataType
    static DataType parse_type_string(const std::string &type_str)
    {
        std::string upper_type = type_str;
        std::transform(upper_type.begin(), upper_type.end(), upper_type.begin(), ::toupper);

        if (upper_type == "INT")
            return DataType::INT;
        else if (upper_type == "DECIMAL")
            return DataType::DECIMAL;
        else if (upper_type == "VARCHAR" || upper_type == "TEXT")
            return DataType::VARCHAR;
        else if (upper_type == "DATETIME")
            return DataType::DATETIME;
        else
            return DataType::VARCHAR; // Default
    }
};

struct CreateTableQuery
{
    std::string table_name;
    std::vector<ColumnDef> columns;
};

struct InsertQuery
{
    std::string table_name;
    std::vector<std::string> values;
    uint64_t expires_at; // Handled per-row
};

struct SelectQuery
{
    std::string table_name;
    std::vector<std::string> columns;

    // WHERE Support
    bool has_where;
    std::string where_column;
    std::string where_value;

    // JOIN Support
    bool is_join;
    std::string join_table;
    std::string join_condition_col1; // tableA.col
    std::string join_condition_col2; // tableB.col
};

struct CheckpointQuery
{
    // Empty struct - checkpoint takes no parameters
};

struct ParsedQuery
{
    QueryType type;
    std::variant<CreateTableQuery, InsertQuery, SelectQuery, CheckpointQuery> query;
};

class Parser
{
public:
    static ParsedQuery parse(const std::string &sql);
};
