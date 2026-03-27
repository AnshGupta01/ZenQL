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
    DELETE,
    UNKNOWN
};

enum class DataType
{
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};
enum class CompareOp
{
    EQ,
    GT,
    LT,
    GTE,
    LTE
};

struct ColumnDef
{
    std::string name;
    DataType type;
    bool is_primary_key = false;

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
    bool if_not_exists = false;
};

struct InsertQuery
{
    std::string table_name;
    std::vector<std::vector<std::string>> rows;
    uint64_t expires_at;
};

struct SelectQuery
{
    std::string table_name;
    std::vector<std::string> columns;

    // WHERE Support
    bool has_where;
    std::string where_column;
    std::string where_value;
    CompareOp where_op = CompareOp::EQ;

    // JOIN Support
    bool is_join;
    std::string join_table;
    std::string join_condition_col1; // tableA.col
    std::string join_condition_col2; // tableB.col
    CompareOp join_op = CompareOp::EQ;

    // ORDER BY Support
    bool has_order_by = false;
    std::string order_by_column;
    bool order_desc = false;
};

struct CheckpointQuery
{
    // Empty struct
};

struct DeleteQuery
{
    std::string table_name;
    bool has_where = false;
    std::string where_column;
    std::string where_value;
    CompareOp where_op = CompareOp::EQ;
};

struct ParsedQuery
{
    QueryType type;
    std::variant<CreateTableQuery, InsertQuery, SelectQuery, CheckpointQuery, DeleteQuery> query;
};

class Parser
{
public:
    static ParsedQuery parse(const std::string &sql);
};
