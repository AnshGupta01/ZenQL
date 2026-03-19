#pragma once
#include <string>
#include <vector>
#include <variant>

enum class QueryType {
    CREATE_TABLE,
    INSERT,
    SELECT,
    UNKNOWN
};

enum class DataType {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

struct ColumnDef {
    std::string name;
    DataType type;
    bool is_primary_key;
};

struct CreateTableQuery {
    std::string table_name;
    std::vector<ColumnDef> columns;
};

struct InsertQuery {
    std::string table_name;
    std::vector<std::string> values;
    uint64_t expires_at; // Handled per-row
};

struct SelectQuery {
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

struct ParsedQuery {
    QueryType type;
    std::variant<CreateTableQuery, InsertQuery, SelectQuery> query;
};

class Parser {
public:
    static ParsedQuery parse(const std::string& sql);
};
