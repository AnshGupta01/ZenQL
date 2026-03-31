#pragma once
#include <string>
#include <string_view>
#include <vector>
#include <variant>
#include <optional>
#include "common/types.h"

// ─── Statement types ─────────────────────────────────────────────────────────

enum class StmtType {
    CREATE_TABLE,
    INSERT,
    SELECT,
    DELETE,
    UNKNOWN,
};

// ─── Column spec in CREATE TABLE ─────────────────────────────────────────────
struct ColSpec {
    std::string name;
    ColType     type;
    uint16_t    max_len;       // for VARCHAR
    bool        primary_key;
    bool        not_null;
};

// ─── WHERE clause ────────────────────────────────────────────────────────────
struct WhereClause {
    std::string col;        // may be "table.col" for joins
    std::string op;         // "=", "<", ">", "<=", ">="
    std::string rhs;        // literal (always as string; executor converts)
};

// ─── JOIN clause ─────────────────────────────────────────────────────────────
struct JoinClause {
    std::string right_table;
    std::string left_col;   // e.g. "EMPS2.DEPTID"
    std::string right_col;  // e.g. "DEPTS2.ID"
};

// ─── Parsed SELECT columns ───────────────────────────────────────────────────
struct SelectCol {
    bool        star;           // SELECT *
    std::string table;          // optional table qualifier
    std::string col;
};

// ─── Statement ───────────────────────────────────────────────────────────────
struct Statement {
    StmtType type = StmtType::UNKNOWN;

    // CREATE TABLE
    std::string            table_name;
    std::vector<ColSpec>   col_specs;

    // INSERT INTO ... VALUES (...), (...), ...
    // table_name already set
    std::vector<std::vector<std::string_view>> insert_rows;  // list of row value sets

    // SELECT
    std::vector<SelectCol>  select_cols;
    std::string             from_table;
    std::optional<JoinClause> join;
    std::optional<WhereClause> where;

    // ORDER BY
    std::string order_by_col;
    bool        order_desc = false;

    // Expiry: parsed from INSERT (last value if special __EXPIRY__ marker)
    int64_t expiry_ms = -1;
};

// ─── Parser ──────────────────────────────────────────────────────────────────
class Parser {
public:
    // Parse one statement from sql. Returns false on parse error.
    bool parse(const std::string& sql, Statement& stmt, std::string& err);

private:
    // Tokenizer
    std::vector<std::string_view> tokens_;
    size_t                   pos_;

    void  tokenize(const std::string& sql);
    bool  has()   const { return pos_ < tokens_.size(); }
    std::string_view peek() const { return tokens_[pos_]; }
    std::string_view consume() { return tokens_[pos_++]; }
    bool  expect(std::string_view tok);
    bool  try_consume(std::string_view tok);
    bool  icase_eq(std::string_view a, std::string_view b) const;

    bool parse_create(Statement& s, std::string& err);
    bool parse_insert(Statement& s, std::string& err);
    bool parse_select(Statement& s, std::string& err);
    bool parse_delete(Statement& s, std::string& err);
    bool parse_where(WhereClause& w, std::string& err);
    ColType parse_type(const std::string& tok, uint16_t& max_len);
};
