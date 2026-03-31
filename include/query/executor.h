#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include "storage/table.h"
#include "index/bptree.h"
#include "parser/parser.h"

// ─── Result row ──────────────────────────────────────────────────────────────
struct ResultRow {
    std::vector<std::string> values;
};

struct ResultSet {
    std::vector<std::string> col_names;
    std::vector<ResultRow>   rows;
    bool ok = true;
    std::string error;
};

// ─── Catalog: all live tables + their indexes ─────────────────────────────────
struct TableEntry {
    std::shared_ptr<Table>  table;
    std::shared_ptr<BPTree> index;   // on PK col (int64 key)
};

class Catalog {
public:
    explicit Catalog(const std::string& data_dir);

    bool has_table(const std::string& name) const;
    TableEntry get(const std::string& name);
    void add(const std::string& name, TableEntry entry);
    const std::string& data_dir() const { return data_dir_; }

    // Rebuild index from existing table data
    void rebuild_index(const std::string& name, TableEntry te);

private:
    std::string data_dir_;
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, TableEntry> tables_;
};

// ─── Executor ─────────────────────────────────────────────────────────────────
class Executor {
public:
    explicit Executor(Catalog& cat);

    // Execute ONE statement (already parsed)
    ResultSet execute(const Statement& stmt);

    // Execute a multi-statement batch (newline-separated SQL)
    std::vector<ResultSet> exec_batch(const std::string& sql_batch);

private:
    Catalog& cat_;

    ResultSet exec_create(const Statement& s);
    ResultSet exec_insert(const Statement& s);
    ResultSet exec_select(const Statement& s);
    ResultSet exec_delete(const Statement& s);

    // Filter helpers
    bool row_matches(const Row& row, const Schema& schema,
                     const WhereClause& w, const std::string& table_prefix = "") const;

    std::string value_to_str(const Value& v) const;
    int64_t     parse_pk(const Value& v) const;
};
