#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include "common/types.h"

// ─── Runtime schema ───────────────────────────────────────────────────────────
struct Schema {
    std::string              table_name;
    std::vector<ColumnDef>   cols;
    uint32_t                 pk_col;      // index of primary key column
    uint32_t                 row_size;    // fixed portion of serialized row
    bool                     has_varlen;  // any VARCHAR cols?

    int col_index(const std::string& name) const {
        for (int i = 0; i < (int)cols.size(); i++)
            if (cols[i].name == name) return i;
        return -1;
    }
};

// ─── In-memory Row ────────────────────────────────────────────────────────────
struct Value {
    ColType type;
    int64_t  i;      // INT / DATETIME
    double   d;      // DECIMAL
    std::string s;   // VARCHAR
};

struct Row {
    std::vector<Value> cols;
    int64_t            expiry_ms; // -1 = never expires
};

// ─── Serialization ────────────────────────────────────────────────────────────
namespace RowSerializer {
    // Returns serialized bytes (variable length)
    std::vector<uint8_t> serialize(const Schema& schema, const Row& row);
    // Deserializes bytes into Row; returns false on failure
    bool deserialize(const Schema& schema, const uint8_t* data, uint16_t len, Row& out);
    // Compute the fixed overhead for a schema (no varchars)
    uint32_t fixed_row_size(const Schema& schema);
}
