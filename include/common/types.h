#pragma once
#include <cstdint>
#include <string>

// ─── Error codes ────────────────────────────────────────────────────────────
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

// ─── Column types ────────────────────────────────────────────────────────────
enum class ColType : uint8_t {
    INT      = 0,
    DECIMAL  = 1,
    VARCHAR  = 2,
    DATETIME = 3,
};

// ─── Column definition (fixed-size POD for disk persistence) ─────────────────
#pragma pack(push, 1)
struct ColumnDef {
    char    name[64];
    ColType type;
    uint16_t max_len;       // for VARCHAR; 0 for fixed types
    uint8_t  primary_key;   // 1 if PK
    uint8_t  not_null;
};
#pragma pack(pop)

static_assert(sizeof(ColumnDef) == 64 + 1 + 2 + 1 + 1, "ColumnDef layout mismatch");

// ─── Schema (stored at head of table file) ───────────────────────────────────
#pragma pack(push, 1)
struct SchemaDisk {
    uint32_t magic;          // 0xF1E431DB
    uint32_t col_count;
    char     table_name[64];
    uint32_t pk_col_index;
    uint64_t row_count;
    uint8_t  _pad[36];
    // followed by col_count * ColumnDef on disk
};
#pragma pack(pop)

static_assert(sizeof(SchemaDisk) == 120, "SchemaDisk layout mismatch");

constexpr uint32_t SCHEMA_MAGIC = 0xF1E431DB;
constexpr uint32_t MAX_COLS     = 32;

// ─── Page constants –– tuned for 4 KB OS pages ───────────────────────────────
constexpr uint32_t PAGE_SIZE       = 8192;   // 8 KB pages for better throughput
constexpr uint32_t MAX_PAGES       = 1 << 20; // 8 GB addressable per table
constexpr uint32_t INVALID_PAGE_ID = 0xFFFFFFFF;
constexpr uint32_t HEADER_PAGES    = 1;      // page 0 = schema

// ─── Fixed-width sizes for binary row layout ─────────────────────────────────
constexpr uint32_t INT_SIZE      = 8;   // int64_t
constexpr uint32_t DECIMAL_SIZE  = 8;   // double
constexpr uint32_t DATETIME_SIZE = 8;   // int64_t (unix ms)
constexpr uint32_t VARCHAR_OVERHEAD = 2; // length prefix (uint16_t)
constexpr uint32_t EXPIRY_SIZE   = 8;   // int64_t unix ms (-1 = never)
