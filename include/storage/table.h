#pragma once
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <optional>
#include "storage/page.h"
#include "storage/pager.h"
#include "storage/row.h"
#include "common/types.h"

// ─── RowLocation: physical address of a row ──────────────────────────────────
struct RowLocation {
    uint32_t page_id;
    uint16_t slot_idx;
};

// ─── Table: heap file + schema management ────────────────────────────────────
//  Layout of table file:
//    Page 0: schema header (SchemaDisk + array of ColumnDef)
//    Page 1+: data pages (slotted page layout)
class Table {
public:
    // Opens (or creates) a table file. Schema is provided at creation time.
    Table(const std::string& data_dir, const std::string& name, Schema schema, bool create = false);
    ~Table();

    void flush();

    const Schema& schema() const { return schema_; }
    const std::string& name() const { return schema_.table_name; }

    // Insert a row. Returns physical location.
    RowLocation insert(const Row& row);

    // Batch insert (holds table write lock for entire batch)
    std::vector<RowLocation> batch_insert(const std::vector<Row>& rows);

    // Full table scan — calls fn for each non-deleted, non-expired row.
    // fn(row, location) — return false to stop early.
    void scan(std::function<bool(const Row&, const RowLocation&)> fn) const;
    void scan_range(uint32_t start_pid, uint32_t end_pid, std::function<bool(const Row&, const RowLocation&)> fn) const;

    // Fetch a row by location (used by index lookup)
    std::optional<Row> fetch(const RowLocation& loc) const;

    uint64_t row_count() const { return row_count_; }
    void     truncate();

    // Pager (for index to access directly)
    Pager& pager() { return *pager_; }

    // Acquire/release table-level read or write lock
    void lock_read()    { rw_.lock_shared(); }
    void unlock_read()  { rw_.unlock_shared(); }
    void lock_write()   { rw_.lock(); }
    void unlock_write() { rw_.unlock(); }

private:
    void persist_schema();
    void load_schema();
    uint32_t find_or_alloc_page(uint16_t row_len);

    std::unique_ptr<Pager> pager_;
    Schema                 schema_;
    uint32_t               last_data_page_;
    uint64_t               row_count_;
    mutable std::shared_mutex rw_;
    std::string            data_dir_;
    bool                   dirty_ = false;
};
