#include "storage/table.h"
#include "storage/row.h"
#include <fcntl.h>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <chrono>

// ─── Schema page encoding ────────────────────────────────────────────────────
//  Page 0 layout: [SchemaDisk header] [col_count × ColumnDef]

Table::Table(const std::string& data_dir, const std::string& name, Schema schema, bool create)
    : schema_(std::move(schema)), last_data_page_(HEADER_PAGES),
      row_count_(0), data_dir_(data_dir) {
    std::string filepath = data_dir + name + ".db";
    pager_ = std::make_unique<Pager>(filepath, LRU_CACHE_CAPACITY);

    if (create || pager_->page_count() == 0) {
        // Allocate schema page (page 0)
        if (pager_->page_count() == 0) pager_->alloc_page();
        persist_schema();
    } else {
        load_schema();
    }
}

Table::~Table() {
    flush();
}

void Table::flush() {
    std::unique_lock lk(rw_);
    if (dirty_) {
        persist_schema();
        dirty_ = false;
    }
}

void Table::persist_schema() {
    Page* p = pager_->fetch_page(0);
    p->init(0);

    SchemaDisk sd{};
    sd.magic       = SCHEMA_MAGIC;
    sd.col_count   = static_cast<uint32_t>(schema_.cols.size());
    sd.pk_col_index = schema_.pk_col;
    sd.row_count   = row_count_;
    std::strncpy(sd.table_name, schema_.table_name.c_str(), sizeof(sd.table_name) - 1);

    std::memcpy(p->data, &sd, sizeof(SchemaDisk));
    ColumnDef* cdefs = reinterpret_cast<ColumnDef*>(p->data + sizeof(SchemaDisk));
    for (size_t i = 0; i < schema_.cols.size(); i++)
        cdefs[i] = schema_.cols[i];

    pager_->unpin(0);
}

void Table::load_schema() {
    Page* p = pager_->fetch_page(0);
    const SchemaDisk& sd = *reinterpret_cast<const SchemaDisk*>(p->data);
    if (sd.magic != SCHEMA_MAGIC) {
        pager_->unpin(0);
        throw std::runtime_error("Table: invalid schema magic");
    }
    schema_.table_name = sd.table_name;
    schema_.pk_col     = sd.pk_col_index;
    row_count_         = sd.row_count;
    schema_.cols.clear();
    const ColumnDef* cdefs = reinterpret_cast<const ColumnDef*>(p->data + sizeof(SchemaDisk));
    for (uint32_t i = 0; i < sd.col_count; i++)
        schema_.cols.push_back(cdefs[i]);
    pager_->unpin(0);

    // Find last data page
    last_data_page_ = std::max((uint32_t)HEADER_PAGES, pager_->page_count() - 1);
}

uint32_t Table::find_or_alloc_page(uint16_t row_len) {
    uint32_t pid = last_data_page_;
    if (pid >= pager_->page_count()) {
        pid = pager_->alloc_page();
        last_data_page_ = pid;
        Page* pg = pager_->fetch_page(pid);
        pg->init(pid);
        pager_->unpin(pid);
    }

    Page* pg = pager_->fetch_page(pid);
    uint32_t needed = row_len + SLOT_SIZE;
    if (pg->free_space() < needed) {
        pager_->unpin(pid);
        pid = pager_->alloc_page();
        last_data_page_ = pid;
        pg = pager_->fetch_page(pid);
        pg->init(pid);
    }
    pager_->unpin(pid);
    return pid;
}

RowLocation Table::insert(const Row& row) {
    auto bytes = RowSerializer::serialize(schema_, row);
    uint16_t rlen = static_cast<uint16_t>(bytes.size());

    lock_write();
    uint32_t pid = find_or_alloc_page(rlen);
    Page* pg = pager_->fetch_page(pid);
    int slot = pg->insert_row(bytes.data(), rlen);
    if (slot < 0) {
        pager_->unpin(pid);
        // alloc fresh page
        pid = pager_->alloc_page();
        last_data_page_ = pid;
        pg = pager_->fetch_page(pid);
        pg->init(pid);
        slot = pg->insert_row(bytes.data(), rlen);
    }
    row_count_++;
    dirty_ = true;
    pager_->unpin(pid);
    unlock_write();

    return {pid, static_cast<uint16_t>(slot)};
}

std::vector<RowLocation> Table::batch_insert(const std::vector<Row>& rows) {
    std::vector<RowLocation> locs;
    locs.reserve(rows.size());
    lock_write();
    for (const auto& row : rows) {
        auto bytes = RowSerializer::serialize(schema_, row);
        uint16_t rlen = static_cast<uint16_t>(bytes.size());
        uint32_t pid = find_or_alloc_page(rlen);
        Page* pg = pager_->fetch_page(pid);
        int slot = pg->insert_row(bytes.data(), rlen);
        if (slot < 0) {
            pager_->unpin(pid);
            pid = pager_->alloc_page();
            last_data_page_ = pid;
            pg = pager_->fetch_page(pid);
            pg->init(pid);
            slot = pg->insert_row(bytes.data(), rlen);
        }
        pager_->unpin(pid);
        row_count_++;
        locs.push_back({pid, static_cast<uint16_t>(slot)});
    }
    dirty_ = true;
    unlock_write();
    return locs;
}

static int64_t now_sec() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

void Table::scan(std::function<bool(const Row&, const RowLocation&)> fn) const {
    scan_range(HEADER_PAGES, pager_->page_count(), std::move(fn));
}

void Table::scan_range(uint32_t start_pid, uint32_t end_pid, 
                       std::function<bool(const Row&, const RowLocation&)> fn) const {
    int64_t now = now_sec();
    uint32_t limit = last_data_page_ + 1;
    std::shared_lock lk(rw_);
    for (uint32_t pid = start_pid; pid < end_pid && pid < limit; pid++) {
        Page* pg = pager_->fetch_page(pid);
        uint16_t sc = pg->header()->slot_count;
        for (uint16_t si = 0; si < sc; si++) {
            const Slot* sl = pg->slot(si);
            if (sl->flags & 0x01) { continue; } // tombstone
            Row row;
            bool ok = RowSerializer::deserialize(schema_, pg->data_area() + sl->offset, sl->length, row);
            if (!ok) continue;
            // Expiry check
            if (row.expiry_ms > 0 && row.expiry_ms < now) continue;
            if (!fn(row, {pid, si})) {
                pager_->unpin(pid);
                return;
            }
        }
        pager_->unpin(pid);
    }
}

std::optional<Row> Table::fetch(const RowLocation& loc) const {
    std::shared_lock lk(rw_);
    int64_t now = now_sec();
    Page* pg = pager_->fetch_page(loc.page_id);
    const Slot* sl = pg->slot(loc.slot_idx);
    if (sl->flags & 0x01) { pager_->unpin(loc.page_id); return std::nullopt; }
    Row row;
    bool ok = RowSerializer::deserialize(schema_, pg->data_area() + sl->offset, sl->length, row);
    pager_->unpin(loc.page_id);
    if (!ok) return std::nullopt;
    if (row.expiry_ms > 0 && row.expiry_ms < now) return std::nullopt;
    return row;
}
void Table::truncate() {
    row_count_ = 0;
    last_data_page_ = HEADER_PAGES;
    // We MUST re-init the first data page to clear any existing rows
    Page* pg = pager_->fetch_page(HEADER_PAGES);
    pg->init(HEADER_PAGES);
    pager_->unpin(HEADER_PAGES);
    persist_schema();
}
