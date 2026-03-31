#include "query/executor.h"
#include <iostream>
#include "concurrency/thread_pool.h"
#include "parser/parser.h"
#include "storage/row.h"
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <cstring>
#include <filesystem>
#include <chrono>
#include <charconv>

namespace fs = std::filesystem;

// ─── Catalog ─────────────────────────────────────────────────────────────────

Catalog::Catalog(const std::string& data_dir) : data_dir_(data_dir) {
    fs::create_directories(data_dir_);
    // Load existing tables on startup
    for (const auto& entry : fs::directory_iterator(data_dir_)) {
        if (entry.path().extension() != ".db") continue;
        std::string tname = entry.path().stem().string();
        std::transform(tname.begin(), tname.end(), tname.begin(), ::toupper);
        try {
            Schema dummy;
            dummy.table_name = tname;
            auto tbl = std::make_shared<Table>(data_dir_, tname, dummy, false);
            auto idx = std::make_shared<BPTree>();
            TableEntry te{tbl, idx};
            this->rebuild_index(tname, te);
            tables_[tname] = std::move(te);
        } catch (...) {}
    }
}

bool Catalog::has_table(const std::string& name) const {
    std::shared_lock lk(mu_);
    return tables_.count(name) > 0;
}

TableEntry Catalog::get(const std::string& name) {
    std::shared_lock lk(mu_);
    return tables_.at(name);
}

void Catalog::add(const std::string& name, TableEntry entry) {
    std::unique_lock lk(mu_);
    tables_[name] = std::move(entry);
}

struct IndexSync {
    std::mutex mu;
    std::condition_variable cv;
    std::atomic<int> pending{0};
};

void Catalog::rebuild_index(const std::string& name, TableEntry te) {
    auto& schema = te.table->schema();
    uint32_t pkc = schema.pk_col;

    auto pool = ThreadPool::get_global();
    uint32_t total_p = te.table->pager().page_count();
    int n_threads = pool ? THREAD_POOL_SIZE : 1;
    uint32_t p_per_t = std::max(1u, (total_p - HEADER_PAGES + n_threads - 1) / n_threads);
    int n_tasks = 0; for (uint32_t p = HEADER_PAGES; p < total_p; p += p_per_t) n_tasks++;
    // The following line is added based on the user's instruction, assuming 's.from_table' is a placeholder
    // and the intent is to log table name and task count for index rebuilding.
    // The 'return rs;' is changed to 'return;' as rebuild_index is void.
    if (n_tasks == 0) return;

    // Collect (key, location) pairs in parallel without lock contention
    struct CollectedPairs {
        std::vector<std::pair<int64_t, RowLocation>> pairs;
        std::mutex mu;
    };
    auto collected = std::make_shared<CollectedPairs>();

    auto sync = std::make_shared<IndexSync>();
    sync->pending = n_tasks;

    for (int i = 0; i < n_tasks; i++) {
        uint32_t start = HEADER_PAGES + i * p_per_t;
        uint32_t end = std::min(total_p, start + p_per_t);
        auto task = [sync, start, end, pkc, te, collected]() {
            // Local buffer to minimize lock contention
            std::vector<std::pair<int64_t, RowLocation>> local_pairs;
            local_pairs.reserve(10000);  // Reserve space to avoid reallocations

            te.table->scan_range(start, end, [&](const Row& row, const RowLocation& loc) {
                int64_t key = 0;
                if (pkc < row.cols.size()) {
                    const Value& v = row.cols[pkc];
                    if (v.type == ColType::INT || v.type == ColType::DATETIME) key = v.i;
                    else if (v.type == ColType::DECIMAL) key = (int64_t)v.d;
                    else key = (int64_t)std::hash<std::string>{}(v.s);
                }
                local_pairs.emplace_back(key, loc);
                return true;
            });

            // Bulk append to shared collection (one lock per task instead of per row)
            if (!local_pairs.empty() && te.index) {
                std::lock_guard<std::mutex> lk(collected->mu);
                collected->pairs.insert(collected->pairs.end(),
                                       local_pairs.begin(), local_pairs.end());
            }

            std::lock_guard<std::mutex> lk(sync->mu);
            if (--sync->pending == 0) sync->cv.notify_all();
        };
        if (pool) pool->submit(std::move(task));
        else task();
    }
    if (pool) {
        std::unique_lock<std::mutex> lk(sync->mu);
        sync->cv.wait(lk, [&]{ return sync->pending == 0; });
    }

    // Build index sequentially from collected pairs (no lock contention)
    if (te.index && !collected->pairs.empty()) {
        te.index->lock_write();
        for (const auto& [key, loc] : collected->pairs) {
            te.index->insert(key, {loc.page_id, loc.slot_idx});
        }
        te.index->unlock_write();
    }
}

// ─── Executor ─────────────────────────────────────────────────────────────────

Executor::Executor(Catalog& cat) : cat_(cat) {}

std::string Executor::value_to_str(const Value& v) const {
    char buf[64];
    switch (v.type) {
        case ColType::INT:
        case ColType::DATETIME: {
            auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v.i);
            return std::string(buf, p - buf);
        }
        case ColType::DECIMAL: {
            auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v.d);
            return std::string(buf, p - buf);
        }
        case ColType::VARCHAR:
            return v.s;
    }
    return "";
}

int64_t Executor::parse_pk(const Value& v) const {
    if (v.type == ColType::INT || v.type == ColType::DATETIME) return v.i;
    if (v.type == ColType::DECIMAL) return (int64_t)v.d;
    // VARCHAR: simple hash
    size_t h = std::hash<std::string>{}(v.s);
    return (int64_t)h;
}

bool Executor::row_matches(const Row& row, const Schema& schema,
                            const WhereClause& w, const std::string& table_prefix) const {
    // Resolve column (may be "TABLE.COL" or just "COL")
    std::string col = w.col;
    auto dot = col.find('.');
    std::string col_name = (dot != std::string::npos) ? col.substr(dot+1) : col;
    std::string tbl_qual = (dot != std::string::npos) ? col.substr(0, dot) : "";

    if (!tbl_qual.empty() && !table_prefix.empty() && tbl_qual != table_prefix) return true; // not for this table, so "matches" by default (filter elsewhere)

    int ci = schema.col_index(col_name);
    if (ci < 0) return true; // Column not in this schema, assume it's for another table (join) or caught by exec_select
    const Value& v = row.cols[ci];
    std::string lhs = value_to_str(v);

    // Numeric comparison
    bool is_num = (v.type == ColType::INT || v.type == ColType::DECIMAL || v.type == ColType::DATETIME);
    bool res = false;
    if (is_num) {
        double lval = (v.type == ColType::DECIMAL) ? v.d : (double)v.i;
        double rval = std::stod(w.rhs);
        if (w.op == "=")  res = (std::abs(lval - rval) < 1e-9);
        else if (w.op == "<")  res = lval <  rval;
        else if (w.op == ">")  res = lval >  rval;
        else if (w.op == "<=") res = lval <= rval;
        else if (w.op == ">=") res = lval >= rval;
    } else {
        if (w.op == "=")  res = lhs == w.rhs;
        else if (w.op == "<")  res = lhs <  w.rhs;
        else if (w.op == ">")  res = lhs >  w.rhs;
        else if (w.op == "<=") res = lhs <= w.rhs;
        else if (w.op == ">=") res = lhs >= w.rhs;
    }
    return res;
}

ResultSet Executor::exec_create(const Statement& s) {
    ResultSet rs;
    if (cat_.has_table(s.table_name)) {
        // "IF NOT EXISTS" logic: just return OK if it exists
        rs.ok = true;
        return rs;
    }
    Schema schema;
    schema.table_name = s.table_name;
    schema.pk_col = 0;
    bool found_pk = false;
    for (size_t i = 0; i < s.col_specs.size(); i++) {
        const auto& cs = s.col_specs[i];
        ColumnDef cd{};
        std::strncpy(cd.name, cs.name.c_str(), sizeof(cd.name)-1);
        cd.type      = cs.type;
        cd.max_len   = cs.max_len;
        cd.primary_key = cs.primary_key ? 1 : 0;
        cd.not_null  = cs.not_null ? 1 : 0;
        schema.cols.push_back(cd);
        if (cs.primary_key && !found_pk) { schema.pk_col = i; found_pk = true; }
    }
    schema.row_size = RowSerializer::fixed_row_size(schema);

    auto tbl = std::make_shared<Table>(cat_.data_dir(), s.table_name, schema, true);
    auto idx = std::make_shared<BPTree>();
    cat_.add(s.table_name, {tbl, idx});
    rs.ok = true;
    return rs;
}

ResultSet Executor::exec_delete(const Statement& s) {
    ResultSet rs;
    if (!cat_.has_table(s.table_name)) {
        rs.ok = false;
        rs.error = "No such table: " + s.table_name;
        return rs;
    }
    auto te = cat_.get(s.table_name);
    te.table->lock_write();
    te.table->truncate();
    if (te.index) {
        te.index->lock_write();
        te.index->clear();
        te.index->unlock_write();
    }
    te.table->unlock_write();
    rs.ok = true;
    return rs;
}

ResultSet Executor::exec_insert(const Statement& s) {
    ResultSet rs;
    if (!cat_.has_table(s.table_name)) {
        rs.ok = false; rs.error = "No such table: " + s.table_name; return rs;
    }
    TableEntry te = cat_.get(s.table_name);
    const Schema& schema = te.table->schema();

    // Parse rows
    std::vector<Row> rows;
    rows.reserve(s.insert_rows.size());

    for (const auto& row_vals : s.insert_rows) {
        Row row;
        row.expiry_ms = -1;
        size_t nv = row_vals.size();
        size_t nc = schema.cols.size();
        row.cols.resize(nc);

        for (size_t i = 0; i < nc && i < nv; i++) {
            std::string_view sv = row_vals[i];
            row.cols[i].type = schema.cols[i].type;
            switch (schema.cols[i].type) {
                case ColType::INT:
                case ColType::DATETIME:
                    row.cols[i].i = 0;
                    std::from_chars(sv.data(), sv.data() + sv.size(), row.cols[i].i);
                    break;
                case ColType::DECIMAL:
                    row.cols[i].d = 0;
#if (defined(__APPLE__) || defined(_LIBCPP_VERSION)) && !defined(__cpp_lib_to_chars)
                    try { row.cols[i].d = std::stod(std::string(sv)); } catch (...) {}
#else
                    std::from_chars(sv.data(), sv.data() + sv.size(), row.cols[i].d);
#endif
                    break;
                case ColType::VARCHAR: row.cols[i].s = std::string(sv); break;
            }
        }
        if (nv > nc) {
            std::string_view sv = row_vals[nc];
            std::from_chars(sv.data(), sv.data() + sv.size(), row.expiry_ms);
        }
        rows.push_back(std::move(row));
    }

    auto locs = te.table->batch_insert(rows);

    // Update index for each row
    te.index->lock_write();
    uint32_t pkc = schema.pk_col;
    for (size_t i = 0; i < rows.size(); i++) {
        int64_t pk_key = 0;
        if (pkc < rows[i].cols.size()) {
            const Value& v = rows[i].cols[pkc];
            if (v.type == ColType::INT || v.type == ColType::DATETIME) pk_key = v.i;
            else if (v.type == ColType::DECIMAL) pk_key = (int64_t)v.d;
            else pk_key = (int64_t)std::hash<std::string>{}(v.s);
        }
        te.index->insert(pk_key, {locs[i].page_id, locs[i].slot_idx});
    }
    te.index->unlock_write();

    rs.ok = true;
    return rs;
}

struct QuerySync {
    std::mutex mu;
    std::condition_variable cv;
    std::atomic<int> pending{0};
    std::mutex rs_mu;
};

ResultSet Executor::exec_select(const Statement& s) {
    ResultSet rs;
    if (!cat_.has_table(s.from_table)) {
        rs.ok = false; rs.error = "No such table: " + s.from_table; return rs;
    }
    TableEntry te = cat_.get(s.from_table);
    const Schema& schema = te.table->schema();
    bool star = !s.select_cols.empty() && s.select_cols[0].star;

    auto pool = ThreadPool::get_global();
    int n_threads = pool ? THREAD_POOL_SIZE : 1;

    auto mk_col = [](const Schema& sc, const std::string& prefix) {
        std::vector<std::string> names;
        for (const auto& c : sc.cols) names.push_back(prefix + "." + c.name);
        return names;
    };
    auto split_col = [](const std::string& tc) -> std::pair<std::string,std::string> {
        auto d = tc.find('.');
        if (d == std::string::npos) return {"", tc};
        return {tc.substr(0, d), tc.substr(d+1)};
    };

    bool ob_missing = false;
    int ob_l_ci = -1, ob_r_ci = -1;
    if (!s.order_by_col.empty() && !star) {
        ob_missing = true;
        for (const auto& sc : s.select_cols) {
            if (sc.col == s.order_by_col) { ob_missing = false; break; }
        }
        if (ob_missing) {
            if (s.join.has_value()) {
                const Schema& schema2 = cat_.get(s.join->right_table).table->schema();
                ob_l_ci = schema.col_index(s.order_by_col);
                ob_r_ci = schema2.col_index(s.order_by_col);
            } else {
                ob_l_ci = schema.col_index(s.order_by_col);
            }
        }
    }

    // ── INNER JOIN path ────────────────────────────────────────────────────
    if (s.join.has_value()) {
        const JoinClause& jc = s.join.value();
        if (!cat_.has_table(jc.right_table)) {
            rs.ok = false; rs.error = "No such table: " + jc.right_table; return rs;
        }
        TableEntry te2 = cat_.get(jc.right_table);
        const Schema& schema2 = te2.table->schema();

        if (star) {
            auto n1 = mk_col(schema,  s.from_table);
            auto n2 = mk_col(schema2, jc.right_table);
            rs.col_names.insert(rs.col_names.end(), n1.begin(), n1.end());
            rs.col_names.insert(rs.col_names.end(), n2.begin(), n2.end());
        } else {
            for (const auto& sc : s.select_cols)
                rs.col_names.push_back(sc.table.empty() ? sc.col : sc.table + "." + sc.col);
        }

        auto [lt, lc] = split_col(jc.left_col);
        auto [rt, rc] = split_col(jc.right_col);
        std::string left_join_col  = lc;
        std::string right_join_col = rc;
        if (!(lt.empty() || lt == s.from_table)) std::swap(left_join_col, right_join_col);

        int r_ci = schema2.col_index(right_join_col);
        if (r_ci < 0) { rs.ok = false; rs.error = "JOIN column not found: " + right_join_col; return rs; }

        if ((uint32_t)r_ci == schema2.pk_col) {
            int l_ci = schema.col_index(left_join_col);
            uint32_t total_p = te.table->pager().page_count();
            uint32_t p_per_t = std::max(1u, (total_p - HEADER_PAGES + n_threads - 1) / n_threads);
            int n_tasks = 0; for (uint32_t p = HEADER_PAGES; p < total_p; p += p_per_t) n_tasks++;
            if (n_tasks == 0) return rs;

            auto sync = std::make_shared<QuerySync>();
            sync->pending = n_tasks;
            for (int i = 0; i < n_tasks; i++) {
                uint32_t start = HEADER_PAGES + i * p_per_t;
                uint32_t end = std::min(total_p, start + p_per_t);
                pool->submit([&, sync, start, end, l_ci, te, te2]() {
                    te.table->scan_range(start, end, [&](const Row& row1, const RowLocation&) {
                        if (s.where.has_value() && !row_matches(row1, schema, s.where.value(), s.from_table)) return true;
                        std::string jkey = value_to_str(row1.cols[l_ci]);
                        int64_t pk_key = 0;
                        try { pk_key = std::stoll(jkey); } catch(...) { pk_key = (int64_t)std::hash<std::string>{}(jkey); }
                        te2.index->lock_read();
                        auto hit = te2.index->find(pk_key);
                        te2.index->unlock_read();
                        if (hit.has_value()) {
                            auto row2_opt = te2.table->fetch({hit->page_id, hit->slot_idx});
                            if (row2_opt) {
                                ResultRow rr;
                                if (star) {
                                    for (const auto& v : row1.cols) rr.values.push_back(value_to_str(v));
                                    for (const auto& v : row2_opt->cols) rr.values.push_back(value_to_str(v));
                                } else {
                                    for (const auto& sc : s.select_cols) {
                                        if (sc.table == s.from_table || sc.table.empty()) {
                                            int ci = schema.col_index(sc.col); if (ci >= 0) rr.values.push_back(value_to_str(row1.cols[ci])); else rr.values.push_back("NULL");
                                        } else {
                                            int ci = schema2.col_index(sc.col); if (ci >= 0) rr.values.push_back(value_to_str(row2_opt->cols[ci])); else rr.values.push_back("NULL");
                                        }
                                    }
                                }
                                if (ob_missing) {
                                    if (ob_l_ci >= 0) rr.values.push_back(value_to_str(row1.cols[ob_l_ci]));
                                    else if (ob_r_ci >= 0) rr.values.push_back(value_to_str(row2_opt->cols[ob_r_ci]));
                                    else rr.values.push_back("NULL");
                                }
                                bool match1 = !s.where.has_value() || row_matches(row1, schema,  s.where.value(), s.from_table);
                                bool match2 = !s.where.has_value() || row_matches(*row2_opt, schema2, s.where.value(), jc.right_table);
                                if (match1 && match2) {
                                    std::lock_guard<std::mutex> lk(sync->rs_mu);
                                    rs.rows.push_back(std::move(rr));
                                }
                            }
                        }
                        return true;
                    });
                    std::lock_guard<std::mutex> lk(sync->mu);
                    if (--sync->pending == 0) sync->cv.notify_all();
                });
            }
            std::unique_lock<std::mutex> lk(sync->mu);
            sync->cv.wait(lk, [&]{ return sync->pending == 0; });
            return rs;
        }

        std::unordered_map<std::string, std::vector<Row>> hash_table;
        std::mutex ht_mu;
        uint32_t r_total_p = te2.table->pager().page_count();
        uint32_t r_p_per_t = std::max(1u, (r_total_p - HEADER_PAGES + n_threads - 1) / n_threads);
        int b_tasks = 0; for (uint32_t p = HEADER_PAGES; p < r_total_p; p += r_p_per_t) b_tasks++;
        if (b_tasks > 0) {
            auto sync = std::make_shared<QuerySync>();
            sync->pending = b_tasks;
            for (int i = 0; i < b_tasks; i++) {
                uint32_t start = HEADER_PAGES + i * r_p_per_t;
                uint32_t end = std::min(r_total_p, start + r_p_per_t);
                pool->submit([&, sync, start, end, r_ci, te2]() {
                    te2.table->scan_range(start, end, [&](const Row& row2, const RowLocation&) {
                        std::string key = value_to_str(row2.cols[r_ci]);
                        std::lock_guard<std::mutex> lk(ht_mu);
                        hash_table[key].push_back(row2);
                        return true;
                    });
                    std::lock_guard<std::mutex> lk(sync->mu);
                    if (--sync->pending == 0) sync->cv.notify_all();
                });
            }
            std::unique_lock<std::mutex> lk(sync->mu);
            sync->cv.wait(lk, [&]{ return sync->pending == 0; });
        }
        int l_ci = schema.col_index(left_join_col);
        uint32_t l_total_p = te.table->pager().page_count();
        uint32_t l_p_per_t = std::max(1u, (l_total_p - HEADER_PAGES + n_threads - 1) / n_threads);
        int p_tasks = 0; for (uint32_t p = HEADER_PAGES; p < l_total_p; p += l_p_per_t) p_tasks++;
        if (p_tasks > 0) {
            auto sync = std::make_shared<QuerySync>();
            sync->pending = p_tasks;
            for (int i = 0; i < p_tasks; i++) {
                uint32_t start = HEADER_PAGES + i * l_p_per_t;
                uint32_t end = std::min(l_total_p, start + l_p_per_t);
                pool->submit([&, sync, start, end, l_ci, te, te2]() {
                    te.table->scan_range(start, end, [&](const Row& row1, const RowLocation&) {
                        std::string jkey = value_to_str(row1.cols[l_ci]);
                        std::vector<Row> matches;
                        { std::lock_guard<std::mutex> lk(ht_mu); auto it = hash_table.find(jkey); if (it != hash_table.end()) matches = it->second; }
                        for (const auto& row2 : matches) {
                            ResultRow rr;
                            if (star) {
                                for (const auto& v : row1.cols) rr.values.push_back(value_to_str(v));
                                for (const auto& v : row2.cols) rr.values.push_back(value_to_str(v));
                            } else {
                                for (const auto& sc : s.select_cols) {
                                    if (sc.table == s.from_table || sc.table.empty()) {
                                        int ci = schema.col_index(sc.col); if (ci >= 0) rr.values.push_back(value_to_str(row1.cols[ci]));
                                        else rr.values.push_back("NULL");
                                    } else {
                                        int ci = schema2.col_index(sc.col); if (ci >= 0) rr.values.push_back(value_to_str(row2.cols[ci]));
                                        else rr.values.push_back("NULL");
                                    }
                                }
                                if (ob_missing) {
                                    if (ob_l_ci >= 0) rr.values.push_back(value_to_str(row1.cols[ob_l_ci]));
                                    else if (ob_r_ci >= 0) rr.values.push_back(value_to_str(row2.cols[ob_r_ci]));
                                    else rr.values.push_back("NULL");
                                }
                            }
                            bool match1 = !s.where.has_value() || row_matches(row1, schema,  s.where.value(), s.from_table);
                            bool match2 = !s.where.has_value() || row_matches(row2, schema2, s.where.value(), jc.right_table);
                            if (match1 && match2) {
                                std::lock_guard<std::mutex> lk(sync->rs_mu);
                                rs.rows.push_back(std::move(rr));
                            }
                        }
                        return true;
                    });
                    std::lock_guard<std::mutex> lk(sync->mu);
                    if (--sync->pending == 0) sync->cv.notify_all();
                });
            }
            std::unique_lock<std::mutex> lk(sync->mu);
            sync->cv.wait(lk, [&]{ return sync->pending == 0; });
        }
        return rs;
    }

    if (star) {
        for (const auto& c : schema.cols) rs.col_names.push_back(c.name);
    } else {
        for (const auto& sc : s.select_cols) {
            if (schema.col_index(sc.col) < 0 && !s.join.has_value()) {
                rs.ok = false; rs.error = "Unknown column: " + sc.col; return rs;
            }
            rs.col_names.push_back(sc.col);
        }
    }

    if (s.where.has_value() && s.where->op == "=") {
        std::string col = s.where->col;
        auto dot = col.find('.'); if (dot != std::string::npos) col = col.substr(dot+1);
        int ci = schema.col_index(col);
        if ((uint32_t)ci == schema.pk_col) {
            int64_t pk_key = 0;
            try { pk_key = std::stoll(s.where->rhs); } catch (...) { pk_key = (int64_t)std::hash<std::string>{}(s.where->rhs); }
            if (!te.index) { rs.ok = false; rs.error = "No index on PK"; return rs; }
            te.index->lock_read();
            auto hit = te.index->find(pk_key);
            te.index->unlock_read();
            if (hit.has_value()) {
                auto row = te.table->fetch({hit->page_id, hit->slot_idx});
                if (row) {
                    ResultRow rr;
                    if (star) for (const auto& v : row->cols) rr.values.push_back(value_to_str(v));
                    else for (const auto& sc : s.select_cols) {
                        int ci = schema.col_index(sc.col);
                        if (ci < 0) { rs.ok = false; rs.error = "Unknown column: " + sc.col; return rs; }
                        rr.values.push_back(value_to_str(row->cols[ci]));
                    }
                    rs.rows.push_back(std::move(rr));
                }
            }
            return rs;
        }
    }

    uint32_t total_p = te.table->pager().page_count();
    uint32_t p_per_t = std::max(1u, (total_p - HEADER_PAGES + n_threads - 1) / n_threads);
    int n_tasks = 0; for (uint32_t p = HEADER_PAGES; p < total_p; p += p_per_t) n_tasks++;
    if (n_tasks > 0) {
        auto sync = std::make_shared<QuerySync>();
        sync->pending = n_tasks;
        for (int i = 0; i < n_tasks; i++) {
            uint32_t start = HEADER_PAGES + i * p_per_t;
            uint32_t end = std::min(total_p, start + p_per_t);
            pool->submit([&, sync, start, end, te]() {
                te.table->scan_range(start, end, [&](const Row& row, const RowLocation&) {
                    if (s.where.has_value() && !row_matches(row, schema, s.where.value(), s.from_table)) return true;
                    ResultRow rr;
                    if (star) for (const auto& v : row.cols) rr.values.push_back(value_to_str(v));
                    else for (const auto& sc : s.select_cols) {
                        int ci = schema.col_index(sc.col);
                        if (ci < 0) {
                            std::lock_guard<std::mutex> lk(sync->mu); // using mu for fatal error
                            rs.ok = false; rs.error = "Unknown column: " + sc.col; 
                            return false; 
                        }
                        rr.values.push_back(value_to_str(row.cols[ci]));
                    }
                    if (ob_missing && ob_l_ci >= 0) {
                        rr.values.push_back(value_to_str(row.cols[ob_l_ci]));
                    }
                    std::lock_guard<std::mutex> lk(sync->rs_mu);
                    rs.rows.push_back(std::move(rr));
                    return true;
                });
                std::lock_guard<std::mutex> lk(sync->mu);
                if (--sync->pending == 0) sync->cv.notify_all();
            });
        }
        std::unique_lock<std::mutex> lk(sync->mu);
        sync->cv.wait(lk, [&]{ return sync->pending == 0; });
    }

    if (!s.order_by_col.empty()) {
        int col_idx = -1;
        if (ob_missing) col_idx = (int)rs.col_names.size();
        else {
            for (size_t i = 0; i < rs.col_names.size(); i++) {
                std::string cn = rs.col_names[i];
                auto dot = cn.find('.');
                if (cn == s.order_by_col || (dot != std::string::npos && cn.substr(dot+1) == s.order_by_col)) {
                    col_idx = (int)i; break;
                }
            }
        }
        if (col_idx != -1) {
            std::sort(rs.rows.begin(), rs.rows.end(), [&](const ResultRow& a, const ResultRow& b) {
                try {
                    double da = std::stod(a.values[col_idx]);
                    double db = std::stod(b.values[col_idx]);
                    if (s.order_desc) return da > db;
                    return da < db;
                } catch (...) {
                    if (s.order_desc) return a.values[col_idx] > b.values[col_idx];
                    return a.values[col_idx] < b.values[col_idx];
                }
            });
        }
        if (ob_missing) {
            for (auto& r : rs.rows) r.values.pop_back();
        }
    }

    return rs;
}

ResultSet Executor::execute(const Statement& stmt) {
    switch (stmt.type) {
        case StmtType::CREATE_TABLE: return exec_create(stmt);
        case StmtType::INSERT:       return exec_insert(stmt);
        case StmtType::SELECT:       return exec_select(stmt);
        case StmtType::DELETE:       return exec_delete(stmt);
        default: { ResultSet rs; rs.ok = false; rs.error = "Unknown statement"; return rs; }
    }
}

std::vector<ResultSet> Executor::exec_batch(const std::string& sql_batch) {
    std::vector<ResultSet> results;
    Parser parser;
    
    std::vector<Statement> insert_batch;
    std::string current_insert_table;

    auto flush_inserts = [&]() {
        if (insert_batch.empty()) return;
        if (!cat_.has_table(current_insert_table)) {
            for (size_t i = 0; i < insert_batch.size(); ++i) {
                ResultSet rs; rs.ok = false; rs.error = "No such table";
                results.push_back(rs);
            }
            insert_batch.clear();
            return;
        }

        TableEntry te = cat_.get(current_insert_table);
        const Schema& schema = te.table->schema();
        std::vector<Row> rows;
        rows.reserve(insert_batch.size());

        for (const auto& s : insert_batch) {
            for (const auto& row_vals : s.insert_rows) {
                Row row;
                row.expiry_ms = -1;
                size_t nv = row_vals.size();
                size_t nc = schema.cols.size();
                row.cols.resize(nc);
                for (size_t i = 0; i < nc && i < nv; i++) {
                    std::string_view sv = row_vals[i];
                    row.cols[i].type = schema.cols[i].type;
                    switch (schema.cols[i].type) {
                        case ColType::INT:
                        case ColType::DATETIME:
                            row.cols[i].i = 0;
                            std::from_chars(sv.data(), sv.data() + sv.size(), row.cols[i].i);
                            break;
                        case ColType::DECIMAL:
                            row.cols[i].d = 0;
#if (defined(__APPLE__) || defined(_LIBCPP_VERSION)) && !defined(__cpp_lib_to_chars)
                            try { row.cols[i].d = std::stod(std::string(sv)); } catch (...) {}
#else
                            std::from_chars(sv.data(), sv.data() + sv.size(), row.cols[i].d);
#endif
                            break;
                        case ColType::VARCHAR: row.cols[i].s = std::string(sv); break;
                    }
                }
                if (nv > nc) {
                    std::string_view sv = row_vals[nc];
                    std::from_chars(sv.data(), sv.data() + sv.size(), row.expiry_ms);
                }
                rows.push_back(std::move(row));
            }
        }

        auto locs = te.table->batch_insert(rows);

        te.index->lock_write();
        uint32_t pkc = schema.pk_col;
        for (size_t i = 0; i < rows.size(); i++) {
            int64_t pk_key = 0;
            if (pkc < rows[i].cols.size()) {
                const Value& v = rows[i].cols[pkc];
                if (v.type == ColType::INT || v.type == ColType::DATETIME) pk_key = v.i;
                else if (v.type == ColType::DECIMAL) pk_key = (int64_t)v.d;
                else pk_key = (int64_t)std::hash<std::string>{}(v.s);
            }
            te.index->insert(pk_key, {locs[i].page_id, locs[i].slot_idx});
        }
        te.index->unlock_write();

        for(size_t i = 0; i < insert_batch.size(); ++i) {
            ResultSet rs; rs.ok = true;
            results.push_back(rs);
        }
        insert_batch.clear();
    };

    auto handle_stmt = [&](const std::string& sql) {
        Statement stmt;
        std::string err;
        if (!parser.parse(sql, stmt, err)) {
            flush_inserts();
            ResultSet rs; rs.ok = false; rs.error = err;
            results.push_back(rs);
            return;
        }
        if (stmt.type == StmtType::INSERT) {
            if (!insert_batch.empty() && stmt.table_name != current_insert_table) {
                flush_inserts();
            }
            current_insert_table = stmt.table_name;
            insert_batch.push_back(std::move(stmt));
        } else {
            flush_inserts();
            results.push_back(execute(stmt));
        }
    };

    std::istringstream iss(sql_batch);
    std::string line;
    std::string acc;
    while (std::getline(iss, line)) {
        while (!line.empty() && std::isspace((unsigned char)line.back())) line.pop_back();
        if (line.empty()) {
            if (!acc.empty()) {
                handle_stmt(acc);
                acc.clear();
            }
            continue;
        }
        acc += " " + line;
        if (line.back() == ';') {
            handle_stmt(acc);
            acc.clear();
        }
    }
    if (!acc.empty()) {
        handle_stmt(acc);
    }
    flush_inserts();
    
    return results;
}
