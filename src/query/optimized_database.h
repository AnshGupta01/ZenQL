#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <chrono>
#include <sstream>
#include <cstring>
#include "../storage/table.h"
#include "../index/fast_hash_index.h"
#include "../index/btree_index.h"
#include "../storage/checkpoint_manager.h"
#include "../cache/lock_free_cache.h"
#include <mutex>
#include <algorithm>
#include <thread>
#include <vector>

inline bool case_insensitive_equal(const std::string_view& s1, const std::string_view& s2) {
    if (s1.size() != s2.size()) return false;
    for (size_t i = 0; i < s1.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(s1[i])) != std::toupper(static_cast<unsigned char>(s2[i]))) return false;
    }
    return true;
}

inline std::string_view strip_prefix(std::string_view s) {
    size_t dot = s.find('.');
    return (dot == std::string_view::npos) ? s : s.substr(dot + 1);
}

struct JoinHashCache {
    std::string table_name;
    int join_col_idx;
    uint64_t build_time;
    size_t table_version;
    std::unordered_map<std::string, std::vector<size_t>> hash_map;
    std::vector<std::vector<std::string>> cached_rows;
};

class OptimizedDatabase
{
public:
    struct TableCache {
        std::string last_name;
        std::shared_ptr<Table> last_table;
        std::shared_ptr<FastHashIndex> last_pk_index;
        std::string precomputed_header;
        std::string lookup_buffer;
    };

private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    std::unordered_map<std::string, std::shared_ptr<FastHashIndex>> primary_indexes;
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> secondary_indexes;
    OptimizedLRUCache query_cache;
    std::shared_mutex rw_lock;
    std::unordered_map<std::string, std::shared_ptr<JoinHashCache>> join_hash_cache;
    std::mutex join_cache_mutex;

    thread_local static TableCache t_cache;
    std::shared_ptr<CheckpointManager> checkpoint_manager;

public:
    uint64_t get_current_time() {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    OptimizedDatabase(const std::string &data_dir = "", bool enable_persistence = false) : query_cache(2048) {
        if (enable_persistence && !data_dir.empty()) {
            std::cout << "[OptimizedDatabase] Initializing persistence: " << data_dir << std::endl;
            checkpoint_manager = std::make_shared<CheckpointManager>(data_dir);
            checkpoint_manager->recover_database(*this);
        }
    }

    std::string execute(const std::string &sql) { std::string res; execute_to_buffer(sql, res); return res; }

    void execute_to_buffer(const std::string_view &sql_view, std::string &output, uint64_t current_time = 0) {
        uint64_t now = (current_time != 0) ? current_time : get_current_time();
        auto trim_sv = [](std::string_view s) -> std::string_view {
            size_t f = s.find_first_not_of(" \n\r\t"); if (f == std::string_view::npos) return "";
            size_t l = s.find_last_not_of(" \n\r\t;"); return s.substr(f, l - f + 1);
        };
        std::string_view trimmed_sql = trim_sv(sql_view);
        if (trimmed_sql.empty()) { output.append("ERROR: Empty query.\n"); return; }

        struct P { int t; int i; };

        // Fast-path point lookup for SELECT * FROM table WHERE pk = val
        if (trimmed_sql.size() > 20 && case_insensitive_equal(trimmed_sql.substr(0, 14), "SELECT * FROM ")) {
            size_t wp = std::string_view::npos;
            for (size_t i = 14; i + 7 < trimmed_sql.size(); ++i) if (case_insensitive_equal(trimmed_sql.substr(i, 7), " WHERE ")) { wp = i; break; }
            if (wp != std::string_view::npos) {
                std::string_view tsv = trim_sv(trimmed_sql.substr(14, wp - 14));
                std::string_view rest = trimmed_sql.substr(wp + 7);
                size_t ep = rest.find('=');
                if (ep != std::string_view::npos) {
                    std::string_view wc = trim_sv(rest.substr(0, ep)), wv = trim_sv(rest.substr(ep + 1));
                    if (!tsv.empty() && !wc.empty() && !wv.empty()) {
                        std::shared_ptr<Table> table; std::shared_ptr<FastHashIndex> pk;
                        if (t_cache.last_name == tsv) { table = t_cache.last_table; pk = t_cache.last_pk_index; }
                        else {
                            std::shared_lock lock(rw_lock); auto it = tables.find(std::string(tsv));
                            if (it != tables.end()) {
                                table = it->second; pk = primary_indexes[std::string(tsv)];
                                t_cache.last_name = std::string(tsv); t_cache.last_table = table; t_cache.last_pk_index = pk;
                                const auto &schema = table->get_schema(); t_cache.precomputed_header = "SUCCESS\n";
                                for (size_t i = 0; i < schema.size(); i++) { t_cache.precomputed_header.append(schema[i].name); t_cache.precomputed_header.push_back('\t'); }
                                t_cache.precomputed_header.push_back('\n');
                            }
                        }
                        if (table && pk) {
                            const auto &schema = table->get_schema(); if (case_insensitive_equal(strip_prefix(wc), schema[0].name)) {
                                t_cache.lookup_buffer.assign(wv); 
                                if (t_cache.lookup_buffer.size() >= 2 && ((t_cache.lookup_buffer.front() == '\'' && t_cache.lookup_buffer.back() == '\'') || (t_cache.lookup_buffer.front() == '"' && t_cache.lookup_buffer.back() == '"')))
                                    t_cache.lookup_buffer = t_cache.lookup_buffer.substr(1, t_cache.lookup_buffer.size()-2);
                                auto row_opt = pk->lookup(t_cache.lookup_buffer);
                                if (row_opt.has_value() && !table->is_expired(row_opt.value(), now)) {
                                    output.append(t_cache.precomputed_header); table->serialize_row_to_buffer(row_opt.value(), output); output.push_back('\n'); return;
                                }
                            }
                        }
                    }
                }
            }
        }

        auto evaluate_comparison = [&](const std::string_view& actual, const std::string_view& target, CompareOp op, DataType type) -> bool {
            if (op == CompareOp::EQ) return actual == target;
            if (op == CompareOp::NEQ) return actual != target;
            if (type == DataType::INT || type == DataType::DECIMAL || type == DataType::DATETIME) {
                long long v1 = 0, v2 = 0; try { v1 = std::stoll(std::string(actual)); } catch(...) { return false; }
                try { v2 = std::stoll(std::string(target)); } catch(...) { return false; }
                switch(op) { case CompareOp::GT: return v1 > v2; case CompareOp::GTE: return v1 >= v2; case CompareOp::LT: return v1 < v2; case CompareOp::LTE: return v1 <= v2; default: return false; }
            } else { switch(op) { case CompareOp::GT: return actual > target; case CompareOp::GTE: return actual >= target; case CompareOp::LT: return actual < target; case CompareOp::LTE: return actual <= target; default: return false; } }
        };

        ParsedQuery pq = Parser::parse(std::string(trimmed_sql));
        if (pq.type == QueryType::UNKNOWN) { output.append("ERROR: Invalid SQL.\n"); return; }

        if (pq.type == QueryType::CREATE_TABLE) {
            auto ct = std::get<CreateTableQuery>(pq.query); if (ct.table_name.empty()) { output.append("ERROR: Missing table name.\n"); return; }
            { std::unique_lock lock(rw_lock); if (tables.count(ct.table_name)) { if (ct.if_not_exists) { output.append("SUCCESS\n"); return; } output.append("ERROR: Table exists.\n"); return; }
                auto table = std::make_shared<Table>(ct.table_name, ct.columns); tables[ct.table_name] = table; primary_indexes[ct.table_name] = std::make_shared<FastHashIndex>(); }
            output.append("SUCCESS\n"); return;
        } else if (pq.type == QueryType::INSERT) {
            auto iq = std::get<InsertQuery>(pq.query); std::shared_ptr<Table> table; std::shared_ptr<FastHashIndex> pk;
            { std::shared_lock lock(rw_lock); auto it = tables.find(iq.table_name); if (it == tables.end()) { output.append("ERROR: Table not found.\n"); return; } table = it->second; pk = primary_indexes[iq.table_name]; }
            for (const auto& row : iq.rows) { size_t rid = table->insert_row(row, iq.expires_at); if (pk) pk->insert(row[0], rid); }
            output.append("SUCCESS\n"); return;
        } else if (pq.type == QueryType::SELECT) {
            auto sq = std::get<SelectQuery>(pq.query); std::shared_ptr<Table> table; { std::shared_lock lock(rw_lock); auto it = tables.find(sq.table_name); if (it == tables.end()) { output.append("ERROR: Table not found.\n"); return; } table = it->second; }
            const auto& sch = table->get_schema(); size_t total = table->get_row_count();
            
            if (sq.is_join) {
                std::shared_ptr<Table> table2; { std::shared_lock lock(rw_lock); auto it2 = tables.find(sq.join_table); if (it2 == tables.end()) { output.append("ERROR: Join table not found.\n"); return; } table2 = it2->second; }
                const auto& sch2 = table2->get_schema(); int jc1 = -1, jc2 = -1;
                for (size_t i=0; i<sch.size(); i++) if (case_insensitive_equal(sch[i].name, strip_prefix(sq.join_condition_col1))) jc1 = (int)i;
                for (size_t i=0; i<sch2.size(); i++) if (case_insensitive_equal(sch2[i].name, strip_prefix(sq.join_condition_col2))) jc2 = (int)i;
                if (jc1 == -1 || jc2 == -1) { output.append("ERROR: Join columns not found.\n"); return; }
                std::shared_ptr<JoinHashCache> cj; { std::lock_guard<std::mutex> cl(join_cache_mutex); std::string ck = sq.join_table + "_" + std::to_string(jc2); if (join_hash_cache.count(ck)) cj = join_hash_cache[ck];
                    if (!cj) { cj = std::make_shared<JoinHashCache>(); cj->table_name = sq.join_table; cj->join_col_idx = jc2;
                        for (size_t i=0; i<table2->get_row_count(); i++) { if (table2->is_expired(i, now)) continue; auto r = table2->get_row_nolock(i); cj->hash_map[r[jc2]].push_back(cj->cached_rows.size()); cj->cached_rows.push_back(std::move(r)); }
                        join_hash_cache[ck] = cj; } }
                int jw1 = -1, jw2 = -1; if (sq.has_where) { std::string_view col = strip_prefix(sq.where_column); for (size_t i=0; i<sch.size(); i++) if (case_insensitive_equal(sch[i].name, col)) jw1 = (int)i;
                    if (jw1 == -1) for (size_t i=0; i<sch2.size(); i++) if (case_insensitive_equal(sch2[i].name, col)) jw2 = (int)i;
                    if (jw1 == -1 && jw2 == -1) { output.append("ERROR: WHERE column not found.\n"); return; } }
                uint32_t nt = std::max(1u, std::thread::hardware_concurrency()); if (total < 1000) nt = 1;
                struct JIdx { size_t r1; size_t r2c; }; std::vector<std::thread> wv; std::vector<std::vector<JIdx>> tr(nt); size_t ch = (total + nt - 1) / nt;
                for (uint32_t t=0; t<nt; t++) { wv.emplace_back([&, t]() { size_t s = t*ch, e = std::min(s+ch, total); auto& t_res = tr[t];
                    for (size_t i=s; i<e; i++) { if (table->is_expired(i, now)) continue; if (jw1 != -1 && !evaluate_comparison(table->get_row_nolock(i)[jw1], sq.where_value, sq.where_op, sch[jw1].type)) continue;
                        std::string k = table->get_row_nolock(i)[jc1]; auto it = cj->hash_map.find(k); if (it != cj->hash_map.end()) for (size_t r2i : it->second) { if (jw2 != -1 && !evaluate_comparison(cj->cached_rows[r2i][jw2], sq.where_value, sq.where_op, sch2[jw2].type)) continue; t_res.push_back({i, r2i}); } } }); }
                for (auto &w : wv) w.join();
                std::vector<JIdx> fi; for (auto& tt : tr) fi.insert(fi.end(), tt.begin(), tt.end());
                if (sq.has_order_by) { int si=-1; DataType st=DataType::VARCHAR; bool i2=false; std::string_view col=strip_prefix(sq.order_by_column);
                    for (size_t i=0; i<sch.size(); i++) if (case_insensitive_equal(sch[i].name, col)) { si=(int)i; st=sch[i].type; break; }
                    if(si==-1) for (size_t i=0; i<sch2.size(); i++) if (case_insensitive_equal(sch2[i].name, col)) { si=(int)i; st=sch2[i].type; i2=true; break; }
                    if(si==-1) { output.append("ERROR: ORDER BY column not found.\n"); return; }
                    std::sort(fi.begin(), fi.end(), [&](const JIdx& a, const JIdx& b) {
                        auto r1a = table->get_row_nolock(a.r1), r1b = table->get_row_nolock(b.r1);
                        std::string_view v1 = i2 ? (std::string_view)cj->cached_rows[a.r2c][si] : r1a[si];
                        std::string_view v2 = i2 ? (std::string_view)cj->cached_rows[b.r2c][si] : r1b[si];
                        if (st == DataType::INT || st == DataType::DECIMAL || st == DataType::DATETIME) { long long l1=0,l2=0; try{l1=std::stoll(std::string(v1));}catch(...){} try{l2=std::stoll(std::string(v2));}catch(...){} return sq.order_desc ? l1 > l2 : l1 < l2; }
                        return sq.order_desc ? v1 > v2 : v1 < v2; }); }
                std::string header = "SUCCESS\n"; std::vector<P> proj;
                if (sq.columns.size() == 1 && sq.columns[0] == "*") { for(int i=0; i<(int)sch.size(); i++) { proj.push_back({1,i}); header.append(sch[i].name); header.push_back('\t'); } for(int i=0; i<(int)sch2.size(); i++) { proj.push_back({2,i}); header.append(sch2[i].name); header.push_back('\t'); } }
                else { for (const auto& c : sq.columns) { std::string_view col = strip_prefix(c); bool f=false; for(int i=0; i<(int)sch.size(); i++) if(case_insensitive_equal(sch[i].name, col)) { proj.push_back({1,i}); header.append(sch[i].name); header.push_back('\t'); f=true; break; }
                    if(!f) for(int i=0; i<(int)sch2.size(); i++) if(case_insensitive_equal(sch2[i].name, col)) { proj.push_back({2,i}); header.append(sch2[i].name); header.push_back('\t'); f=true; break; }
                    if(!f) { output.append("ERROR: Column " + std::string(col) + " not found.\n"); return; } } }
                header.push_back('\n'); output.append(header);
                for (const auto& x : fi) { auto r1 = table->get_row_nolock(x.r1); const auto& r2 = cj->cached_rows[x.r2c];
                    for (size_t i=0; i<proj.size(); i++) { output.append(proj[i].t == 1 ? (std::string)r1[proj[i].i] : r2[proj[i].i]); output.push_back('\t'); } output.push_back('\n'); }
                if (fi.empty()) output.append("SUCCESS: 0 rows returned.\n");
                return;
            }
            std::vector<size_t> m; int w = -1; if (sq.has_where) { std::string_view col = strip_prefix(sq.where_column);
                for (size_t i=0; i<sch.size(); i++) if (case_insensitive_equal(sch[i].name, col)) { w=(int)i; break; }
                if (w == -1) { output.append("ERROR: Column " + std::string(col) + " not found.\n"); return; } }
            for (size_t i=0; i<total; i++) { if (table->is_expired(i, now)) continue; if (w != -1 && !evaluate_comparison(table->get_row_nolock(i)[w], sq.where_value, sq.where_op, sch[w].type)) continue; m.push_back(i); }
            if (sq.has_order_by) { int si=-1; DataType st=DataType::VARCHAR; std::string_view col = strip_prefix(sq.order_by_column);
                for (size_t i=0; i<sch.size(); i++) if (case_insensitive_equal(sch[i].name, col)) { si=(int)i; st=sch[i].type; break; }
                if (si == -1) { output.append("ERROR: ORDER BY column not found.\n"); return; }
                std::sort(m.begin(), m.end(), [&](size_t a, size_t b) { 
                    auto r_a = table->get_row_nolock(a), r_b = table->get_row_nolock(b);
                    std::string_view v1 = r_a[si], v2 = r_b[si];
                    if (st == DataType::INT || st == DataType::DECIMAL || st == DataType::DATETIME) { long long l1=0, l2=0; try{l1=std::stoll(std::string(v1));}catch(...){} try{l2=std::stoll(std::string(v2));}catch(...){} return sq.order_desc ? l1 > l2 : l1 < l2; }
                    return sq.order_desc ? v1 > v2 : v1 < v2; }); }
            std::string header = "SUCCESS\n"; std::vector<int> ci;
            if (sq.columns.size() == 1 && sq.columns[0] == "*") { for(int i=0; i<(int)sch.size(); i++) { ci.push_back(i); header.append(sch[i].name); header.push_back('\t'); } }
            else { for (const auto& c : sq.columns) { std::string_view t = strip_prefix(c); bool f=false; for (int i=0; i<(int)sch.size(); i++) if (case_insensitive_equal(sch[i].name, t)) { ci.push_back(i); header.append(sch[i].name); header.push_back('\t'); f=true; break; }
                if (!f) { output.append("ERROR: Column " + std::string(t) + " not found.\n"); return; } } }
            header.push_back('\n'); output.append(header);
            for (size_t x : m) { auto r = table->get_row_nolock(x);
                for (size_t i=0; i<ci.size(); i++) { output.append((std::string)r[ci[i]]); output.push_back('\t'); } output.push_back('\n'); }
            if (m.empty()) output.append("SUCCESS: 0 rows returned.\n");
            return;
        } else if (pq.type == QueryType::DELETE) {
            auto dq = std::get<DeleteQuery>(pq.query); std::shared_ptr<Table> table; { std::shared_lock lock(rw_lock); auto it = tables.find(dq.table_name); if (it == tables.end()){ output.append("ERROR: Table not found.\n"); return; } table = it->second; }
            table->clear(); if (primary_indexes.count(dq.table_name)) primary_indexes[dq.table_name]->clear(); output.append("SUCCESS\n"); return;
        }
        output.append("ERROR: Invalid SQL.\n");
    }

    std::vector<std::string> get_table_names() { std::shared_lock lock(rw_lock); std::vector<std::string> res; for (auto const& [name, _] : tables) res.push_back(name); return res; }
    std::vector<ColumnDef> export_table_schema(const std::string& name) { std::shared_lock lock(rw_lock); if (!tables.count(name)) return {}; return tables[name]->get_schema(); }
    std::vector<std::vector<std::string>> export_table_data(const std::string& name) { std::shared_lock lock(rw_lock); if (!tables.count(name)) return {}; size_t count = tables[name]->get_row_count(); std::vector<std::vector<std::string>> res; for (size_t i=0; i<count; i++) res.push_back(tables[name]->get_row_nolock(i)); return res; }
    std::vector<uint64_t> export_table_expiry(const std::string& name) { std::shared_lock lock(rw_lock); if (!tables.count(name)) return {}; return tables[name]->export_expiry_data(); }
    std::unordered_map<std::string, size_t> export_primary_index(const std::string& name) { std::shared_lock lock(rw_lock); if (!primary_indexes.count(name)) return {}; return primary_indexes[name]->export_data(); }
    bool create_table_from_schema(const std::string& name, const std::vector<ColumnDef>& sch) { std::unique_lock lock(rw_lock); if (tables.count(name)) return false; auto table = std::make_shared<Table>(name, sch); tables[name] = table; primary_indexes[name] = std::make_shared<FastHashIndex>(); return true; }
    bool load_table_data(const std::string& name, const std::vector<std::vector<std::string>>& rows, const std::vector<uint64_t>& expiry) { std::unique_lock lock(rw_lock); if (!tables.count(name)) return false; for (size_t i=0; i<rows.size(); i++) tables[name]->insert_row(rows[i], expiry[i]); return true; }
    bool load_primary_index(const std::string& name, const std::unordered_map<std::string, size_t>& data) { std::unique_lock lock(rw_lock); if (!primary_indexes.count(name)) return false; primary_indexes[name]->import_data(data); return true; }

    void clear_all_data() { std::unique_lock lock(rw_lock); tables.clear(); primary_indexes.clear(); secondary_indexes.clear(); }
    bool save_checkpoint() { if (!checkpoint_manager) return false; return checkpoint_manager->create_checkpoint(*this); }
};