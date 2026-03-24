#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <chrono>
#include "../storage/table.h"
#include "../index/hash_index.h"
#include "../index/btree_index.h"
#include "../parser/parser.h"
#include "../cache/lru_cache.h"

class Database {
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    std::unordered_map<std::string, std::shared_ptr<HashIndex>> primary_indexes;
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> secondary_indexes;
    LRUCache query_cache; 
    std::shared_mutex rw_lock;

    uint64_t get_current_time() {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

public:
    Database() : query_cache(1024) {} 

    std::string execute(const std::string& sql) {
        ParsedQuery query = Parser::parse(sql);
        
        if (query.type == QueryType::CREATE_TABLE) {
            auto ct = std::get<CreateTableQuery>(query.query);
            std::unique_lock lock(rw_lock);
            tables[ct.table_name] = std::make_shared<Table>(ct.table_name, ct.columns);
            primary_indexes[ct.table_name] = std::make_shared<HashIndex>();
            return "SUCCESS: Table " + ct.table_name + " created successfully.\n";
        }
        else if (query.type == QueryType::INSERT) {
            auto iq = std::get<InsertQuery>(query.query);
            std::shared_ptr<Table> table;
            {
                std::shared_lock lock(rw_lock);
                auto it = tables.find(iq.table_name);
                if (it == tables.end()) return "ERROR: Table not found.\n";
                table = it->second;
            }
            
            size_t row_id = table->insert_row(iq.values, iq.expires_at);
            if (!iq.values.empty()) {
                primary_indexes[iq.table_name]->insert(iq.values[0], row_id);
            }
            
            query_cache.invalidate(iq.table_name + "_all"); 
            return "SUCCESS: 1 row inserted.\n";
        }
        else if (query.type == QueryType::SELECT) {
            auto sq = std::get<SelectQuery>(query.query);
            std::string cache_key = sq.table_name + "_all";

            // If it's a simple SELECT * (no join, no where), check cache
            if (!sq.is_join && !sq.has_where) {
                auto cached_result = query_cache.get(cache_key);
                if (cached_result.has_value()) return cached_result.value();
            }

            // Find table once
            std::shared_ptr<Table> table;
            {
                std::shared_lock lock(rw_lock);
                auto it = tables.find(sq.table_name);
                if (it == tables.end()) return "ERROR: Table not found.\n";
                table = it->second;
            }
            
            // Lock the table ONCE for the entire scan; use _nolock variants inside loops
            std::shared_lock table_lock(table->get_rw_lock());
            size_t total_rows = table->get_row_count_nolock();
            auto schema = table->get_schema();
            uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // Optimization: Index Check for Primary Key lookup (Minimal Locking)
            if (sq.has_where && !sq.is_join) {
                // Determine if filtering on PK (Column 0)
                bool is_pk = false;
                std::string up_where = sq.where_column;
                std::string up_sname = schema[0].name;
                std::transform(up_where.begin(), up_where.end(), up_where.begin(), ::toupper);
                std::transform(up_sname.begin(), up_sname.end(), up_sname.begin(), ::toupper);
                if (up_where == up_sname) is_pk = true;

                if (is_pk) {
                    auto idx = primary_indexes[sq.table_name];
                    auto row_opt = idx->lookup(sq.where_value);
                    if (row_opt.has_value()) {
                        size_t i = row_opt.value();
                        if (!table->is_expired_nolock(i, now)) {
                            auto row = table->get_row_nolock(i);
                            std::string result = "SUCCESS\n";
                            for (const auto& col : schema) result += col.name + "\t";
                            result += "\n";
                            for (const auto& val : row) result += val + "\t";
                            result += "\n";
                            return result;
                        }
                    }
                    return "SUCCESS\n0 rows returned.\n";
                }
            }

            std::string result = "SUCCESS\n";
            result.reserve(4096); 
            // If JOIN logic
            if (sq.is_join) {
                std::shared_ptr<Table> table2;
                {
                    std::shared_lock lock(rw_lock);
                    auto it = tables.find(sq.join_table);
                    if (it == tables.end()) return "ERROR: Join Table not found.\n";
                    table2 = it->second;
                }
                
                auto schema2 = table2->get_schema();
                // Join Headers
                for (const auto& col : schema) result += sq.table_name + "." + col.name + "\t";
                for (const auto& col : schema2) result += sq.join_table + "." + col.name + "\t";
                result += "\n";

                // ... Hash Join logic ...
                int join_col1_idx = -1;
                for (size_t i = 0; i < schema.size(); i++) {
                    std::string s_up = schema[i].name;
                    std::transform(s_up.begin(), s_up.end(), s_up.begin(), ::toupper);
                    std::string q_up = sq.join_condition_col1;
                    std::transform(q_up.begin(), q_up.end(), q_up.begin(), ::toupper);
                    if (s_up == q_up) join_col1_idx = (int)i;
                }
                int join_col2_idx = -1;
                for (size_t i = 0; i < schema2.size(); i++) {
                    std::string s_up = schema2[i].name;
                    std::transform(s_up.begin(), s_up.end(), s_up.begin(), ::toupper);
                    std::string q_up = sq.join_condition_col2;
                    std::transform(q_up.begin(), q_up.end(), q_up.begin(), ::toupper);
                    if (s_up == q_up) join_col2_idx = (int)i;
                }

                if (join_col1_idx == -1 || join_col2_idx == -1) return "ERROR: Join columns not found.\n";

                // Parallel Hash Join (O(N + M))
                std::unordered_map<std::string, std::vector<size_t>> hash_map;
                std::vector<std::vector<std::string>> cached_table2;
                // Lock table2 once to build the hash map
                {
                    std::shared_lock t2_lock(table2->get_rw_lock());
                    size_t t2_count = table2->get_row_count_nolock();
                    cached_table2.reserve(t2_count);
                    for (size_t j = 0; j < t2_count; j++) {
                        if (table2->is_expired_nolock(j, now)) continue;
                        auto row = table2->get_row_nolock(j);
                        hash_map[row[join_col2_idx]].push_back(cached_table2.size());
                        cached_table2.push_back(std::move(row));
                    }
                }

                unsigned int num_threads = std::thread::hardware_concurrency();
                if (num_threads == 0) num_threads = 2;
                if (total_rows < 100) num_threads = 1;

                std::vector<std::thread> workers;
                std::vector<std::string> buffers(num_threads);
                size_t chunk_size = (total_rows + num_threads - 1) / num_threads;

                for (unsigned int t = 0; t < num_threads; ++t) {
                    workers.emplace_back([&, t]() {
                        size_t start = t * chunk_size;
                        size_t end = std::min(start + chunk_size, total_rows);
                        std::string& local_buffer = buffers[t];
                        for (size_t i = start; i < end; ++i) {
                            if (table->is_expired_nolock(i, now)) continue;
                            auto row1 = table->get_row_nolock(i);
                            const std::string& key = row1[join_col1_idx];
                            auto it = hash_map.find(key);
                            if (it != hash_map.end()) {
                                for (size_t row2_idx : it->second) {
                                    const auto& row2 = cached_table2[row2_idx];
                                    for (const auto& val : row1) local_buffer += val + "\t";
                                    for (const auto& val : row2) local_buffer += val + "\t";
                                    local_buffer += "\n";
                                }
                            }
                        }
                    });
                }
                for (auto& worker : workers) worker.join();
                for (const auto& buf : buffers) result += buf;
                return result;
            }

            // Standard SELECT (with column filtering)
            std::vector<int> col_indices;
            bool select_all = (sq.columns.size() == 1 && sq.columns[0] == "*");
            for (size_t i = 0; i < schema.size(); i++) {
                bool include = select_all;
                if (!select_all) {
                    for (const auto& req_col : sq.columns) {
                        std::string up_req = req_col;
                        if (up_req.find(".") != std::string::npos) up_req = up_req.substr(up_req.find(".") + 1);
                        std::string up_schema = schema[i].name;
                        std::transform(up_req.begin(), up_req.end(), up_req.begin(), ::toupper);
                        std::transform(up_schema.begin(), up_schema.end(), up_schema.begin(), ::toupper);
                        if (up_req == up_schema) { include = true; break; }
                    }
                }
                if (include) {
                    result += schema[i].name + "\t";
                    col_indices.push_back(i);
                }
            }
            result += "\n";

            unsigned int num_threads = std::thread::hardware_concurrency();
            if (num_threads == 0) num_threads = 2;
            if (total_rows < 1000) num_threads = 1;

            std::vector<std::thread> workers;
            std::vector<std::string> buffers(num_threads);
            size_t chunk_size = (total_rows + num_threads - 1) / num_threads;

            for (unsigned int t = 0; t < num_threads; ++t) {
                workers.emplace_back([&, t]() {
                    size_t start = t * chunk_size;
                    size_t end = std::min(start + chunk_size, total_rows);
                    std::string& local_buffer = buffers[t];
                    for (size_t i = start; i < end; i++) {
                        if (table->is_expired_nolock(i, now)) continue;
                        auto row = table->get_row_nolock(i);
                        for (int idx_pos : col_indices) local_buffer += row[idx_pos] + "\t";
                        local_buffer += "\n";
                    }
                });
            }

            for (auto& worker : workers) worker.join();
            size_t prev_size = result.size();
            for (const auto& buf : buffers) result += buf;
            
            if (result.size() == prev_size) return "SUCCESS\n0 rows returned.\n";
            if (!sq.is_join && !sq.has_where && select_all) query_cache.put(cache_key, result);
            return result;
        }

        return "ERROR: Invalid or unknown SQL syntax.\n";
    }
};
