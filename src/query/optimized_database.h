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
#include "../parser/parser.h"
#include "../cache/lock_free_cache.h"
#include "../storage/checkpoint_manager.h"

class OptimizedDatabase
{
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    std::unordered_map<std::string, std::shared_ptr<FastHashIndex>> primary_indexes;
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> secondary_indexes;
    OptimizedLRUCache query_cache;
    std::shared_mutex rw_lock;

    // Optional persistence support (nullptr if disabled)
    std::shared_ptr<CheckpointManager> checkpoint_manager;

    // Thread-local string builders for better performance
    thread_local static std::string result_buffer;

    uint64_t get_current_time()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

public:
    // Constructor with optional persistence support
    OptimizedDatabase(const std::string &data_dir = "", bool enable_persistence = false)
        : query_cache(2048)
    {

        if (enable_persistence && !data_dir.empty())
        {
            std::cout << "[OptimizedDatabase] Initializing with persistence enabled (data_dir: "
                      << data_dir << ")" << std::endl;

            checkpoint_manager = std::make_shared<CheckpointManager>(data_dir);

            // Attempt recovery on startup
            if (!checkpoint_manager->recover_database(*this))
            {
                std::cerr << "[OptimizedDatabase] Warning: Recovery failed, starting with empty database"
                          << std::endl;
            }
            else if (checkpoint_manager->has_checkpoint())
            {
                std::cout << "[OptimizedDatabase] Successfully recovered from checkpoint (timestamp: "
                          << checkpoint_manager->get_checkpoint_timestamp() << ")" << std::endl;
            }
        }
        else
        {
            std::cout << "[OptimizedDatabase] Initialized without persistence (in-memory only)" << std::endl;
        }
    }

    std::string execute(const std::string &sql)
    {
        // Trim and validate input
        std::string trimmed_sql = sql;
        const size_t first_non_ws = trimmed_sql.find_first_not_of(" \n\r\t");
        if (first_non_ws == std::string::npos)
        {
            return "ERROR: Empty query.\n";
        }

        trimmed_sql.erase(0, first_non_ws);
        const size_t last_non_sep = trimmed_sql.find_last_not_of(" \n\r\t;");
        if (last_non_sep == std::string::npos)
        {
            return "ERROR: Empty query.\n";
        }
        trimmed_sql.erase(last_non_sep + 1);

        if (trimmed_sql.empty())
        {
            return "ERROR: Empty query.\n";
        }

        ParsedQuery query = Parser::parse(sql);

        if (query.type == QueryType::CREATE_TABLE)
        {
            auto ct = std::get<CreateTableQuery>(query.query);

            // Validate CREATE TABLE query
            if (ct.table_name.empty())
            {
                return "ERROR: CREATE TABLE requires a table name.\nUsage: CREATE TABLE table_name (column1 type1, column2 type2, ...);\n";
            }
            if (ct.columns.empty())
            {
                return "ERROR: CREATE TABLE requires at least one column.\nUsage: CREATE TABLE table_name (column1 type1, column2 type2, ...);\n";
            }

            // Check if table already exists
            {
                std::shared_lock lock(rw_lock);
                if (tables.find(ct.table_name) != tables.end())
                {
                    return "ERROR: Table '" + ct.table_name + "' already exists.\n";
                }
            }

            std::unique_lock lock(rw_lock);
            tables[ct.table_name] = std::make_shared<Table>(ct.table_name, ct.columns);
            primary_indexes[ct.table_name] = std::make_shared<FastHashIndex>();
            return "SUCCESS: Table " + ct.table_name + " created successfully.\n";
        }
        else if (query.type == QueryType::CHECKPOINT)
        {
            if (save_checkpoint())
            {
                return "SUCCESS: Database checkpoint created successfully.\n";
            }
            else
            {
                return "ERROR: Failed to create checkpoint.\n";
            }
        }
        else if (query.type == QueryType::INSERT)
        {
            auto iq = std::get<InsertQuery>(query.query);

            // Validate INSERT query
            if (iq.table_name.empty())
            {
                return "ERROR: INSERT requires a table name.\nUsage: INSERT INTO table_name VALUES (value1, value2, ...);\n";
            }
            if (iq.values.empty())
            {
                return "ERROR: INSERT requires at least one value.\nUsage: INSERT INTO table_name VALUES (value1, value2, ...);\n";
            }

            std::shared_ptr<Table> table;
            {
                std::shared_lock lock(rw_lock);
                auto it = tables.find(iq.table_name);
                if (it == tables.end())
                {
                    return "ERROR: Table '" + iq.table_name + "' not found.\nUse CREATE TABLE to create it first.\n";
                }
                table = it->second;
            }

            // Validate column count matches
            auto schema = table->get_schema();
            if (iq.values.size() != schema.size())
            {
                return "ERROR: Column count mismatch. Expected " +
                       std::to_string(schema.size()) + " values, got " +
                       std::to_string(iq.values.size()) + ".\n";
            }

            size_t row_id = table->insert_row(iq.values, iq.expires_at);
            if (!iq.values.empty())
            {
                primary_indexes[iq.table_name]->insert(iq.values[0], row_id);
            }

            query_cache.invalidate(iq.table_name + "_all");
            return "SUCCESS: 1 row inserted.\n";
        }
        else if (query.type == QueryType::SELECT)
        {
            auto sq = std::get<SelectQuery>(query.query);

            // Validate SELECT query
            if (sq.table_name.empty())
            {
                return "ERROR: SELECT requires a table name.\nUsage: SELECT * FROM table_name;\n";
            }
            if (sq.columns.empty())
            {
                return "ERROR: SELECT requires at least one column.\nUsage: SELECT * FROM table_name;\n";
            }

            std::string cache_key = sq.table_name + "_all";

            // Enhanced caching for simple SELECT *
            if (!sq.is_join && !sq.has_where)
            {
                auto cached_result = query_cache.get(cache_key);
                if (cached_result.has_value())
                    return cached_result.value();
            }

            std::shared_ptr<Table> table;
            {
                std::shared_lock lock(rw_lock);
                auto it = tables.find(sq.table_name);
                if (it == tables.end())
                {
                    return "ERROR: Table '" + sq.table_name + "' not found.\nUse CREATE TABLE to create it first.\n";
                }
                table = it->second;
            }

            size_t total_rows = table->get_row_count();
            auto schema = table->get_schema();
            uint64_t now = get_current_time();

            // Optimized primary key lookup
            if (sq.has_where && !sq.is_join)
            {
                bool is_pk = false;
                std::string up_where = sq.where_column;
                std::string up_sname = schema[0].name;
                std::transform(up_where.begin(), up_where.end(), up_where.begin(), ::toupper);
                std::transform(up_sname.begin(), up_sname.end(), up_sname.begin(), ::toupper);
                if (up_where == up_sname)
                    is_pk = true;

                if (is_pk)
                {
                    auto idx = primary_indexes[sq.table_name];
                    auto row_opt = idx->lookup(sq.where_value);
                    if (row_opt.has_value())
                    {
                        size_t i = row_opt.value();
                        if (!table->is_expired(i, now))
                        {
                            auto row = table->get_row(i);

                            // Use optimized string building
                            result_buffer.clear();
                            result_buffer.reserve(256); // Pre-allocate
                            result_buffer = "SUCCESS\n";
                            for (const auto &col : schema)
                            {
                                result_buffer += col.name + "\t";
                            }
                            result_buffer += "\n";
                            for (const auto &val : row)
                            {
                                result_buffer += val + "\t";
                            }
                            result_buffer += "\n";
                            return result_buffer;
                        }
                    }
                    return "SUCCESS\n0 rows returned.\n";
                }
            }

            // Improved string building for results
            result_buffer.clear();

            // Better initial size estimation
            size_t estimated_result_size = total_rows * schema.size() * 16; // Rough estimate
            result_buffer.reserve(estimated_result_size);
            result_buffer = "SUCCESS\n";

            // JOIN optimization (keep existing logic but with better string management)
            if (sq.is_join)
            {
                // Validate join query components
                if (sq.join_table.empty())
                {
                    return "ERROR: JOIN requires a second table name.\nUsage: SELECT * FROM table1 INNER JOIN table2 ON table1.col = table2.col;\n";
                }
                if (sq.join_condition_col1.empty() || sq.join_condition_col2.empty())
                {
                    return "ERROR: JOIN requires ON condition with two columns.\nUsage: SELECT * FROM table1 INNER JOIN table2 ON table1.col = table2.col;\n";
                }

                std::shared_ptr<Table> table2;
                {
                    std::shared_lock lock(rw_lock);
                    auto it = tables.find(sq.join_table);
                    if (it == tables.end())
                    {
                        return "ERROR: Join table '" + sq.join_table + "' not found.\n";
                    }
                    table2 = it->second;
                }

                auto schema2 = table2->get_schema();

                // Pre-build headers
                for (const auto &col : schema)
                {
                    result_buffer += sq.table_name + "." + col.name + "\t";
                }
                for (const auto &col : schema2)
                {
                    result_buffer += sq.join_table + "." + col.name + "\t";
                }
                result_buffer += "\n";

                // Optimized join with better memory management
                int join_col1_idx = -1;
                for (size_t i = 0; i < schema.size(); i++)
                {
                    std::string s_up = schema[i].name;
                    std::transform(s_up.begin(), s_up.end(), s_up.begin(), ::toupper);
                    std::string q_up = sq.join_condition_col1;
                    std::transform(q_up.begin(), q_up.end(), q_up.begin(), ::toupper);
                    if (s_up == q_up)
                        join_col1_idx = (int)i;
                }
                int join_col2_idx = -1;
                for (size_t i = 0; i < schema2.size(); i++)
                {
                    std::string s_up = schema2[i].name;
                    std::transform(s_up.begin(), s_up.end(), s_up.begin(), ::toupper);
                    std::string q_up = sq.join_condition_col2;
                    std::transform(q_up.begin(), q_up.end(), q_up.begin(), ::toupper);
                    if (s_up == q_up)
                        join_col2_idx = (int)i;
                }

                if (join_col1_idx == -1 || join_col2_idx == -1)
                {
                    return "ERROR: Join columns not found.\n";
                }

                // Optimized hash join with pre-allocated structures
                std::unordered_map<std::string, std::vector<size_t>> hash_map;
                hash_map.reserve(table2->get_row_count() / 4); // Reserve some space

                std::vector<std::vector<std::string>> cached_table2;
                size_t t2_count = table2->get_row_count();
                cached_table2.reserve(t2_count);

                for (size_t j = 0; j < t2_count; j++)
                {
                    if (table2->is_expired(j, now))
                        continue;
                    auto row = table2->get_row(j);
                    hash_map[row[join_col2_idx]].push_back(cached_table2.size());
                    cached_table2.push_back(std::move(row)); // Move instead of copy
                }

                // Improved parallel join with better thread management
                unsigned int num_threads = std::thread::hardware_concurrency();
                if (num_threads == 0)
                    num_threads = 4;

                // Conservative threading for smaller datasets
                if (total_rows < 5000)
                    num_threads = 1;
                else if (total_rows < 25000)
                    num_threads = std::min(num_threads, 2u);

                std::vector<std::thread> workers;
                std::vector<std::string> buffers(num_threads);
                size_t chunk_size = (total_rows + num_threads - 1) / num_threads;

                for (unsigned int t = 0; t < num_threads; ++t)
                {
                    workers.emplace_back([&, t]()
                                         {
                        size_t start = t * chunk_size;
                        size_t end = std::min(start + chunk_size, total_rows);
                        std::string& local_buffer = buffers[t];

                        // Pre-allocate local buffer
                        local_buffer.reserve((end - start) * 64); // Estimate

                        for (size_t i = start; i < end; ++i) {
                            if (table->is_expired(i, now)) continue;
                            auto row1 = table->get_row(i);
                            const std::string& key = row1[join_col1_idx];
                            auto it = hash_map.find(key);
                            if (it != hash_map.end()) {
                                for (size_t row2_idx : it->second) {
                                    const auto& row2 = cached_table2[row2_idx];
                                    for (const auto& val : row1) {
                                        local_buffer += val + "\t";
                                    }
                                    for (const auto& val : row2) {
                                        local_buffer += val + "\t";
                                    }
                                    local_buffer += "\n";
                                }
                            }
                        } });
                }
                for (auto &worker : workers)
                    worker.join();
                for (const auto &buf : buffers)
                    result_buffer += buf;
                return result_buffer;
            }

            // Standard SELECT with optimized string building
            std::vector<int> col_indices;
            bool select_all = (sq.columns.size() == 1 && sq.columns[0] == "*");

            // Build header using buffer to avoid repeated concatenations
            size_t header_size = 0;
            for (size_t i = 0; i < schema.size(); i++)
            {
                bool include = select_all;
                if (!select_all)
                {
                    for (const auto &req_col : sq.columns)
                    {
                        std::string up_req = req_col;
                        if (up_req.find(".") != std::string::npos)
                            up_req = up_req.substr(up_req.find(".") + 1);
                        std::string up_schema = schema[i].name;
                        std::transform(up_req.begin(), up_req.end(), up_req.begin(), ::toupper);
                        std::transform(up_schema.begin(), up_schema.end(), up_schema.begin(), ::toupper);
                        if (up_req == up_schema)
                        {
                            include = true;
                            break;
                        }
                    }
                }
                if (include)
                {
                    header_size += schema[i].name.size() + 1;
                    col_indices.push_back(i);
                }
            }

            // Pre-allocate and build header efficiently
            result_buffer.reserve(header_size + 1024);
            for (int idx : col_indices)
            {
                result_buffer.append(schema[idx].name);
                result_buffer.push_back('\t');
            }
            result_buffer.push_back('\n');

            // Adaptive parallel processing
            unsigned int num_threads = std::thread::hardware_concurrency();
            if (num_threads == 0)
                num_threads = 4;
            if (total_rows < 2000)
                num_threads = 1;
            else if (total_rows < 10000)
                num_threads = std::min(num_threads, 2u);

            std::vector<std::thread> workers;
            std::vector<std::string> buffers(num_threads);
            size_t chunk_size = (total_rows + num_threads - 1) / num_threads;

            for (unsigned int t = 0; t < num_threads; ++t)
            {
                workers.emplace_back([&, t]()
                                     {
                    size_t start = t * chunk_size;
                    size_t end = std::min(start + chunk_size, total_rows);
                    std::string& local_buffer = buffers[t];

                    // Optimized buffer pre-allocation based on estimated size
                    local_buffer.reserve((end - start) * col_indices.size() * 12);

                    // Use thread-local char buffer for batching to eliminate O(n²) string concatenation
                    thread_local std::vector<char> temp_buf(8192);
                    size_t buf_pos = 0;

                    for (size_t i = start; i < end; i++) {
                        if (table->is_expired(i, now)) continue;
                        auto row = table->get_row(i);

                        for (int idx_pos : col_indices) {
                            const auto& val = row[idx_pos];
                            size_t val_size = val.size();

                            // Check if we need to flush temp buffer
                            if (buf_pos + val_size + 2 > temp_buf.size()) {
                                // Flush accumulated data to string
                                local_buffer.append(temp_buf.data(), buf_pos);
                                buf_pos = 0;

                                // If single value is huge, resize buffer
                                if (val_size + 2 > temp_buf.size()) {
                                    temp_buf.resize(val_size + 1024);
                                }
                            }

                            // Batch write to temp buffer
                            std::memcpy(temp_buf.data() + buf_pos, val.data(), val_size);
                            buf_pos += val_size;
                            temp_buf[buf_pos++] = '\t';
                        }
                        temp_buf[buf_pos++] = '\n';
                    }

                    // Final flush of remaining data
                    if (buf_pos > 0) {
                        local_buffer.append(temp_buf.data(), buf_pos);
                    } });
            }

            for (auto &worker : workers)
                worker.join();
            size_t prev_size = result_buffer.size();
            for (const auto &buf : buffers)
                result_buffer += buf;

            if (result_buffer.size() == prev_size)
                return "SUCCESS\n0 rows returned.\n";
            if (!sq.is_join && !sq.has_where && select_all)
                query_cache.put(cache_key, result_buffer);
            return result_buffer;
        }

        return "ERROR: Invalid or unknown SQL syntax.\n";
    }

    // ===== PERSISTENCE SUPPORT METHODS =====

    // Thread-safe data export methods for checkpointing
    std::vector<std::string> get_table_names()
    {
        std::shared_lock lock(rw_lock);
        std::vector<std::string> names;
        names.reserve(tables.size());
        for (const auto &[name, table] : tables)
        {
            names.push_back(name);
        }
        return names;
    }

    std::vector<ColumnDef> export_table_schema(const std::string &table_name)
    {
        std::shared_lock lock(rw_lock);
        auto it = tables.find(table_name);
        if (it != tables.end())
        {
            return it->second->get_schema();
        }
        return {}; // Empty if table not found
    }

    std::vector<std::vector<std::string>> export_table_data(const std::string &table_name)
    {
        std::shared_lock lock(rw_lock);
        auto it = tables.find(table_name);
        if (it != tables.end())
        {
            return it->second->export_all_rows();
        }
        return {}; // Empty if table not found
    }

    std::vector<uint64_t> export_table_expiry(const std::string &table_name)
    {
        std::shared_lock lock(rw_lock);
        auto it = tables.find(table_name);
        if (it != tables.end())
        {
            return it->second->export_expiry_data();
        }
        return {}; // Empty if table not found
    }

    std::unordered_map<std::string, size_t> export_primary_index(const std::string &table_name)
    {
        std::shared_lock lock(rw_lock);
        auto it = primary_indexes.find(table_name);
        if (it != primary_indexes.end())
        {
            return it->second->export_data();
        }
        return {}; // Empty if index not found
    }

    // Recovery methods for startup restoration
    bool create_table_from_schema(const std::string &table_name, const std::vector<ColumnDef> &schema)
    {
        std::unique_lock lock(rw_lock);

        // Check if table already exists
        if (tables.find(table_name) != tables.end())
        {
            return false; // Table already exists
        }

        // Create new table and index
        tables[table_name] = std::make_shared<Table>(table_name, schema);
        primary_indexes[table_name] = std::make_shared<FastHashIndex>();
        return true;
    }

    bool load_table_data(const std::string &table_name,
                         const std::vector<std::vector<std::string>> &rows,
                         const std::vector<uint64_t> &expires_at)
    {
        std::shared_lock lock(rw_lock);
        auto it = tables.find(table_name);
        if (it != tables.end())
        {
            return it->second->import_rows(rows, expires_at);
        }
        return false; // Table not found
    }

    bool load_primary_index(const std::string &table_name,
                            const std::unordered_map<std::string, size_t> &index_data)
    {
        std::shared_lock lock(rw_lock);
        auto it = primary_indexes.find(table_name);
        if (it != primary_indexes.end())
        {
            it->second->import_data(index_data);
            return true;
        }
        return false; // Index not found
    }

    // Clear all data (useful for testing)
    void clear_all_data()
    {
        std::unique_lock lock(rw_lock);
        tables.clear();
        primary_indexes.clear();
        secondary_indexes.clear();
        query_cache.clear();
    }

    // ===== PERSISTENCE API =====

    /**
     * Create a manual checkpoint (saves current database state to disk)
     * Returns true if successful, false otherwise
     * No-op if persistence is disabled
     */
    bool save_checkpoint()
    {
        if (!checkpoint_manager)
        {
            std::cout << "[OptimizedDatabase] Checkpoint requested but persistence is disabled" << std::endl;
            return false;
        }

        return checkpoint_manager->create_checkpoint(*this);
    }

    /**
     * Start automatic periodic checkpointing
     * interval: Time between checkpoints (default: 5 minutes)
     * No-op if persistence is disabled
     */
    void start_auto_checkpoints(std::chrono::minutes interval = std::chrono::minutes(5))
    {
        if (!checkpoint_manager)
        {
            std::cout << "[OptimizedDatabase] Auto-checkpoint requested but persistence is disabled" << std::endl;
            return;
        }

        checkpoint_manager->start_periodic_checkpointing(*this, interval);
    }

    /**
     * Stop automatic periodic checkpointing
     * No-op if persistence is disabled
     */
    void stop_auto_checkpoints()
    {
        if (!checkpoint_manager)
            return;
        checkpoint_manager->stop_periodic_checkpointing();
    }

    /**
     * Check if persistence is enabled
     */
    bool is_persistence_enabled() const
    {
        return checkpoint_manager != nullptr;
    }

    /**
     * Check if a saved checkpoint exists
     */
    bool has_saved_data() const
    {
        return checkpoint_manager && checkpoint_manager->has_checkpoint();
    }

    /**
     * Get the timestamp of the last checkpoint
     */
    uint64_t get_last_checkpoint_time() const
    {
        return checkpoint_manager ? checkpoint_manager->get_checkpoint_timestamp() : 0;
    }
};