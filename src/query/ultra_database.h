#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <chrono>
#include <filesystem>
#include <thread>
#include <atomic>
#include <vector>
#include <algorithm>
#include "../storage/optimized_table.h"
#include "../storage/wal.h"
#include "../storage/disk_storage.h"
#include "../index/fast_hash_index.h"
#include "../index/btree_index.h"
#include "../parser/parser.h"
#include "../cache/lru_cache.h"
#include "../concurrency/optimized_locks.h"

class UltraDatabase
{
private:
    // Use standard containers with optimized locks for now
    std::unordered_map<std::string, std::shared_ptr<OptimizedTable>> tables;
    std::unordered_map<std::string, std::shared_ptr<FastHashIndex>> primary_indexes;
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> secondary_indexes;
    OptimizedRWSpinLock tables_lock;

    LRUCache query_cache;
    std::string data_directory;

    // Persistence components
    std::shared_ptr<WAL> wal;
    std::shared_ptr<DiskStorage> disk_storage;

    // Background operations
    std::thread checkpoint_thread;
    std::thread compaction_thread;
    std::atomic<bool> should_stop{false};
    std::atomic<size_t> checkpoint_interval_seconds{180}; // 3 minutes
    std::atomic<size_t> compaction_interval_seconds{600}; // 10 minutes

    // Performance statistics
    AtomicCounter total_inserts;
    AtomicCounter total_selects;
    AtomicCounter cache_hits;
    AtomicCounter cache_misses;

    uint64_t get_current_time() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    void background_checkpoint_loop()
    {
        while (!should_stop.load())
        {
            std::this_thread::sleep_for(std::chrono::seconds(checkpoint_interval_seconds.load()));

            if (!should_stop.load())
            {
                perform_checkpoint();
            }
        }
    }

    void background_compaction_loop()
    {
        while (!should_stop.load())
        {
            std::this_thread::sleep_for(std::chrono::seconds(compaction_interval_seconds.load()));

            if (!should_stop.load())
            {
                perform_compaction();
            }
        }
    }

    void perform_checkpoint()
    {
        std::shared_lock lock(tables_lock);
        for (const auto &[table_name, table] : tables)
        {
            table->checkpoint_to_disk();

            // Save primary index
            auto index_it = primary_indexes.find(table_name);
            if (index_it != primary_indexes.end())
            {
                auto index = index_it->second;
                auto index_data = index->export_data();
                disk_storage->save_hash_index(table_name, index_data);
            }
        }

        if (wal)
        {
            wal->log_checkpoint();
        }
    }

    void perform_compaction()
    {
        std::shared_lock lock(tables_lock);
        for (const auto &[table_name, table] : tables)
        {
            table->compact();
        }
    }

    std::vector<std::string> get_table_names() const
    {
        std::vector<std::string> names;
        if (disk_storage)
        {
            names = disk_storage->get_table_names();
        }
        return names;
    }

public:
    explicit UltraDatabase(const std::string &data_dir = "data")
        : query_cache(4096), data_directory(data_dir)
    {
        // Initialize persistence components
        try
        {
            disk_storage = std::make_shared<DiskStorage>(data_directory);

            std::string wal_path = data_directory + "/wal.log";
            wal = std::make_shared<WAL>(wal_path);

            std::cout << "Ultra persistence enabled. Data directory: " << data_directory << std::endl;

            // Recover existing tables
            recover_existing_tables();

            // Start background threads
            checkpoint_thread = std::thread(&UltraDatabase::background_checkpoint_loop, this);
            compaction_thread = std::thread(&UltraDatabase::background_compaction_loop, this);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Failed to initialize persistence: " << e.what() << std::endl;
        }
    }

    ~UltraDatabase()
    {
        should_stop.store(true);

        if (checkpoint_thread.joinable())
        {
            checkpoint_thread.join();
        }
        if (compaction_thread.joinable())
        {
            compaction_thread.join();
        }

        // Final checkpoint
        perform_checkpoint();
    }

    void recover_existing_tables()
    {
        if (!disk_storage)
            return;

        auto table_names = disk_storage->get_table_names();
        std::unique_lock lock(tables_lock);

        for (const auto &table_name : table_names)
        {
            auto schema = disk_storage->load_table_schema(table_name);
            if (!schema.empty())
            {
                auto table = std::make_shared<OptimizedTable>(table_name, schema, wal, disk_storage);
                tables[table_name] = table;

                auto index = std::make_shared<FastHashIndex>();
                // Try to recover index
                std::unordered_map<std::string, size_t> index_data;
                if (disk_storage->load_hash_index(table_name, index_data))
                {
                    index->import_data(index_data);
                }
                primary_indexes[table_name] = index;

                std::cout << "Recovered table: " << table_name << std::endl;
            }
        }
    }

    std::string execute(const std::string &sql)
    {
        ParsedQuery query = Parser::parse(sql);

        if (query.type == QueryType::CREATE_TABLE)
        {
            auto ct = std::get<CreateTableQuery>(query.query);

            // Log to WAL first
            if (wal)
            {
                std::string schema_def = serialize_schema(ct.columns);
                wal->log_create_table(ct.table_name, schema_def);
            }

            std::unique_lock lock(tables_lock);
            auto table = std::make_shared<OptimizedTable>(ct.table_name, ct.columns, wal, disk_storage);
            tables[ct.table_name] = table;

            auto index = std::make_shared<FastHashIndex>();
            primary_indexes[ct.table_name] = index;

            return "SUCCESS: Table " + ct.table_name + " created successfully.\n";
        }
        else if (query.type == QueryType::INSERT)
        {
            auto iq = std::get<InsertQuery>(query.query);

            std::shared_ptr<OptimizedTable> table;
            std::shared_ptr<FastHashIndex> index;
            {
                std::shared_lock lock(tables_lock);
                auto table_it = tables.find(iq.table_name);
                if (table_it == tables.end())
                {
                    return "ERROR: Table not found.\n";
                }
                table = table_it->second;

                auto index_it = primary_indexes.find(iq.table_name);
                if (index_it != primary_indexes.end())
                {
                    index = index_it->second;
                }
            }

            size_t row_id = table->insert_row(iq.values, iq.expires_at);

            if (!iq.values.empty() && index)
            {
                index->insert(iq.values[0], row_id);
            }

            query_cache.invalidate(iq.table_name + "_all");
            total_inserts.increment();
            return "SUCCESS: 1 row inserted.\n";
        }
        else if (query.type == QueryType::SELECT)
        {
            auto sq = std::get<SelectQuery>(query.query);
            total_selects.increment();

            std::string cache_key = sq.table_name + "_all";

            // Enhanced cache check
            if (!sq.is_join && !sq.has_where)
            {
                auto cached_result = query_cache.get(cache_key);
                if (cached_result.has_value())
                {
                    cache_hits.increment();
                    return cached_result.value();
                }
                cache_misses.increment();
            }

            // Find table
            std::shared_ptr<OptimizedTable> table;
            std::shared_ptr<FastHashIndex> index;
            {
                std::shared_lock lock(tables_lock);
                auto table_it = tables.find(sq.table_name);
                if (table_it == tables.end())
                {
                    return "ERROR: Table not found.\n";
                }
                table = table_it->second;

                auto index_it = primary_indexes.find(sq.table_name);
                if (index_it != primary_indexes.end())
                {
                    index = index_it->second;
                }
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

                if (is_pk && index)
                {
                    auto row_opt = index->lookup(sq.where_value);
                    if (row_opt.has_value())
                    {
                        size_t i = row_opt.value();
                        if (!table->is_expired(i, now))
                        {
                            auto row = table->get_row(i);
                            std::string result = "SUCCESS\n";
                            for (const auto &col : schema)
                                result += col.name + "\t";
                            result += "\n";
                            for (const auto &val : row)
                                result += val + "\t";
                            result += "\n";
                            return result;
                        }
                    }
                    return "SUCCESS\n0 rows returned.\n";
                }
            }

            // Standard SELECT with advanced parallel processing
            std::string result = "SUCCESS\n";
            result.reserve(16384); // Larger initial buffer

            // Parallel scan optimization
            std::vector<int> col_indices;
            bool select_all = (sq.columns.size() == 1 && sq.columns[0] == "*");

            // Build column indices
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
                    result += schema[i].name + "\t";
                    col_indices.push_back(i);
                }
            }
            result += "\n";

            // Ultra-parallel scan with adaptive threading
            unsigned int num_threads = std::thread::hardware_concurrency();
            if (num_threads == 0)
                num_threads = 8;

            // Adaptive thread count based on workload
            if (total_rows < 5000)
                num_threads = std::max(1u, num_threads / 4);
            else if (total_rows < 50000)
                num_threads = std::max(2u, num_threads / 2);

            std::vector<std::thread> workers;
            std::vector<std::string> buffers(num_threads);
            size_t chunk_size = (total_rows + num_threads - 1) / num_threads;

            auto start_time = std::chrono::high_resolution_clock::now();

            for (unsigned int t = 0; t < num_threads; ++t)
            {
                workers.emplace_back([&, t]()
                                     {
                    size_t start = t * chunk_size;
                    size_t end = std::min(start + chunk_size, total_rows);
                    std::string& local_buffer = buffers[t];

                    // Pre-allocate based on expected data size
                    size_t estimated_size = (end - start) * col_indices.size() * 10; // Rough estimate
                    local_buffer.reserve(estimated_size);

                    for (size_t i = start; i < end; i++) {
                        if (table->is_expired(i, now)) continue;

                        // Optimized cell access
                        for (int idx_pos : col_indices) {
                            local_buffer += table->get_cell(i, idx_pos) + "\t";
                        }
                        local_buffer += "\n";
                    } });
            }

            for (auto &worker : workers)
            {
                worker.join();
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

            size_t prev_size = result.size();
            for (const auto &buf : buffers)
            {
                result += buf;
            }

            if (result.size() == prev_size)
            {
                return "SUCCESS\n0 rows returned.\n";
            }

            // Cache simple SELECT * queries
            if (!sq.is_join && !sq.has_where && select_all)
            {
                query_cache.put(cache_key, result);
            }

            return result;
        }

        return "ERROR: Invalid or unknown SQL syntax.\n";
    }

    // Performance statistics
    void print_stats() const
    {
        std::cout << "\n=== Ultra Database Statistics ===" << std::endl;
        std::cout << "Total Inserts: " << total_inserts.get() << std::endl;
        std::cout << "Total Selects: " << total_selects.get() << std::endl;
        std::cout << "Cache Hits: " << cache_hits.get() << std::endl;
        std::cout << "Cache Misses: " << cache_misses.get() << std::endl;

        uint64_t total_cache_ops = cache_hits.get() + cache_misses.get();
        if (total_cache_ops > 0)
        {
            double hit_rate = (double)cache_hits.get() / total_cache_ops * 100;
            std::cout << "Cache Hit Rate: " << hit_rate << "%" << std::endl;
        }

        std::cout << "=================================" << std::endl;
    }

    void trigger_checkpoint()
    {
        perform_checkpoint();
    }

private:
    std::string serialize_schema(const std::vector<ColumnDef> &columns)
    {
        std::string result;
        for (const auto &col : columns)
        {
            result += col.name + " " + col.get_type_string();
            if (col.is_primary_key)
                result += " PRIMARY KEY";
            result += ",";
        }
        if (!result.empty())
            result.pop_back();
        return result;
    }
};