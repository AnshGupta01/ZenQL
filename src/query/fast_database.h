#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <chrono>
#include <algorithm>
#include <thread>
#include <atomic>
#include "../storage/memory_optimized_table.h"
#include "../index/fast_hash_index.h"
#include "../index/btree_index.h"
#include "../parser/parser.h"
#include "../cache/lru_cache.h"

class FastDatabase
{
    std::unordered_map<std::string, std::shared_ptr<MemoryOptimizedTable>> tables;
    std::unordered_map<std::string, std::shared_ptr<FastHashIndex>> primary_indexes;
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> secondary_indexes;
    LRUCache query_cache;
    std::shared_mutex rw_lock;

    // Performance counters
    std::atomic<uint64_t> total_inserts{0};
    std::atomic<uint64_t> total_selects{0};
    std::atomic<uint64_t> cache_hits{0};

    uint64_t get_current_time()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

public:
    FastDatabase() : query_cache(2048) {} // Larger cache

    std::string execute(const std::string &sql)
    {
        ParsedQuery query = Parser::parse(sql);

        if (query.type == QueryType::CREATE_TABLE)
        {
            auto ct = std::get<CreateTableQuery>(query.query);
            std::unique_lock lock(rw_lock);

            tables[ct.table_name] = std::make_shared<MemoryOptimizedTable>(ct.table_name, ct.columns);
            primary_indexes[ct.table_name] = std::make_shared<FastHashIndex>();

            return "SUCCESS: Table " + ct.table_name + " created successfully.\n";
        }
        else if (query.type == QueryType::INSERT)
        {
            auto iq = std::get<InsertQuery>(query.query);

            std::shared_ptr<MemoryOptimizedTable> table;
            std::shared_ptr<FastHashIndex> index;
            {
                std::shared_lock lock(rw_lock);
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
            total_inserts.fetch_add(1, std::memory_order_relaxed);
            return "SUCCESS: 1 row inserted.\n";
        }
        else if (query.type == QueryType::SELECT)
        {
            auto sq = std::get<SelectQuery>(query.query);
            total_selects.fetch_add(1, std::memory_order_relaxed);

            std::string cache_key = sq.table_name + "_all";

            // Enhanced cache check for simple SELECT *
            if (!sq.is_join && !sq.has_where)
            {
                auto cached_result = query_cache.get(cache_key);
                if (cached_result.has_value())
                {
                    cache_hits.fetch_add(1, std::memory_order_relaxed);
                    return cached_result.value();
                }
            }

            // Find table
            std::shared_ptr<MemoryOptimizedTable> table;
            std::shared_ptr<FastHashIndex> index;
            {
                std::shared_lock lock(rw_lock);
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

            // Advanced parallel SELECT with memory-optimized batch processing
            std::string result = "SUCCESS\n";
            result.reserve(total_rows * 32); // Better initial reserve estimate

            // Build column indices
            std::vector<int> col_indices;
            bool select_all = (sq.columns.size() == 1 && sq.columns[0] == "*");

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

            // Optimized parallel processing with cache-friendly batches
            unsigned int num_threads = std::thread::hardware_concurrency();
            if (num_threads == 0)
                num_threads = 4;

            // Adaptive threading based on data size
            if (total_rows < 5000)
            {
                num_threads = 1; // Single threaded for small datasets
            }
            else if (total_rows < 50000)
            {
                num_threads = std::min(num_threads, 2u);
            }
            else
            {
                num_threads = std::min(num_threads, 8u); // Cap at 8 threads
            }

            if (num_threads == 1)
            {
                // Single-threaded optimized path
                for (size_t i = 0; i < total_rows; i++)
                {
                    if (table->is_expired(i, now))
                        continue;
                    auto row = table->get_row(i);
                    for (int idx_pos : col_indices)
                    {
                        result += row[idx_pos] + "\t";
                    }
                    result += "\n";
                }
            }
            else
            {
                // Multi-threaded with cache-optimized batching
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

                        // Pre-allocate buffer size
                        local_buffer.reserve((end - start) * col_indices.size() * 16);

                        // Process in cache-friendly batches
                        constexpr size_t BATCH_SIZE = 64;
                        for (size_t batch_start = start; batch_start < end; batch_start += BATCH_SIZE) {
                            size_t batch_end = std::min(batch_start + BATCH_SIZE, end);

                            for (size_t i = batch_start; i < batch_end; i++) {
                                if (table->is_expired(i, now)) continue;
                                auto row = table->get_row(i);
                                for (int idx_pos : col_indices) {
                                    local_buffer += row[idx_pos] + "\t";
                                }
                                local_buffer += "\n";
                            }
                        } });
                }

                for (auto &worker : workers)
                {
                    worker.join();
                }

                // Combine results
                size_t prev_size = result.size();
                for (const auto &buf : buffers)
                {
                    result += buf;
                }

                if (result.size() == prev_size)
                {
                    return "SUCCESS\n0 rows returned.\n";
                }
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
        std::cout << "\n=== FastDatabase Performance Stats ===" << std::endl;
        std::cout << "Total Inserts: " << total_inserts.load() << std::endl;
        std::cout << "Total Selects: " << total_selects.load() << std::endl;
        std::cout << "Cache Hits: " << cache_hits.load() << std::endl;

        uint64_t total_queries = total_selects.load();
        if (total_queries > 0)
        {
            double hit_rate = (double)cache_hits.load() / total_queries * 100;
            std::cout << "Cache Hit Rate: " << hit_rate << "%" << std::endl;
        }
        std::cout << "====================================" << std::endl;
    }
};