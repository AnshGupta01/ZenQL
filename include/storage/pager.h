#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include "storage/page.h"
#include "common/config.h"

// ─── LRU Page Cache + Pager ───────────────────────────────────────────────────
//  The Pager owns the file fd (or mmap region) and the page cache.
//  All upper layers fetch pages through fetch_page() / dirty_page().
//  On eviction the pager writes dirty pages back to disk.

class Pager {
public:
    explicit Pager(const std::string& filepath, size_t ignored = 0);
    ~Pager();

    Page* fetch_page(uint32_t page_id);
    uint32_t alloc_page();

    void dirty_page(uint32_t page_id) {} // no-op with mmap
    void unpin(uint32_t page_id) {}      // no-op with mmap
    void flush_all();

    uint32_t page_count() const { return page_count_; }

private:
    void map_chunk(size_t chunk_idx);

    int         fd_;
    std::string filepath_;
    uint32_t    page_count_;
    mutable std::shared_mutex mu_; 

    static constexpr size_t CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB
    static constexpr size_t PAGES_PER_CHUNK = CHUNK_SIZE / PAGE_SIZE;

    std::vector<uint8_t*> chunks_;
};
