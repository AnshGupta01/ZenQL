#pragma once
#include <cstddef>

// ─── LRU Page Cache size (pages) ─────────────────────────────────────────────
//  8192 pages × 8 KB = 64 MB working set — sweet spot for 10M row workloads
constexpr size_t LRU_CACHE_CAPACITY = 8192;

// ─── Thread pool ─────────────────────────────────────────────────────────────
constexpr int THREAD_POOL_SIZE = 128;

// ─── Network ─────────────────────────────────────────────────────────────────
constexpr int DEFAULT_PORT     = 9000;
constexpr int BACKLOG          = 256;
constexpr int RECV_BUF_SIZE    = 1 << 20;  // 1 MB receive buffer per conn
constexpr int SEND_BUF_SIZE    = 1 << 20;

// ─── Data directory ──────────────────────────────────────────────────────────
inline constexpr const char* DATA_DIR = "data/tables/";
inline constexpr const char* IDX_DIR  = "data/indexes/";

// ─── B+ Tree fan-out ─────────────────────────────────────────────────────────
//  Each internal node fits in one on-disk page.  Leaf nodes store (key, page_id, slot).
//  ORDER = floor((PAGE_SIZE - header) / (key + ptr)) — empirically 200 for 8KB pages.
constexpr int BPTREE_ORDER = 200;

// ─── Response framing ────────────────────────────────────────────────────────
// Protocol: 4-byte little-endian length prefix  then payload  then sentinel
constexpr uint32_t FRAME_SENTINEL  = 0xDEADBEEF;
