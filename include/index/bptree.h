#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <mutex>
#include <memory>
#include "storage/table.h"
#include "common/config.h"

// ─── B+ Tree (in-memory) for primary key index ───────────────────────────────
//  Key type: int64_t (covers INT and DATETIME PKs; VARCHAR PKs hashed)
//  Strategy: in-memory B+ tree (faster than page-backed for 10M rows,
//            fits comfortably in ~150 MB for 10M entries at 24 bytes/entry).
//  Read/write protected by a shared_mutex.
#include <shared_mutex>

using BPKey = int64_t;

struct BPValue {
    uint32_t page_id;
    uint16_t slot_idx;
};

struct BPNode {
    bool     is_leaf;
    int      n;                         // number of keys
    BPKey    keys[BPTREE_ORDER + 1];
    // Internal: children[n+1] pointers
    // Leaf:     vals[n] RowLocations + next leaf ptr
    BPNode*  children[BPTREE_ORDER + 2];   // internal only
    BPValue  vals[BPTREE_ORDER + 1];       // leaf only
    BPNode*  next;                          // next leaf (for range scans)

    BPNode(bool leaf) : is_leaf(leaf), n(0), next(nullptr) {
        std::fill(children, children + BPTREE_ORDER + 2, nullptr);
    }
};

class BPTree {
public:
    BPTree();
    ~BPTree();

    // Insert key -> value. Overwrites existing entry.
    void insert(BPKey key, BPValue val);

    // Destroys all nodes in the tree
    void clear();

    // Exact lookup. Returns nullopt if not found.
    std::optional<BPValue> find(BPKey key) const;

    // Range scan [lo, hi] inclusive. Returns matching values.
    std::vector<BPValue> range(BPKey lo, BPKey hi) const;

    size_t size() const { return size_; }

    void lock_read()    const { mtx_.lock_shared(); }
    void unlock_read()  const { mtx_.unlock_shared(); }
    void lock_write()   { mtx_.lock(); }
    void unlock_write() { mtx_.unlock(); }

private:
    BPNode* root_;
    size_t  size_;
    mutable std::shared_mutex mtx_;

    BPNode* insert_rec(BPNode* node, BPKey key, BPValue val, BPKey& push_key, BPNode*& push_right);
    BPNode* find_leaf(BPKey key) const;
    void    destroy(BPNode* node);
};
