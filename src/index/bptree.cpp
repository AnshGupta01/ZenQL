#include "index/bptree.h"
#include <algorithm>
#include <cassert>
#include <cstring>

BPTree::BPTree() : size_(0) {
    root_ = new BPNode(true);
}

BPTree::~BPTree() { destroy(root_); }

void BPTree::destroy(BPNode* node) {
    if (!node) return;
    if (!node->is_leaf) {
        for (int i = 0; i <= node->n; i++) destroy(node->children[i]);
    }
    delete node;
}

BPNode* BPTree::find_leaf(BPKey key) const {
    BPNode* cur = root_;
    while (!cur->is_leaf) {
        int i = (int)(std::upper_bound(cur->keys, cur->keys + cur->n, key) - cur->keys);
        cur = cur->children[i];
    }
    return cur;
}
void BPTree::clear() {
    destroy(root_);
    root_ = new BPNode(true);
    size_ = 0;
}

std::optional<BPValue> BPTree::find(BPKey key) const {
    BPNode* leaf = find_leaf(key);
    for (int i = 0; i < leaf->n; i++)
        if (leaf->keys[i] == key) return leaf->vals[i];
    return std::nullopt;
}

std::vector<BPValue> BPTree::range(BPKey lo, BPKey hi) const {
    std::vector<BPValue> out;
    BPNode* leaf = find_leaf(lo);
    while (leaf) {
        for (int i = 0; i < leaf->n; i++) {
            if (leaf->keys[i] > hi) return out;
            if (leaf->keys[i] >= lo) out.push_back(leaf->vals[i]);
        }
        leaf = leaf->next;
    }
    return out;
}

// ─── Insert ──────────────────────────────────────────────────────────────────

static void leaf_insert_at(BPNode* leaf, int pos, BPKey key, BPValue val) {
    // Shift right
    for (int i = leaf->n; i > pos; i--) {
        leaf->keys[i] = leaf->keys[i-1];
        leaf->vals[i] = leaf->vals[i-1];
    }
    leaf->keys[pos] = key;
    leaf->vals[pos] = val;
    leaf->n++;
}

static BPNode* split_leaf(BPNode* left, BPKey& push_key) {
    int mid = (BPTREE_ORDER + 1) / 2;
    BPNode* right = new BPNode(true);
    right->n = left->n - mid;
    for (int i = 0; i < right->n; i++) {
        right->keys[i] = left->keys[mid + i];
        right->vals[i] = left->vals[mid + i];
    }
    left->n = mid;
    right->next = left->next;
    left->next  = right;
    push_key = right->keys[0];
    return right;
}

static void internal_insert_at(BPNode* node, int pos, BPKey key, BPNode* right_child) {
    for (int i = node->n; i > pos; i--) {
        node->keys[i]       = node->keys[i-1];
        node->children[i+1] = node->children[i];
    }
    node->keys[pos]       = key;
    node->children[pos+1] = right_child;
    node->n++;
}

static BPNode* split_internal(BPNode* left, BPKey& push_key) {
    int mid = BPTREE_ORDER / 2;
    push_key = left->keys[mid];
    BPNode* right = new BPNode(false);
    right->n = left->n - mid - 1;
    for (int i = 0; i < right->n; i++)
        right->keys[i] = left->keys[mid + 1 + i];
    for (int i = 0; i <= right->n; i++)
        right->children[i] = left->children[mid + 1 + i];
    left->n = mid;
    return right;
}

BPNode* BPTree::insert_rec(BPNode* node, BPKey key, BPValue val,
                             BPKey& push_key, BPNode*& push_right) {
    push_right = nullptr;
    if (node->is_leaf) {
        // Find position
        int pos = (int)(std::lower_bound(node->keys, node->keys + node->n, key) - node->keys);
        // Overwrite if exists
        if (pos < node->n && node->keys[pos] == key) {
            node->vals[pos] = val;
            return nullptr; // no split
        }
        leaf_insert_at(node, pos, key, val);
        size_++;
        if (node->n <= BPTREE_ORDER) return nullptr;
        // Split needed
        push_right = split_leaf(node, push_key);
        return push_right;
    }
    // Internal node
    int pos = (int)(std::upper_bound(node->keys, node->keys + node->n, key) - node->keys);
    BPKey child_push_key;
    BPNode* child_push_right;
    insert_rec(node->children[pos], key, val, child_push_key, child_push_right);
    if (!child_push_right) return nullptr;
    // Insert promoted key into this internal node
    internal_insert_at(node, pos, child_push_key, child_push_right);
    if (node->n <= BPTREE_ORDER) return nullptr;
    push_right = split_internal(node, push_key);
    return push_right;
}

void BPTree::insert(BPKey key, BPValue val) {
    BPKey push_key;
    BPNode* push_right;
    BPNode* ret = insert_rec(root_, key, val, push_key, push_right);
    if (ret) {
        // Root split — create new root
        BPNode* new_root = new BPNode(false);
        new_root->keys[0]       = push_key;
        new_root->children[0]   = root_;
        new_root->children[1]   = push_right;
        new_root->n             = 1;
        root_ = new_root;
    }
}
