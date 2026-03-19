#pragma once
#include <map>
#include <shared_mutex>
#include <string>
#include <vector>

// Proxy for a B-Tree: std::map is implemented as a Red-Black Tree in STL, providing O(log n)
// properties heavily mirroring B-Tree concurrency. 
// Standard B-Tree algorithms for disk-paging are usually preferred for true off-memory
// indices, but this covers the memory logic for the framework's phase 1 secondary requirements.
class BTreeIndex {
    std::map<std::string, std::vector<size_t>> index; 
    std::shared_mutex rw_lock;

public:
    void insert(const std::string& key, size_t row_id) {
        std::unique_lock lock(rw_lock);
        index[key].push_back(row_id); // Allow duplicate keys for secondary columns
    }

    std::vector<size_t> lookup(const std::string& key) {
        std::shared_lock lock(rw_lock);
        auto it = index.find(key);
        if (it != index.end()) {
            return it->second;
        }
        return {};
    }
};
