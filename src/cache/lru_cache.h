#pragma once
#include <unordered_map>
#include <list>
#include <string>
#include <mutex>
#include <optional>

class LRUCache {
    size_t capacity;
    std::list<std::pair<std::string, std::string>> list;
    std::unordered_map<std::string, decltype(list.begin())> map;
    std::mutex mtx;

public:
    LRUCache(size_t cap) : capacity(cap) {}

    std::optional<std::string> get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(key);
        if (it == map.end()) return std::nullopt;
        
        // Move to front (Most Recently Used)
        list.splice(list.begin(), list, it->second);
        return it->second->second;
    }

    void put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(key);
        if (it != map.end()) {
            it->second->second = value;
            list.splice(list.begin(), list, it->second);
            return;
        }

        if (list.size() >= capacity) {
            auto last = list.back();
            map.erase(last.first);
            list.pop_back();
        }

        list.emplace_front(key, value);
        map[key] = list.begin();
    }

    // Call instantly and effectively on INSERT query to invalidate cached blocks
    void invalidate(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(key);
        if (it != map.end()) {
            list.erase(it->second);
            map.erase(it);
        }
    }
};
