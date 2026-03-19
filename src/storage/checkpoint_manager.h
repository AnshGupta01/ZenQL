#pragma once
#include <string>
#include <memory>
#include <fstream>
#include <chrono>
#include <thread>
#include <atomic>
#include <iostream>
#include "disk_storage.h"

// Forward declaration
class OptimizedDatabase;

/**
 * CheckpointManager: Safe snapshot-based persistence for OptimizedDatabase
 *
 * Key Features:
 * - Zero hot-path impact: No persistence code in insert/select operations
 * - Snapshot-based: Captures consistent point-in-time database state
 * - Manual triggers: On-demand checkpointing with optional periodic mode
 * - Fast recovery: Direct binary loading on database startup
 * - Thread-safe: Uses shared locks for read-only database access
 */
class CheckpointManager
{
private:
    std::shared_ptr<DiskStorage> disk_storage;
    std::string metadata_file;
    std::string checkpoint_dir;

    // Optional periodic checkpointing
    std::atomic<bool> periodic_enabled{false};
    std::thread periodic_thread;
    std::chrono::minutes checkpoint_interval{5}; // Default 5 minutes

    // Write checkpoint metadata to ensure atomic completion
    bool write_checkpoint_metadata(uint64_t timestamp, const std::vector<std::string> &table_names);

    // Read checkpoint metadata
    bool read_checkpoint_metadata(uint64_t &timestamp, std::vector<std::string> &table_names);

    // Cleanup old checkpoint files
    void cleanup_old_checkpoints();

    // Periodic checkpoint worker thread
    void periodic_checkpoint_worker(OptimizedDatabase *db);

public:
    explicit CheckpointManager(const std::string &data_dir);
    ~CheckpointManager();

    /**
     * Create a complete database checkpoint
     * Uses shared locks to read database state without blocking queries
     */
    bool create_checkpoint(OptimizedDatabase &db);

    /**
     * Recover database from the latest checkpoint
     * Called during database initialization
     */
    bool recover_database(OptimizedDatabase &db);

    /**
     * Start periodic checkpointing in background thread
     */
    void start_periodic_checkpointing(OptimizedDatabase &db, std::chrono::minutes interval);

    /**
     * Stop periodic checkpointing
     */
    void stop_periodic_checkpointing();

    /**
     * Check if a checkpoint exists
     */
    bool has_checkpoint() const;

    /**
     * Get checkpoint timestamp
     */
    uint64_t get_checkpoint_timestamp() const;
};