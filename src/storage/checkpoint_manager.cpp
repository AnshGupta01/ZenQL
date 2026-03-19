#include "checkpoint_manager.h"
#include "../query/optimized_database.h"

CheckpointManager::CheckpointManager(const std::string &data_dir)
    : disk_storage(std::make_shared<DiskStorage>(data_dir)), checkpoint_dir(data_dir)
{
    metadata_file = checkpoint_dir + "/checkpoint.meta";
    std::cout << "[CheckpointManager] Initialized with data directory: " << data_dir << std::endl;
}

CheckpointManager::~CheckpointManager()
{
    stop_periodic_checkpointing();
}

bool CheckpointManager::write_checkpoint_metadata(uint64_t timestamp, const std::vector<std::string> &table_names)
{
    std::ofstream meta_file(metadata_file, std::ios::binary | std::ios::trunc);
    if (!meta_file.is_open())
        return false;

    // Write checkpoint timestamp
    meta_file.write(reinterpret_cast<const char *>(&timestamp), sizeof(timestamp));

    // Write table count and names
    uint32_t table_count = table_names.size();
    meta_file.write(reinterpret_cast<const char *>(&table_count), sizeof(table_count));

    for (const auto &name : table_names)
    {
        uint32_t name_len = name.length();
        meta_file.write(reinterpret_cast<const char *>(&name_len), sizeof(name_len));
        meta_file.write(name.c_str(), name_len);
    }

    return true;
}

bool CheckpointManager::read_checkpoint_metadata(uint64_t &timestamp, std::vector<std::string> &table_names)
{
    std::ifstream meta_file(metadata_file, std::ios::binary);
    if (!meta_file.is_open())
        return false;

    // Read checkpoint timestamp
    meta_file.read(reinterpret_cast<char *>(&timestamp), sizeof(timestamp));

    // Read table count and names
    uint32_t table_count;
    meta_file.read(reinterpret_cast<char *>(&table_count), sizeof(table_count));

    table_names.clear();
    table_names.reserve(table_count);

    for (uint32_t i = 0; i < table_count; ++i)
    {
        uint32_t name_len;
        meta_file.read(reinterpret_cast<char *>(&name_len), sizeof(name_len));

        std::string name;
        name.resize(name_len);
        meta_file.read(&name[0], name_len);
        table_names.push_back(name);
    }

    return true;
}

void CheckpointManager::cleanup_old_checkpoints()
{
    // For now, keep only the latest checkpoint
    // Could be extended to keep N recent checkpoints
}

void CheckpointManager::periodic_checkpoint_worker(OptimizedDatabase *db)
{
    while (periodic_enabled.load())
    {
        std::this_thread::sleep_for(checkpoint_interval);

        if (periodic_enabled.load())
        {
            std::cout << "[CheckpointManager] Creating periodic checkpoint..." << std::endl;
            if (!create_checkpoint(*db))
            {
                std::cerr << "[CheckpointManager] Periodic checkpoint failed!" << std::endl;
            }
        }
    }
}

bool CheckpointManager::create_checkpoint(OptimizedDatabase &db)
{
    try
    {
        std::cout << "[CheckpointManager] Starting checkpoint creation..." << std::endl;

        auto start_time = std::chrono::high_resolution_clock::now();
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();

        // Get list of all tables (thread-safe)
        auto table_names = db.get_table_names();
        std::cout << "[CheckpointManager] Found " << table_names.size() << " tables to checkpoint" << std::endl;

        // Save each table's data, schema, and indexes
        for (const auto &table_name : table_names)
        {
            std::cout << "[CheckpointManager] Checkpointing table: " << table_name << std::endl;

            // Export and save table schema
            auto schema = db.export_table_schema(table_name);
            if (!disk_storage->save_table_schema(table_name, schema))
            {
                std::cerr << "[CheckpointManager] Failed to save schema for table: " << table_name << std::endl;
                return false;
            }

            // Export and save table data
            auto rows = db.export_table_data(table_name);
            auto expiry_data = db.export_table_expiry(table_name);
            if (!disk_storage->save_table_data(table_name, rows, expiry_data))
            {
                std::cerr << "[CheckpointManager] Failed to save data for table: " << table_name << std::endl;
                return false;
            }

            // Export and save primary index
            auto index_data = db.export_primary_index(table_name);
            if (!disk_storage->save_hash_index(table_name, index_data))
            {
                std::cerr << "[CheckpointManager] Failed to save index for table: " << table_name << std::endl;
                return false;
            }
        }

        // Write metadata atomically to mark checkpoint as complete
        if (!write_checkpoint_metadata(timestamp, table_names))
        {
            std::cerr << "[CheckpointManager] Failed to write checkpoint metadata" << std::endl;
            return false;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "[CheckpointManager] Checkpoint completed successfully in "
                  << duration.count() << "ms" << std::endl;

        cleanup_old_checkpoints();
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "[CheckpointManager] Checkpoint failed with exception: " << e.what() << std::endl;
        return false;
    }
}

bool CheckpointManager::recover_database(OptimizedDatabase &db)
{
    try
    {
        std::cout << "[CheckpointManager] Starting database recovery..." << std::endl;

        auto start_time = std::chrono::high_resolution_clock::now();

        // Check if checkpoint metadata exists
        uint64_t timestamp;
        std::vector<std::string> table_names;
        if (!read_checkpoint_metadata(timestamp, table_names))
        {
            std::cout << "[CheckpointManager] No checkpoint found, starting with empty database" << std::endl;
            return true; // No checkpoint is OK for a fresh database
        }

        std::cout << "[CheckpointManager] Found checkpoint with " << table_names.size()
                  << " tables (timestamp: " << timestamp << ")" << std::endl;

        // Clear existing data first
        db.clear_all_data();

        // Recover each table
        for (const auto &table_name : table_names)
        {
            std::cout << "[CheckpointManager] Recovering table: " << table_name << std::endl;

            // Load and create table schema
            auto schema = disk_storage->load_table_schema(table_name);
            if (schema.empty())
            {
                std::cerr << "[CheckpointManager] Failed to load schema for table: " << table_name << std::endl;
                return false;
            }

            if (!db.create_table_from_schema(table_name, schema))
            {
                std::cerr << "[CheckpointManager] Failed to create table: " << table_name << std::endl;
                return false;
            }

            // Load table data
            std::vector<std::vector<std::string>> rows;
            std::vector<uint64_t> expiry_data;
            if (!disk_storage->load_table_data(table_name, rows, expiry_data))
            {
                std::cerr << "[CheckpointManager] Failed to load data for table: " << table_name << std::endl;
                return false;
            }

            if (!db.load_table_data(table_name, rows, expiry_data))
            {
                std::cerr << "[CheckpointManager] Failed to import data for table: " << table_name << std::endl;
                return false;
            }

            // Load primary index
            std::unordered_map<std::string, size_t> index_data;
            if (disk_storage->load_hash_index(table_name, index_data))
            {
                if (!db.load_primary_index(table_name, index_data))
                {
                    std::cerr << "[CheckpointManager] Failed to load index for table: " << table_name << std::endl;
                    return false;
                }
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "[CheckpointManager] Database recovery completed successfully in "
                  << duration.count() << "ms" << std::endl;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "[CheckpointManager] Recovery failed with exception: " << e.what() << std::endl;
        return false;
    }
}

void CheckpointManager::start_periodic_checkpointing(OptimizedDatabase &db, std::chrono::minutes interval)
{
    stop_periodic_checkpointing(); // Stop any existing thread

    checkpoint_interval = interval;
    periodic_enabled.store(true);
    periodic_thread = std::thread(&CheckpointManager::periodic_checkpoint_worker, this, &db);

    std::cout << "[CheckpointManager] Started periodic checkpointing every "
              << interval.count() << " minutes" << std::endl;
}

void CheckpointManager::stop_periodic_checkpointing()
{
    if (periodic_enabled.load())
    {
        periodic_enabled.store(false);
        if (periodic_thread.joinable())
        {
            periodic_thread.join();
        }
        std::cout << "[CheckpointManager] Stopped periodic checkpointing" << std::endl;
    }
}

bool CheckpointManager::has_checkpoint() const
{
    std::ifstream meta_file(metadata_file);
    return meta_file.good();
}

uint64_t CheckpointManager::get_checkpoint_timestamp() const
{
    uint64_t timestamp = 0;
    std::vector<std::string> table_names;
    const_cast<CheckpointManager *>(this)->read_checkpoint_metadata(timestamp, table_names);
    return timestamp;
}