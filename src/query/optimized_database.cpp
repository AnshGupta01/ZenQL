#include "optimized_database.h"

// Initialize thread-local storage
thread_local OptimizedDatabase::TableCache OptimizedDatabase::t_cache;