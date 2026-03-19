#include "optimized_database.h"

// Initialize thread-local storage
thread_local std::string OptimizedDatabase::result_buffer;