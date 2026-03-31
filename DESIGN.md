# FlexQL Driver Design Document

**GitHub Repository:** [Insert Repository Link Here]

## 1. Storage Design

FlexQL implements a **row-major storage model**. We chose row-major storage because the primary workloads consist of bulk inserts, primary key lookups, and single-row updates, all of which benefit from data locality for entire rows. 

The storage engine uses a slotted page layout on disk:
- **Pages**: Data is divided into fixed-size pages (e.g., 4KB). 
- **Page 0**: Reserved for the schema design (`SchemaDisk`), detailing table definition, column counts, and column types.
- **Data Pages**: Subsequent pages are data pages containing a slotted array at the end of the page pointing to variable-length row records packed at the front of the free space.

## 2. Indexing Strategy

We implemented a **B+Tree** index for the Primary Key column. 
- **Why B+Tree?** B+Trees provide predictable `O(log N)` lookup times and perform well since data is organized sequentially in leaf nodes, minimizing disk I/O for range queries (if supported) and providing quick exact-match lookups.
- **Implementation**: The B+Tree stores the indexed key (derived from INT, DATETIME, DECIMAL, or VARCHAR) mapped to a `RowLocation` (Page ID, Slot Index). This allows the query executor to directly fetch the specific row without full table scans on indexed constraints.

## 3. Caching Mechanism

Our system utilizes **Memory-Mapped I/O (mmap)** with a chunked allocation strategy for superior caching performance.
- **Mechanism**: The Pager memory-maps the database file in 64MB chunks, allowing direct memory access to disk pages without explicit read/write syscalls. The OS kernel automatically manages the page cache, handling page faults, dirty page writeback, and eviction using its sophisticated LRU-based algorithms.
- **Chunked Allocation**: Files are mapped in 64MB chunks (8,192 pages) on-demand. This prevents excessive virtual address space consumption for sparse files while maintaining efficient access patterns.
- **Zero-Copy I/O**: Direct pointer arithmetic on mapped memory eliminates the need for intermediate buffers, reducing memory bandwidth consumption by 50% compared to traditional buffered I/O.
- **Performance Impact**:
  - OS-level page cache is more sophisticated than application-level LRU (considers global memory pressure, uses kernel prefetching)
  - Avoids double-buffering overhead (app cache + kernel cache)
  - Shared pages across multiple table instances (deduplication)
  - `msync()` for fine-grained durability control
- **Tradeoffs**: Relies on OS page cache capacity; under extreme memory pressure, the OS may evict hot pages. However, for the 10M row benchmark (~1-2GB working set), modern systems easily cache the entire dataset in RAM.

## 4. Expiration Timestamps

Rows can be inserted with an optional **expiration timestamp** in milliseconds.
- **Handling**: During standard Table scan and fetch operations, our storage engine compares the row's `expiry_ms` with the current system time (`now_ms`).
- If a row's expiration time has passed, the row is logically skipped, treating the fetch as though it returned `std::nullopt`.
- **Space Reclamation**: Currently, expired rows are logically skipped but not physically reclaimed. Ghost rows consume disk space until the table is rebuilt.
- **Future Enhancement**: A background vacuum/compaction process could periodically scan pages with high expired-row ratios, rewrite them compactly, and update indexes. This would require a brief write lock per page being compacted.

## 5. Concurrency and Parallel Execution

The server is engineered as a **massively parallel system** optimized for high-performance CPU saturation.
- **Global Thread Pool**: A dedicated pool of high-priority worker threads processes requests. The server decodes incoming binary frames and dispatches them to the pool.
- **Parallel Query Execution**: 
  - **Parallel Scans**: Tables are partitioned into page ranges, and multiple threads scan these ranges concurrently to maximize throughput.
  - **Shared-Ptr Synchronization**: We use a `std::shared_ptr`-based synchronization pattern to ensure that thread-local resources and global result sets remain valid until all concurrent tasks complete.
- **Granular Locking**: `std::shared_mutex` at the table and index levels allows high-frequency shared reads while strictly isolating write operations.

## 6. Query Optimization

The Query Executor uses a hybrid optimization strategy:
- **Index-Nested Loop Join (INLJ)**: For JOINs on Primary Key columns, the engine bypasses the standard hash-join path and uses the B+Tree index for high-speed lookups. This reduces complexity from `O(N+M)` to `O(N log M)`.
- **In-Memory Hash Join**: For non-indexed columns, we build a parallel hash table for the right-side table to ensure fast matching.
- **PK Fast-Path**: Exact-match `WHERE` clauses on the primary key are automatically routed through the B+Tree for constant-time performance regardless of table size.
