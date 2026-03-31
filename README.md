<p align="center">
  <img src="public/ZenQL-logo.svg" width="90" style="border-radius: 16px;" />
</p>

<h1 align="center">ZenQL</h1>

<p align="center">
  <strong>CS69202: Design Lab 2026</strong> • IIT Kharagpur
</p>

<p align="center">
  A high-performance SQL engine with a compact wire protocol, columnar in-memory storage, and checkpoint-backed persistence.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <img src="https://img.shields.io/badge/Build-Make-427819?logo=gnu&logoColor=white" alt="Make" />
  <img src="https://img.shields.io/badge/Protocol-TCP-0A66C2" alt="TCP" />
  <img src="https://img.shields.io/badge/Persistence-Checkpoint-9A3412" alt="Checkpoint" />
</p>

---

## ⚡ Quick Start

### Build
```bash
make clean && make
```

### Run
1. **Server**: `make run-server`
2. **REPL**: `make run-repl`
3. **Benchmark**: `make run-benchmark ARGS="10000000"`

---

## Architecture & Design

ZenQL is optimized for high-throughput, in-memory analytical workloads:

- **Columnar Storage**: Data is stored in decomposed vectors (`StringColumn`) rather than row-based structs, significantly improving cache locality for filtered scans and joins.
- **Multi-Layered Caching**: 
  - **Thread-Local TableCache**: Minimizes lock contention for frequent point-lookups.
  - **Versioned JoinCache**: Uses a "build-once, probe-many" strategy for hash joins, invalidated only on table mutations.
- **ThreadPool Concurrency**: A fixed-size thread pool manages both connection handling and parallelized query execution (e.g., parallelized join probing).
- **Efficient SQL Parsing**: The parser identifies statement types and literals directly from the incoming buffer with minimal allocations.

---

## Features

- **SQL Subset**: `CREATE`, `INSERT` (batched), `SELECT`, `WHERE`, `ORDER BY`, `INNER JOIN`, and `DELETE`.
- **High Performance**: ~460k+ inserts/sec via columnar storage and zero-copy parsing paths.
- **Persistence**: Snapshot-based recovery via `CheckpointManager`.
- **TTL Support**: Built-in row expiration (e.g., `INSERT ... EXPIRY <seconds>`).
- **Concurrency**: Multi-threaded TCP server with thread-pooled query execution.

---

## Benchmark results (10M Rows)

| Date | Rows | Elapsed | Throughput | Unit Tests |
| :--- | :--- | :--- | :--- | :--- |
| 2026-03-31 | 10,000,000 | 21,651 ms | **461,872 rows/sec** | 22/22 (100%) |

---

## Repository Structure

- `src/server`: Connection handling & ThreadPool.
- `src/parser`: Zero-copy SQL parser.
- `src/query`: `OptimizedDatabase` execution engine.
- `src/storage`: `Table` (columnar) & `CheckpointManager`.
- `src/index`: Fast Hash & B-Tree indexing.
- `src/client`: C API (`libflexql`) and REPL frontend.

---

**Author**: Ansh Gupta, IIT Kharagpur
