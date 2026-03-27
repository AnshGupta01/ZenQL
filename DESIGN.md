# ZenQL Design Document

This document describes the current architecture and behavior of ZenQL as implemented in this repository.

## 1. System Overview

ZenQL is a TCP client/server SQL-like engine written in C++17.

Core runtime path:

1. Client calls `flexql_exec` in `src/client/libflexql.cpp`.
2. SQL is sent to server as newline-terminated text.
3. Server in `src/server/server.cpp` reads lines, executes each statement through `OptimizedDatabase`, and sends text output back.
4. Client reads until response sentinel `\x00END\x00`, parses tab-delimited rows, and invokes callback per row.

## 2. Server And Concurrency Model

Concurrency model has two layers:

- Connection-level concurrency:
  - Server accepts sockets and schedules each client on a shared `ThreadPool`.
  - `TCP_NODELAY` is enabled for lower interactive latency.
- Database-level concurrency:
  - `OptimizedDatabase` uses a global `std::shared_mutex` around table/index maps.
  - Read-heavy operations acquire shared locks; structural writes (for example `CREATE TABLE`) acquire unique locks.

Within `INNER JOIN`, equal-key joins are parallelized by splitting the probe side into chunks across worker threads.

## 3. SQL Subset And Parsing

Parser lives in `src/parser/parser.cpp` and outputs `ParsedQuery` variants from `src/parser/parser.h`.

Implemented query types:

- `CREATE TABLE [IF NOT EXISTS]`
- `INSERT INTO ... VALUES (...)` (single or batched rows)
- `SELECT ... FROM ...`
- Optional `WHERE` with comparison operators: `=`, `>`, `<`, `>=`, `<=`
- Optional `ORDER BY <column> [DESC]`
- `INNER JOIN ... ON ...`
- `DELETE FROM <table> [WHERE ...]`

Notes:

- Table/column identifiers are validated to alphanumeric plus underscore.
- SQL is case-insensitive for keywords.
- Semicolon is optional at parse stage; trailing `;` is stripped.
- `CHECKPOINT` is recognized by the parser but is not currently executed as a dedicated branch in `OptimizedDatabase`.

## 4. Storage And Indexing

Table storage and typed schema are in `src/storage`.

Current indexing behavior:

- `FastHashIndex` is maintained per table for point lookup acceleration.
- Primary key column index is derived from schema (`PRIMARY KEY`) when provided.
- If no primary key is declared, index position defaults to column `0`.

`BTreeIndex` scaffolding exists in `src/index/btree_index.h`, but primary query execution is currently centered on hash index plus scan/filter/order operations.

## 5. Query Execution Path

`OptimizedDatabase::execute_to_buffer` in `src/query/optimized_database.h` handles execution.

Execution highlights:

- Fast path for `SELECT * FROM <table> WHERE <pk> = <value>` using cached table pointers and hash index lookup.
- General select path supports:
  - Full scan with optional filter and ordering
  - Projection (`*` or column subset)
- Join path supports:
  - Hash-assisted equality joins
  - Non-equality join predicates via nested comparison loop
  - Optional post-join filter and ordering
- Delete path supports full-table delete or conditional delete with rebuild of retained rows and index.

All results are serialized to a string buffer as tab-delimited rows with a status/header prefix.

## 6. TTL And Expiration

Rows have separate expiration metadata in table storage.

Behavior:

- `INSERT ... EXPIRY <seconds>` computes absolute expiry as `now + seconds`.
- Expired rows are skipped by select/join lookups.
- Expired rows are not immediately compacted unless modified through operations such as delete/rewrite.

## 7. Persistence And Recovery

Persistence is enabled in server startup via:

- `OptimizedDatabase global_db("data", true);`

Checkpoint lifecycle:

- On startup: `CheckpointManager` attempts recovery from `data/`.
- On shutdown signals (`SIGINT` / `SIGTERM`): server triggers `save_checkpoint()` before exit.

This provides crash-restart durability at checkpoint granularity.

## 8. Network Protocol

Protocol is plaintext and line-based:

- Request framing: newline (`\n`) separated SQL statements.
- Response framing: response text terminated by sentinel bytes `\x00END\x00`.
- Row format: tab-separated columns.

The protocol is intentionally simple and optimized for low overhead in the provided client library.

## 9. Current Limitations

- No transaction semantics (`BEGIN/COMMIT/ROLLBACK` not implemented).
- No `UPDATE` command.
- No multi-statement SQL parser in one logical request beyond newline-delimited independent statements.
- Error reporting is concise and not yet highly diagnostic.
- Secondary index usage is limited in current execution paths.

## 10. Design Intent

ZenQL prioritizes:

- Fast local development cycle (single Makefile, no external runtime dependencies).
- High-throughput in-memory operations with practical persistence checkpoints.
- A compact SQL subset suitable for benchmarking parser, index, and concurrency strategies.
