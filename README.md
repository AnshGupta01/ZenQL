<p align="center">
  <img src="public/ZenQL-logo.svg" width="90" style="border-radius: 16px;" />
</p>

<h1 align="center">ZenQL</h1>

<p align="center">
  <strong>Built for</strong><br/>
  <strong>CS69202: Design Lab 2026</strong><br/>
  IIT Kharagpur
</p>

<p align="center">
  A modern, high-performance SQL-like client/server engine with a compact wire protocol and checkpoint-backed persistence.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B&logoColor=white" alt="C++17" />
  <img src="https://img.shields.io/badge/Build-Make-427819?logo=gnu&logoColor=white" alt="Make" />
  <img src="https://img.shields.io/badge/Protocol-TCP-0A66C2" alt="TCP" />
  <img src="https://img.shields.io/badge/API-FlexQL-111827" alt="FlexQL API" />
  <img src="https://img.shields.io/badge/Persistence-Checkpoint-9A3412" alt="Checkpoint" />
</p>

The public client API and binaries are named `flexql_*`, so this repo uses both names:

- Project name: ZenQL
- C API / binaries: FlexQL

## What Works

- Multithreaded TCP server on port `9000`.
- C API in `include/flexql.h`:
  - `flexql_open`
  - `flexql_exec`
  - `flexql_close`
  - `flexql_free`
- SQL subset:
  - `CREATE TABLE [IF NOT EXISTS] ...`
  - `INSERT INTO ... VALUES (...)` (multi-row batches supported)
  - `SELECT ... FROM ...`
  - `WHERE` with `=`, `>`, `<`, `>=`, `<=`
  - `ORDER BY <col> [DESC]`
  - `INNER JOIN ... ON ...`
  - `DELETE FROM <table> [WHERE ...]`
- Optional row TTL using `INSERT ... EXPIRY <seconds>`.
- Checkpoint-based persistence in `data/` on startup/shutdown.

## Build

```bash
make clean && make
```

Build output:

- `bin/flexql_server`
- `bin/flexql_repl`
- `bin/benchmark_flexql`
- `bin/libflexql.a`

## Run

Start server:

```bash
make run-server
```

In another terminal, start REPL:

```bash
make run-repl
```

## Quick SQL Example

```sql
CREATE TABLE USERS(ID INT PRIMARY KEY, NAME VARCHAR, BALANCE DECIMAL);
INSERT INTO USERS VALUES (1, 'Alice', 1200), (2, 'Bob', 450);
SELECT NAME, BALANCE FROM USERS WHERE BALANCE > 500 ORDER BY BALANCE DESC;
DELETE FROM USERS WHERE ID = 2;
```

## Benchmarks And Data-Level Tests

One benchmark binary is currently maintained: `bin/benchmark_flexql`.

Run insertion benchmark (server is auto-started by the target):

```bash
make run-benchmark ARGS="1000000"
```

Run built-in data-level unit tests:

```bash
bin/benchmark_flexql --unit-test
```

Notes:

- `make run-benchmark` kills any existing `flexql_server`, starts a fresh one, and then runs the benchmark.
- There is no `benchmarks/run_all.sh` script in this repository.

## Protocol Notes

- Client sends one SQL statement per line (`\n` terminated).
- Server responds with text rows and terminates each response with a sentinel `\x00END\x00`.

## Current Constraints

- SQL dialect is intentionally limited and non-standard in places.
- No `UPDATE`, no transactions, no multi-statement parsing in a single request.
- `CHECKPOINT` is parsed but not currently wired as an executable command path.

## Repository Layout

- `src/server`: TCP server and connection handling.
- `src/client`: C API implementation and REPL.
- `src/parser`: SQL subset parser.
- `src/query`: query execution engine.
- `src/storage`: table storage and checkpoint manager.
- `src/index`: hash and btree index implementations.
- `benchmarks`: benchmark source.

## Benchmark Results Log

Command:

```bash
make run-benchmark ARGS="10000000"
```

Observed output summary:

| Date       |       Rows | Elapsed (ms) | Throughput (rows/sec) | Unit Tests   |
| :--------- | ---------: | -----------: | --------------------: | :----------- |
| 2026-03-27 | 10,000,000 |       21,637 |               462,171 | 22/22 passed |

Benchmark output is hardware and system-load dependent.

---

Author: Ansh Gupta, IIT Kharagpur
