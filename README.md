# FlexQL

A high-performance, multithreaded SQL Database Engine implemented natively in C++17.

## Features
- **SQL Parsing**: Supports string parsing for `CREATE TABLE`, `INSERT`, `SELECT`, `WHERE`, and `INNER JOIN`.
- **High Performance Storage**: Column-major memory layout, HashMap Primary Keys, and B-Tree Secondary Indexing.
- **Multithreading**: Thread-pooled server with partition-level read-write locking (`std::shared_mutex`) and `SpinLock` structures for lock-free optimistic concurrency control.
- **Caching**: Concurrent LRU Query caching.

---

## 🚀 Getting Started

FlexQL requires **zero external dependencies** and builds via a standard Makefile.

### 1. Build the Database
```bash
make clean && make
```

### 2. Start the Server
Automatically kills any previous server instances and starts a new one:
```bash
make run-server
```

### 3. Connect via the Client (REPL)
Open another terminal to launch the interactive SQL shell:
```bash
make run-repl
```

```sql
flexql> CREATE TABLE USERS(ID INT PRIMARY KEY, NAME VARCHAR);
flexql> CREATE TABLE POSTS(ID INT PRIMARY KEY, USER_ID INT, CONTENT VARCHAR);
flexql> INSERT INTO USERS VALUES (1, 'Alice');
flexql> INSERT INTO POSTS VALUES (101, 1, 'Hello ZenQL');
flexql> SELECT * FROM USERS INNER JOIN POSTS ON USERS.ID = POSTS.USER_ID;
flexql> exit
```

### 4. Run the Automated Benchmark Suite
To measure insertion and read throughput speeds across mixed-workload tiers (100 to 100M rows), execute:
```bash
./benchmarks/run_all.sh
```
*(Ensure the server is running first.)*

### 🏗️ Running Custom Benchmarks
You can execute individual benchmark tools for specific workload tuning:
FlexQL includes a comprehensive master benchmark that tests multiple workload phases (Pure Insertion, Mixed Read/Write, Point Lookups, and Hash JOINs).

To run the master benchmark at a 10M row scale:
```bash
make run-benchmark ARGS="10000000 10000000"
```
*(Ensure the server is running first with `make run-server`.)*

## 📊 Performance Results @ 10M Scale
The following results were achieved on a 10M row dataset with our optimized TCP protocol and lock-free concurrency fixes:

| Phase | Metric | Result | Notes |
|:---|:---|:---|:---|
| **Insertion** | Throughput | **502,326 rows/sec** | 10M rows, batched |
| **Mixed** | Inserts under load | **657,962 rows/sec** | 618 concurrent reads survived |
| **PK Query** | Point Lookup | **397,380 queries/sec** | 2.52 µs/query avg latency |
| **JOIN** | Hash Join | **8.5 JOINs/sec** | 100K × 100 table scan |

---
*Note: JOIN performance is currently bottlenecked by the transport of large result sets over TCP.*
