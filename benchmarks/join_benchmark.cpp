#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include "flexql.h"

int null_callback(void* data, int columnCount, char** values, char** columnNames) {
    (void)data; (void)columnCount; (void)values; (void)columnNames;
    return 0; 
}

int main(int argc, char** argv) {
    int DEPT_COUNT = 100; // Small table
    int EMP_COUNT = 1000;   // Large table
    int JOIN_ITERATIONS = 100; // How many times to repeat the JOIN query
    
    if (argc > 1) DEPT_COUNT = std::stoi(argv[1]);
    if (argc > 2) EMP_COUNT = std::stoi(argv[2]);
    if (argc > 3) JOIN_ITERATIONS = std::stoi(argv[3]);

    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL Server." << std::endl;
        return 1;
    }

    char* errMsg = nullptr;
    std::cout << "--- INNER JOIN Performance Benchmark ---" << std::endl;
    
    // 1. Setup Tables
    flexql_exec(db, "CREATE TABLE DEPTS (ID INT PRIMARY KEY, DNAME VARCHAR);", nullptr, nullptr, &errMsg);
    flexql_exec(db, "CREATE TABLE EMPS (ID INT PRIMARY KEY, DEPTID INT, ENAME VARCHAR);", nullptr, nullptr, &errMsg);

    // 2. Pre-fill DEPTS
    std::cout << "Pre-filling " << DEPT_COUNT << " departments..." << std::endl;
    std::string batch = "";
    int BATCH_SIZE = 500;
    for (int i = 0; i < DEPT_COUNT; i++) {
        batch += "INSERT INTO DEPTS VALUES (" + std::to_string(i) + ", 'Dept_" + std::to_string(i) + "');\n";
        if ((i + 1) % BATCH_SIZE == 0 || i == DEPT_COUNT - 1) {
            flexql_exec(db, batch.c_str(), nullptr, nullptr, &errMsg);
            batch = "";
        }
    }

    // 3. Pre-fill EMPS
    std::cout << "Pre-filling " << EMP_COUNT << " employees across " << DEPT_COUNT << " departments..." << std::endl;
    batch = "";
    for (int i = 0; i < EMP_COUNT; i++) {
        int dept_id = i % DEPT_COUNT;
        batch += "INSERT INTO EMPS VALUES (" + std::to_string(i) + ", " + std::to_string(dept_id) + ", 'Emp_" + std::to_string(i) + "');\n";
        if ((i + 1) % BATCH_SIZE == 0 || i == EMP_COUNT - 1) {
            flexql_exec(db, batch.c_str(), nullptr, nullptr, &errMsg);
            batch = "";
        }
        if (i > 0 && i % 100000 == 0) std::cout << "Inserted " << i << " rows..." << std::endl;
    }

    // 4. JOIN Benchmark
    std::cout << "Executing " << JOIN_ITERATIONS << " JOIN queries (" << EMP_COUNT << " x " << DEPT_COUNT << " scan)..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    std::string join_query = "SELECT EMPS.ENAME, DEPTS.DNAME FROM EMPS INNER JOIN DEPTS ON EMPS.DEPTID = DEPTS.ID;";
    for (int i = 0; i < JOIN_ITERATIONS; i++) {
        flexql_exec(db, join_query.c_str(), null_callback, nullptr, &errMsg);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "\n--- JOIN Benchmark Results ---" << std::endl;
    std::cout << "Left Table (EMPS)   : " << EMP_COUNT << " rows" << std::endl;
    std::cout << "Right Table (DEPTS) : " << DEPT_COUNT << " rows" << std::endl;
    std::cout << "Total Joins executed: " << JOIN_ITERATIONS << std::endl;
    std::cout << "Total Time          : " << diff.count() << " seconds" << std::endl;
    std::cout << "Throughput          : " << (JOIN_ITERATIONS / diff.count()) << " JOIN-queries/sec" << std::endl;
    std::cout << "Avg Latency         : " << (diff.count() * 1000.0 / JOIN_ITERATIONS) << " ms/JOIN" << std::endl;

    flexql_close(db);
    return 0;
}
