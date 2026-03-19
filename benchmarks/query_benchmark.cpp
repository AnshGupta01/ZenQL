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
    int ROW_COUNT = 100000;
    int QUERY_COUNT = 1000000;
    
    if (argc > 1) ROW_COUNT = std::stoi(argv[1]);
    if (argc > 2) QUERY_COUNT = std::stoi(argv[2]);

    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL Server." << std::endl;
        return 1;
    }

    char* errMsg = nullptr;
    std::cout << "Pre-filling " << ROW_COUNT << " rows for query benchmark..." << std::endl;
    flexql_exec(db, "CREATE TABLE QBENCH (ID INT PRIMARY KEY, VAL VARCHAR);", nullptr, nullptr, &errMsg);
    
    std::string batch = "";
    int BATCH_SIZE = 500;
    for (int i = 0; i < ROW_COUNT; i++) {
        batch += "INSERT INTO QBENCH VALUES (" + std::to_string(i) + ", 'Val_" + std::to_string(i) + "');\n";
        if ((i + 1) % BATCH_SIZE == 0 || i == ROW_COUNT - 1) {
            flexql_exec(db, batch.c_str(), nullptr, nullptr, &errMsg);
            batch = "";
        }
        if (i > 0 && i % 100000 == 0) std::cout << "Inserted " << i << " rows..." << std::endl;
    }

    std::cout << "Executing " << QUERY_COUNT << " batched single-row queries..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    batch = "";
    BATCH_SIZE = 200;
    for (int i = 0; i < QUERY_COUNT; i++) {
        int id = i % ROW_COUNT;
        batch += "SELECT * FROM QBENCH WHERE ID = " + std::to_string(id) + ";\n";
        
        if ((i + 1) % BATCH_SIZE == 0 || i == QUERY_COUNT - 1) {
            flexql_exec(db, batch.c_str(), null_callback, nullptr, &errMsg);
            batch = "";
        }
        if (i > 0 && i % 100000 == 0) std::cout << "Processed " << i << " queries..." << std::endl;
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "\n--- Query Benchmark Results ---" << std::endl;
    std::cout << "Data Size           : " << ROW_COUNT << " rows" << std::endl;
    std::cout << "Total Queries       : " << QUERY_COUNT << std::endl;
    std::cout << "Total Query Time    : " << diff.count() << " seconds" << std::endl;
    std::cout << "Throughput (Queries): " << (QUERY_COUNT / diff.count()) << " queries/sec" << std::endl;
    std::cout << "Avg Latency         : " << (diff.count() * 1000000.0 / QUERY_COUNT) << " us/query" << std::endl;

    flexql_close(db);
    return 0;
}
