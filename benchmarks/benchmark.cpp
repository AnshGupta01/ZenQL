#include <iostream>
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>
#include "flexql.h"

int main(int argc, char** argv) {
    int ROW_COUNT = 1000;
    if (argc > 1) ROW_COUNT = std::stoi(argv[1]);

    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL Server." << std::endl;
        return 1;
    }

    char* errMsg = nullptr;
    flexql_exec(db, "CREATE TABLE BENCH (ID INT PRIMARY KEY, VAL VARCHAR);", nullptr, nullptr, &errMsg);

    auto start = std::chrono::high_resolution_clock::now();
    std::string batch = "";
    int BATCH_SIZE = 500;
    for (int i = 0; i < ROW_COUNT; i++) {
        batch += "INSERT INTO BENCH VALUES (" + std::to_string(i) + ", 'value_" + std::to_string(i) + "');\n";
        if ((i + 1) % BATCH_SIZE == 0 || i == ROW_COUNT - 1) {
            flexql_exec(db, batch.c_str(), nullptr, nullptr, &errMsg);
            batch = "";
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "\n--- Benchmark Results ---" << std::endl;
    std::cout << "Rows Inserted       : " << ROW_COUNT << std::endl;
    std::cout << "Total Time          : " << diff.count() << " seconds" << std::endl;
    std::cout << "Throughput (Inserts): " << (ROW_COUNT / diff.count()) << " rows/sec" << std::endl;

    flexql_close(db);
    return 0;
}
