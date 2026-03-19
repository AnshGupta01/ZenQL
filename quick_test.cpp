#include <iostream>
#include <chrono>
#include "../include/flexql.h"

int main() {
    FlexQL* db = nullptr;

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to server" << std::endl;
        return 1;
    }

    char* errMsg = nullptr;

    // Create a test table
    flexql_exec(db, "CREATE TABLE QUICK_TEST (ID INT PRIMARY KEY, VAL VARCHAR);", nullptr, nullptr, &errMsg);

    // Quick insert test
    auto start = std::chrono::high_resolution_clock::now();

    std::string batch = "";
    for (int i = 0; i < 1000; i++) {
        batch += "INSERT INTO QUICK_TEST VALUES (" + std::to_string(i) + ", 'test_" + std::to_string(i) + "');\n";
        if ((i + 1) % 100 == 0 || i == 999) {
            flexql_exec(db, batch.c_str(), nullptr, nullptr, &errMsg);
            batch = "";
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    double throughput = 1000.0 / (duration.count() / 1000.0);

    std::cout << "Quick Test Results:" << std::endl;
    std::cout << "1000 rows inserted in " << duration.count() << "ms" << std::endl;
    std::cout << "Throughput: " << static_cast<int>(throughput) << " rows/sec" << std::endl;

    flexql_close(db);
    return 0;
}