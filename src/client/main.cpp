#include <iostream>
#include <string>
#include "flexql.h"

int callback(void *data, int columnCount, char **values, char **columnNames) {
    (void)data;
    for(int i = 0; i < columnCount; i++) {
        std::cout << columnNames[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << std::endl;
    return 0;
}

int main() {
    FlexQL *db = nullptr;
    int rc = flexql_open("127.0.0.1", 9000, &db);
    
    if (rc != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL server at 127.0.0.1:9000" << std::endl;
        return 1;
    }
    std::cout << "Connected to FlexQL server.\nType 'exit' to quit.\n";

    std::string line;
    std::string query_buffer;
    char *errMsg = nullptr;

    while (true) {
        if (query_buffer.empty()) {
            std::cout << "flexql> ";
        } else {
            std::cout << "    ...> ";
        }

        if (!std::getline(std::cin, line)) break;
        
        if (line == "exit" || line == "quit") {
            break;
        }

        // Basic comment skipping
        size_t comment_pos = line.find("--");
        if (comment_pos != std::string::npos) {
            line = line.substr(0, comment_pos);
        }

        if (line.find_first_not_of(" \n\r\t") == std::string::npos) continue;

        // Trim whitespace
        line.erase(0, line.find_first_not_of(" \n\r\t"));
        line.erase(line.find_last_not_of(" \n\r\t") + 1);

        if (line.empty()) continue;

        query_buffer += (query_buffer.empty() ? "" : " ") + line;

        // Check for semicolon
        if (query_buffer.back() == ';') {
            rc = flexql_exec(db, query_buffer.c_str(), callback, nullptr, &errMsg);
            if (rc != FLEXQL_OK) {
                std::cerr << "Error: " << (errMsg ? errMsg : "Unknown") << std::endl;
                flexql_free(errMsg);
                errMsg = nullptr;
            }
            query_buffer.clear();
        }
    }

    flexql_close(db);
    return 0;
}
