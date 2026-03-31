#include "flexql.h"
#include <cstdio>
#include <cstring>
#include <string>
#include <iostream>

// ─── Callback: print one row ──────────────────────────────────────────────────
static int print_row(void* /*data*/, int ncols, char** vals, char** names) {
    for (int i = 0; i < ncols; i++) {
        printf("%s = %s\n", names[i], vals[i] ? vals[i] : "NULL");
    }
    printf("\n");
    return 0;
}

// ─── REPL ────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    const char* host = "127.0.0.1";
    int port = 9000;
    if (argc > 1) host = argv[1];
    if (argc > 2) port = std::atoi(argv[2]);

    FlexQL* db = nullptr;
    int rc = flexql_open(host, port, &db);
    if (rc != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to FlexQL server at %s:%d\n", host, port);
        return 1;
    }
    printf("Connected to FlexQL server at %s:%d\n", host, port);
    printf("Type SQL queries (end with ;). Type .exit or Ctrl-D to quit.\n\n");

    std::string acc;
    while (true) {
        printf(acc.empty() ? "flexql> " : "     -> ");
        fflush(stdout);
        std::string line;
        if (!std::getline(std::cin, line)) break;
        if (line == ".exit" || line == "\\q") break;

        acc += " " + line;
        // Execute when we see a semicolon (possibly multiple stmts)
        if (!acc.empty() && acc.find(';') != std::string::npos) {
            char* errmsg = nullptr;
            rc = flexql_exec(db, acc.c_str(), print_row, nullptr, &errmsg);
            if (rc != FLEXQL_OK && errmsg) {
                fprintf(stderr, "Error: %s\n", errmsg);
                flexql_free(errmsg);
            } else if (rc == FLEXQL_OK) {
                printf("Query executed successfully\n");
            }
            acc.clear();
        }
    }

    flexql_close(db);
    return 0;
}
