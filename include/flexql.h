#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

// Opaque database handle
typedef struct FlexQL FlexQL;

// Open a connection to the FlexQL server
int flexql_open(const char *host, int port, FlexQL **db);

// Close the connection and free resources
int flexql_close(FlexQL *db);

// Execute an SQL statement; callback invoked for each result row
int flexql_exec(FlexQL *db,
                const char *sql,
                int (*callback)(void*, int, char**, char**),
                void *arg,
                char **errmsg);

// Free memory allocated by the FlexQL library
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif
