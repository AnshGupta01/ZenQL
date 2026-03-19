#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

// Error Codes
#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

// Database Handle (Opaque Structure)
typedef struct FlexQL FlexQL;

// API Specifications

/**
 * Establishes a connection to the FlexQL database server.
 * @param host Hostname or IP address of the server
 * @param port Port number
 * @param db Pointer to the database handle
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_open(const char *host, int port, FlexQL **db);

/**
 * Executes an SQL statement on the FlexQL database server.
 * @param db Open database connection
 * @param sql SQL statement to execute
 * @param callback Callback invoked for every row returned
 * @param arg User-provided pointer passed via data
 * @param errmsg Pointer to error message string
 * @return FLEXQL_OK or FLEXQL_ERROR
 */
int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *data, int columnCount, char **values, char **columnNames),
    void *arg,
    char **errmsg
);

/**
 * Closes the connection to the FlexQL server and releases resources.
 * @param db Database handle
 * @return FLEXQL_OK on success
 */
int flexql_close(FlexQL *db);

/**
 * Frees memory allocated by the FlexQL API (such as error messages).
 * @param ptr Pointer to free
 */
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif // FLEXQL_H
