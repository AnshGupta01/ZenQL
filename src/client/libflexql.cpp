#include "flexql.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <sstream>

struct FlexQL
{
    int socket_fd;
    bool is_connected;
};

int flexql_open(const char *host, int port, FlexQL **db)
{
    if (!db || !host)
        return FLEXQL_ERROR;

    *db = (FlexQL *)malloc(sizeof(FlexQL));
    if (!*db)
        return FLEXQL_ERROR;

    (*db)->socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if ((*db)->socket_fd < 0)
    {
        free(*db);
        return FLEXQL_ERROR;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0)
    {
        close((*db)->socket_fd);
        free(*db);
        return FLEXQL_ERROR;
    }

    if (connect((*db)->socket_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        close((*db)->socket_fd);
        free(*db);
        return FLEXQL_ERROR;
    }

    int nodelay = 1;
    setsockopt((*db)->socket_fd, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay));

    (*db)->is_connected = true;
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *data, int columnCount, char **values, char **columnNames),
    void *arg,
    char **errmsg)
{
    if (!db || !db->is_connected || !sql)
    {
        if (errmsg)
            *errmsg = strdup("Invalid database connection or SQL query");
        return FLEXQL_ERROR;
    }

    std::string sql_with_nl = std::string(sql) + "\n";
    send(db->socket_fd, sql_with_nl.c_str(), sql_with_nl.length(), 0);

    // Read until the server's sentinel \x00END\x00 is received
    std::string response;
    response.reserve(4 * 1024 * 1024); // 4MB initial reserve
    char buffer[65536];
    static const std::string SENTINEL("\x00END\x00", 5);

    while (true)
    {
        int valread = read(db->socket_fd, buffer, sizeof(buffer));
        if (valread <= 0)
            break; // Connection closed or error

        response.append(buffer, valread);

        // Check if the sentinel has arrived (it's always at the very end)
        if (response.size() >= SENTINEL.size() &&
            response.compare(response.size() - SENTINEL.size(),
                             SENTINEL.size(), SENTINEL) == 0)
        {
            // Strip the sentinel before parsing
            response.resize(response.size() - SENTINEL.size());
            break;
        }
    }


    std::stringstream ss(response);
    std::string line;
    bool header_read = false;
    std::vector<std::string> colNames;
    std::vector<char *> colNamesPtrs;

    while (std::getline(ss, line))
    {
        if (line.empty())
            continue;
        if (line.find("ERROR") == 0)
        {
            if (errmsg)
                *errmsg = strdup(line.c_str());
            return FLEXQL_ERROR;
        }
        if (line.find("OK") == 0 || line.find("SUCCESS") == 0)
        {
            if (line.find("rows returned") != std::string::npos)
                break;
            continue;
        }

        if (!header_read)
        {
            std::stringstream ss_cols(line);
            std::string col;
            while (std::getline(ss_cols, col, '\t'))
            {
                if (!col.empty())
                    colNames.push_back(col);
            }
            if (colNames.empty())
                continue;
            for (auto &s : colNames)
                colNamesPtrs.push_back((char *)s.c_str());
            header_read = true;
            continue;
        }

        // Row data
        std::vector<std::string> rowValues;
        std::stringstream ss_vals(line);
        std::string val;
        while (std::getline(ss_vals, val, '\t'))
        {
            rowValues.push_back(val);
        }
        while (rowValues.size() < colNames.size())
            rowValues.push_back("");

        std::vector<char *> rowValuesPtrs;
        for (auto &s : rowValues)
            rowValuesPtrs.push_back((char *)s.c_str());

        if (callback && callback(arg, colNames.size(), rowValuesPtrs.data(), colNamesPtrs.data()) != 0)
            break;
    }

    return FLEXQL_OK;
}

int flexql_close(FlexQL *db)
{
    if (!db)
        return FLEXQL_ERROR;
    if (db->socket_fd >= 0)
        close(db->socket_fd);
    db->is_connected = false;
    free(db);
    return FLEXQL_OK;
}

void flexql_free(void *ptr)
{
    if (ptr)
    {
        free(ptr);
    }
}
