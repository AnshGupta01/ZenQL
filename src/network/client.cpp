// FlexQL client library — implements the public C API
// Internal struct kept private from consumers.

#include "flexql.h"
#include "network/protocol.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <sstream>

// ─── Opaque handle ────────────────────────────────────────────────────────────
struct FlexQL {
    int fd;
};

extern "C" {

int flexql_open(const char* host, int port, FlexQL** db) {
    if (!db) return FLEXQL_ERROR;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return FLEXQL_ERROR;

    // TCP performance tuning
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    int bufsz = 1 << 20; // 1 MB
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof(bufsz));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return FLEXQL_ERROR;
    }

    *db = new FlexQL{fd};
    return FLEXQL_OK;
}

int flexql_close(FlexQL* db) {
    if (!db) return FLEXQL_ERROR;
    close(db->fd);
    delete db;
    return FLEXQL_OK;
}

// ─── flexql_exec ─────────────────────────────────────────────────────────────
// Wire:
//   Send: [4-byte len LE][sql bytes]
//   Recv: [4-byte len LE][response text][4-byte sentinel]
//
// Response text lines:
//   COL\tc1\tc2\n   — column names (first line of a result set)
//   ROW\tv1\tv2\n   — one result row
//   DONE\n          — end of result set
//   ERR\tmsg\n      — error

int flexql_exec(FlexQL* db, const char* sql,
                int (*callback)(void*, int, char**, char**),
                void* arg, char** errmsg) {
    if (!db || !sql) return FLEXQL_ERROR;

    // Send request
    uint32_t len = (uint32_t)strlen(sql);
    // length prefix
    if (send(db->fd, &len, 4, MSG_NOSIGNAL) != 4) return FLEXQL_ERROR;
    if (len > 0 && send(db->fd, sql, len, MSG_NOSIGNAL) != (ssize_t)len) return FLEXQL_ERROR;
    // sentinel
    if (send(db->fd, &Protocol::SENTINEL, 4, MSG_NOSIGNAL) != 4) return FLEXQL_ERROR;

    // Read response frame
    auto payload = Protocol::read_frame(db->fd);
    if (payload.empty() && len > 0) {
        if (errmsg) { *errmsg = strdup("Connection lost"); }
        return FLEXQL_ERROR;
    }

    // Parse response text
    std::string text(reinterpret_cast<const char*>(payload.data()), payload.size());
    std::istringstream iss(text);
    std::string line;

    std::vector<std::string> col_names;
    int result = FLEXQL_OK;

    while (std::getline(iss, line)) {
        if (line.empty()) continue;
        if (line.substr(0, 4) == "ERR\t") {
            if (errmsg) *errmsg = strdup(line.substr(4).c_str());
            result = FLEXQL_ERROR;
            break;
        }
        if (line.substr(0, 4) == "COL\t") {
            col_names.clear();
            std::istringstream cs(line.substr(4));
            std::string c;
            while (std::getline(cs, c, '\t')) col_names.push_back(c);
        } else if (line.substr(0, 4) == "ROW\t" && callback) {
            std::vector<std::string> vals;
            std::istringstream vs(line.substr(4));
            std::string v;
            while (std::getline(vs, v, '\t')) vals.push_back(v);

            int nc = (int)col_names.size();
            std::vector<char*> vptrs(nc), nptrs(nc);
            for (int i = 0; i < nc; i++) {
                vptrs[i] = (char*)vals[i].c_str();
                nptrs[i] = (char*)col_names[i].c_str();
            }
            if (callback(arg, nc, vptrs.data(), nptrs.data()) != 0) break;
        }
        // DONE — continue to next result set in batch response
    }
    return result;
}

void flexql_free(void* ptr) {
    free(ptr);
}

} // extern "C"
