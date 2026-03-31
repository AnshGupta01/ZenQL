#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <signal.h>
#include <filesystem>

#include "query/executor.h"
#include "concurrency/thread_pool.h"
#include "network/protocol.h"
#include "common/config.h"

namespace fs = std::filesystem;

static Catalog*          g_catalog   = nullptr;
static ThreadPool*       g_pool      = nullptr;
static volatile bool     g_running   = true;
static std::atomic<int>  g_server_fd = -1;

static inline void append_str(std::vector<uint8_t>& out, const std::string& s) {
    out.insert(out.end(), s.begin(), s.end());
}
static inline void append_char(std::vector<uint8_t>& out, char c) {
    out.push_back(c);
}

// ─── Format a ResultSet into the wire protocol text ──────────────────────────
static void format_result_into(const ResultSet& rs, std::vector<uint8_t>& out) {
    if (!rs.ok) {
        append_str(out, "ERR\t");
        append_str(out, rs.error);
        append_char(out, '\n');
        return;
    }
    if (!rs.col_names.empty()) {
        append_str(out, "COL");
        for (const auto& c : rs.col_names) {
            append_char(out, '\t');
            append_str(out, c);
        }
        append_char(out, '\n');
        for (const auto& row : rs.rows) {
            append_str(out, "ROW");
            for (const auto& v : row.values) {
                append_char(out, '\t');
                append_str(out, v);
            }
            append_char(out, '\n');
        }
    }
    append_str(out, "DONE\n");
}

// ─── Handle one client connection ────────────────────────────────────────────
static void handle_client(int fd) {
    Executor exec(*g_catalog);

    // Set socket options for performance
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    // Socket timeouts (5 seconds) to allow thread to check g_running
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    // Increase socket buffer sizes
    int bufsz = SEND_BUF_SIZE;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof(bufsz));

    try {
        for (;;) {
            // Read request frame
            auto payload = Protocol::read_frame(fd);
            if (payload.empty()) {
                if (!g_running) break;
                // Check if it was a timeout or error
                break; 
            }

            std::string sql(reinterpret_cast<const char*>(payload.data()), payload.size());

            auto results = exec.exec_batch(sql);

            // Build response directly into a contiguous vector bypass std::string
            std::vector<uint8_t> out_buf;
            out_buf.reserve(65536); // 64KB initial size
            for (const auto& rs : results) {
                format_result_into(rs, out_buf);
            }
            if (results.empty()) append_str(out_buf, "DONE\n");

            if (!Protocol::write_frame(fd, out_buf)) break;
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "[FATAL] Client handler exception: %s\n", e.what());
    } catch (...) {
        fprintf(stderr, "[FATAL] Client handler unknown exception\n");
    }
    close(fd);
}

// ─── Main ────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    int port = DEFAULT_PORT;
    if (argc > 1) port = std::atoi(argv[1]);

    // Ensure data directories exist
    fs::create_directories(DATA_DIR);
    fs::create_directories(IDX_DIR);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, [](int) { 
        g_running = false; 
        int sfd = g_server_fd.load();
        if (sfd != -1) {
            shutdown(sfd, SHUT_RDWR);
            close(sfd);
            g_server_fd.store(-1);
        }
    });

    g_pool    = new ThreadPool(THREAD_POOL_SIZE);
    ThreadPool::set_global(g_pool);
    g_catalog = new Catalog(DATA_DIR);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }
    g_server_fd.store(server_fd);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    listen(server_fd, BACKLOG);

    std::cout << "FlexQL server listening on port " << port
              << " (" << THREAD_POOL_SIZE << " workers)\n";

    while (g_running) {
        sockaddr_in client_addr{};
        socklen_t clen = sizeof(client_addr);
        int cfd = accept(server_fd, (sockaddr*)&client_addr, &clen);
        if (cfd < 0) { if (g_running) perror("accept"); break; }
        // printf("Client connected: %d\n", cfd);
        g_pool->submit([cfd] { handle_client(cfd); });
    }

    int sfd = g_server_fd.exchange(-1);
    if (sfd != -1) close(sfd);
    
    delete g_pool;
    delete g_catalog;
    std::cout << "Server stopped.\n";
    return 0;
}
