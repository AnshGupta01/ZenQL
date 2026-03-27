#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include "../concurrency/thread_pool.h"
#include "../query/optimized_database.h"
#include <csignal>
#include <future> // Added for std::future

#include <netinet/tcp.h>
#define PORT 9000
#define MAX_CONN 100
#define BUFFER_SIZE 262144

ThreadPool *global_pool = nullptr; // Moved ThreadPool to global scope

OptimizedDatabase global_db("data", true); // Enable persistence with "data" directory

// Graceful shutdown flag
std::atomic<bool> server_running{true};

// Signal handler for graceful shutdown
void signal_handler(int signal)
{
    if (signal == SIGINT || signal == SIGTERM)
    {
        std::cout << "\n[Server] Received shutdown signal, saving checkpoint..." << std::endl;
        server_running.store(false, std::memory_order_release);

        // Trigger explicit checkpoint before shutdown
        global_db.save_checkpoint();
        std::cout << "[Server] Checkpoint saved. Shutting down gracefully." << std::endl;

        // Clean up thread pool
        if (global_pool)
        {
            delete global_pool;
            global_pool = nullptr;
        }

        exit(0);
    }
}

void handle_client(int client_socket)
{
    char buffer[BUFFER_SIZE];
    std::string remainder;
    remainder.reserve(4096); // Pre-allocate for typical query size

    while (true)
    {
        int bytes_read = recv(client_socket, buffer, BUFFER_SIZE, 0);

        if (bytes_read <= 0)
        {
            if (bytes_read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
            if (bytes_read < 0) {
                std::cerr << "Socket receive error (errno: " << errno << ")" << std::endl;
            } else { // bytes_read == 0
                std::cout << "Client disconnected." << std::endl;
            }
            close(client_socket);
            break;
        }

        // Optimize: Append to remainder instead of creating new string
        remainder.append(buffer, bytes_read);

        std::string response;
        response.reserve(8192); // Pre-allocate response buffer

        std::vector<std::string_view> queries;
        queries.reserve(64);

        size_t last_pos = 0;
        size_t next_pos = 0;

        // Parse queries in-place from combined buffer
        while ((next_pos = remainder.find('\n', last_pos)) != std::string::npos)
        {
            if (next_pos > last_pos)
            {
                std::string_view query_view(&remainder[last_pos], next_pos - last_pos);
                // Trim trailing whitespace
                while (!query_view.empty() && (query_view.back() == ' ' ||
                                               query_view.back() == '\r' || query_view.back() == '\t'))
                {
                    query_view.remove_suffix(1);
                }

                if (!query_view.empty())
                {
                    queries.emplace_back(query_view);
                }
            }
            last_pos = next_pos + 1;
        }

        // Process collected queries BEFORE modifying remainder (to keep query views valid)
        if (!queries.empty())
        {
            uint64_t now = global_db.get_current_time();
            response.reserve(queries.size() * 64);
            for (const auto& q : queries) {
                global_db.execute_to_buffer(q, response, now);
            }
        }

        // Keep unparsed data for next iteration
        if (last_pos < remainder.size())
        {
            remainder = remainder.substr(last_pos);
        }
        else
        {
            remainder.clear();
        }

        if (!response.empty())
        {
            // Append sentinel so the client knows the full response has arrived.
            // Using \x00END\x00 — a sequence that cannot appear in normal SQL output.
            static const char SENTINEL[] = "\x00END\x00";
            static const int  SENTINEL_LEN = 5;

            // Stream in chunks to avoid a single blocking send for huge responses
            const char* data = response.c_str();
            size_t remaining = response.size();
            while (remaining > 0) {
                ssize_t sent = send(client_socket, data, remaining, MSG_NOSIGNAL);
                if (sent <= 0) break;
                data += sent;
                remaining -= sent;
            }
            send(client_socket, SENTINEL, SENTINEL_LEN, MSG_NOSIGNAL);
        }
    }
}

int main()
{
    // Setup signal handlers for graceful shutdown
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Initialize global pool early
    unsigned int n_threads = std::thread::hardware_concurrency();
    if (n_threads == 0)
        n_threads = 4;
    global_pool = new ThreadPool(n_threads); // Initialize global_pool

    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        std::cerr << "Socket failed" << std::endl;
        return 1;
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)))
    {
        std::cerr << "Setsockopt failed" << std::endl;
        return 1;
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        std::cerr << "Bind failed" << std::endl;
        return 1;
    }

    if (listen(server_fd, MAX_CONN) < 0)
    {
        std::cerr << "Listen failed" << std::endl;
        return 1;
    }

    // ThreadPool pool(n_threads); // Removed local ThreadPool

    std::cout << "FlexQL Server listening on port " << PORT << "..." << std::endl;
    std::cout << "[Persistence] Checkpoint-based persistence enabled (data dir: data/)" << std::endl;
    std::cout << "[Shutdown] Press Ctrl+C to save and shutdown gracefully" << std::endl;

    while (server_running.load(std::memory_order_relaxed))
    {
        socklen_t addrlen = sizeof(address);
        int client_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen);
        if (client_socket < 0)
        {
            std::cerr << "Accept failed" << std::endl;
            continue;
        }

        int nodelay = 1;
        setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay));

        std::cout << "New connection accepted!" << std::endl;
        global_pool->enqueue([client_socket]
                             { handle_client(client_socket); });
    }

    // Graceful shutdown cleanup
    std::cout << "[Server] Shutting down, creating final checkpoint..." << std::endl;
    global_db.save_checkpoint();

    if (global_pool)
    {
        delete global_pool;
        global_pool = nullptr;
    }

    close(server_fd);
    std::cout << "[Server] Shutdown complete." << std::endl;
    return 0;
}
