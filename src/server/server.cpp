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
        memset(buffer, 0, BUFFER_SIZE);
        int bytes_read = read(client_socket, buffer, BUFFER_SIZE - 1);

        if (bytes_read <= 0)
        {
            std::cout << "Client disconnected." << std::endl;
            close(client_socket);
            break;
        }

        // Optimize: Append to remainder instead of creating new string
        remainder.append(buffer, bytes_read);

        std::string response;
        response.reserve(8192); // Pre-allocate response buffer

        std::vector<std::string> queries;
        queries.reserve(16); // Pre-allocate for typical batch

        size_t last_pos = 0;
        size_t next_pos = 0;

        // Parse queries in-place from combined buffer
        while ((next_pos = remainder.find('\n', last_pos)) != std::string::npos)
        {
            if (next_pos > last_pos)
            {
                // Extract query view and trim
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

        // Keep unparsed data for next iteration
        if (last_pos < remainder.size())
        {
            if (last_pos > 0)
            {
                // Move unparsed data to front
                remainder = remainder.substr(last_pos);
            }
        }
        else
        {
            remainder.clear();
        }

        if (queries.empty())
            continue;

        if (queries.size() == 1)
        {
            response = global_db.execute(queries[0]);
        }
        else
        { // Modified to use global_pool for batch processing
            unsigned int n_cores = std::thread::hardware_concurrency();
            if (n_cores == 0)
                n_cores = 2;
            std::vector<std::string> chunk_res(n_cores);
            size_t q_per_core = (queries.size() + n_cores - 1) / n_cores;

            std::vector<std::future<void>> futures; // Using futures to wait for tasks
            for (unsigned int t = 0; t < n_cores; ++t)
            {
                futures.push_back(global_pool->enqueue([&, t, q_per_core]()
                                                       {
                    size_t start = t * q_per_core;
                    size_t end = std::min(start + q_per_core, queries.size());
                    for (size_t i = start; i < end; ++i) {
                        chunk_res[t] += global_db.execute(queries[i]);
                    } }));
            }
            for (auto &f : futures)
                f.get(); // Wait for all tasks to complete
            for (const auto &r : chunk_res)
                response += r;
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
