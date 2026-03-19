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

OptimizedDatabase global_db("data", true);  // Enable persistence with "data" directory

void handle_client(int client_socket)
{
    char buffer[BUFFER_SIZE];
    std::string remainder = "";
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

        std::string raw_data = remainder + std::string(buffer, bytes_read);
        remainder = "";

        std::string response = "";
        std::vector<std::string> queries;
        size_t last_pos = 0;
        size_t next_pos = 0;
        while ((next_pos = raw_data.find('\n', last_pos)) != std::string::npos)
        {
            std::string single_query = raw_data.substr(last_pos, next_pos - last_pos);
            last_pos = next_pos + 1;

            single_query.erase(single_query.find_last_not_of(" \r\t") + 1);
            if (!single_query.empty())
                queries.push_back(single_query);
        }

        if (queries.empty())
            continue; // Added check for empty queries

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

        if (last_pos < raw_data.length())
        {
            remainder = raw_data.substr(last_pos);
        }

        if (!response.empty())
        {
            send(client_socket, response.c_str(), response.length(), 0);
        }
    }
}

int main()
{
    signal(SIGPIPE, SIG_IGN);

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

    while (true)
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

    close(server_fd);
    return 0;
}
