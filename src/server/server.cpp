#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include "../concurrency/thread_pool.h"
#include "../query/database.h"
#include <csignal>

#define PORT 9000
#define MAX_CONN 100
#define BUFFER_SIZE 262144

Database global_db;

void handle_client(int client_socket) {
    char buffer[BUFFER_SIZE];
    std::string remainder = "";
    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes_read = read(client_socket, buffer, BUFFER_SIZE - 1);
        
        if (bytes_read <= 0) {
            std::cout << "Client disconnected." << std::endl;
            close(client_socket);
            break;
        }

        std::string raw_data = remainder + std::string(buffer, bytes_read);
        remainder = "";
        
        std::string response = "";
        size_t last_pos = 0;
        size_t next_pos = 0;

        while ((next_pos = raw_data.find('\n', last_pos)) != std::string::npos) {
            std::string single_query = raw_data.substr(last_pos, next_pos - last_pos);
            last_pos = next_pos + 1;

            single_query.erase(single_query.find_last_not_of(" \r\t") + 1);
            if (single_query.empty()) continue;
            
            std::string res = global_db.execute(single_query);
            if (res.find("1 row inserted") != std::string::npos) response += "OK\n"; 
            else response += res;
        }

        if (last_pos < raw_data.length()) {
            remainder = raw_data.substr(last_pos);
        }

        if (!response.empty()) {
            send(client_socket, response.c_str(), response.length(), 0);
        }
    }
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        std::cerr << "Socket failed" << std::endl;
        return 1;
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "Setsockopt failed" << std::endl;
        return 1;
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        return 1;
    }

    if (listen(server_fd, MAX_CONN) < 0) {
        std::cerr << "Listen failed" << std::endl;
        return 1;
    }

    // Initialize thread pool for incoming requests
    unsigned int n_threads = std::thread::hardware_concurrency();
    if (n_threads == 0) n_threads = 4;
    ThreadPool pool(n_threads); 

    std::cout << "FlexQL Server listening on port " << PORT << "..." << std::endl;

    while (true) {
        socklen_t addrlen = sizeof(address);
        int client_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen);
        if (client_socket < 0) {
            std::cerr << "Accept failed" << std::endl;
            continue;
        }
        std::cout << "New connection accepted!" << std::endl;
        pool.enqueue([client_socket] {
            handle_client(client_socket);
        });
    }

    close(server_fd);
    return 0;
}
