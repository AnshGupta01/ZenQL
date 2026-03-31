#include "network/protocol.h"
#include <unistd.h>
#include <cstring>
#include <sys/socket.h>

namespace Protocol {

std::vector<uint8_t> encode_request(const std::string& sql) {
    uint32_t len = static_cast<uint32_t>(sql.size());
    std::vector<uint8_t> frame(4 + len);
    std::memcpy(frame.data(), &len, 4);
    std::memcpy(frame.data() + 4, sql.data(), len);
    return frame;
}

static bool read_exact(int fd, void* buf, size_t n) {
    size_t received = 0;
    while (received < n) {
        ssize_t r = ::recv(fd, (char*)buf + received, n - received, MSG_WAITALL);
        if (r <= 0) return false;
        received += r;
    }
    return true;
}

static bool write_exact(int fd, const void* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = ::send(fd, (const char*)buf + sent, n - sent, MSG_NOSIGNAL);
        if (w <= 0) return false;
        sent += w;
    }
    return true;
}

std::vector<uint8_t> read_frame(int fd) {
    uint32_t len = 0;
    if (!read_exact(fd, &len, 4)) return {};
    std::vector<uint8_t> payload(len);
    if (len > 0 && !read_exact(fd, payload.data(), len)) return {};
    uint32_t sentinel = 0;
    if (!read_exact(fd, &sentinel, 4)) return {};
    if (sentinel != SENTINEL) return {};
    return payload;
}

bool write_frame(int fd, const std::vector<uint8_t>& payload) {
    uint32_t len = static_cast<uint32_t>(payload.size());
    if (!write_exact(fd, &len, 4)) return false;
    if (len > 0 && !write_exact(fd, payload.data(), len)) return false;
    return write_exact(fd, &SENTINEL, 4);
}

bool write_frame(int fd, const std::string& payload) {
    uint32_t len = static_cast<uint32_t>(payload.size());
    if (!write_exact(fd, &len, 4)) return false;
    if (len > 0 && !write_exact(fd, payload.data(), len)) return false;
    return write_exact(fd, &SENTINEL, 4);
}

} // namespace Protocol
