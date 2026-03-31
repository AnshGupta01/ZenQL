#pragma once
#include <string>
#include <vector>
#include <cstdint>

// ─── Wire protocol ───────────────────────────────────────────────────────────
//  Client writes:  [4-byte length LE] [SQL bytes]
//  Server replies: [4-byte length LE] [response bytes] [4-byte sentinel 0xDEADBEEF]
//
//  Response format (text, newline-delimited):
//    OK\n                        → for non-SELECT
//    COL\t<c1>\t<c2>\n           → column header
//    ROW\t<v1>\t<v2>\n           → one row
//    DONE\n                      → end of result set
//    ERR\t<message>\n            → error

namespace Protocol {
    constexpr uint32_t SENTINEL = 0xDEADBEEF;

    // Encode a query for sending
    std::vector<uint8_t> encode_request(const std::string& sql);

    // Decode a response frame (returns payload bytes, empty on frame error)
    std::vector<uint8_t> read_frame(int fd);

    // Write full frame (len prefix + payload + sentinel)
    bool write_frame(int fd, const std::vector<uint8_t>& payload);
    bool write_frame(int fd, const std::string& payload);
}
