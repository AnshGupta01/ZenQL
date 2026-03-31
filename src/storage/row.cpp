#include "storage/row.h"
#include <cstring>
#include <cstdint>
#include <stdexcept>

// Binary layout per row:
//  [8 bytes]  expiry_ms   (int64_t, little-endian)
//  per column:
//    INT:      8 bytes (int64_t)
//    DECIMAL:  8 bytes (double)
//    DATETIME: 8 bytes (int64_t unix ms)
//    VARCHAR:  2 bytes length prefix + N bytes string data

namespace RowSerializer {

static inline void write_i64(uint8_t* buf, int64_t v) {
    std::memcpy(buf, &v, 8);
}
static inline int64_t read_i64(const uint8_t* buf) {
    int64_t v; std::memcpy(&v, buf, 8); return v;
}
static inline void write_f64(uint8_t* buf, double v) {
    std::memcpy(buf, &v, 8);
}
static inline double read_f64(const uint8_t* buf) {
    double v; std::memcpy(&v, buf, 8); return v;
}
static inline void write_u16(uint8_t* buf, uint16_t v) {
    std::memcpy(buf, &v, 2);
}
static inline uint16_t read_u16(const uint8_t* buf) {
    uint16_t v; std::memcpy(&v, buf, 2); return v;
}

uint32_t fixed_row_size(const Schema& schema) {
    uint32_t sz = EXPIRY_SIZE;
    for (const auto& c : schema.cols) {
        switch (c.type) {
            case ColType::INT:      sz += INT_SIZE; break;
            case ColType::DECIMAL:  sz += DECIMAL_SIZE; break;
            case ColType::DATETIME: sz += DATETIME_SIZE; break;
            case ColType::VARCHAR:  sz += VARCHAR_OVERHEAD; break; // + variable
        }
    }
    return sz;
}

std::vector<uint8_t> serialize(const Schema& schema, const Row& row) {
    size_t sz = EXPIRY_SIZE;
    for (size_t i = 0; i < schema.cols.size(); i++) {
        switch (schema.cols[i].type) {
            case ColType::INT:
            case ColType::DECIMAL:
            case ColType::DATETIME: sz += 8; break;
            case ColType::VARCHAR:
                sz += VARCHAR_OVERHEAD + row.cols[i].s.size();
                break;
        }
    }
    std::vector<uint8_t> buf(sz);
    uint8_t* p = buf.data();
    write_i64(p, row.expiry_ms); p += 8;
    for (size_t i = 0; i < schema.cols.size(); i++) {
        switch (schema.cols[i].type) {
            case ColType::INT:
                write_i64(p, row.cols[i].i); p += 8; break;
            case ColType::DECIMAL:
                write_f64(p, row.cols[i].d); p += 8; break;
            case ColType::DATETIME:
                write_i64(p, row.cols[i].i); p += 8; break;
            case ColType::VARCHAR: {
                uint16_t len = static_cast<uint16_t>(row.cols[i].s.size());
                write_u16(p, len); p += 2;
                std::memcpy(p, row.cols[i].s.data(), len); p += len;
                break;
            }
        }
    }
    return buf;
}

bool deserialize(const Schema& schema, const uint8_t* data, uint16_t len, Row& out) {
    const uint8_t* p   = data;
    const uint8_t* end = data + len;
    if (p + 8 > end) return false;
    out.expiry_ms = read_i64(p); p += 8;
    out.cols.resize(schema.cols.size());
    for (size_t i = 0; i < schema.cols.size(); i++) {
        out.cols[i].type = schema.cols[i].type;
        switch (schema.cols[i].type) {
            case ColType::INT:
                if (p + 8 > end) return false;
                out.cols[i].i = read_i64(p); p += 8; break;
            case ColType::DECIMAL:
                if (p + 8 > end) return false;
                out.cols[i].d = read_f64(p); p += 8; break;
            case ColType::DATETIME:
                if (p + 8 > end) return false;
                out.cols[i].i = read_i64(p); p += 8; break;
            case ColType::VARCHAR: {
                if (p + 2 > end) return false;
                uint16_t slen = read_u16(p); p += 2;
                if (p + slen > end) return false;
                out.cols[i].s.assign(reinterpret_cast<const char*>(p), slen);
                p += slen;
                break;
            }
        }
    }
    return true;
}

} // namespace RowSerializer
