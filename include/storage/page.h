#pragma once
#include <cstdint>
#include <cstring>
#include "common/types.h"

// ─── Page Header (32 bytes) ───────────────────────────────────────────────────
//  Layout:
//    [0..3]   magic (0xBEEF1234)
//    [4..7]   page_id
//    [8..11]  free_space_offset   (offset from start of data area to first free byte)
//    [12..15] slot_count          (number of active slots)
//    [16..19] free_slot_start     (offset of slot array from page start; slots grow downward)
//    [20..23] next_page           (linked list of overflow pages)
//    [24..27] flags               (0x01 = leaf, 0x02 = dirty)
//    [28..31] _pad
#pragma pack(push, 1)
struct PageHeader {
    uint32_t magic;               // 4
    uint32_t page_id;             // 4
    uint16_t free_space_offset;   // 2
    uint16_t slot_count;          // 2
    uint32_t next_page;           // 4
    uint32_t flags;               // 4
    uint8_t  _pad[12];            // 12  → total 32
};
#pragma pack(pop)
static_assert(sizeof(PageHeader) == 32, "PageHeader must be 32 bytes");

// ─── Slot (8 bytes): offset + length within page ─────────────────────────────
#pragma pack(push, 1)
struct Slot {
    uint16_t offset;   // byte offset from start of page data area
    uint16_t length;   // byte size of the row
    uint32_t flags;    // 0x01 = deleted (tombstone)
};
#pragma pack(pop)
static_assert(sizeof(Slot) == 8, "Slot must be 8 bytes");

constexpr uint32_t PAGE_MAGIC       = 0xBEEF1234;
constexpr uint32_t PAGE_HEADER_SIZE = sizeof(PageHeader);   // 32
constexpr uint32_t SLOT_SIZE        = sizeof(Slot);         // 8
constexpr uint32_t PAGE_DATA_SIZE   = PAGE_SIZE - PAGE_HEADER_SIZE;

// ─── Page (in-memory representation) ─────────────────────────────────────────
struct Page {
    alignas(64) uint8_t data[PAGE_SIZE];  // raw bytes (header + data)

    PageHeader*       header()       { return reinterpret_cast<PageHeader*>(data); }
    const PageHeader* header() const { return reinterpret_cast<const PageHeader*>(data); }

    // Slot array lives at end of page, growing toward lower addresses
    Slot* slot(int idx) {
        return reinterpret_cast<Slot*>(data + PAGE_SIZE - (idx + 1) * SLOT_SIZE);
    }
    const Slot* slot(int idx) const {
        return reinterpret_cast<const Slot*>(data + PAGE_SIZE - (idx + 1) * SLOT_SIZE);
    }

    // Data area starts right after header
    uint8_t* data_area() { return data + PAGE_HEADER_SIZE; }

    void init(uint32_t pid) {
        std::memset(data, 0, PAGE_SIZE);
        auto* h = header();
        h->magic            = PAGE_MAGIC;
        h->page_id          = pid;
        h->free_space_offset = 0;
        h->slot_count       = 0;
        h->next_page        = INVALID_PAGE_ID;
        h->flags            = 0;
    }

    // Returns slot index on success, -1 if no space
    int insert_row(const uint8_t* row, uint16_t row_len) {
        auto* h = header();
        uint32_t slot_area_start = PAGE_SIZE - (h->slot_count + 1) * SLOT_SIZE;
        uint32_t data_end        = PAGE_HEADER_SIZE + h->free_space_offset + row_len;
        if (data_end > slot_area_start) return -1;

        uint16_t off = h->free_space_offset;
        std::memcpy(data_area() + off, row, row_len);
        h->free_space_offset = static_cast<uint16_t>(off + row_len);

        int si = h->slot_count;
        auto* s = slot(si);
        s->offset = off;
        s->length = row_len;
        s->flags  = 0;
        h->slot_count++;
        return si;
    }

    uint32_t free_space() const {
        const auto* h = header();
        uint32_t slot_area = PAGE_SIZE - h->slot_count * SLOT_SIZE;
        uint32_t data_end  = PAGE_HEADER_SIZE + h->free_space_offset;
        return slot_area > data_end ? slot_area - data_end : 0;
    }
};
