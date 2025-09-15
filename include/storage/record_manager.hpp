#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/buffer_manager.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_manager.hpp"

namespace pcsql {

struct RID {
    std::uint32_t page_id{0};
    std::uint16_t slot_id{0};
};

class RecordManager {
public:
    RecordManager(DiskManager& disk, BufferManager& buffer, TableManager& tables)
        : disk_(disk), buffer_(buffer), tables_(tables) {}

    // Insert a raw record into table; returns RID
    RID insert(std::int32_t table_id, const char* data, std::size_t size);
    RID insert(std::int32_t table_id, std::string_view bytes) {
        return insert(table_id, bytes.data(), bytes.size());
    }

    // Read a record into out (returns false if RID invalid or deleted)
    bool read(const RID& rid, std::string& out);

    // Update record in-place; only succeeds if new size <= original size
    bool update(const RID& rid, const char* data, std::size_t size);
    bool update(const RID& rid, std::string_view bytes) { return update(rid, bytes.data(), bytes.size()); }

    // Delete (tombstone) a record; returns false if RID invalid
    bool erase(const RID& rid);

    // Sequential scan: return all (RID, bytes) in table
    std::vector<std::pair<RID, std::string>> scan(std::int32_t table_id);

private:
    struct Header { std::uint16_t free_off; std::uint16_t slot_count; };
    struct Slot { std::int16_t off; std::uint16_t len; }; // off == -1 => deleted

    static Header& header(Page& page) { return *reinterpret_cast<Header*>(page.data.data()); }
    static const Header& header(const Page& page) { return *reinterpret_cast<const Header*>(page.data.data()); }

    static Slot* slot_at(Page& page, std::uint16_t idx) {
        auto base = page.data.data();//page.data 是一个存放整个页字节的数组，base 指向页的开头
        return reinterpret_cast<Slot*>(base + PAGE_SIZE) - (idx + 1);
        //把这个地址转换成 Slot* 类型，得到一个“指向页末的 Slot 指针”。
    }
    static const Slot* slot_at(const Page& page, std::uint16_t idx) {
        auto base = page.data.data();
        return reinterpret_cast<const Slot*>(base + PAGE_SIZE) - (idx + 1);
    }

    static std::size_t free_space(const Page& page) {
        const auto& h = header(page);
        // Available contiguous space between data region end and slot directory
        std::size_t slots_bytes = static_cast<std::size_t>(h.slot_count) * sizeof(Slot);
        return PAGE_SIZE - slots_bytes - h.free_off;
    }

    static bool header_valid(const Header& h) {
        if (h.free_off < sizeof(Header) || h.free_off > PAGE_SIZE) return false;
        std::size_t slots_bytes = static_cast<std::size_t>(h.slot_count) * sizeof(Slot);
        if (slots_bytes > PAGE_SIZE) return false;
        if (h.free_off + slots_bytes > PAGE_SIZE) return false;
        return true;
    }

    static void ensure_initialized(Page& page) {
        auto& h = header(page);
        if (!header_valid(h)) {
            h.free_off = static_cast<std::uint16_t>(sizeof(Header));
            h.slot_count = 0;
        }
    }

    static void compact(Page& page);

    DiskManager& disk_;
    BufferManager& buffer_;
    TableManager& tables_;
};

} // namespace pcsql