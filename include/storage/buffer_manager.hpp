#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <limits>

#include "storage/common.hpp"
#include "storage/disk_manager.hpp"

namespace pcsql {

struct Page {
    std::uint32_t page_id{std::numeric_limits<std::uint32_t>::max()};
    std::array<char, PAGE_SIZE> data{};//4096字节大小的数组
};

class BufferManager {
public:
    BufferManager(DiskManager& disk, std::size_t capacity, Policy policy = Policy::LRU, bool enable_logging = true);

    // Pin and get a page from buffer (load from disk on miss)
    Page& get_page(std::uint32_t page_id);

    // Unpin a page (dirty indicates if modified)
    void unpin_page(std::uint32_t page_id, bool dirty);

    // Flush single page if present and dirty
    void flush_page(std::uint32_t page_id);

    // Flush all dirty pages
    void flush_all();

    const Stats& stats() const { return stats_; }
    Policy policy() const { return policy_; }
    std::size_t capacity() const { return capacity_; }

private:
    struct Frame {
        Page page;
        bool dirty{false};
        int pin_count{0};
        // When not pinned, the frame index appears in repl_list_
    };

    // replacement helpers
    void on_unpinned(std::size_t frame_idx);
    std::size_t pick_victim();
    void log(const std::string& msg) const { if (enable_logging_) std::cout << msg << std::endl; }

    DiskManager& disk_;
    std::size_t capacity_;
    Policy policy_;
    bool enable_logging_;
    Stats stats_{};

    std::vector<Frame> frames_;                 // fixed size
    std::vector<bool> used_;                    // whether frame slot is used
    std::vector<std::size_t> free_list_;        // indices of free frames
    std::unordered_map<std::uint32_t, std::size_t> page_table_; // page_id -> frame index

    // For LRU/FIFO among frames; front = victim candidate
    std::list<std::size_t> repl_list_; // holds frame indices
    std::unordered_map<std::size_t, std::list<std::size_t>::iterator> repl_pos_; // frame_idx -> iterator
};

} // namespace pcsql