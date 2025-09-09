#pragma once
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "storage/common.hpp"

namespace pcsql {

class DiskManager {
public:
    explicit DiskManager(const std::string& base_dir = ".",
                         const std::string& db_file = "data.db",
                         const std::string& meta_file = "meta.json");

    // Allocate a page and return page id
    std::uint32_t allocate_page();

    // Free page id (add to free list)
    void free_page(std::uint32_t page_id);

    // Read/Write fixed-size page
    void read_page(std::uint32_t page_id, void* out_buffer, std::size_t size = PAGE_SIZE);
    void write_page(std::uint32_t page_id, const void* buffer, std::size_t size = PAGE_SIZE);

    std::filesystem::path db_path() const { return db_path_; }

private:
    void init_files();
    void load_meta();
    void save_meta() const;
    void ensure_file_size_for(std::uint32_t page_id);

    std::filesystem::path base_dir_;
    std::filesystem::path db_path_;
    std::filesystem::path meta_path_;

    std::uint32_t next_page_id_{0};
    std::vector<std::uint32_t> free_list_;
};

} // namespace pcsql