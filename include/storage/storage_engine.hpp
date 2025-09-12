#pragma once
#include <cstdint>
#include <memory>
#include <string>

#include "storage/buffer_manager.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_manager.hpp"
#include "storage/record_manager.hpp"

namespace pcsql {

class StorageEngine {
public:
    explicit StorageEngine(const std::string& base_dir = ".",
                           std::size_t buffer_capacity = 64,
                           Policy policy = Policy::LRU,
                           bool log = true)
        : disk_(base_dir), buffer_(disk_, buffer_capacity, policy, log),
          tables_(base_dir), records_(disk_, buffer_, tables_) {}

    // Disk-level page operations
    std::uint32_t allocate_page() { return disk_.allocate_page(); }
    void free_page(std::uint32_t pid) { disk_.free_page(pid); }

    // Buffer operations
    Page& get_page(std::uint32_t pid) { return buffer_.get_page(pid); }
    void unpin_page(std::uint32_t pid, bool dirty) { buffer_.unpin_page(pid, dirty); }
    void flush_page(std::uint32_t pid) { buffer_.flush_page(pid); }
    void flush_all() { buffer_.flush_all(); }

    const Stats& stats() const { return buffer_.stats(); }

    // Table operations
    std::int32_t create_table(const std::string& name) { return tables_.create_table(name); }
    bool drop_table_by_id(std::int32_t tid) { return tables_.drop_table_by_id(tid, disk_); }
    bool drop_table_by_name(const std::string& name) { return tables_.drop_table_by_name(name, disk_); }
    std::int32_t get_table_id(const std::string& name) const { return tables_.get_table_id(name); }
    std::string get_table_name(std::int32_t tid) const { return tables_.get_table_name(tid); }
    std::uint32_t allocate_table_page(std::int32_t tid) { return tables_.allocate_table_page(tid, disk_); }
    const std::vector<std::uint32_t>& get_table_pages(std::int32_t tid) const { return tables_.get_table_pages(tid); }

    // Record operations
    RID insert_record(std::int32_t table_id, std::string_view data) { return records_.insert(table_id, data); }
    bool read_record(const RID& rid, std::string& out) { return records_.read(rid, out); }
    bool update_record(const RID& rid, std::string_view data) { return records_.update(rid, data); }
    bool delete_record(const RID& rid) { return records_.erase(rid); }
    std::vector<std::pair<RID, std::string>> scan_table(std::int32_t table_id) { return records_.scan(table_id); }

private:
    DiskManager disk_;
    BufferManager buffer_;
    TableManager tables_;
    RecordManager records_;
};

} // namespace pcsql