#pragma once
#include <cstdint>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

namespace pcsql {

class DiskManager; // forward decl

// Simple table catalog: map table name/id to a list of page_ids
// Persistent text format (no spaces in table name):
//   line 1: next_table_id
//   subsequent lines: table_id table_name page_id1 page_id2 ...
class TableManager {
public:
    explicit TableManager(const std::string& base_dir = ".",
                          const std::string& tables_file = "tables.meta");

    // Create/drop table
    std::int32_t create_table(const std::string& name);
    bool drop_table_by_id(std::int32_t table_id);
    bool drop_table_by_name(const std::string& name);

    // Lookup
    std::int32_t get_table_id(const std::string& name) const;
    std::string get_table_name(std::int32_t table_id) const;

    // Page mapping ops
    std::uint32_t allocate_table_page(std::int32_t table_id, DiskManager& disk);
    const std::vector<std::uint32_t>& get_table_pages(std::int32_t table_id) const;

    // Persistence
    void load();
    void save() const;

private:
    void init_file();

    std::filesystem::path base_dir_;
    std::filesystem::path file_path_;

    std::int32_t next_table_id_{0};
    std::unordered_map<std::int32_t, std::string> id_to_name_;
    std::unordered_map<std::string, std::int32_t> name_to_id_;
    std::unordered_map<std::int32_t, std::vector<std::uint32_t>> table_pages_;
};

} // namespace pcsql