#pragma once
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>

#include "storage/buffer_manager.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_manager.hpp"
#include "storage/record_manager.hpp"
#include "system_catalog/types.hpp"

namespace pcsql {

class StorageEngine {
public:
    explicit StorageEngine(const std::string& base_dir = ".",
                           std::size_t buffer_capacity = 64,
                           Policy policy = Policy::LRU,
                           bool log = true)
        : disk_(base_dir), buffer_(disk_, buffer_capacity, policy, log),
          tables_(base_dir), records_(disk_, buffer_, tables_) {
        // Bootstrap system catalog tables stored as regular relations
        bootstrapping_ = true;
        ensure_system_catalog();
        bootstrapping_ = false;
    }

    // Disk-level page operations
    std::uint32_t allocate_page() { return disk_.allocate_page(); }
    void free_page(std::uint32_t pid) { return disk_.free_page(pid); }

    // Buffer operations
    Page& get_page(std::uint32_t pid) { return buffer_.get_page(pid); }
    void unpin_page(std::uint32_t pid, bool dirty) { buffer_.unpin_page(pid, dirty); }
    void flush_page(std::uint32_t pid) { buffer_.flush_page(pid); }
    void flush_all() { buffer_.flush_all(); }

    const Stats& stats() const { return buffer_.stats(); }

    // Table operations
    std::int32_t create_table(const std::string& name) { return tables_.create_table(name); }
    // 新增：同时登记系统目录中的表元数据（以 sys_* 为单一事实来源）
    std::int32_t create_table(const std::string& name, const std::vector<ColumnMetadata>& columns) {
        auto tid = tables_.create_table(name);
        // 写入系统目录表（避免在自举阶段或自身系统表时写入）
        if (!bootstrapping_ && !is_system_table(name)) {
            insert_into_sys_tables(tid, name);
            insert_into_sys_columns(tid, columns);
            std::cout << "[StorageEngine] save metadata successfully" << std::endl;
        }
        return tid;
    }
    bool drop_table_by_id(std::int32_t tid) {
        auto name = get_table_name(tid);
        auto ok = tables_.drop_table_by_id(tid, disk_);
        if (ok && !name.empty()) {
            if (!is_system_table(name)) remove_from_sys_catalog(tid);
        }
        return ok;
    }
    bool drop_table_by_name(const std::string& name) {
        auto ok = tables_.drop_table_by_name(name, disk_);
        if (ok) {
            // remove rows if not system table
            if (!is_system_table(name)) {
                auto tid = get_table_id(name);
                if (tid >= 0) remove_from_sys_catalog(tid);
            }
        }
        return ok;
    }
    // 在删除表时释放页回收到 DiskManager（提供显式重载）
    bool drop_table_by_id(std::int32_t tid, DiskManager& disk) {
        auto name = get_table_name(tid);
        auto ok = tables_.drop_table_by_id(tid, disk);
        if (ok && !name.empty()) {
            if (!is_system_table(name)) remove_from_sys_catalog(tid);
        }
        return ok;
    }
    bool drop_table_by_name(const std::string& name, DiskManager& disk) {
        auto ok = tables_.drop_table_by_name(name, disk);
        if (ok) {
            if (!is_system_table(name)) {
                auto tid = get_table_id(name);
                if (tid >= 0) remove_from_sys_catalog(tid);
            }
        }
        return ok;
    }

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

    // Schema query from system tables (single source of truth)
    TableSchema get_table_schema(const std::string& table_name) {
        TableSchema schema;
        int tid = get_table_id(table_name);
        if (tid < 0) return schema;
        int sys_cid = tables_.get_table_id("sys_columns");
        if (sys_cid < 0) return schema;
        auto rows = records_.scan(sys_cid);
        std::vector<std::pair<int, ColumnMetadata>> cols;
        cols.reserve(rows.size());
        for (const auto& kv : rows) {
            const std::string& row = kv.second;
            // format: table_id|col_index|name|type|constraints
            std::vector<std::string> fields; fields.reserve(5);
            std::string cur; std::istringstream iss(row);
            while (std::getline(iss, cur, '|')) fields.push_back(cur);
            if (fields.size() < 4) continue;
            try {
                int row_tid = std::stoi(fields[0]);
                if (row_tid != tid) continue;
                int col_idx = std::stoi(fields[1]);
                ColumnMetadata cm;
                cm.name = fields[2];
                cm.type = stringToDataType(fields[3]);
                cm.constraints.clear();
                if (fields.size() >= 5 && !fields[4].empty()) {
                    std::istringstream css(fields[4]);
                    std::string c;
                    while (std::getline(css, c, ',')) cm.constraints.push_back(c);
                }
                cols.emplace_back(col_idx, std::move(cm));
            } catch (...) { /* ignore parse errors */ }
        }
        std::sort(cols.begin(), cols.end(), [](const auto& a, const auto& b){ return a.first < b.first; });
        schema.columns.clear();
        schema.columns.reserve(cols.size());
        schema.columnTypes.clear();
        for (auto& p : cols) {
            schema.columnTypes[to_lower(p.second.name)] = p.second.type;
            schema.columns.push_back(std::move(p.second));
        }
        return schema;
    }

private:
    // ---------- System catalog helpers ----------
    static inline std::string to_lower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
        return s;
    }
    static inline bool is_system_table(const std::string& name) {
        std::string n = to_lower(name);
        return n == "sys_tables" || n == "sys_columns" || n == "sys_indexes" || n == "sys_users";
    }
    static inline std::string type_to_string(DataType t) {
        switch (t) {
            case DataType::INT: return "INT";
            case DataType::VARCHAR: return "VARCHAR";
            case DataType::DOUBLE: return "DOUBLE";
            case DataType::BOOLEAN: return "BOOLEAN";
            default: return "UNKNOWN";
        }
    }
    static inline std::string join(const std::vector<std::string>& v, const char* sep = ",") {
        std::ostringstream os; for (size_t i=0;i<v.size();++i){ if(i) os<<sep; os<<v[i]; } return os.str();
    }
    static inline std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> out; std::string cur; std::istringstream iss(s);
        while (std::getline(iss, cur, delim)) out.push_back(cur);
        return out;
    }

    void ensure_system_catalog() {
        // Define schemas
        std::vector<ColumnMetadata> t_cols = {
            {"id", DataType::INT, {}},
            {"name", DataType::VARCHAR, {}}
        };
        std::vector<ColumnMetadata> c_cols = {
            {"table_id", DataType::INT, {}},
            {"col_index", DataType::INT, {}},
            {"name", DataType::VARCHAR, {}},
            {"type", DataType::VARCHAR, {}},
            {"constraints", DataType::VARCHAR, {}}
        };
        std::vector<ColumnMetadata> i_cols = {
            {"index_name", DataType::VARCHAR, {}},
            {"table_id", DataType::INT, {}},
            {"column", DataType::VARCHAR, {}},
            {"unique", DataType::BOOLEAN, {}}
        };
        std::vector<ColumnMetadata> u_cols = {
            {"user", DataType::VARCHAR, {}},
            {"password", DataType::VARCHAR, {}}
        };

        // Create sys tables if not exist (create physical tables directly)
        if (tables_.get_table_id("sys_tables") < 0) {
            (void)tables_.create_table("sys_tables");
        }
        if (tables_.get_table_id("sys_columns") < 0) {
            (void)tables_.create_table("sys_columns");
        }
        if (tables_.get_table_id("sys_indexes") < 0) {
            (void)tables_.create_table("sys_indexes");
        }
        if (tables_.get_table_id("sys_users") < 0) {
            (void)tables_.create_table("sys_users");
        }

        // Seed system tables' own metadata into sys_tables/sys_columns if empty
        auto sys_tid = tables_.get_table_id("sys_tables");
        auto sys_cid = tables_.get_table_id("sys_columns");
        auto rows = records_.scan(sys_tid);
        if (rows.empty()) {
            int tid_tables = tables_.get_table_id("sys_tables");
            int tid_columns = tables_.get_table_id("sys_columns");
            int tid_indexes = tables_.get_table_id("sys_indexes");
            int tid_users = tables_.get_table_id("sys_users");
            // insert table entries
            insert_into_sys_tables(tid_tables, "sys_tables");
            insert_into_sys_tables(tid_columns, "sys_columns");
            insert_into_sys_tables(tid_indexes, "sys_indexes");
            insert_into_sys_tables(tid_users, "sys_users");
            // insert column entries
            insert_into_sys_columns(tid_tables, t_cols);
            insert_into_sys_columns(tid_columns, c_cols);
            insert_into_sys_columns(tid_indexes, i_cols);
            insert_into_sys_columns(tid_users, u_cols);
        }
    }

    void insert_into_sys_tables(int tid, const std::string& name) {
        int sys_tid = tables_.get_table_id("sys_tables");
        if (sys_tid < 0) return;
        std::ostringstream row; row << tid << "|" << name;
        (void)records_.insert(sys_tid, row.str());
    }

    void insert_into_sys_columns(int tid, const std::vector<ColumnMetadata>& columns) {
        int sys_cid = tables_.get_table_id("sys_columns");
        if (sys_cid < 0) return;
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& col = columns[i];
            std::ostringstream row;
            row << tid << "|" << i << "|" << col.name << "|" << type_to_string(col.type) << "|";
            // naive constraints join (no encoding)
            for (size_t k=0; k<col.constraints.size(); ++k) {
                if (k) row << ',';
                row << col.constraints[k];
            }
            (void)records_.insert(sys_cid, row.str());
        }
    }

    void remove_from_sys_catalog(int tid) {
        int sys_tid = tables_.get_table_id("sys_tables");
        int sys_cid = tables_.get_table_id("sys_columns");
        if (sys_tid >= 0) {
            auto rows = records_.scan(sys_tid);
            for (const auto& kv : rows) {
                auto fields = split(kv.second, '|');
                if (!fields.empty()) {
                    try {
                        int id = std::stoi(fields[0]);
                        if (id == tid) { (void)records_.erase(kv.first); }
                    } catch (...) { /* ignore parse errors */ }
                }
            }
        }
        if (sys_cid >= 0) {
            auto rows = records_.scan(sys_cid);
            for (const auto& kv : rows) {
                auto fields = split(kv.second, '|');
                if (!fields.empty()) {
                    try {
                        int id = std::stoi(fields[0]);
                        if (id == tid) { (void)records_.erase(kv.first); }
                    } catch (...) { /* ignore parse errors */ }
                }
            }
        }
    }

private:
    DiskManager disk_;
    BufferManager buffer_;
    TableManager tables_;
    RecordManager records_;
    bool bootstrapping_ = false;
};

} // namespace pcsql