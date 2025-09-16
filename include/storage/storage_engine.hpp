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
#include "storage/bplus_tree.hpp"

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

    ~StorageEngine() noexcept {
        try {
            flush_all();
            std::cout << "[StorageEngine] flushed all dirty pages before exit" << std::endl;
        } catch (...) {
            // ignore exceptions during shutdown
        }
    }

    // Toggle index tracing (forwarded to B+Tree internally)
    void set_index_trace(bool on) { index_trace_ = on; }

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
            std::cout << "[StorageEngine] save metadata successfully: table '" << name << "' (tid=" << tid << ")" << std::endl;
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
    // FIX: call correct TableManager API
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
            // format (new): table_id|col_index|name|type|length|constraints
            // format (legacy): table_id|col_index|name|type|constraints
            std::vector<std::string> fields; fields.reserve(6);
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
                cm.length = 0; // default for legacy
                cm.constraints.clear();
                if (fields.size() >= 6) {
                    // new format with length
                    try { cm.length = static_cast<std::size_t>(std::stoul(fields[4])); } catch (...) { cm.length = 0; }
                    // constraints list separated by ','
                    for (const auto& c : split(fields[5], ',')) if (!c.empty()) cm.constraints.push_back(c);
                } else if (fields.size() >= 5) {
                    // legacy: constraints without length
                    for (const auto& c : split(fields[4], ',')) if (!c.empty()) cm.constraints.push_back(c);
                }
                cols.emplace_back(col_idx, std::move(cm));
            } catch (...) {
                // ignore
            }
        }
        if (!cols.empty()) {
            std::sort(cols.begin(), cols.end(), [](const auto& a, const auto& b){ return a.first < b.first; });
            schema.columns.clear();
            schema.columnTypes.clear();
            for (const auto& kv : cols) {
                schema.columns.push_back(kv.second);
                schema.columnTypes[to_lower(kv.second.name)] = kv.second.type;
            }
        }
        return schema;
    }

    // -------- Index management (B+Tree over INT keys; UNIQUE only for now) --------
    struct IndexInfo {
        std::string name;
        int table_id{ -1 };
        std::string column;
        bool unique{ true };
        std::uint32_t root{ 0 };
        int column_index{ -1 }; // position in table row
    };

    // Create a UNIQUE index on given table.column; builds B+Tree and persists root in sys_indexes
    bool create_index(const std::string& index_name,
                      const std::string& table_name,
                      const std::string& column_name,
                      bool unique = true) {
        int tid = get_table_id(to_lower(table_name));
        if (tid < 0) throw std::runtime_error("Table not found: " + table_name);
        // find column index and type
        auto schema = get_table_schema(to_lower(table_name));
        int col_idx = -1; DataType dtype = DataType::INT;
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            if (to_lower(schema.columns[i].name) == to_lower(column_name)) {
                col_idx = static_cast<int>(i);
                dtype = schema.columns[i].type;
                break;
            }
        }
        if (col_idx < 0) throw std::runtime_error("Column not found: " + column_name);
        // Build B+Tree depending on column type
        std::uint32_t root = 0;
        std::cout << "[StorageEngine] Building index '" << index_name << "' on "
                  << to_lower(table_name) << "(" << to_lower(column_name) << ") type="
                  << (dtype == DataType::INT ? "INT" : (dtype == DataType::VARCHAR ? "VARCHAR" : "OTHER"))
                  << std::endl;
        if (dtype == DataType::INT) {
            BPlusTree tree(disk_, buffer_);
            tree.set_trace(index_trace_);
            root = tree.create();
            // insert existing rows
            auto rows = scan_table(tid);
            for (const auto& kv : rows) {
                const RID& rid = kv.first;
                const std::string& row = kv.second;
                // split row by '|'
                std::vector<std::string> fields; std::string cur; std::istringstream iss(row);
                while (std::getline(iss, cur, '|')) fields.push_back(cur);
                if (col_idx >= static_cast<int>(fields.size()))
                    throw std::runtime_error("Row parse error when building index");
                long long key_ll = 0; try { key_ll = std::stoll(fields[col_idx]); } catch (...) { throw std::runtime_error("Non-integer value encountered while building index"); }
                if (!tree.insert(static_cast<std::int64_t>(key_ll), rid)) {
                    throw std::runtime_error("Duplicate key detected when building UNIQUE index");
                }
            }
        } else if (dtype == DataType::VARCHAR) {
            // Use fixed-size key for VARCHAR index
            using StrKey = FixedString<128>;
            BPlusTreeT<StrKey> tree(disk_, buffer_);
            tree.set_trace(index_trace_);
            root = tree.create();
            // insert existing rows
            auto rows = scan_table(tid);
            for (const auto& kv : rows) {
                const RID& rid = kv.first;
                const std::string& row = kv.second;
                std::vector<std::string> fields; std::string cur; std::istringstream iss(row);
                while (std::getline(iss, cur, '|')) fields.push_back(cur);
                if (col_idx >= static_cast<int>(fields.size()))
                    throw std::runtime_error("Row parse error when building index");
                StrKey key(fields[col_idx]);
                if (!tree.insert(key, rid)) {
                    throw std::runtime_error("Duplicate key detected when building UNIQUE index");
                }
            }
        } else {
            throw std::runtime_error("Only INT/VARCHAR column is supported for index currently");
        }
        std::cout << "[StorageEngine] Index '" << index_name << "' built, root page id=" << root << std::endl;
        // persist index metadata
        insert_into_sys_indexes(index_name, tid, to_lower(column_name), unique, root);
        return true;
    }

    std::vector<IndexInfo> get_table_indexes(int tid) {
        std::vector<IndexInfo> out;
        int sys_i = tables_.get_table_id("sys_indexes");
        if (sys_i < 0) return out;
        auto rows = records_.scan(sys_i);
        for (const auto& kv : rows) {
            const std::string& row = kv.second;
            // index_name|table_id|column|unique|root
            std::vector<std::string> f; std::string cur; std::istringstream iss(row);
            while (std::getline(iss, cur, '|')) f.push_back(cur);
            if (f.size() < 5) continue;
            try {
                int row_tid = std::stoi(f[1]);
                if (row_tid != tid) continue;
                IndexInfo info; info.name = f[0]; info.table_id = row_tid; info.column = f[2];
                std::string ul = to_lower(f[3]); info.unique = (ul == "1" || ul == "true");
                info.root = static_cast<std::uint32_t>(std::stoul(f[4]));
                // compute column index
                auto schema = get_table_schema(get_table_name(tid));
                info.column_index = -1;
                for (size_t i=0;i<schema.columns.size();++i) {
                    if (to_lower(schema.columns[i].name) == to_lower(info.column)) { info.column_index = static_cast<int>(i); break; }
                }
                if (info.column_index >= 0) out.push_back(info);
            } catch (...) { /* ignore */ }
        }
        return out;
    }

    // After inserting a row into table, update all indexes on that table
    void update_indexes_on_insert(int table_id, const std::string& row, const RID& rid) {
        auto idxs = get_table_indexes(table_id);
        if (idxs.empty()) return;
        // parse row once
        std::vector<std::string> fields; std::string cur; std::istringstream iss(row);
        while (std::getline(iss, cur, '|')) fields.push_back(cur);
        // get schema once for types
        auto schema = get_table_schema(get_table_name(table_id));
        for (const auto& idx : idxs) {
            if (idx.column_index < 0 || idx.column_index >= static_cast<int>(fields.size())) continue;
            DataType dtype = DataType::UNKNOWN;
            if (idx.column_index < static_cast<int>(schema.columns.size())) {
                dtype = schema.columns[idx.column_index].type;
            }
            if (dtype == DataType::INT) {
                long long key_ll = 0; try { key_ll = std::stoll(fields[idx.column_index]); } catch (...) { continue; }
                BPlusTree tree(disk_, buffer_);
                tree.open(idx.root);
                tree.set_trace(index_trace_);
                bool ok = tree.insert(static_cast<std::int64_t>(key_ll), rid);
                if (!ok && idx.unique) {
                    std::cerr << "[StorageEngine] UNIQUE index violation on '" << idx.name << "' for key=" << key_ll << std::endl;
                }
            } else if (dtype == DataType::VARCHAR) {
                using StrKey = FixedString<128>;
                BPlusTreeT<StrKey> tree(disk_, buffer_);
                tree.open(idx.root);
                tree.set_trace(index_trace_);
                StrKey key(fields[idx.column_index]);
                bool ok = tree.insert(key, rid);
                if (!ok && idx.unique) {
                    std::cerr << "[StorageEngine] UNIQUE index violation on '" << idx.name << "' for key='" << fields[idx.column_index] << "'" << std::endl;
                }
            } else {
                // other types not supported yet
                continue;
            }
        }
    }

    // Index-assisted selection (INT-only). Returns matching rows via RID lookup.
    std::vector<std::pair<RID, std::string>> index_select_eq_int(int table_id, int column_index, long long key) {
        std::vector<std::pair<RID, std::string>> out;
        auto idxs = get_table_indexes(table_id);
        const IndexInfo* found = nullptr;
        for (const auto& idx : idxs) { if (idx.column_index == column_index) { found = &idx; break; } }
        if (!found) return out;
        if (index_trace_) {
            std::cout << "[StorageEngine] Index search EQ on table_id=" << table_id << ", column_index=" << column_index << ", key=" << key << std::endl;
        }
        BPlusTree tree(disk_, buffer_);
        tree.open(found->root);
        tree.set_trace(index_trace_);
        RID rid; if (tree.search(static_cast<std::int64_t>(key), rid)) {
            std::string row; if (read_record(rid, row)) out.emplace_back(rid, std::move(row));
        }
        return out;
    }

    std::vector<std::pair<RID, std::string>> index_select_range_int(int table_id, int column_index, long long low, long long high) {
        std::vector<std::pair<RID, std::string>> out;
        if (low > high) return out;
        auto idxs = get_table_indexes(table_id);
        const IndexInfo* found = nullptr;
        for (const auto& idx : idxs) { if (idx.column_index == column_index) { found = &idx; break; } }
        if (!found) return out;
        if (index_trace_) {
            std::cout << "[StorageEngine] Index range search on table_id=" << table_id << ", column_index=" << column_index
                      << ", range=[" << low << ", " << high << "]" << std::endl;
        }
        BPlusTree tree(disk_, buffer_);
        tree.open(found->root);
        tree.set_trace(index_trace_);
        auto kvs = tree.range(static_cast<std::int64_t>(low), static_cast<std::int64_t>(high));
        out.reserve(kvs.size());
        for (const auto& kv : kvs) {
            const RID& rid = kv.second;
            std::string row; if (read_record(rid, row)) out.emplace_back(rid, std::move(row));
        }
        return out;
    }

    // VARCHAR index-assisted selection
    std::vector<std::pair<RID, std::string>> index_select_eq_varchar(int table_id, int column_index, const std::string& key) {
        std::vector<std::pair<RID, std::string>> out;
        auto idxs = get_table_indexes(table_id);
        const IndexInfo* found = nullptr;
        for (const auto& idx : idxs) { if (idx.column_index == column_index) { found = &idx; break; } }
        if (!found) return out;
        if (index_trace_) {
            std::cout << "[StorageEngine] Index search EQ(varchar) on table_id=" << table_id << ", column_index=" << column_index << ", key='" << key << "'" << std::endl;
        }
        using StrKey = FixedString<128>;
        BPlusTreeT<StrKey> tree(disk_, buffer_);
        tree.open(found->root);
        tree.set_trace(index_trace_);
        RID rid; if (tree.search(StrKey(key), rid)) {
            std::string row; if (read_record(rid, row)) out.emplace_back(rid, std::move(row));
        }
        return out;
    }

    // 新增：VARCHAR index-assisted range selection (inclusive)
    std::vector<std::pair<RID, std::string>> index_select_range_varchar(int table_id, int column_index, const std::string& low, const std::string& high) {
        std::vector<std::pair<RID, std::string>> out;
        if (low > high) return out;
        auto idxs = get_table_indexes(table_id);
        const IndexInfo* found = nullptr;
        for (const auto& idx : idxs) { if (idx.column_index == column_index) { found = &idx; break; } }
        if (!found) return out;
        if (index_trace_) {
            std::cout << "[StorageEngine] Index range search (varchar) on table_id=" << table_id
                      << ", column_index=" << column_index
                      << ", range=['" << low << "', '" << high << "']" << std::endl;
        }
        using StrKey = FixedString<128>;
        BPlusTreeT<StrKey> tree(disk_, buffer_);
        tree.open(found->root);
        tree.set_trace(index_trace_);
        auto kvs = tree.range(StrKey(low), StrKey(high));
        out.reserve(kvs.size());
        for (const auto& kv : kvs) {
            const RID& rid = kv.second;
            std::string row; if (read_record(rid, row)) out.emplace_back(rid, std::move(row));
        }
        return out;
    }

private:
    // ---------- System catalog helpers ----------
    static inline std::string to_lower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
        return s;
    }
    static inline bool is_system_table(const std::string& name) {
        auto n = to_lower(name);
        return (n == "sys_tables" || n == "sys_columns" || n == "sys_indexes" || n == "sys_users");
    }
    static inline std::string type_to_string(DataType t) {
        switch (t) {
            case DataType::INT: return "INT";
            case DataType::VARCHAR: return "VARCHAR";
            case DataType::DOUBLE: return "DOUBLE";
            case DataType::BOOLEAN: return "BOOLEAN";
            case DataType::TIMESTAMP: return "TIMESTAMP";
            default: return "UNKNOWN";
        }
    }
    static inline std::string join(const std::vector<std::string>& v, const char* sep = ",") {
        std::ostringstream os; for (size_t i=0;i<v.size();++i){ if(i) os<<sep; os<<v[i]; } return os.str();
    }
    static inline std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> out; std::string cur; std::istringstream iss(s); while (std::getline(iss, cur, delim)) out.push_back(cur); return out;
    }
    void ensure_system_catalog() {
        // Create system tables if not exist
        if (tables_.get_table_id("sys_tables") < 0) {
            tables_.create_table("sys_tables");
        }
        if (tables_.get_table_id("sys_columns") < 0) {
            tables_.create_table("sys_columns");
        }
        if (tables_.get_table_id("sys_indexes") < 0) {
            tables_.create_table("sys_indexes");
        }
        if (tables_.get_table_id("sys_users") < 0) {
            tables_.create_table("sys_users");
        }
    }

    void insert_into_sys_tables(int tid, const std::string& name) {
        int sys_tid = tables_.get_table_id("sys_tables");
        if (sys_tid < 0) return;
        std::ostringstream os; os << tid << "|" << name;
        records_.insert(sys_tid, os.str());
    }
    void insert_into_sys_columns(int tid, const std::vector<ColumnMetadata>& columns) {
        int sys_cid = tables_.get_table_id("sys_columns");
        if (sys_cid < 0) return;
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& c = columns[i];
            std::ostringstream cons; for (size_t j=0;j<c.constraints.size();++j){ if(j) cons << ','; cons << c.constraints[j]; }
            std::ostringstream os;
            // table_id|col_index|name|type|length|constraints
            os << tid << '|' << i << '|' << c.name << '|' << type_to_string(c.type) << '|' << c.length << '|' << cons.str();
            records_.insert(sys_cid, os.str());
        }
    }
    void remove_from_sys_catalog(int tid) {
        // remove from sys_tables/sys_columns/sys_indexes by scanning and filtering out matching TID
        auto filter_out = [&](const std::string& sys_table){
            int id = tables_.get_table_id(sys_table);
            if (id < 0) return;
            auto rows = records_.scan(id);
            // naive: drop and recreate table entries without deleted rows
            tables_.drop_table_by_id(id, disk_);
            tables_.create_table(sys_table);
            int nid = tables_.get_table_id(sys_table);
            for (const auto& kv : rows) {
                const std::string& row = kv.second;
                auto fields = split(row, '|');
                if (fields.empty()) continue;
                bool keep = true;
                if (sys_table == "sys_tables") {
                    if (fields.size() >= 1 && std::stoi(fields[0]) == tid) keep = false;
                } else if (sys_table == "sys_columns") {
                    if (fields.size() >= 1 && std::stoi(fields[0]) == tid) keep = false;
                } else if (sys_table == "sys_indexes") {
                    if (fields.size() >= 2 && std::stoi(fields[1]) == tid) keep = false;
                }
                if (keep) records_.insert(nid, row);
            }
        };
        filter_out("sys_tables");
        filter_out("sys_columns");
        filter_out("sys_indexes");
    }
    void insert_into_sys_indexes(const std::string& index_name,
                                 int table_id,
                                 const std::string& column,
                                 bool unique,
                                 std::uint32_t root) {
        int sys_i = tables_.get_table_id("sys_indexes");
        if (sys_i < 0) return;
        std::ostringstream os;
        os << index_name << '|' << table_id << '|' << to_lower(column) << '|' << (unique ? 1 : 0) << '|' << root;
        records_.insert(sys_i, os.str());
    }

private:
    DiskManager disk_;
    BufferManager buffer_;
    TableManager tables_;
    RecordManager records_;
    bool bootstrapping_ = false;
    bool index_trace_ = false; // forward tracing to B+Tree operations
};

} // namespace pcsql