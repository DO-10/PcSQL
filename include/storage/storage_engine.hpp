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
                    if (!fields[5].empty()) {
                        std::istringstream css(fields[5]);
                        std::string c;
                        while (std::getline(css, c, ',')) cm.constraints.push_back(c);
                    }
                } else if (fields.size() >= 5) {
                    // legacy: attempt to parse length; if not numeric, treat as constraints
                    bool parsed_len = false;
                    try {
                        std::size_t len = static_cast<std::size_t>(std::stoul(fields[4]));
                        cm.length = len;
                        parsed_len = true;
                    } catch (...) { /* not a length */ }
                    if (!parsed_len && !fields[4].empty()) {
                        std::istringstream css(fields[4]);
                        std::string c;
                        while (std::getline(css, c, ',')) cm.constraints.push_back(c);
                    }
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
        if (dtype == DataType::INT) {
            BPlusTree tree(disk_, buffer_);
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
                bool ok = tree.insert(static_cast<std::int64_t>(key_ll), rid);
                if (!ok && idx.unique) {
                    std::cerr << "[StorageEngine] UNIQUE index violation on '" << idx.name << "' for key=" << key_ll << std::endl;
                }
            } else if (dtype == DataType::VARCHAR) {
                using StrKey = FixedString<128>;
                BPlusTreeT<StrKey> tree(disk_, buffer_);
                tree.open(idx.root);
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
        BPlusTree tree(disk_, buffer_);
        tree.open(found->root);
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
        BPlusTree tree(disk_, buffer_);
        tree.open(found->root);
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
        using StrKey = FixedString<128>;
        BPlusTreeT<StrKey> tree(disk_, buffer_);
        tree.open(found->root);
        RID rid; if (tree.search(StrKey(key), rid)) {
            std::string row; if (read_record(rid, row)) out.emplace_back(rid, std::move(row));
        }
        return out;
    }

    std::vector<std::pair<RID, std::string>> index_select_range_varchar(int table_id, int column_index, const std::string& low, const std::string& high) {
        std::vector<std::pair<RID, std::string>> out;
        auto idxs = get_table_indexes(table_id);
        const IndexInfo* found = nullptr;
        for (const auto& idx : idxs) { if (idx.column_index == column_index) { found = &idx; break; } }
        if (!found) return out;
        using StrKey = FixedString<128>;
        BPlusTreeT<StrKey> tree(disk_, buffer_);
        tree.open(found->root);
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
        std::string n = to_lower(name);
        return n == "sys_tables" || n == "sys_columns" || n == "sys_indexes" || n == "sys_users";
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
            {"unique", DataType::BOOLEAN, {}},
            {"root", DataType::INT, {}}
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
            // 关键：立即持久化，避免下次启动再次判空并重复自举
            flush_all();
            std::cout << "[StorageEngine] bootstrapped system catalog rows in sys_tables/sys_columns (flushed)" << std::endl;
        }
    }

    void insert_into_sys_tables(int tid, const std::string& name) {
        // format: id|name
        std::ostringstream os; os << tid << "|" << name;
        int sys_tid = tables_.get_table_id("sys_tables");
        (void)records_.insert(sys_tid, os.str());
    }

    void insert_into_sys_columns(int tid, const std::vector<ColumnMetadata>& columns) {
        int sys_cid = tables_.get_table_id("sys_columns");
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& c = columns[i];
            std::ostringstream os;
            // format: table_id|col_index|name|type|length|constraints
            os << tid << "|" << i << "|" << c.name << "|" << type_to_string(c.type) << "|" << c.length << "|" << join(c.constraints);
            (void)records_.insert(sys_cid, os.str());
        }
    }

    void remove_from_sys_catalog(int tid) {
        // remove from sys_tables
        int sys_tid = tables_.get_table_id("sys_tables");
        auto trs = records_.scan(sys_tid);
        for (const auto& kv : trs) {
            if (kv.second.rfind(std::to_string(tid) + "|", 0) == 0) records_.erase(kv.first);
        }
        // remove from sys_columns
        int sys_cid = tables_.get_table_id("sys_columns");
        auto crs = records_.scan(sys_cid);
        for (const auto& kv : crs) {
            if (kv.second.rfind(std::to_string(tid) + "|", 0) == 0) records_.erase(kv.first);
        }
        // remove from sys_indexes
        int sys_i = tables_.get_table_id("sys_indexes");
        auto irs = records_.scan(sys_i);
        for (const auto& kv : irs) {
            // row format: index_name|table_id|column|unique|root
            auto f = split(kv.second, '|');
            if (f.size() >= 2) {
                try { if (std::stoi(f[1]) == tid) records_.erase(kv.first); } catch (...) {}
            }
        }
    }

    void insert_into_sys_indexes(const std::string& index_name,
                                 int table_id,
                                 const std::string& column,
                                 bool unique,
                                 std::uint32_t root) {
        int sys_i = tables_.get_table_id("sys_indexes");
        std::ostringstream os;
        os << index_name << "|" << table_id << "|" << column << "|" << (unique ? "1" : "0") << "|" << root;
        (void)records_.insert(sys_i, os.str());
    }

private:
    DiskManager disk_;
    BufferManager buffer_;
    TableManager tables_;
    RecordManager records_;
    bool bootstrapping_ = false;
};

} // namespace pcsql