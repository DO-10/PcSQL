#pragma once
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "system_catalog/types.hpp"

namespace pcsql {

// Persisted schema catalog stored under base_dir_/schemas.meta
class SchemaCatalog {
public:
    explicit SchemaCatalog(const std::string& base_dir = ".",
                           const std::string& file = "schemas.meta");

    // CRUD
    bool table_exists(const std::string& table_name) const;
    void add_table(const std::string& table_name, const std::vector<ColumnMetadata>& columns);
    bool drop_table(const std::string& table_name);
    const TableSchema& get_table_schema(const std::string& table_name) const;

    // Enumeration
    std::vector<std::pair<std::string, TableSchema>> list_all() const {
        std::vector<std::pair<std::string, TableSchema>> out;
        out.reserve(schemas_.size());
        for (const auto& kv : schemas_) out.emplace_back(kv.first, kv.second);
        return out;
    }

    // Persistence
    void load();
    void save() const;

private:
    static std::string to_lower(std::string s);
    static std::string data_type_to_string(DataType t);
    static DataType data_type_from_string(const std::string& s);
    static std::string encode_constraint(const std::string& s); // replace spaces with '_'
    static std::string decode_constraint(const std::string& s); // replace '_' with spaces

private:
    std::filesystem::path base_dir_;
    std::filesystem::path file_path_;
    std::unordered_map<std::string, TableSchema> schemas_; // key: lower-case table name
};

} // namespace pcsql