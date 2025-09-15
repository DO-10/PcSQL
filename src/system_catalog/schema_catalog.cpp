#include "system_catalog/schema_catalog.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <cctype>

namespace pcsql {

static std::string lc(std::string s) {
    for (auto& ch : s) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return s;
}

SchemaCatalog::SchemaCatalog(const std::string& base_dir, const std::string& file)
    : base_dir_(std::filesystem::absolute(base_dir)), file_path_(base_dir_ / file) {
    std::filesystem::create_directories(base_dir_);
    load();
}

std::string SchemaCatalog::to_lower(std::string s) { return lc(std::move(s)); }

std::string SchemaCatalog::data_type_to_string(DataType t) {
    switch (t) {
        case DataType::INT: return "INT";
        case DataType::VARCHAR: return "VARCHAR";
        case DataType::DOUBLE: return "DOUBLE";
        case DataType::BOOLEAN: return "BOOLEAN";
        case DataType::TIMESTAMP: return "TIMESTAMP";
        default: return "UNKNOWN";
    }
}

DataType SchemaCatalog::data_type_from_string(const std::string& s) {
    return stringToDataType(s);
}

std::string SchemaCatalog::encode_constraint(const std::string& s) {
    std::string r = s; for (auto& ch : r) if (ch == ' ') ch = '_'; return r;
}
std::string SchemaCatalog::decode_constraint(const std::string& s) {
    std::string r = s; for (auto& ch : r) if (ch == '_') ch = ' '; return r;
}

bool SchemaCatalog::table_exists(const std::string& table_name) const {
    return schemas_.count(lc(table_name)) > 0;
}

void SchemaCatalog::add_table(const std::string& table_name, const std::vector<ColumnMetadata>& columns) {
    auto t = lc(table_name);
    if (schemas_.count(t)) throw std::runtime_error("SchemaCatalog: table exists: " + table_name);
    TableSchema schema;
    schema.columns = columns;
    schema.columnTypes.clear();
    for (const auto& col : columns) schema.columnTypes[lc(col.name)] = col.type;
    schemas_[t] = schema;
    save();
}

bool SchemaCatalog::drop_table(const std::string& table_name) {
    auto t = lc(table_name);
    auto it = schemas_.find(t);
    if (it == schemas_.end()) return false;
    schemas_.erase(it);
    save();
    return true;
}

const TableSchema& SchemaCatalog::get_table_schema(const std::string& table_name) const {
    auto t = lc(table_name);
    auto it = schemas_.find(t);
    if (it == schemas_.end()) throw std::runtime_error("SchemaCatalog: table not found: " + table_name);
    return it->second;
}

void SchemaCatalog::load() {
    schemas_.clear();
    std::ifstream ifs(file_path_);
    if (!ifs) {
        // initialize empty file
        std::ofstream ofs(file_path_);
        if (!ofs) throw std::runtime_error("SchemaCatalog: cannot create schemas file");
        return;
    }
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        std::string table;
        if (!(iss >> table)) continue;
        TableSchema schema;
        schema.columns.clear();
        schema.columnTypes.clear();
        // Remaining tokens: name:type[:c1,c2,...]
        std::string token;
        while (iss >> token) {
            // split by ':'
            std::string name, type, cons;
            size_t p1 = token.find(':');
            if (p1 == std::string::npos) continue;
            name = token.substr(0, p1);
            size_t p2 = token.find(':', p1 + 1);
            if (p2 == std::string::npos) {
                type = token.substr(p1 + 1);
            } else {
                type = token.substr(p1 + 1, p2 - (p1 + 1));
                cons = token.substr(p2 + 1);
            }
            ColumnMetadata col;
            col.name = name;
            col.type = data_type_from_string(type);
            col.constraints.clear();
            if (!cons.empty()) {
                std::stringstream css(cons);
                std::string c;
                while (std::getline(css, c, ',')) col.constraints.push_back(decode_constraint(c));
            }
            schema.columns.push_back(col);
            schema.columnTypes[lc(col.name)] = col.type;
        }
        schemas_[lc(table)] = std::move(schema);
    }
}

void SchemaCatalog::save() const {
    std::ofstream ofs(file_path_);
    if (!ofs) throw std::runtime_error("SchemaCatalog: cannot write schemas file");
    for (const auto& kv : schemas_) {
        const std::string& table = kv.first;
        const TableSchema& schema = kv.second;
        ofs << table;
        for (const auto& col : schema.columns) {
            ofs << ' ' << col.name << ':' << data_type_to_string(col.type);
            if (!col.constraints.empty()) {
                ofs << ':';
                for (size_t i = 0; i < col.constraints.size(); ++i) {
                    if (i) ofs << ',';
                    ofs << encode_constraint(col.constraints[i]);
                }
            }
        }
        ofs << '\n';
    }
}

} // namespace pcsql