#include "execution/execution_engine.h"
#include <sstream>
#include <iostream>
#include <cctype>
#include <utility>
#include <limits>

using namespace pcsql;

static std::string join(const std::vector<std::string>& v, const char* sep = ", ") {
    std::ostringstream os; for (size_t i=0;i<v.size();++i){ if(i) os<<sep; os<<v[i]; } return os.str();
}

static std::string to_lower(std::string s){ for(auto& c:s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); return s; }

// helpers for simple WHERE parsing
static inline std::string trim(const std::string& s) {
    size_t b = 0, e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e-1]))) --e;
    return s.substr(b, e - b);
}
static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out; std::string cur; std::istringstream iss(s);
    while (std::getline(iss, cur, delim)) out.push_back(cur);
    return out;
}
static std::string strip_quotes(const std::string& s) {
    if (s.size() >= 2 && ((s.front()=='\'' && s.back()=='\'') || (s.front()=='"' && s.back()=='"'))) {
        return s.substr(1, s.size()-2);
    }
    return s;
}

// Parse condition of form: <col> <op> <value>, where op in {=, !=, >, <, >=, <=}
static bool parse_condition(const std::string& cond, std::string& col, std::string& op, std::string& val) {
    std::string s = trim(cond);
    // Try operators in order of length to avoid splitting ">=" as ">" first
    const char* ops[] = {">=", "<=", "!=", "=", ">", "<"};
    for (const char* candidate : ops) {
        auto pos = s.find(candidate);
        if (pos != std::string::npos) {
            col = trim(s.substr(0, pos));
            op = candidate;
            val = trim(s.substr(pos + std::string(candidate).size()));
            return !col.empty() && !op.empty() && !val.empty();
        }
    }
    // Fallback to whitespace split: col op val
    std::istringstream iss(s);
    if (iss >> col >> op >> val) {
        col = trim(col); op = trim(op); val = trim(val);
        return !col.empty() && !op.empty() && !val.empty();
    }
    return false;
}

static bool parse_bool(const std::string& s, bool& out) {
    std::string t = to_lower(trim(s));
    if (t == "true" || t == "1") { out = true; return true; }
    if (t == "false" || t == "0") { out = false; return true; }
    return false;
}

static bool safe_stoll(const std::string& s, long long& out) {
    try {
        size_t idx=0; long long v = std::stoll(trim(s), &idx); if (idx != trim(s).size()) return false; out = v; return true;
    } catch (...) { return false; }
}
static bool safe_stod(const std::string& s, double& out) {
    try {
        size_t idx=0; double v = std::stod(trim(s), &idx); if (idx != trim(s).size()) return false; out = v; return true;
    } catch (...) { return false; }
}

// Typed compare according to DataType. Returns true if lhs (field string) <op> rhs (condition literal) holds.
static bool compare_typed(DataType type, const std::string& lhs_field_raw, const std::string& op, const std::string& rhs_literal_raw) {
    const std::string lhs_field = trim(lhs_field_raw);
    const std::string rhs_literal = strip_quotes(trim(rhs_literal_raw));

    switch (type) {
        case DataType::INT: {
            long long a, b; if (!safe_stoll(lhs_field, a) || !safe_stoll(rhs_literal, b)) return false; // if parse fails, treat as not match
            if (op == "=") return a == b;
            if (op == "!=") return a != b;
            if (op == ">") return a > b;
            if (op == "<") return a < b;
            if (op == ">=") return a >= b;
            if (op == "<=") return a <= b;
            return false;
        }
        case DataType::DOUBLE: {
            double a, b; if (!safe_stod(lhs_field, a) || !safe_stod(rhs_literal, b)) return false;
            if (op == "=") return a == b;
            if (op == "!=") return a != b;
            if (op == ">") return a > b;
            if (op == "<") return a < b;
            if (op == ">=") return a >= b;
            if (op == "<=") return a <= b;
            return false;
        }
        case DataType::BOOLEAN: {
            bool a, b; if (!parse_bool(lhs_field, a) || !parse_bool(rhs_literal, b)) return false;
            if (op == "=") return a == b;
            if (op == "!=") return a != b;
            // Other comparisons not supported for boolean
            return false;
        }
        case DataType::VARCHAR:
        default: {
            // For strings, support equality/inequality only (case-sensitive for now)
            if (op == "=") return lhs_field == rhs_literal;
            if (op == "!=") return lhs_field != rhs_literal;
            return false;
        }
    }
}

std::string ExecutionEngine::format_rows(const std::vector<std::pair<RID, std::string>>& rows) {
    std::ostringstream os;
    for (const auto& [rid, bytes] : rows) {
        os << "(" << rid.page_id << "," << rid.slot_id << ") => " << bytes << "\n";
    }
    return os.str();
}

// 接收 Compiler 返回的 CompiledUnit 并根据 AST 分派
std::string ExecutionEngine::execute(const Compiler::CompiledUnit& unit) {
    try {
        if (auto* s = dynamic_cast<SelectStatement*>(unit.ast.get())) return handleSelect(s);
        if (auto* c = dynamic_cast<CreateTableStatement*>(unit.ast.get())) return handleCreate(c);
        if (auto* i = dynamic_cast<InsertStatement*>(unit.ast.get())) return handleInsert(i);
        if (auto* d = dynamic_cast<DeleteStatement*>(unit.ast.get())) return handleDelete(d);
        if (auto* u = dynamic_cast<UpdateStatement*>(unit.ast.get())) return handleUpdate(u);
        return "[ExecutionEngine] Unsupported statement";
    } catch (const std::exception& ex) {
        return std::string("[ExecutionEngine] Error: ") + ex.what();
    }
}
//执行创建表
std::string ExecutionEngine::handleCreate(CreateTableStatement* stmt) {
    //从 AST 组装列定义 -> ColumnMetadata（占位保留 constraints 原样存储）
    std::vector<ColumnMetadata> cols;
    cols.reserve(stmt->columns.size());
    for (const auto& c : stmt->columns) {
        ColumnMetadata m;
        m.name = c.name;
        m.type = stringToDataType(c.type);
        m.constraints = c.constraints; // 先占位记录
        cols.push_back(std::move(m));
    }

    // 获取表名
    std::string table_lc = to_lower(stmt->tableName);
    try {
        // 通过 StorageEngine 的 create_table 统一完成：
        // - 物理表创建（TableManager）
        // - 系统目录记录表元数据（SchemaCatalog）
        //   注意：需要传递列元数据
        int tid = storage_.create_table(table_lc, cols);
        (void)tid;
    } catch (const std::exception& e) {
        return std::string("CREATE TABLE failed: ") + e.what();
    }

    return "CREATE TABLE OK (table=" + stmt->tableName + ")";
}

std::string ExecutionEngine::handleInsert(InsertStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->tableName));
    if (tid < 0) return "Table not found: " + stmt->tableName;

    std::string row = join(stmt->values, "|");
    auto rid = storage_.insert_record(tid, row);
    std::ostringstream os; os << "INSERT OK rid=(" << rid.page_id << "," << rid.slot_id << ")";
    return os.str();
}

std::string ExecutionEngine::handleSelect(SelectStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->fromTable));
    if (tid < 0) return "Table not found: " + stmt->fromTable;
    auto rows = storage_.scan_table(tid);

    // WHERE filtering: support single predicate of form: col {=,!=,>,<,>=,<=} value, with type-aware comparison
    if (stmt->whereClause) {
        if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
            std::string col, op, val;
            if (parse_condition(where->condition, col, op, val)) {
                std::string col_lc = to_lower(col);
                // find column index and type from schema
                const auto& schema = storage_.get_table_schema(to_lower(stmt->fromTable));
                int idx = -1; DataType dtype = DataType::UNKNOWN;
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
                }
                if (idx >= 0) {
                    std::vector<std::pair<RID, std::string>> filtered;
                    filtered.reserve(rows.size());
                    for (const auto& kv : rows) {
                        auto fields = split(kv.second, '|');
                        if (idx < static_cast<int>(fields.size())) {
                            const std::string& f = fields[idx];
                            if (compare_typed(dtype, f, op, val)) filtered.push_back(kv);
                        }
                    }
                    rows.swap(filtered);
                }
            }
        }
    }

    std::ostringstream os;
    os << "SELECT " << join(stmt->columns) << " FROM " << stmt->fromTable << "\n";
    os << format_rows(rows);
    return os.str();
}

std::string ExecutionEngine::handleDelete(DeleteStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->tableName));
    if (tid < 0) return "Table not found: " + stmt->tableName;

    auto rows = storage_.scan_table(tid);

    // If there is a WHERE clause, filter rows first (type-aware comparison, same as SELECT)
    std::vector<std::pair<RID, std::string>> targets;
    if (stmt->whereClause) {
        if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
            std::string col, op, val;
            if (parse_condition(where->condition, col, op, val)) {
                std::string col_lc = to_lower(col);
                const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));
                int idx = -1; DataType dtype = DataType::UNKNOWN;
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
                }
                if (idx >= 0) {
                    targets.reserve(rows.size());
                    for (const auto& kv : rows) {
                        auto fields = split(kv.second, '|');
                        if (idx < static_cast<int>(fields.size())) {
                            const std::string& f = fields[idx];
                            if (compare_typed(dtype, f, op, val)) targets.push_back(kv);
                        }
                    }
                } else {
                    // Column not found by some reason (should have been caught by semantic analyzer) -> no-op for safety
                    targets.clear();
                }
            } else {
                // WHERE exists but cannot be parsed -> do not delete anything for safety
                targets.clear();
            }
        }
    } else {
        // No WHERE -> delete all rows (explicit full table delete)
        targets = std::move(rows);
    }

    size_t n = 0;
    for (auto& kv : targets) {
        n += storage_.delete_record(kv.first) ? 1 : 0;
    }

    std::ostringstream os; os << "DELETE OK count=" << n; return os.str();
}

std::string ExecutionEngine::handleUpdate(UpdateStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->tableName));
    if (tid < 0) return "Table not found: " + stmt->tableName;

    // Load all rows to determine targets
    auto rows = storage_.scan_table(tid);

    // Build assignment plan: map column name -> (index, type, value)
    const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));
    struct Assign { int idx; DataType type; std::string value; };
    std::vector<Assign> assigns; assigns.reserve(stmt->assignments.size());
    for (const auto& kv : stmt->assignments) {
        const std::string& col = kv.first;
        const std::string& val = kv.second;
        std::string col_lc = to_lower(col);
        int idx = -1; DataType dtype = DataType::UNKNOWN;
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
        }
        if (idx >= 0) {
            assigns.push_back({idx, dtype, val});
        }
        // If column not found (should be prevented by semantic analyzer), just skip for safety
    }

    // Determine target rows based on WHERE clause (same logic as SELECT/DELETE)
    std::vector<std::pair<RID, std::string>> targets;
    if (stmt->whereClause) {
        if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
            std::string col, op, val;
            if (parse_condition(where->condition, col, op, val)) {
                std::string col_lc = to_lower(col);
                int idx = -1; DataType dtype = DataType::UNKNOWN;
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
                }
                if (idx >= 0) {
                    targets.reserve(rows.size());
                    for (const auto& row : rows) {
                        auto fields = split(row.second, '|');
                        if (idx < static_cast<int>(fields.size())) {
                            const std::string& f = fields[idx];
                            if (compare_typed(dtype, f, op, val)) targets.push_back(row);
                        }
                    }
                } else {
                    // WHERE references unknown column -> do nothing for safety
                    targets.clear();
                }
            } else {
                // WHERE cannot be parsed -> do nothing for safety
                targets.clear();
            }
        }
    } else {
        // No WHERE: update all rows
        targets = std::move(rows);
    }

    // Apply assignments to each target row and write back
    size_t n = 0;
    for (const auto& kv : targets) {
        auto fields = split(kv.second, '|');
        bool changed = false;
        for (const auto& a : assigns) {
            if (a.idx >= 0 && a.idx < static_cast<int>(fields.size())) {
                // For strings, lexer already removed quotes; for numbers they are raw digits.
                fields[a.idx] = a.value;
                changed = true;
            }
        }
        if (changed) {
            std::string new_row = join(fields, "|");
            if (storage_.update_record(kv.first, new_row)) ++n;
        }
    }

    std::ostringstream os; os << "UPDATE OK count=" << n; return os.str();
}