// #include "execution/execution_engine.h"
// #include <sstream>
// #include <iostream>
// #include <cctype>
// #include <utility>
// #include <limits>
// #include <chrono>
// #include <ctime>
// #include <iomanip>

// using namespace pcsql;

// // Forward declarations for helper functions used in handleInsert
// static bool has_auto_increment(const std::vector<std::string>& cons);
// static bool has_default_current_timestamp(const std::vector<std::string>& cons);
// static inline bool is_null_or_default_literal(const std::string& v);
// static std::string now_timestamp_string();

// static std::string join(const std::vector<std::string>& v, const char* sep = ", ") {
//     std::ostringstream os; for (size_t i=0;i<v.size();++i){ if(i) os<<sep; os<<v[i]; } return os.str();
// }

// static std::string to_lower(std::string s){ for(auto& c:s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); return s; }

// // helpers for simple WHERE parsing
// static inline std::string trim(const std::string& s) {
//     size_t b = 0, e = s.size();
//     while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
//     while (e > b && std::isspace(static_cast<unsigned char>(s[e-1]))) --e;
//     return s.substr(b, e - b);
// }
// static std::vector<std::string> split(const std::string& s, char delim) {
//     std::vector<std::string> out; std::string cur; std::istringstream iss(s);
//     while (std::getline(iss, cur, delim)) out.push_back(cur);
//     return out;
// }
// static std::string strip_quotes(const std::string& s) {
//     if (s.size() >= 2 && ((s.front()=='\'' && s.back()=='\'') || (s.front()=='"' && s.back()=='"'))) {
//         return s.substr(1, s.size()-2);
//     }
//     return s;
// }

// // Parse condition of form: <col> <op> <value>, where op in {=, !=, >, <, >=, <=}
// static bool parse_condition(const std::string& cond, std::string& col, std::string& op, std::string& val) {
//     std::string s = trim(cond);
//     // Try operators in order of length to avoid splitting ">=" as ">" first
//     const char* ops[] = {">=", "<=", "!=", "=", ">", "<"};
//     for (const char* candidate : ops) {
//         auto pos = s.find(candidate);
//         if (pos != std::string::npos) {
//             col = trim(s.substr(0, pos));
//             op = candidate;
//             val = trim(s.substr(pos + std::string(candidate).size()));
//             return !col.empty() && !op.empty() && !val.empty();
//         }
//     }
//     // Fallback to whitespace split: col op val
//     std::istringstream iss(s);
//     if (iss >> col >> op >> val) {
//         col = trim(col); op = trim(op); val = trim(val);
//         return !col.empty() && !op.empty() && !val.empty();
//     }
//     return false;
// }

// static bool parse_bool(const std::string& s, bool& out) {
//     std::string t = to_lower(trim(s));
//     if (t == "true" || t == "1") { out = true; return true; }
//     if (t == "false" || t == "0") { out = false; return true; }
//     return false;
// }

// static bool safe_stoll(const std::string& s, long long& out) {
//     try {
//         size_t idx=0; long long v = std::stoll(trim(s), &idx); if (idx != trim(s).size()) return false; out = v; return true;
//     } catch (...) { return false; }
// }
// static bool safe_stod(const std::string& s, double& out) {
//     try {
//         size_t idx=0; double v = std::stod(trim(s), &idx); if (idx != trim(s).size()) return false; out = v; return true;
//     } catch (...) { return false; }
// }

// // Typed compare according to DataType. Returns true if lhs (field string) <op> rhs (condition literal) holds.
// static bool compare_typed(DataType type, const std::string& lhs_field_raw, const std::string& op, const std::string& rhs_literal_raw) {
//     const std::string lhs_field = trim(lhs_field_raw);
//     const std::string rhs_literal = strip_quotes(trim(rhs_literal_raw));

//     switch (type) {
//         case DataType::INT: {
//             long long a, b; if (!safe_stoll(lhs_field, a) || !safe_stoll(rhs_literal, b)) return false; // if parse fails, treat as not match
//             if (op == "=") return a == b;
//             if (op == "!=") return a != b;
//             if (op == ">") return a > b;
//             if (op == "<") return a < b;
//             if (op == ">=") return a >= b;
//             if (op == "<=") return a <= b;
//             return false;
//         }
//         case DataType::DOUBLE: {
//             double a, b; if (!safe_stod(lhs_field, a) || !safe_stod(rhs_literal, b)) return false;
//             if (op == "=") return a == b;
//             if (op == "!=") return a != b;
//             if (op == ">") return a > b;
//             if (op == "<") return a < b;
//             if (op == ">=") return a >= b;
//             if (op == "<=") return a <= b;
//             return false;
//         }
//         case DataType::BOOLEAN: {
//             bool a, b; if (!parse_bool(lhs_field, a) || !parse_bool(rhs_literal, b)) return false;
//             if (op == "=") return a == b;
//             if (op == "!=") return a != b;
//             // Other comparisons not supported for boolean
//             return false;
//         }
//         case DataType::VARCHAR:
//         default: {
//             // For strings, support equality/inequality only (case-sensitive for now)
//             if (op == "=") return lhs_field == rhs_literal;
//             if (op == "!=") return lhs_field != rhs_literal;
//             return false;
//         }
//     }
// }

// std::string ExecutionEngine::format_rows(const std::vector<std::pair<RID, std::string>>& rows) {
//     std::ostringstream os;
//     for (const auto& [rid, bytes] : rows) {
//         os << "(" << rid.page_id << "," << rid.slot_id << ") => " << bytes << "\n";
//     }
//     return os.str();
// }

// // 接收 Compiler 返回的 CompiledUnit 并根据 AST 分派
// std::string ExecutionEngine::execute(const Compiler::CompiledUnit& unit) {
//     try {
//         if (auto* s = dynamic_cast<SelectStatement*>(unit.ast.get())) return handleSelect(s);
//         if (auto* c = dynamic_cast<CreateTableStatement*>(unit.ast.get())) return handleCreate(c);
//         if (auto* i = dynamic_cast<InsertStatement*>(unit.ast.get())) return handleInsert(i);
//         if (auto* d = dynamic_cast<DeleteStatement*>(unit.ast.get())) return handleDelete(d);
//         if (auto* u = dynamic_cast<UpdateStatement*>(unit.ast.get())) return handleUpdate(u);
//         return "[ExecutionEngine] Unsupported statement";
//     } catch (const std::exception& ex) {
//         return std::string("[ExecutionEngine] Error: ") + ex.what();
//     }
// }
// //执行创建表
// std::string ExecutionEngine::handleCreate(CreateTableStatement* stmt) {
//     //从 AST 组装列定义 -> ColumnMetadata（占位保留 constraints 原样存储）
//     std::vector<ColumnMetadata> cols;
//     cols.reserve(stmt->columns.size());
//     for (const auto& c : stmt->columns) {
//         ColumnMetadata m;
//         m.name = c.name;
//         m.type = stringToDataType(c.type);
//         m.constraints = c.constraints; // 先占位记录
//         cols.push_back(std::move(m));
//     }

//     // 获取表名
//     std::string table_lc = to_lower(stmt->tableName);
//     try {
//         // 通过 StorageEngine 的 create_table 统一完成：
//         // - 物理表创建（TableManager）
//         // - 系统目录记录表元数据（SchemaCatalog）
//         //   注意：需要传递列元数据
//         int tid = storage_.create_table(table_lc, cols);
//         (void)tid;
//     } catch (const std::exception& e) {
//         return std::string("CREATE TABLE failed: ") + e.what();
//     }

//     return "CREATE TABLE OK (table=" + stmt->tableName + ")";
// }

// std::string ExecutionEngine::handleInsert(InsertStatement* stmt) {
//     int tid = storage_.get_table_id(to_lower(stmt->tableName));
//     if (tid < 0) return "Table not found: " + stmt->tableName;

//     // Load schema to interpret constraints and types
//     const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));

//     // Start with provided values (already validated by semantic analyzer for count/type basics)
//     std::vector<std::string> vals = stmt->values;
//     vals.resize(schema.columns.size());

//     // Preload existing rows for AUTO_INCREMENT max scanning (only if needed)
//     bool any_auto_inc = false;
//     for (const auto& col : schema.columns) if (has_auto_increment(col.constraints)) { any_auto_inc = true; break; }
//     std::vector<std::pair<RID, std::string>> rows;
//     if (any_auto_inc) {
//         rows = storage_.scan_table(tid);
//     }

//     for (size_t i = 0; i < schema.columns.size(); ++i) {
//         const auto& col = schema.columns[i];
//         std::string& v = vals[i];
//         // Handle DEFAULT CURRENT_TIMESTAMP
//         if (has_default_current_timestamp(col.constraints)) {
//             // When user writes DEFAULT or NULL, fill with current timestamp
//             if (is_null_or_default_literal(v) || to_lower(v) == "current_timestamp") {
//                 v = now_timestamp_string();
//             }
//         } else if (to_lower(v) == "current_timestamp") {
//             // If user explicitly provided CURRENT_TIMESTAMP for a TIMESTAMP column, evaluate it as literal now
//             if (col.type == DataType::TIMESTAMP) {
//                 v = now_timestamp_string();
//             }
//         }
//         // Handle AUTO_INCREMENT for integer columns
//         if (has_auto_increment(col.constraints)) {
//             bool need_generate = is_null_or_default_literal(v) || v.empty();
//             if (need_generate) {
//                 long long max_val = 0;
//                 bool found = false;
//                 for (const auto& kv : rows) {
//                     auto fields = split(kv.second, '|');
//                     if (i < fields.size()) {
//                         try {
//                             long long cur = std::stoll(fields[i]);
//                             if (!found || cur > max_val) { max_val = cur; found = true; }
//                         } catch (...) {
//                             // ignore non-numeric
//                         }
//                     }
//                 }
//                 long long next = found ? (max_val + 1) : 1;
//                 v = std::to_string(next);
//             }
//         }
//     }

//     std::string row = join(vals, "|");
//     auto rid = storage_.insert_record(tid, row);
//     std::ostringstream os; os << "INSERT OK rid=(" << rid.page_id << "," << rid.slot_id << ")";
//     return os.str();
// }

// std::string ExecutionEngine::handleSelect(SelectStatement* stmt) {
//     int tid = storage_.get_table_id(to_lower(stmt->fromTable));
//     if (tid < 0) return "Table not found: " + stmt->fromTable;
//     auto rows = storage_.scan_table(tid);

//     // WHERE filtering: support single predicate of form: col {=,!=,>,<,>=,<=} value, with type-aware comparison
//     if (stmt->whereClause) {
//         if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
//             std::string col, op, val;
//             if (parse_condition(where->condition, col, op, val)) {
//                 std::string col_lc = to_lower(col);
//                 // find column index and type from schema
//                 const auto& schema = storage_.get_table_schema(to_lower(stmt->fromTable));
//                 int idx = -1; DataType dtype = DataType::UNKNOWN;
//                 for (size_t i = 0; i < schema.columns.size(); ++i) {
//                     if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
//                 }
//                 if (idx >= 0) {
//                     std::vector<std::pair<RID, std::string>> filtered;
//                     filtered.reserve(rows.size());
//                     for (const auto& kv : rows) {
//                         auto fields = split(kv.second, '|');
//                         if (idx < static_cast<int>(fields.size())) {
//                             const std::string& f = fields[idx];
//                             if (compare_typed(dtype, f, op, val)) filtered.push_back(kv);
//                         }
//                     }
//                     rows.swap(filtered);
//                 }
//             }
//         }
//     }

//     std::ostringstream os;
//     os << "SELECT " << join(stmt->columns) << " FROM " << stmt->fromTable << "\n";
//     os << format_rows(rows);
//     return os.str();
// }

// std::string ExecutionEngine::handleDelete(DeleteStatement* stmt) {
//     int tid = storage_.get_table_id(to_lower(stmt->tableName));
//     if (tid < 0) return "Table not found: " + stmt->tableName;

//     auto rows = storage_.scan_table(tid);

//     // If there is a WHERE clause, filter rows first (type-aware comparison, same as SELECT)
//     std::vector<std::pair<RID, std::string>> targets;
//     if (stmt->whereClause) {
//         if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
//             std::string col, op, val;
//             if (parse_condition(where->condition, col, op, val)) {
//                 std::string col_lc = to_lower(col);
//                 const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));
//                 int idx = -1; DataType dtype = DataType::UNKNOWN;
//                 for (size_t i = 0; i < schema.columns.size(); ++i) {
//                     if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
//                 }
//                 if (idx >= 0) {
//                     targets.reserve(rows.size());
//                     for (const auto& kv : rows) {
//                         auto fields = split(kv.second, '|');
//                         if (idx < static_cast<int>(fields.size())) {
//                             const std::string& f = fields[idx];
//                             if (compare_typed(dtype, f, op, val)) targets.push_back(kv);
//                         }
//                     }
//                 } else {
//                     // Column not found by some reason (should have been caught by semantic analyzer) -> no-op for safety
//                     targets.clear();
//                 }
//             } else {
//                 // WHERE exists but cannot be parsed -> do not delete anything for safety
//                 targets.clear();
//             }
//         }
//     } else {
//         // No WHERE -> delete all rows (explicit full table delete)
//         targets = std::move(rows);
//     }

//     size_t n = 0;
//     for (auto& kv : targets) {
//         n += storage_.delete_record(kv.first) ? 1 : 0;
//     }

//     std::ostringstream os; os << "DELETE OK count=" << n; return os.str();
// }

// std::string ExecutionEngine::handleUpdate(UpdateStatement* stmt) {
//     int tid = storage_.get_table_id(to_lower(stmt->tableName));
//     if (tid < 0) return "Table not found: " + stmt->tableName;

//     // Load all rows to determine targets
//     auto rows = storage_.scan_table(tid);

//     // Build assignment plan: map column name -> (index, type, value)
//     const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));
//     struct Assign { int idx; DataType type; std::string value; };
//     std::vector<Assign> assigns; assigns.reserve(stmt->assignments.size());
//     for (const auto& kv : stmt->assignments) {
//         const std::string& col = kv.first;
//         const std::string& val = kv.second;
//         std::string col_lc = to_lower(col);
//         int idx = -1; DataType dtype = DataType::UNKNOWN;
//         for (size_t i = 0; i < schema.columns.size(); ++i) {
//             if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
//         }
//         if (idx >= 0) {
//             assigns.push_back({idx, dtype, val});
//         }
//         // If column not found (should be prevented by semantic analyzer), just skip for safety
//     }

//     // Determine target rows based on WHERE clause (same logic as SELECT/DELETE)
//     std::vector<std::pair<RID, std::string>> targets;
//     if (stmt->whereClause) {
//         if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
//             std::string col, op, val;
//             if (parse_condition(where->condition, col, op, val)) {
//                 std::string col_lc = to_lower(col);
//                 int idx = -1; DataType dtype = DataType::UNKNOWN;
//                 for (size_t i = 0; i < schema.columns.size(); ++i) {
//                     if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
//                 }
//                 if (idx >= 0) {
//                     targets.reserve(rows.size());
//                     for (const auto& row : rows) {
//                         auto fields = split(row.second, '|');
//                         if (idx < static_cast<int>(fields.size())) {
//                             const std::string& f = fields[idx];
//                             if (compare_typed(dtype, f, op, val)) targets.push_back(row);
//                         }
//                     }
//                 } else {
//                     // WHERE references unknown column -> do nothing for safety
//                     targets.clear();
//                 }
//             } else {
//                 // WHERE cannot be parsed -> do nothing for safety
//                 targets.clear();
//             }
//         }
//     } else {
//         // No WHERE: update all rows
//         targets = std::move(rows);
//     }

//     // Apply assignments to each target row and write back
//     size_t n = 0;
//     for (const auto& kv : targets) {
//         auto fields = split(kv.second, '|');
//         bool changed = false;
//         for (const auto& a : assigns) {
//             if (a.idx >= 0 && a.idx < static_cast<int>(fields.size())) {
//                 // For strings, lexer already removed quotes; for numbers they are raw digits.
//                 fields[a.idx] = a.value;
//                 changed = true;
//             }
//         }
//         if (changed) {
//             std::string new_row = join(fields, "|");
//             if (storage_.update_record(kv.first, new_row)) ++n;
//         }
//     }

//     std::ostringstream os; os << "UPDATE OK count=" << n; return os.str();
// }

// // Helpers for INSERT default value handling
// // Removed duplicate to_lower definition (a to_lower helper already exists near the top of this file)

// // Removed duplicate to_lower definition (a to_lower helper already exists near the top of this file)

// static bool constraint_set_contains(const std::vector<std::string>& cons, const std::string& token_lower) {
//     for (auto c : cons) {
//         auto lc = to_lower(c);
//         if (lc == token_lower) return true;
//     }
//     return false;
// }

// static bool has_auto_increment(const std::vector<std::string>& cons) {
//     return constraint_set_contains(cons, "auto_increment");
// }

// static bool has_default_current_timestamp(const std::vector<std::string>& cons) {
//     // DEFAULT CURRENT_TIMESTAMP represented as tokens ["DEFAULT", "CURRENT_TIMESTAMP"]
//     return constraint_set_contains(cons, "default") && constraint_set_contains(cons, "current_timestamp");
// }

// static inline bool is_null_or_default_literal(const std::string& v) {
//     auto s = to_lower(v);
//     return (s == "null" || s == "default");
// }

// static std::string now_timestamp_string() {
//     using namespace std::chrono;
//     auto now = system_clock::now();
//     std::time_t t = system_clock::to_time_t(now);
//     std::tm tm{};
//     // thread-safe localtime
// #if defined(_WIN32)
//     localtime_s(&tm, &t);
// #else
//     localtime_r(&t, &tm);
// #endif
//     std::ostringstream os;
//     os << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
//     return os.str();
// }
#include "execution/execution_engine.h"
#include <sstream>
#include <iostream>
#include <cctype>
#include <utility>
#include <limits>
#include <chrono>
#include <ctime>
#include <iomanip>

using namespace pcsql;

// Forward declarations for helper functions used in handleInsert
static bool has_auto_increment(const std::vector<std::string>& cons);
static bool has_default_current_timestamp(const std::vector<std::string>& cons);
static inline bool is_null_or_default_literal(const std::string& v);
static std::string now_timestamp_string();

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

// NOTE: This function is now outdated as the AST uses an expression tree.
// A full implementation would require an AST visitor/evaluator here.
// This is kept for reference but the logic calling it will be simplified.
static bool parse_condition(const std::string& cond, std::string& col, std::string& op, std::string& val) {
    std::string s = trim(cond);
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

static bool compare_typed(DataType type, const std::string& lhs_field_raw, const std::string& op, const std::string& rhs_literal_raw) {
    const std::string lhs_field = trim(lhs_field_raw);
    const std::string rhs_literal = strip_quotes(trim(rhs_literal_raw));

    switch (type) {
        case DataType::INT: {
            long long a, b; if (!safe_stoll(lhs_field, a) || !safe_stoll(rhs_literal, b)) return false;
            if (op == "=") return a == b; if (op == "!=") return a != b; if (op == ">") return a > b;
            if (op == "<") return a < b; if (op == ">=") return a >= b; if (op == "<=") return a <= b;
            return false;
        }
        case DataType::DOUBLE: {
            double a, b; if (!safe_stod(lhs_field, a) || !safe_stod(rhs_literal, b)) return false;
            if (op == "=") return a == b; if (op == "!=") return a != b; if (op == ">") return a > b;
            if (op == "<") return a < b; if (op == ">=") return a >= b; if (op == "<=") return a <= b;
            return false;
        }
        case DataType::BOOLEAN: {
            bool a, b; if (!parse_bool(lhs_field, a) || !parse_bool(rhs_literal, b)) return false;
            if (op == "=") return a == b; if (op == "!=") return a != b;
            return false;
        }
        case DataType::VARCHAR:
        default: {
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

std::string ExecutionEngine::handleCreate(CreateTableStatement* stmt) {
    std::vector<ColumnMetadata> cols;
    // [FIXED] 成员名从 'columns' 改为 'columnDefinitions'，并修复循环语法
    cols.reserve(stmt->columnDefinitions.size());
    for (const auto& c : stmt->columnDefinitions) {
        ColumnMetadata m;
        m.name = c.name;
        m.type = stringToDataType(c.type);
        m.constraints = c.constraints;
        cols.push_back(std::move(m));
    }

    std::string table_lc = to_lower(stmt->tableName);
    try {
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
    const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));
    std::vector<std::string> vals = stmt->values;
    vals.resize(schema.columns.size());
    bool any_auto_inc = false;
    for (const auto& col : schema.columns) if (has_auto_increment(col.constraints)) { any_auto_inc = true; break; }
    std::vector<std::pair<RID, std::string>> rows;
    if (any_auto_inc) {
        rows = storage_.scan_table(tid);
    }
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto& col = schema.columns[i];
        std::string& v = vals[i];
        if (has_default_current_timestamp(col.constraints)) {
            if (is_null_or_default_literal(v) || to_lower(v) == "current_timestamp") {
                v = now_timestamp_string();
            }
        } else if (to_lower(v) == "current_timestamp") {
            if (col.type == DataType::TIMESTAMP) {
                v = now_timestamp_string();
            }
        }
        if (has_auto_increment(col.constraints)) {
            bool need_generate = is_null_or_default_literal(v) || v.empty();
            if (need_generate) {
                long long max_val = 0;
                bool found = false;
                for (const auto& kv : rows) {
                    auto fields = split(kv.second, '|');
                    if (i < fields.size()) {
                        try {
                            long long cur = std::stoll(fields[i]);
                            if (!found || cur > max_val) { max_val = cur; found = true; }
                        } catch (...) {}
                    }
                }
                long long next = found ? (max_val + 1) : 1;
                v = std::to_string(next);
            }
        }
    }
    std::string row = join(vals, "|");
    auto rid = storage_.insert_record(tid, row);
    std::ostringstream os; os << "INSERT OK rid=(" << rid.page_id << "," << rid.slot_id << ")";
    return os.str();
}

std::string ExecutionEngine::handleSelect(SelectStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->fromTable));
    if (tid < 0) return "Table not found: " + stmt->fromTable;
    auto rows = storage_.scan_table(tid);

    // [FIXED] 成员名从 'whereClause' 改为 'whereCondition'
    if (stmt->whereCondition) {
        // !!重要提示!!
        // 这里的逻辑需要重写以处理新的表达式树(AST)。
        // 旧的 parse_condition 只能处理简单的字符串，无法处理嵌套表达式。
        // 作为一个临时解决方案，我们可以先让它不执行过滤，以保证代码可以编译通过。
        // 要完全支持，您需要写一个AST visitor来评估whereCondition表达式。
        std::cout << "[ExecutionEngine] WARNING: WHERE clause is present but not yet evaluated due to new AST structure." << std::endl;
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
    std::vector<std::pair<RID, std::string>> targets;

    // [FIXED] 成员名从 'whereClause' 改为 'whereCondition'
    if (stmt->whereCondition) {
        // !!重要提示!!
        // 同上，这里的WHERE逻辑需要重写来评估表达式树。
        // 临时方案：WHERE子句暂时不起作用。
        std::cout << "[ExecutionEngine] WARNING: WHERE clause present in DELETE, but not yet evaluated. No rows will be deleted for safety." << std::endl;
        targets.clear(); // 为安全起见，不删除任何行
    } else {
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

    auto rows = storage_.scan_table(tid);
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
    }

    std::vector<std::pair<RID, std::string>> targets;
    // [FIXED] 成员名从 'whereClause' 改为 'whereCondition'
    if (stmt->whereCondition) {
        // !!重要提示!!
        // 同上，这里的WHERE逻辑需要重写来评估表达式树。
        // 临时方案：WHERE子句暂时不起作用。
        std::cout << "[ExecutionEngine] WARNING: WHERE clause present in UPDATE, but not yet evaluated. No rows will be updated for safety." << std::endl;
        targets.clear(); // 为安全起见，不更新任何行
    } else {
        targets = std::move(rows);
    }

    size_t n = 0;
    for (const auto& kv : targets) {
        auto fields = split(kv.second, '|');
        bool changed = false;
        for (const auto& a : assigns) {
            if (a.idx >= 0 && a.idx < static_cast<int>(fields.size())) {
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

// Helpers for INSERT default value handling
static bool constraint_set_contains(const std::vector<std::string>& cons, const std::string& token_lower) {
    for (auto c : cons) {
        auto lc = to_lower(c);
        if (lc == token_lower) return true;
    }
    return false;
}

static bool has_auto_increment(const std::vector<std::string>& cons) {
    return constraint_set_contains(cons, "auto_increment");
}

static bool has_default_current_timestamp(const std::vector<std::string>& cons) {
    return constraint_set_contains(cons, "default") && constraint_set_contains(cons, "current_timestamp");
}

static inline bool is_null_or_default_literal(const std::string& v) {
    auto s = to_lower(v);
    return (s == "null" || s == "default");
}

static std::string now_timestamp_string() {
    using namespace std::chrono;
    auto now = system_clock::now();
    std::time_t t = system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    std::ostringstream os;
    os << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return os.str();
}