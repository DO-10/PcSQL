#include "execution/execution_engine.h"
#include <sstream>
#include <iostream>
#include <cctype>
#include <utility>
#include <limits>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <algorithm>

// ---- Local helpers: string utils and condition evaluation ----
static std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}

static std::string trim(std::string s) {
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

static std::string join(const std::vector<std::string>& v, const char* sep) {
    std::ostringstream os;
    for (size_t i = 0; i < v.size(); ++i) { if (i) os << sep; os << v[i]; }
    return os.str();
}

static bool parse_condition(const std::string& cond, std::string& col, std::string& op, std::string& val) {
    std::string s = trim(cond);
    const char* ops[] = {">=", "<=", "!=", "=", ">", "<"};
    for (const char* o : ops) {
        auto pos = s.find(o);
        if (pos != std::string::npos) {
            col = trim(s.substr(0, pos));
            op = o;
            val = trim(s.substr(pos + std::strlen(o)));
            if (!val.empty() && ((val.front()=='\'' && val.back()=='\'') || (val.front()=='\"' && val.back()=='\"'))) {
                if (val.size() >= 2) val = val.substr(1, val.size()-2);
            }
            return !col.empty() && !op.empty() && !val.empty();
        }
    }
    return false;
}

static bool to_bool_ci(const std::string& s) {
    auto t = to_lower(s);
    return (t=="true" || t=="1" || t=="yes" || t=="y");
}

static bool compare_typed(DataType type, const std::string& l, const std::string& op, const std::string& r) {
    auto cmp = [&](auto lhs, auto rhs)->int { if (lhs < rhs) return -1; if (lhs > rhs) return 1; return 0; };
    int c = 0;
    try {
        switch (type) {
            case DataType::INT: { long long li = std::stoll(l); long long ri = std::stoll(r); c = cmp(li, ri); break; }
            case DataType::DOUBLE: { double ld = std::stod(l); double rd = std::stod(r); c = cmp(ld, rd); break; }
            case DataType::BOOLEAN: { bool lb = to_bool_ci(l); bool rb = to_bool_ci(r); c = cmp(lb, rb); break; }
            case DataType::TIMESTAMP:
            case DataType::VARCHAR:
            default: { c = cmp(l, r); break; }
        }
    } catch (...) {
        c = (l < r ? -1 : (l > r ? 1 : 0));
    }
    if (op == "=") return c == 0;
    if (op == "!=") return c != 0;
    if (op == ">") return c > 0;
    if (op == "<") return c < 0;
    if (op == ">=") return c >= 0;
    if (op == "<=") return c <= 0;
    return false;
}

// Forward declarations for helper utilities defined later in this file
static bool constraint_set_contains(const std::vector<std::string>& cons, const std::string& token_lower);
static bool has_auto_increment(const std::vector<std::string>& cons);
static bool has_default_current_timestamp(const std::vector<std::string>& cons);
static bool is_null_or_default_literal(const std::string& v);
static std::string now_timestamp_string();
std::string ExecutionEngine::format_rows(const std::vector<std::pair<pcsql::RID, std::string>>& rows) {
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
        if (auto* ci = dynamic_cast<CreateIndexStatement*>(unit.ast.get())) return handleCreateIndex(ci);
        if (auto* i = dynamic_cast<InsertStatement*>(unit.ast.get())) return handleInsert(i);
        if (auto* d = dynamic_cast<DeleteStatement*>(unit.ast.get())) return handleDelete(d);
        if (auto* u = dynamic_cast<UpdateStatement*>(unit.ast.get())) return handleUpdate(u);
        // 新增：DROP TABLE
        if (auto* dt = dynamic_cast<DropTableStatement*>(unit.ast.get())) return handleDropTable(dt);
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

// 新增：执行 CREATE INDEX（当前仅支持 INT 列/唯一索引）
std::string ExecutionEngine::handleCreateIndex(CreateIndexStatement* stmt) {
    try {
        bool ok = storage_.create_index(stmt->indexName, stmt->tableName, stmt->columnName, true);
        if (!ok) return "CREATE INDEX failed";
    } catch (const std::exception& e) {
        return std::string("CREATE INDEX failed: ") + e.what();
    }
    return "CREATE INDEX OK (" + stmt->indexName + ")";
}

std::string ExecutionEngine::handleInsert(InsertStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->tableName));
    if (tid < 0) return "Table not found: " + stmt->tableName;

    // Load schema to interpret constraints and types
    const auto& schema = storage_.get_table_schema(to_lower(stmt->tableName));

    // Start with provided values (already validated by semantic analyzer for count/type basics)
    std::vector<std::string> vals = stmt->values;
    vals.resize(schema.columns.size());

    // Preload existing rows for AUTO_INCREMENT max scanning (only if needed)
    bool any_auto_inc = false;
    for (const auto& col : schema.columns) if (has_auto_increment(col.constraints)) { any_auto_inc = true; break; }
    std::vector<std::pair<pcsql::RID, std::string>> rows;
    if (any_auto_inc) {
        rows = storage_.scan_table(tid);
    }

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto& col = schema.columns[i];
        std::string& v = vals[i];
        // Handle DEFAULT CURRENT_TIMESTAMP
        if (has_default_current_timestamp(col.constraints)) {
            if (is_null_or_default_literal(v) || to_lower(v) == "current_timestamp") {
                v = now_timestamp_string();
            }
        } else if (to_lower(v) == "current_timestamp") {
            if (col.type == DataType::TIMESTAMP) {
                v = now_timestamp_string();
            }
        }
        // Handle AUTO_INCREMENT for integer columns
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
                        } catch (...) {
                            // ignore non-numeric
                        }
                    }
                }
                long long next = found ? (max_val + 1) : 1;
                v = std::to_string(next);
            }
        }
    }

    std::string row = join(vals, "|");
    auto rid = storage_.insert_record(tid, row);
    // 新增：插入后更新该表相关索引
    storage_.update_indexes_on_insert(tid, row, rid);

    std::ostringstream os; os << "INSERT OK rid=(" << rid.page_id << "," << rid.slot_id << ")";
    return os.str();
}

std::string ExecutionEngine::handleSelect(SelectStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->fromTable));
    if (tid < 0) return "Table not found: " + stmt->fromTable;

    // Diagnostics to describe the query process
    std::vector<std::string> diag;

    // Try index path first; now supports INT equality and range operators
    std::vector<std::pair<pcsql::RID, std::string>> rows;
    bool used_index = false;
    std::string strategy = "full_scan";
    std::string parsed_col, parsed_op, parsed_val;

    if (stmt->whereClause) {
        if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
            if (parse_condition(where->condition, parsed_col, parsed_op, parsed_val)) {
                diag.push_back("WHERE parsed: " + parsed_col + " " + parsed_op + " " + parsed_val);
                const auto& schema = storage_.get_table_schema(to_lower(stmt->fromTable));
                std::string col_lc = to_lower(parsed_col);
                int where_col_idx = -1; DataType where_dtype = DataType::UNKNOWN;
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (to_lower(schema.columns[i].name) == col_lc) { where_col_idx = static_cast<int>(i); where_dtype = schema.columns[i].type; break; }
                }
                if (where_col_idx >= 0) {
                    diag.push_back("WHERE column index: " + std::to_string(where_col_idx) + ", type: " + std::to_string(static_cast<int>(where_dtype)));
                }
                if (where_col_idx >= 0 && where_dtype == DataType::INT) {
                    // Check index existence on this column
                    auto idxs = storage_.get_table_indexes(tid);
                    bool has_idx = false;
                    for (const auto& idx : idxs) { if (idx.column_index == where_col_idx) { has_idx = true; break; } }
                    diag.push_back(std::string("Index exists on column: ") + (has_idx ? "yes" : "no"));

                    if (has_idx) {
                        try {
                            long long v = std::stoll(parsed_val);
                            if (parsed_op == "=") {
                                rows = storage_.index_select_eq_int(tid, where_col_idx, v);
                                used_index = true; strategy = "index_eq";
                            } else if (parsed_op == ">" || parsed_op == ">=" || parsed_op == "<" || parsed_op == "<=") {
                                long long low = std::numeric_limits<long long>::min();
                                long long high = std::numeric_limits<long long>::max();
                                if (parsed_op == ">") {
                                    if (v == std::numeric_limits<long long>::max()) {
                                        // empty
                                        rows.clear(); used_index = true; strategy = "index_range(> empty)";
                                    } else {
                                        // use [v, +inf], rely on post WHERE filter to drop equality
                                        low = v; // [v, +inf]
                                        rows = storage_.index_select_range_int(tid, where_col_idx, low, high);
                                        used_index = true; strategy = "index_range(>)";
                                    }
                                } else if (parsed_op == ">=") {
                                    low = v; // [v, +inf]
                                    rows = storage_.index_select_range_int(tid, where_col_idx, low, high);
                                    used_index = true; strategy = "index_range(>=)";
                                } else if (parsed_op == "<") {
                                    if (v == std::numeric_limits<long long>::min()) {
                                        rows.clear(); used_index = true; strategy = "index_range(< empty)";
                                    } else {
                                        // use [-inf, v], rely on post WHERE filter to drop equality
                                        high = v; // [-inf, v]
                                        rows = storage_.index_select_range_int(tid, where_col_idx, low, high);
                                        used_index = true; strategy = "index_range(<)";
                                    }
                                } else if (parsed_op == "<=") {
                                    high = v; // [-inf, v]
                                    rows = storage_.index_select_range_int(tid, where_col_idx, low, high);
                                    used_index = true; strategy = "index_range(<=)";
                                }
                                if (used_index) {
                                    diag.push_back("Range low/high used: [" + std::to_string(low) + ", " + std::to_string(high) + "]");
                                }
                            } else if (parsed_op == "!=") {
                                // Use two ranges: (-inf, v] U [v, +inf) and rely on post-filter to drop equality
                                std::vector<std::pair<pcsql::RID, std::string>> left, right;
                                if (v != std::numeric_limits<long long>::min()) {
                                    long long high = v; // include v (will be filtered out)
                                    left = storage_.index_select_range_int(tid, where_col_idx, std::numeric_limits<long long>::min(), high);
                                }
                                if (v != std::numeric_limits<long long>::max()) {
                                    long long low = v; // include v (will be filtered out)
                                    right = storage_.index_select_range_int(tid, where_col_idx, low, std::numeric_limits<long long>::max());
                                }
                                rows.reserve(left.size() + right.size());
                                rows.insert(rows.end(), left.begin(), left.end());
                                rows.insert(rows.end(), right.begin(), right.end());
                                used_index = true; strategy = "index_range(!= as two ranges)";
                            }
                        } catch (...) {
                            // fall back to full scan if cannot parse integer
                            diag.push_back("WHERE value not integer, fall back to scan");
                        }
                    }
                } else if (where_col_idx >= 0 && where_dtype == DataType::VARCHAR) {
                    // VARCHAR index path
                    auto idxs = storage_.get_table_indexes(tid);
                    bool has_idx = false;
                    for (const auto& idx : idxs) { if (idx.column_index == where_col_idx) { has_idx = true; break; } }
                    diag.push_back(std::string("Index exists on column: ") + (has_idx ? "yes" : "no"));
                    if (has_idx) {
                        const std::string& v = parsed_val;
                        const std::string minStr = ""; // minimal sentinel
                        const std::string maxStr(128, static_cast<char>(0xFF)); // maximal sentinel for FixedString<128>
                        if (parsed_op == "=") {
                            rows = storage_.index_select_eq_varchar(tid, where_col_idx, v);
                            used_index = true; strategy = "index_eq(varchar)";
                        } else if (parsed_op == ">" || parsed_op == ">=" || parsed_op == "<" || parsed_op == "<=") {
                            std::string low = minStr;
                            std::string high = maxStr;
                            if (parsed_op == ">") {
                                // use [v, +inf] and rely on post-filter to drop equality
                                low = v; // [v, +inf]
                                rows = storage_.index_select_range_varchar(tid, where_col_idx, low, high);
                                used_index = true; strategy = "index_range(> varchar)";
                            } else if (parsed_op == ">=") {
                                low = v; // [v, +inf]
                                rows = storage_.index_select_range_varchar(tid, where_col_idx, low, high);
                                used_index = true; strategy = "index_range(>= varchar)";
                            } else if (parsed_op == "<") {
                                // use [-inf, v] and rely on post-filter to drop equality
                                high = v; // [-inf, v]
                                rows = storage_.index_select_range_varchar(tid, where_col_idx, low, high);
                                used_index = true; strategy = "index_range(< varchar)";
                            } else if (parsed_op == "<=") {
                                high = v; // [-inf, v]
                                rows = storage_.index_select_range_varchar(tid, where_col_idx, low, high);
                                used_index = true; strategy = "index_range(<= varchar)";
                            }
                            if (used_index) {
                                diag.push_back(std::string("Range low/high used (varchar): [") + low + ", " + (high == maxStr ? "MAX" : high) + "]");
                            }
                        } else if (parsed_op == "!=") {
                            // two ranges: [-inf, v] and [v, +inf], rely on post-filter to drop equality
                            auto left = storage_.index_select_range_varchar(tid, where_col_idx, minStr, v);
                            auto right = storage_.index_select_range_varchar(tid, where_col_idx, v, maxStr);
                            rows.reserve(left.size() + right.size());
                            rows.insert(rows.end(), left.begin(), left.end());
                            rows.insert(rows.end(), right.begin(), right.end());
                            used_index = true; strategy = "index_range(!= varchar as two ranges)";
                        }
                    }
                }
            } else {
                diag.push_back("WHERE parse failed, fall back to scan");
            }
        }
    }

    if (!used_index) {
        rows = storage_.scan_table(tid);
        strategy = "full_scan";
    }
    diag.push_back(std::string("Index hit: ") + (used_index ? "true" : "false") + ", strategy: " + strategy + ", candidates: " + std::to_string(rows.size()));

    // Apply WHERE filtering on the working set (for correctness)
    if (stmt->whereClause) {
        if (auto* where = dynamic_cast<WhereClause*>(stmt->whereClause.get())) {
            std::string col, op, val;
            if (parse_condition(where->condition, col, op, val)) {
                std::string col_lc = to_lower(col);
                const auto& schema = storage_.get_table_schema(to_lower(stmt->fromTable));
                int idx = -1; DataType dtype = DataType::UNKNOWN;
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (to_lower(schema.columns[i].name) == col_lc) { idx = static_cast<int>(i); dtype = schema.columns[i].type; break; }
                }
                if (idx >= 0) {
                    std::vector<std::pair<pcsql::RID, std::string>> filtered;
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

    diag.push_back("Final rows: " + std::to_string(rows.size()));

    std::ostringstream os;
    os << "SELECT " << join(stmt->columns, ",") << " FROM " << stmt->fromTable << "\n";
    for (const auto& ln : diag) os << "[QUERY] " << ln << "\n";
    os << format_rows(rows);
    return os.str();
}

std::string ExecutionEngine::handleDelete(DeleteStatement* stmt) {
    int tid = storage_.get_table_id(to_lower(stmt->tableName));
    if (tid < 0) return "Table not found: " + stmt->tableName;

    auto rows = storage_.scan_table(tid);

    // If there is a WHERE clause, filter rows first (type-aware comparison, same as SELECT)
    std::vector<std::pair<pcsql::RID, std::string>> targets;
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
    std::vector<std::pair<pcsql::RID, std::string>> targets;
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

// Helpers for INSERT default value handling
// Removed duplicate to_lower definition (a to_lower helper already exists near the top of this file)

// Removed duplicate to_lower definition (a to_lower helper already exists near the top of this file)

std::string ExecutionEngine::handleDropTable(DropTableStatement* stmt) {
    // 统一转小写以匹配存储层命名规范
    std::string table_lc = to_lower(stmt->tableName);
    try {
        bool ok = storage_.drop_table_by_name(table_lc);
        if (ok) {
            return "DROP TABLE OK (table=" + stmt->tableName + ")";
        } else {
            // 未删除成功：若 IF EXISTS 则视为 no-op 成功，否则提示不存在
            if (stmt->ifExists) {
                return "DROP TABLE skipped (not exists)";
            }
            return "DROP TABLE failed: table not found: " + stmt->tableName;
        }
    } catch (const std::exception& e) {
        return std::string("DROP TABLE failed: ") + e.what();
    }
}

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
    // DEFAULT CURRENT_TIMESTAMP represented as tokens ["DEFAULT", "CURRENT_TIMESTAMP"]
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
    // thread-safe localtime
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    std::ostringstream os;
    os << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return os.str();
}