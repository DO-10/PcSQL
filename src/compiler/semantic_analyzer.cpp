#include "compiler/semantic_analyzer.h"
#include <iostream>
#include <sstream>
#include <cctype>
#include <unordered_set>
#include <set>

using namespace std;

// helpers for lowercase
std::string SemanticAnalyzer::to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}

// 检查表是否存在的函数
bool SemanticAnalyzer::tableExists(const std::string& tableName) const {
    if (!storage_) return false;
    // 直接通过物理目录映射判断是否存在（O(1)）：
    // 注意：全局采用小写作为规范名
    return storage_->get_table_id(to_lower(tableName)) >= 0;
}

TableSchema SemanticAnalyzer::loadSchemaFromSys(const std::string& tableName) const {
    // 统一使用 StorageEngine 的系统目录查询，保证返回包含 constraints
    if (!storage_) { std::cout << "[SemA] loadSchemaFromSys: storage_ is null" << std::endl; return {}; }
    auto schema = storage_->get_table_schema(to_lower(tableName));
    if (schema.columns.empty()) {
        std::cout << "[SemA] schema is empty for table '" << tableName << "' (sys_* is the single source)." << std::endl;
    }
    return schema;
}
// 重要：语义分析入口
void SemanticAnalyzer::analyze(const std::unique_ptr<ASTNode>& ast, const std::vector<Token>& tokens) {
    if (!ast) {
        throw std::runtime_error("[语义, (line 0, column 0), AST is empty]");
    }
    std::cout << "Starting semantic analysis..." << std::endl;
    try {
        if (auto selectStmt = dynamic_cast<SelectStatement*>(ast.get())) {
            visit(selectStmt, tokens);
        } else if (auto createTableStmt = dynamic_cast<CreateTableStatement*>(ast.get())) {
            visit(createTableStmt, tokens);
        } else if (auto insertStmt = dynamic_cast<InsertStatement*>(ast.get())) {
            visit(insertStmt, tokens);
        } else if (auto deleteStmt = dynamic_cast<DeleteStatement*>(ast.get())) {
            visit(deleteStmt, tokens);
        } else if (auto updateStmt = dynamic_cast<UpdateStatement*>(ast.get())) {
            visit(updateStmt, tokens);
        } else if (auto createIndexStmt = dynamic_cast<CreateIndexStatement*>(ast.get())) {
            visit(createIndexStmt, tokens);
        } else if (auto dropTableStmt = dynamic_cast<DropTableStatement*>(ast.get())) {
            visit(dropTableStmt, tokens);
        } else {
            throw std::runtime_error("[语义, (line 0, column 0), Unsupported AST node type]");
        }
        std::cout << "Semantic analysis successful." << std::endl;
    } catch (const std::runtime_error& e) {
        throw;
    }
}

bool SemanticAnalyzer::isValidDataType(const std::string& type) const {
    // 将类型字符串转换为小写进行不区分大小写的比较
    std::string lowerType = to_lower(type);
    // 使用一个 std::unordered_set 来高效地检查合法类型（与系统可识别类型尽量对齐）
    static const std::unordered_set<std::string> validTypes = {
        "int", "double", "varchar", "char", "boolean", "timestamp"
    };
    return validTypes.count(lowerType) > 0;
}

void SemanticAnalyzer::visit(SelectStatement* node, const std::vector<Token>& tokens) {
    if (!tableExists(node->fromTable)) {
        reportError("Table '" + node->fromTable + "' does not exist.", node->tableTokenIndex, tokens);
    }
    if (!node->columns.empty()) {
        checkColumnExistence(node->fromTable, node->columns, tokens);
    }
    checkWhereClause(dynamic_cast<WhereClause*>(node->whereClause.get()), node->fromTable, tokens);
}
//分析创建表语句
void SemanticAnalyzer::visit(CreateTableStatement* node, const std::vector<Token>& tokens) {
    // 1) 改为查询系统表（或缓存）
    if (tableExists(node->tableName)) {
        reportError("Table '" + node->tableName + "' already exists.", node->tableTokenIndex, tokens);
    } else {
        std::cout << "[Semantic Analyzer] Table '" << node->tableName << "' does not exist." << std::endl;
    }
    // 2) 校验各列的数据类型是否合法
    for (const auto& colDef : node->columns) {
        if (!isValidDataType(colDef.type)) {
            reportError("Unsupported data type '" + colDef.type + "' for column '" + colDef.name + "'.", node->tableTokenIndex, tokens);
        }
    }
}

void SemanticAnalyzer::visit(InsertStatement* node, const std::vector<Token>& tokens) {
    //先检查表是否存在
    if (!tableExists(node->tableName)) {
        reportError("Table '" + node->tableName + "' does not exist.", node->tableTokenIndex, tokens);
    }
    //检查列
    const auto schema = loadSchemaFromSys(node->tableName);
    if (schema.columns.size() != node->values.size()) {
        std::stringstream ss;
        ss << "Number of values (" << node->values.size()
           << ") does not match the number of columns (" << schema.columns.size()
           << ") in table '" << node->tableName << "'.";
        reportError(ss.str(), node->tableTokenIndex, tokens);
    }
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto& column = schema.columns[i];
        const auto& value = node->values[i];
        // Allow placeholders to be handled by execution layer/defaults
        std::string lv = to_lower(value);
        if (lv == "null" || lv == "default") {
            continue; // placeholders are acceptable; NOT NULL will be checked later
        }
        if (column.type == DataType::TIMESTAMP && lv == "current_timestamp") {
            continue; // will be evaluated at execution time
        }
        checkValueType(value, column.type, node->tableTokenIndex, tokens);
    }
    // 完整性约束检查（NOT NULL / UNIQUE / PRIMARY KEY）
    checkConstraintsOnInsert(node->tableName, schema, node->values, node->tableTokenIndex, tokens);
    std::cout << "Semantic analysis for INSERT statement passed." << std::endl;
}

void SemanticAnalyzer::visit(DeleteStatement* node, const std::vector<Token>& tokens) {
    if (!tableExists(node->tableName)) {
        reportError("Table '" + node->tableName + "' does not exist.", node->tableTokenIndex, tokens);
    }
    checkWhereClause(dynamic_cast<WhereClause*>(node->whereClause.get()), node->tableName, tokens);
}

void SemanticAnalyzer::visit(UpdateStatement* node, const std::vector<Token>& tokens) {
    if (!tableExists(node->tableName)) {
        reportError("Table '" + node->tableName + "' does not exist.", node->tableTokenIndex, tokens);
    }
    const auto schema = loadSchemaFromSys(node->tableName);
    for (const auto& pair : node->assignments) {
        const std::string& column = pair.first;
        const std::string& value = pair.second;
        std::string lowerCol = column; for (auto& ch : lowerCol) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        auto it = schema.columnTypes.find(lowerCol);
        if (it == schema.columnTypes.end()) {
            reportError("Column '" + column + "' does not exist in table '" + node->tableName + "'.", node->tableTokenIndex, tokens);
        }
        DataType expectedType = it->second;
        checkValueType(value, expectedType, node->tableTokenIndex, tokens);
    }
    checkWhereClause(dynamic_cast<WhereClause*>(node->whereClause.get()), node->tableName, tokens);
    // 完整性约束（基础版）：禁止将 NOT NULL/PRIMARY 列更新为 NULL；对 UNIQUE/PRIMARY 做基本重复值检查
    checkConstraintsOnUpdate(node, schema, tokens);
}

// 新增：DROP TABLE 语义分析
void SemanticAnalyzer::visit(DropTableStatement* node, const std::vector<Token>& tokens) {
    // IF EXISTS: 如果表不存在则不报错
    if (!tableExists(node->tableName)) {
        if (node->ifExists) {
            std::cout << "[Semantic Analyzer] DROP TABLE: table '" << node->tableName << "' does not exist; IF EXISTS suppresses error." << std::endl;
            return;
        }
        reportError("Table '" + node->tableName + "' does not exist.", node->tableTokenIndex, tokens);
    }
}

void SemanticAnalyzer::checkValueType(const std::string& value, DataType expectedType, size_t tokenIndex, const std::vector<Token>& tokens) {
    if (expectedType == DataType::INT) {
        bool isNumber = !value.empty() && std::all_of(value.begin(), value.end(), ::isdigit);
        if (!isNumber) {
            reportError("Type mismatch. Expected INT, but got '" + value + "'.", tokenIndex, tokens);
        }
    } else if (expectedType == DataType::DOUBLE) {
        try { (void)std::stod(value); }
        catch (...) { reportError("Type mismatch. Expected DOUBLE, but got '" + value + "'.", tokenIndex, tokens); }
    }
}

void SemanticAnalyzer::checkColumnExistence(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Token>& tokens) {
    const auto schema = loadSchemaFromSys(tableName);
    for (const auto& column : columns) {
        std::string lowerCol = column; for (auto& ch : lowerCol) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (schema.columnTypes.find(lowerCol) == schema.columnTypes.end()) {
            reportError("Column '" + column + "' does not exist in table '" + tableName + "'.", 0, tokens);
        }
    }
}

void SemanticAnalyzer::checkWhereClause(WhereClause* whereClause, const std::string& tableName, const std::vector<Token>& tokens) {
    if (!whereClause) return;
    std::stringstream ss(whereClause->condition);
    std::string column, op, value; ss >> column >> op >> value;
    const auto schema = loadSchemaFromSys(tableName);
    std::string lowerCol = column; for (auto& ch : lowerCol) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    if (schema.columnTypes.find(lowerCol) == schema.columnTypes.end()) {
        reportError("Column '" + column + "' in WHERE clause does not exist in table '" + tableName + "'.", whereClause->tokenIndex, tokens);
    }
}

void SemanticAnalyzer::reportError(const std::string& message, size_t tokenIndex, const std::vector<Token>& tokens) {
    size_t line = 0; size_t column = 0;
    if (tokenIndex < tokens.size()) { line = tokens[tokenIndex].line; column = tokens[tokenIndex].column; }
    std::stringstream ss; ss << "[语义, (line " << line << ", column " << column << "), " << message << "]";
    throw std::runtime_error(ss.str());
}
void SemanticAnalyzer::visit(CreateIndexStatement* node, const std::vector<Token>& tokens) {
    std::cout << "Semantic analysis for CREATE INDEX statement." << std::endl;

    // 检查表是否存在
    if (!tableExists(node->tableName)) {
        reportError("Table '" + node->tableName + "' does not exist.", 0, tokens);
    }

    // 检查列是否存在
    // 注意：这里的实现需要你确保 loadSchemaFromSys 函数能够正确访问存储引擎中的系统表
    auto schema = loadSchemaFromSys(node->tableName);
    // 这里我们将列名转换为小写进行检查
    std::string lowerCol = node->columnName;
    for (auto& ch : lowerCol) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));

    if (schema.columnTypes.find(lowerCol) == schema.columnTypes.end()) {
        reportError("Column '" + node->columnName + "' does not exist in table '" + node->tableName + "'.", 0, tokens);
    }
}

// ------ 约束检查实现 ------
static inline bool is_null_literal(const std::string& v) {
    if (v.empty()) return true;
    std::string s = v; std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return (char)std::tolower(c); });
    return s == "null"; // 简化：识别 NULL 关键字
}

SemanticAnalyzer::ConstraintFlags SemanticAnalyzer::parseConstraintFlags(const std::vector<std::string>& cons) {
    ConstraintFlags f{};
    // cons 是词法/解析阶段按 token 原样保存的字符串（如 PRIMARY, KEY, NOT, NULL, UNIQUE）
    std::unordered_set<std::string> set;
    for (auto c : cons) {
        for (auto& ch : c) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        set.insert(c);
        // 兼容像 "primary key" 被拼成一个 token 的情况
        if (c == "primarykey") { set.insert("primary"); set.insert("key"); }
        if (c == "notnull") { set.insert("not"); set.insert("null"); }
    }
    f.primary = (set.count("primary") && set.count("key"));
    f.unique  = set.count("unique") > 0;
    f.not_null = (set.count("not") && set.count("null")) || f.primary; // PRIMARY 隐含 NOT NULL
    return f;
}

void SemanticAnalyzer::checkConstraintsOnInsert(const std::string& tableName,
                                                const TableSchema& schema,
                                                const std::vector<std::string>& values,
                                                size_t tokenIndex,
                                                const std::vector<Token>& tokens) {
    if (!storage_) return;
    int tid = storage_->get_table_id(to_lower(tableName));
    if (tid < 0) return;
    auto rows = storage_->scan_table(tid);

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto& col = schema.columns[i];
        auto flags = parseConstraintFlags(col.constraints);
        const std::string& v = values[i];
        if (flags.not_null && is_null_literal(v)) {
            reportError("NOT NULL constraint violated for column '" + col.name + "' on INSERT.", tokenIndex, tokens);
        }
        if (flags.unique || flags.primary) {
            // 简单：文本相等即重复（与存储层一致）
            for (const auto& kv : rows) {
                auto fields_row = std::vector<std::string>{};
                std::string cur; std::istringstream iss(kv.second);
                while (std::getline(iss, cur, '|')) fields_row.push_back(cur);
                if (i < fields_row.size() && fields_row[i] == v) {
                    reportError("UNIQUE/PRIMARY KEY constraint violated for column '" + col.name + "' on INSERT.", tokenIndex, tokens);
                }
            }
        }
    }
}

bool SemanticAnalyzer::parse_simple_condition(const std::string& cond, std::string& col, std::string& op, std::string& val) {
    std::stringstream ss(cond);
    if (!(ss >> col >> op >> val)) return false;
    return !col.empty() && !op.empty() && !val.empty();
}

static inline int cmp_numeric(double a, double b) { return (a < b) ? -1 : ((a > b) ? 1 : 0); }

bool SemanticAnalyzer::compare_typed(DataType type, const std::string& left, const std::string& op, const std::string& right) {
    auto do_cmp = [&](int c)->bool {
        if (op == "=") return c == 0; if (op == "!=") return c != 0; if (op == ">") return c > 0; if (op == "<") return c < 0; if (op == ">=") return c >= 0; if (op == "<=") return c <= 0; return false; };
    if (type == DataType::INT) {
        try { long long a = stoll(left); long long b = stoll(right); if (a==b) return do_cmp(0); return do_cmp(a<b?-1:1); } catch (...) { return false; }
    } else if (type == DataType::DOUBLE) {
        try { double a = stod(left); double b = stod(right); return do_cmp(cmp_numeric(a,b)); } catch (...) { return false; }
    } else {
        // 其余按字典序文本比较
        int c = (left==right)?0:((left<right)?-1:1);
        return do_cmp(c);
    }
}

void SemanticAnalyzer::checkConstraintsOnUpdate(UpdateStatement* node,
                                                const TableSchema& schema,
                                                const std::vector<Token>& tokens) {
    if (!storage_) return;
    int tid = storage_->get_table_id(to_lower(node->tableName));
    if (tid < 0) return;

    // 预加载所有行
    auto rows = storage_->scan_table(tid);

    // 解析 WHERE（仅支持单一谓词 col op val，与执行引擎保持一致）
    std::set<std::pair<std::uint32_t,std::uint32_t>> target_rids; // page_id, slot_id
    if (node->whereClause) {
        if (auto* where = dynamic_cast<WhereClause*>(node->whereClause.get())) {
            std::string colw, op, val;
            if (parse_simple_condition(where->condition, colw, op, val)) {
                std::string lcw = to_lower(colw);
                int idxw = -1; DataType dtw = DataType::UNKNOWN;
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    if (to_lower(schema.columns[i].name) == lcw) { idxw = static_cast<int>(i); dtw = schema.columns[i].type; break; }
                }
                if (idxw >= 0) {
                    for (const auto& kv : rows) {
                        std::vector<std::string> fields; fields.reserve(schema.columns.size());
                        std::string cur; std::istringstream iss(kv.second);
                        while (std::getline(iss, cur, '|')) fields.push_back(cur);
                        if (idxw < static_cast<int>(fields.size())) {
                            if (compare_typed(dtw, fields[idxw], op, val)) {
                                target_rids.insert({kv.first.page_id, kv.first.slot_id});
                            }
                        }
                    }
                }
            }
        }
    } else {
        // 无 WHERE => 目标为所有行
        for (const auto& kv : rows) target_rids.insert({kv.first.page_id, kv.first.slot_id});
    }

    // 针对每个被赋值的列检查约束
    for (const auto& assign : node->assignments) {
        std::string colname = assign.first; std::string new_val = assign.second;
        int idx = -1; auto flags = ConstraintFlags{}; DataType dtype = DataType::UNKNOWN; std::string logicalName;
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            if (to_lower(schema.columns[i].name) == to_lower(colname)) {
                idx = static_cast<int>(i); flags = parseConstraintFlags(schema.columns[i].constraints); dtype = schema.columns[i].type; logicalName = schema.columns[i].name; break;
            }
        }
        if (idx < 0) continue; // 不存在的列已在前面报错

        // NOT NULL / PRIMARY: 禁止设为 NULL
        if (flags.not_null && is_null_literal(new_val)) {
            reportError("NOT NULL constraint violated for column '" + logicalName + "' on UPDATE.", node->tableTokenIndex, tokens);
        }

        // UNIQUE/PRIMARY：基本检查
        if (flags.unique || flags.primary) {
            // 统计非目标行中是否已存在该值
            for (const auto& kv : rows) {
                bool is_target = target_rids.count({kv.first.page_id, kv.first.slot_id}) > 0;
                if (is_target) continue; // 非目标行的重复才会导致冲突（目标行会被赋为同一值，下方再判）
                std::vector<std::string> fields; fields.reserve(schema.columns.size());
                std::string cur; std::istringstream iss(kv.second);
                while (std::getline(iss, cur, '|')) fields.push_back(cur);
                if (idx < static_cast<int>(fields.size()) && fields[idx] == new_val) {
                    reportError("UNIQUE/PRIMARY KEY constraint violated for column '" + logicalName + "' on UPDATE: value already exists in another row.", node->tableTokenIndex, tokens);
                }
            }
            // 若目标包含 2 行以上，全部设置为同一值也会冲突
            if (target_rids.size() >= 2) {
                reportError("UNIQUE/PRIMARY KEY constraint violated for column '" + logicalName + "' on UPDATE: multiple target rows would share the same value.", node->tableTokenIndex, tokens);
            }
        }
    }
}