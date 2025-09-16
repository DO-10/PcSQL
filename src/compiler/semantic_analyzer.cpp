#include "compiler/semantic_analyzer.h"
#include <iostream>
#include <sstream>
#include <cctype>
#include <unordered_set>
#include <set>
#include <algorithm>

// 正确的列元数据类型，可能定义在 types.hpp 中
#include "system_catalog/types.hpp" 

using namespace std;
using namespace pcsql;

// helpers for lowercase
std::string SemanticAnalyzer::to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}

// 检查表是否存在的函数
bool SemanticAnalyzer::tableExists(const std::string& tableName) const {
    if (!storage_) return false;
    return storage_->get_table_id(to_lower(tableName)) >= 0;
}

TableSchema SemanticAnalyzer::loadSchemaFromSys(const std::string& tableName) const {
    if (!storage_) { return {}; }
    return storage_->get_table_schema(to_lower(tableName));
}

// ---------------------- 错误报告 ----------------------

void SemanticAnalyzer::reportErrorWithToken(const std::string& message, const Token& token) const {
    std::stringstream ss;
    ss << message << " at line " << token.line << ", column " << token.column << ".";
    throw std::runtime_error(ss.str());
}

void SemanticAnalyzer::reportError(const std::string& message, size_t tokenIndex, const std::vector<Token>& tokens) const {
    if (tokenIndex >= tokens.size()) {
        reportErrorWithToken(message, tokens.back());
    } else {
        reportErrorWithToken(message, tokens[tokenIndex]);
    }
}

// ---------------------- 主分析方法 ----------------------

void SemanticAnalyzer::analyze(const std::unique_ptr<ASTNode>& ast, const std::vector<Token>& tokens) {
    if (!ast) {
        return;
    }
    if (auto sel = dynamic_cast<SelectStatement*>(ast.get())) {
        visit(sel, tokens);
    }
    else if (auto create = dynamic_cast<CreateTableStatement*>(ast.get())) {
        visit(create, tokens);
    }
    else if (auto insert = dynamic_cast<InsertStatement*>(ast.get())) {
        visit(insert, tokens);
    }
    else if (auto del = dynamic_cast<DeleteStatement*>(ast.get())) {
        visit(del, tokens);
    }
    else if (auto update = dynamic_cast<UpdateStatement*>(ast.get())) {
        visit(update, tokens);
    }
    else if (auto createIndex = dynamic_cast<CreateIndexStatement*>(ast.get())) {
        visit(createIndex, tokens);
    }
    else {
        throw std::runtime_error("Unsupported statement type.");
    }
}

void SemanticAnalyzer::visit(SelectStatement* node, const std::vector<Token>& tokens) {
    std::cout << "[SemA] Analyzing SELECT statement..." << std::endl;

    SemanticContext ctx;

    string mainTableName = to_lower(node->fromTable);
    if (!tableExists(mainTableName)) {
        size_t tokenIndex = findTokenIndex(node->fromTable, tokens);
        reportError("Table '" + node->fromTable + "' does not exist.", tokenIndex, tokens);
    }
    if (ctx.schemas.count(mainTableName) > 0) {
        size_t tokenIndex = findTokenIndex(node->fromTable, tokens);
        reportError("Table name '" + node->fromTable + "' is used more than once.", tokenIndex, tokens);
    }
    ctx.schemas[mainTableName] = loadSchemaFromSys(mainTableName);

    if (node->joinClause) {
        auto currentJoin = node->joinClause.get();
        while (currentJoin) {
            string rightTableName = to_lower(currentJoin->rightTable);
            if (!tableExists(rightTableName)) {
                size_t tokenIndex = findTokenIndex(currentJoin->rightTable, tokens);
                reportError("Table '" + currentJoin->rightTable + "' in JOIN clause does not exist.", tokenIndex, tokens);
            }
            if (ctx.schemas.count(rightTableName) > 0) {
                size_t tokenIndex = findTokenIndex(currentJoin->rightTable, tokens);
                reportError("Table name '" + currentJoin->rightTable + "' is used more than once.", tokenIndex, tokens);
            }
            ctx.schemas[rightTableName] = loadSchemaFromSys(rightTableName);
            validateExpression(currentJoin->onCondition.get(), ctx, tokens);
            currentJoin = currentJoin->nextJoin.get();
        }
    }

    for (const auto& table_entry : ctx.schemas) {
        const auto& tableName = table_entry.first;
        const auto& schema = table_entry.second;
        for (const auto& col : schema.columns) {
            string colName = to_lower(col.name);
            string fullColName = tableName + "." + colName;
            if (ctx.full_column_names.count(colName) > 0) {
                ctx.ambiguous_columns.insert(colName);
            } else {
                ctx.full_column_names[colName] = fullColName;
            }
        }
    }

    if (!node->columns.empty()) {
        validateSelectColumns(node->columns, ctx, tokens);
    }
    
    if (node->whereCondition) {
        validateExpression(node->whereCondition.get(), ctx, tokens);
    }

    std::cout << "[SemA] SELECT statement analyzed successfully." << std::endl;
}

void SemanticAnalyzer::validateExpression(const ASTNode* expr, const SemanticContext& ctx, const std::vector<Token>& tokens) const {
    if (!expr) return;

    if (auto bin = dynamic_cast<const BinaryExpression*>(expr)) {
        validateExpression(bin->left.get(), ctx, tokens);
        validateExpression(bin->right.get(), ctx, tokens);
        return;
    }

    if (auto col = dynamic_cast<const ColumnReference*>(expr)) {
        string colName = to_lower(col->columnName);
        string tableName = to_lower(col->tableName);

        if (!tableName.empty()) {
            if (ctx.schemas.count(tableName) == 0) {
                size_t tokenIndex = findTokenIndex(col->tableName, tokens);
                reportError("Table '" + col->tableName + "' is not part of the query.", tokenIndex, tokens);
            }
            const auto& schema = ctx.schemas.at(tableName);
            bool col_found = false;
            for (const auto& c : schema.columns) {
                if (to_lower(c.name) == colName) {
                    col_found = true;
                    break;
                }
            }
            if (!col_found) {
                size_t tokenIndex = findTokenIndex(col->columnName, tokens);
                reportError("Column '" + col->columnName + "' not found in table '" + col->tableName + "'.", tokenIndex, tokens);
            }
        } else {
            if (ctx.ambiguous_columns.count(colName) > 0) {
                size_t tokenIndex = findTokenIndex(col->columnName, tokens);
                reportError("Column '" + col->columnName + "' is ambiguous. Please qualify it with a table name (e.g., table." + col->columnName + ").", tokenIndex, tokens);
            }
            if (ctx.full_column_names.count(colName) == 0) {
                size_t tokenIndex = findTokenIndex(col->columnName, tokens);
                reportError("Column '" + col->columnName + "' not found in any of the specified tables.", tokenIndex, tokens);
            }
        }
        return;
    }

    if (auto lit = dynamic_cast<const Literal*>(expr)) {
        return;
    }
    
    throw std::runtime_error("Unknown expression node during semantic analysis.");
}

void SemanticAnalyzer::validateSelectColumns(const std::vector<std::string>& columns, const SemanticContext& ctx, const std::vector<Token>& tokens) const {
    for (const auto& col_str : columns) {
        if (col_str == "*") {
            continue;
        }
        
        size_t dot_pos = col_str.find('.');
        if (dot_pos != string::npos) {
            string tableName = to_lower(col_str.substr(0, dot_pos));
            string colName = to_lower(col_str.substr(dot_pos + 1));

            if (ctx.schemas.count(tableName) == 0) {
                size_t tokenIndex = findTokenIndex(tableName, tokens);
                reportError("Table '" + tableName + "' specified in select list is not part of the query.", tokenIndex, tokens);
            }
            
            bool col_found = false;
            for(const auto& c : ctx.schemas.at(tableName).columns) {
                if(to_lower(c.name) == colName) {
                    col_found = true;
                    break;
                }
            }
            if (!col_found) {
                size_t tokenIndex = findTokenIndex(colName, tokens);
                reportError("Column '" + colName + "' not found in table '" + tableName + "'.", tokenIndex, tokens);
            }
        } else {
            string colName = to_lower(col_str);
            if (ctx.ambiguous_columns.count(colName) > 0) {
                size_t tokenIndex = findTokenIndex(colName, tokens);
                reportError("Column '" + colName + "' in select list is ambiguous. Please qualify it.", tokenIndex, tokens);
            }
            if (ctx.full_column_names.count(colName) == 0) {
                size_t tokenIndex = findTokenIndex(colName, tokens);
                reportError("Column '" + colName + "' in select list not found.", tokenIndex, tokens);
            }
        }
    }
}

size_t SemanticAnalyzer::findTokenIndex(const std::string& value, const std::vector<Token>& tokens, size_t start_pos) const {
    for (size_t i = start_pos; i < tokens.size(); ++i) {
        if (to_lower(tokens[i].value) == to_lower(value)) {
            return i;
        }
    }
    return tokens.size(); 
}

// ---------------------- 其他语句的 visit 方法 ----------------------
void SemanticAnalyzer::visit(CreateTableStatement* node, const std::vector<Token>& tokens) {
    std::cout << "[SemA] Analyzing CREATE TABLE statement..." << std::endl;
    std::string tableName = to_lower(node->tableName);
    if (tableExists(tableName)) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' already exists.", tokens[tokenIndex]);
    }

    TableSchema schema;
    for (const auto& col_def : node->columnDefinitions) {
        if (!isValidDataType(col_def.type)) {
            size_t tokenIndex = findTokenIndex(col_def.type, tokens);
            reportErrorWithToken("Invalid data type '" + col_def.type + "'.", tokens[tokenIndex]);
        }
        
        // [FIXED] 使用 ColumnMetadata 替代 TableSchema::Column
        ColumnMetadata new_col;
        new_col.name = to_lower(col_def.name);
        new_col.type = stringToDataType(col_def.type); // 假设有 stringToDataType 函数
        new_col.constraints = col_def.constraints;
        schema.columns.push_back(new_col);
    }
    std::cout << "[SemA] CREATE TABLE statement analyzed successfully." << std::endl;
}

void SemanticAnalyzer::visit(InsertStatement* node, const std::vector<Token>& tokens) {
    std::cout << "[SemA] Analyzing INSERT statement..." << std::endl;
    std::string tableName = to_lower(node->tableName);
    if (!tableExists(tableName)) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' does not exist.", tokens[tokenIndex]);
    }
    
    TableSchema schema = loadSchemaFromSys(tableName);
    
    if (schema.columns.empty()) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' schema not found.", tokens[tokenIndex]);
    }

    checkConstraintsOnInsert(tableName, schema, node->values, tokens);

    std::cout << "[SemA] INSERT statement analyzed successfully." << std::endl;
}

void SemanticAnalyzer::visit(DeleteStatement* node, const std::vector<Token>& tokens) {
    std::cout << "[SemA] Analyzing DELETE statement..." << std::endl;
    std::string tableName = to_lower(node->tableName);
    if (!tableExists(tableName)) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' does not exist.", tokens[tokenIndex]);
    }
    
    SemanticContext ctx;
    ctx.schemas[tableName] = loadSchemaFromSys(tableName);
    for (const auto& col : ctx.schemas.at(tableName).columns) {
        ctx.full_column_names[to_lower(col.name)] = tableName + "." + to_lower(col.name);
    }

    if (node->whereCondition) {
        validateExpression(node->whereCondition.get(), ctx, tokens);
    }
    
    std::cout << "[SemA] DELETE statement analyzed successfully." << std::endl;
}

void SemanticAnalyzer::visit(UpdateStatement* node, const std::vector<Token>& tokens) {
    std::cout << "[SemA] Analyzing UPDATE statement..." << std::endl;
    std::string tableName = to_lower(node->tableName);
    if (!tableExists(tableName)) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' does not exist.", tokens[tokenIndex]);
    }

    TableSchema schema = loadSchemaFromSys(tableName);
    if (schema.columns.empty()) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' schema not found.", tokens[tokenIndex]);
    }

    SemanticContext ctx;
    ctx.schemas[tableName] = schema;
    for (const auto& col : schema.columns) {
        ctx.full_column_names[to_lower(col.name)] = tableName + "." + to_lower(col.name);
    }

    for (const auto& pair : node->assignments) {
        string colName = to_lower(pair.first);
        bool col_found = false;
        for (const auto& col : schema.columns) {
            if (to_lower(col.name) == colName) {
                col_found = true;
                break;
            }
        }
        if (!col_found) {
            size_t tokenIndex = findTokenIndex(pair.first, tokens);
            reportError("Column '" + pair.first + "' not found in table '" + node->tableName + "'.", tokenIndex, tokens);
        }
    }

    if (node->whereCondition) {
        validateExpression(node->whereCondition.get(), ctx, tokens);
    }
    
    checkConstraintsOnUpdate(node, schema, tokens);

    std::cout << "[SemA] UPDATE statement analyzed successfully." << std::endl;
}

void SemanticAnalyzer::visit(CreateIndexStatement* node, const std::vector<Token>& tokens) {
    std::cout << "[SemA] Analyzing CREATE INDEX statement..." << std::endl;
    std::string tableName = to_lower(node->tableName);
    if (!tableExists(tableName)) {
        size_t tokenIndex = findTokenIndex(node->tableName, tokens);
        reportErrorWithToken("Table '" + node->tableName + "' does not exist.", tokens[tokenIndex]);
    }
    
    TableSchema schema = loadSchemaFromSys(tableName);
    bool col_found = false;
    for(const auto& col : schema.columns) {
        if(to_lower(col.name) == to_lower(node->columnName)) {
            col_found = true;
            break;
        }
    }
    if (!col_found) {
        size_t tokenIndex = findTokenIndex(node->columnName, tokens);
        reportErrorWithToken("Column '" + node->columnName + "' not found in table '" + node->tableName + "'.", tokens[tokenIndex]);
    }

    std::cout << "[SemA] CREATE INDEX statement analyzed successfully." << std::endl;
}

bool SemanticAnalyzer::isValidDataType(const std::string& type) const {
    std::string lower_type = to_lower(type);
    return lower_type == "int" || lower_type == "double" || lower_type == "varchar" || lower_type == "char" || lower_type == "timestamp";
}

bool SemanticAnalyzer::is_null_literal(const std::string& v) {
    return to_lower(v) == "null";
}

// ---------------------- 约束检查辅助 ----------------------

SemanticAnalyzer::ConstraintFlags SemanticAnalyzer::parseConstraintFlags(const std::vector<std::string>& cons) {
    ConstraintFlags flags;
    for (const auto& c : cons) {
        std::string lower_c = to_lower(c);
        if (lower_c == "not" || lower_c == "null") {
            if (lower_c == "not") {
                bool found_null = false;
                for (size_t i = 0; i < cons.size(); ++i) {
                    if (to_lower(cons[i]) == "not" && i + 1 < cons.size() && to_lower(cons[i+1]) == "null") {
                        found_null = true;
                        break;
                    }
                }
                if (found_null) {
                    flags.not_null = true;
                }
            }
        }
        else if (lower_c == "unique") {
            flags.unique = true;
        }
        else if (lower_c == "primary") {
            bool found_key = false;
            for (size_t i = 0; i < cons.size(); ++i) {
                if (to_lower(cons[i]) == "primary" && i + 1 < cons.size() && to_lower(cons[i+1]) == "key") {
                    found_key = true;
                    break;
                }
            }
            if (found_key) {
                flags.primary = true;
            }
        }
    }
    return flags;
}

void SemanticAnalyzer::checkConstraintsOnInsert(const std::string& tableName, const TableSchema& schema, const std::vector<std::string>& values, const std::vector<Token>& tokens) {
    if (values.size() != schema.columns.size()) {
        size_t tokenIndex = findTokenIndex("VALUES", tokens);
        reportError("INSERT VALUES count (" + std::to_string(values.size()) + ") does not match table column count (" + std::to_string(schema.columns.size()) + ").", tokenIndex, tokens);
    }

    for (size_t i = 0; i < values.size(); ++i) {
        const auto& col = schema.columns[i];
        const auto& val = values[i];
        ConstraintFlags flags = parseConstraintFlags(col.constraints);

        if (flags.not_null && is_null_literal(val)) {
            size_t tokenIndex = findTokenIndex(val, tokens);
            reportError("NOT NULL constraint violated for column '" + col.name + "'.", tokenIndex, tokens);
        }
    }
}

void SemanticAnalyzer::checkConstraintsOnUpdate(UpdateStatement* node, const TableSchema& schema, const std::vector<Token>& tokens) {
    for (const auto& assignment : node->assignments) {
        std::string colName = to_lower(assignment.first);
        const std::string& newValue = assignment.second;

        // [FIXED] 使用 ColumnMetadata 替代 TableSchema::Column
        auto it = std::find_if(schema.columns.begin(), schema.columns.end(), 
                               [&](const ColumnMetadata& c){ return to_lower(c.name) == colName; });
        
        if (it != schema.columns.end()) {
            ConstraintFlags flags = parseConstraintFlags(it->constraints);
            
            if (flags.not_null && is_null_literal(newValue)) {
                size_t tokenIndex = findTokenIndex(newValue, tokens);
                reportError("NOT NULL constraint violated on update for column '" + it->name + "'.", tokenIndex, tokens);
            }
        }
    }
}