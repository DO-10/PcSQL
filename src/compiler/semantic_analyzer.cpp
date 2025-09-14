#include "compiler/semantic_analyzer.h"
#include <iostream>
#include <sstream>
#include <cctype>

using namespace std;

// helpers for lowercase
std::string SemanticAnalyzer::to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}

// Query system catalog via system tables persisted in storage
bool SemanticAnalyzer::tableExists(const std::string& tableName) const {
    if (!storage_) return false;
    // 直接通过物理目录映射判断是否存在（O(1)）：
    // 注意：全局采用小写作为规范名
    return storage_->get_table_id(to_lower(tableName)) >= 0;
}

TableSchema SemanticAnalyzer::loadSchemaFromSys(const std::string& tableName) const {
    TableSchema schema;
    if (!storage_) return schema;
    int sys_tid = storage_->get_table_id("sys_tables");
    int sys_cid = storage_->get_table_id("sys_columns");
    if (sys_tid < 0 || sys_cid < 0) return schema;

    // Find table id by name
    int table_id = -1;
    std::string target = to_lower(tableName);
    for (const auto& kv : storage_->scan_table(sys_tid)) {
        const std::string& row = kv.second;
        auto pos = row.find('|');
        if (pos == std::string::npos) continue;
        std::string id_str = row.substr(0, pos);
        std::string name = row.substr(pos + 1);
        if (to_lower(name) == target) {
            try { table_id = std::stoi(id_str); } catch (...) { table_id = -1; }
            break;
        }
    }
    if (table_id < 0) return schema;

    // Collect columns for this table id
    for (const auto& kv : storage_->scan_table(sys_cid)) {
        const std::string& row = kv.second;
        // format: table_id|col_index|name|type|constraints
        std::vector<std::string> parts; parts.reserve(5);
        size_t start = 0; size_t pos = 0;
        while ((pos = row.find('|', start)) != std::string::npos) {
            parts.emplace_back(row.substr(start, pos - start));
            start = pos + 1;
        }
        parts.emplace_back(row.substr(start));
        if (parts.size() < 4) continue;
        try {
            int tid = std::stoi(parts[0]);
            if (tid != table_id) continue;
            std::string col_name = parts[2];
            std::string type_str = parts[3];
            DataType dt = stringToDataType(type_str);
            ColumnMetadata col{col_name, dt, {}};
            schema.columns.push_back(col);
            std::string lower = col_name; for (auto& ch : lower) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            schema.columnTypes[lower] = dt;
        } catch (...) {
            continue;
        }
    }
    return schema;
}
// 重要：语义分析入口
void SemanticAnalyzer::analyze(const std::unique_ptr<ASTNode>& ast, const std::vector<Token>& tokens) {
    if (!ast) {
        throw std::runtime_error("Semantic analysis error: AST is empty.");
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
        } else {
            throw std::runtime_error("Semantic analysis error: Unsupported AST node type.");
        }
        std::cout << "Semantic analysis completed successfully." << std::endl;
    } catch (const std::runtime_error& e) {
        throw e;
    }
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
        DataType type = stringToDataType(colDef.type);
        if (type == DataType::UNKNOWN) {
            reportError("Unsupported data type '" + colDef.type + "' for column '" + colDef.name + "'.", node->tableTokenIndex, tokens);
        }
    }
}

void SemanticAnalyzer::visit(InsertStatement* node, const std::vector<Token>& tokens) {
    if (!tableExists(node->tableName)) {
        reportError("Table '" + node->tableName + "' does not exist.", node->tableTokenIndex, tokens);
    }
    const auto schema = loadSchemaFromSys(node->tableName);
    if (schema.columns.size() != node->values.size()) {
        std::stringstream ss;
        ss << "Semantic Error: Number of values (" << node->values.size()
           << ") does not match the number of columns (" << schema.columns.size()
           << ") in table '" << node->tableName << "'.";
        reportError(ss.str(), node->tableTokenIndex, tokens);
    }
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto& column = schema.columns[i];
        const auto& value = node->values[i];
        checkValueType(value, column.type);
    }
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
        checkValueType(value, expectedType);
    }
    checkWhereClause(dynamic_cast<WhereClause*>(node->whereClause.get()), node->tableName, tokens);
}

void SemanticAnalyzer::checkValueType(const std::string& value, DataType expectedType) {
    if (expectedType == DataType::INT) {
        bool isNumber = !value.empty() && std::all_of(value.begin(), value.end(), ::isdigit);
        if (!isNumber) {
            throw std::runtime_error("Semantic Error: Type mismatch. Expected INT, but got '" + value + "'.");
        }
    } else if (expectedType == DataType::DOUBLE) {
        try { (void)std::stod(value); }
        catch (...) { throw std::runtime_error("Semantic Error: Type mismatch. Expected DOUBLE, but got '" + value + "'."); }
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
    std::stringstream ss; ss << "Semantic Error: " << message << " at line " << line << ", column " << column << ".";
    throw std::runtime_error(ss.str());
}
