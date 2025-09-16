#include "compiler/parser.h"
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype> // for ::toupper

namespace pcsql {
    // 将 TokenType 转换为字符串，方便调试打印
    std::string tokenTypeToString(TokenType type) {
        switch (type) {
            case TokenType::KEYWORD: return "KEYWORD";
            case TokenType::IDENTIFIER: return "IDENTIFIER";
            case TokenType::INTEGER_LITERAL: return "INTEGER_LITERAL";
            case TokenType::DOUBLE_LITERAL: return "DOUBLE_LITERAL";
            case TokenType::STRING_LITERAL: return "STRING_LITERAL";
            case TokenType::OPERATOR: return "OPERATOR";
            case TokenType::PUNCTUATION: return "PUNCTUATION";
            case TokenType::SEMICOLON: return "SEMICOLON";
            case TokenType::END_OF_FILE: return "END_OF_FILE";
            default: return "UNKNOWN";
        }
    }
}

// ===== AST 调试打印辅助函数 =====
namespace {
    std::string indent(int level) { return std::string(level * 2, ' '); }

    void printExpression(const ASTNode* node, int level); // 前向声明

    // 新增：将 JoinType 转换为字符串，方便调试打印
    std::string joinTypeToString(JoinType type) {
        switch (type) {
            case JoinType::INNER: return "INNER";
            case JoinType::LEFT: return "LEFT";
            case JoinType::RIGHT: return "RIGHT";
            case JoinType::FULL: return "FULL";
            default: return "UNKNOWN";
        }
    }

    void printASTNode(const ASTNode* node, int level = 0) {
        if (!node) {
            std::cout << indent(level) << "<null ASTNode>" << std::endl;
            return;
        }

        if (auto sel = dynamic_cast<const SelectStatement*>(node)) {
            std::cout << indent(level) << "SelectStatement" << std::endl;
            std::cout << indent(level + 1) << "columns: ";
            for (size_t i = 0; i < sel->columns.size(); ++i) {
                std::cout << sel->columns[i] << (i + 1 < sel->columns.size() ? ", " : "");
            }
            std::cout << std::endl;
            std::cout << indent(level + 1) << "from: " << sel->fromTable << std::endl;
            if (sel->joinClause) {
                std::cout << indent(level + 1) << "join:" << std::endl;
                auto currentJoin = sel->joinClause.get();
                while (currentJoin) {
                    // 修改：打印 JoinType
                    std::cout << indent(level + 2) << "type: " << joinTypeToString(currentJoin->type) << std::endl;
                    std::cout << indent(level + 2) << "rightTable: " << currentJoin->rightTable << std::endl;
                    std::cout << indent(level + 2) << "onCondition:" << std::endl;
                    printExpression(currentJoin->onCondition.get(), level + 3);
                    currentJoin = currentJoin->nextJoin.get();
                }
            }
            if (sel->whereCondition) {
                std::cout << indent(level + 1) << "where:" << std::endl;
                printExpression(sel->whereCondition.get(), level + 2);
            }
        }
        else if (auto create = dynamic_cast<const CreateTableStatement*>(node)) {
            std::cout << indent(level) << "CreateTableStatement" << std::endl;
            std::cout << indent(level + 1) << "tableName: " << create->tableName << std::endl;
            std::cout << indent(level + 1) << "columns:" << std::endl;
            for (const auto& col : create->columnDefinitions) {
                std::cout << indent(level + 2) << "name: " << col.name << ", type: " << col.type;
                if (!col.constraints.empty()) {
                    std::cout << ", constraints: ";
                    for (const auto& cons : col.constraints) {
                        std::cout << cons << " ";
                    }
                }
                std::cout << std::endl;
            }
        }
        else if (auto insert = dynamic_cast<const InsertStatement*>(node)) {
            std::cout << indent(level) << "InsertStatement" << std::endl;
            std::cout << indent(level + 1) << "tableName: " << insert->tableName << std::endl;
            std::cout << indent(level + 1) << "values: ";
            for (size_t i = 0; i < insert->values.size(); ++i) {
                std::cout << insert->values[i] << (i + 1 < insert->values.size() ? ", " : "");
            }
            std::cout << std::endl;
        }
        else if (auto update = dynamic_cast<const UpdateStatement*>(node)) {
            std::cout << indent(level) << "UpdateStatement" << std::endl;
            std::cout << indent(level + 1) << "tableName: " << update->tableName << std::endl;
            std::cout << indent(level + 1) << "assignments:" << std::endl;
            for (const auto& assign : update->assignments) {
                std::cout << indent(level + 2) << assign.first << " = " << assign.second << std::endl;
            }
            if (update->whereCondition) {
                std::cout << indent(level + 1) << "where:" << std::endl;
                printExpression(update->whereCondition.get(), level + 2);
            }
        }
        else if (auto del = dynamic_cast<const DeleteStatement*>(node)) {
            std::cout << indent(level) << "DeleteStatement" << std::endl;
            std::cout << indent(level + 1) << "tableName: " << del->tableName << std::endl;
            if (del->whereCondition) {
                std::cout << indent(level + 1) << "where:" << std::endl;
                printExpression(del->whereCondition.get(), level + 2);
            }
        }
        else if (auto createIndex = dynamic_cast<const CreateIndexStatement*>(node)) {
            std::cout << indent(level) << "CreateIndexStatement" << std::endl;
            std::cout << indent(level + 1) << "indexName: " << createIndex->indexName << std::endl;
            std::cout << indent(level + 1) << "tableName: " << createIndex->tableName << std::endl;
            std::cout << indent(level + 1) << "columnName: " << createIndex->columnName << std::endl;
        }
        else {
            std::cout << indent(level) << "Unknown ASTNode" << std::endl;
        }
    }

    void printExpression(const ASTNode* node, int level) {
        if (!node) {
            std::cout << indent(level) << "<null expr>" << std::endl;
            return;
        }
        if (auto bin = dynamic_cast<const BinaryExpression*>(node)) {
            std::cout << indent(level) << "BinaryExpression (" << bin->op << ")" << std::endl;
            printExpression(bin->left.get(), level + 1);
            printExpression(bin->right.get(), level + 1);
        }
        else if (auto col = dynamic_cast<const ColumnReference*>(node)) {
            if (col->tableName.empty()) {
                std::cout << indent(level) << "ColumnReference: " << col->columnName << std::endl;
            }
            else {
                std::cout << indent(level) << "ColumnReference: " << col->tableName << "." << col->columnName << std::endl;
            }
        }
        else if (auto lit = dynamic_cast<const Literal*>(node)) {
            // 修复：直接打印 lit->type，因为它已经是字符串
            std::cout << indent(level) << "Literal (" << lit->type << "): " << lit->value << std::endl;
        }
        else {
            std::cout << indent(level) << "Unknown Expression Node" << std::endl;
        }
    }
}

// ===== Parser 实现 =====

Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens), current_token_index_(0) {}

std::unique_ptr<ASTNode> Parser::parse() {
    if (tokens_.empty()) {
        throw std::runtime_error("Empty input");
    }

    std::unique_ptr<ASTNode> ast;
    std::string token_val_upper = tokens_[0].value;
    std::transform(token_val_upper.begin(), token_val_upper.end(), token_val_upper.begin(), ::toupper);

    if (token_val_upper == "CREATE") {
        if (tokens_.size() > 1) {
            std::string next_token_val_upper = tokens_[1].value;
            std::transform(next_token_val_upper.begin(), next_token_val_upper.end(), next_token_val_upper.begin(), ::toupper);
            if (next_token_val_upper == "TABLE") {
                ast = parseCreateTableStatement();
            }
            else if (next_token_val_upper == "INDEX") {
                ast = parseCreateIndexStatement();
            }
        }
        else {
            throw std::runtime_error("CREATE statement is incomplete.");
        }
    }
    else if (token_val_upper == "INSERT") {
        ast = parseInsertStatement();
    }
    else if (token_val_upper == "DELETE") {
        ast = parseDeleteStatement();
    }
    else if (token_val_upper == "UPDATE") {
        ast = parseUpdateStatement();
    }
    else if (token_val_upper == "SELECT") {
        ast = parseSelectStatement();
    }
    else {
        throw std::runtime_error("Unexpected token: " + tokens_[0].value + " at the beginning of a statement.");
    }
    printASTNode(ast.get());
    return ast;
}

void Parser::advance() {
    // 新增: 在前进到下一个 Token 之前，打印当前 Token 的信息
    const Token& currentToken = tokens_[current_token_index_];
    std::cout << "DEBUG: Consuming Token -> "
              << "Type: " << pcsql::tokenTypeToString(currentToken.type)
              << ", Value: '" << currentToken.value << "'"
              << ", Pos: (" << currentToken.line << ", " << currentToken.column << ")\n";

    if (!isAtEnd()) {
        current_token_index_++;
    }
}

void Parser::eat(const std::string& value) {
    if (!isAtEnd() && currentToken().value == value) {
        advance();
    }
    else {
        std::stringstream ss;
        ss << "Expected '" << value << "', but got '" << currentToken().value << "'";
        throw std::runtime_error(ss.str());
    }
}

void Parser::eat(TokenType type) {
    if (!isAtEnd() && currentToken().type == type) {
        advance();
    }
    else {
        std::stringstream ss;
        ss << "Expected token of type " << pcsql::tokenTypeToString(type) << ", but got '" << currentToken().value << "' of type " << pcsql::tokenTypeToString(currentToken().type);
        throw std::runtime_error(ss.str());
    }
}

const Token& Parser::currentToken() const {
    if (isAtEnd()) {
        throw std::runtime_error("Attempt to access token past the end of input.");
    }
    return tokens_[current_token_index_];
}

const Token& Parser::peekToken() const {
    if (current_token_index_ + 1 >= tokens_.size()) {
        throw std::runtime_error("Attempt to peek token past the end of input.");
    }
    return tokens_[current_token_index_ + 1];
}

bool Parser::isAtEnd() const {
    return current_token_index_ >= tokens_.size();
}

// ---------------------- 表达式解析器 ----------------------

std::unique_ptr<ASTNode> Parser::parseExpression() {
    auto left = parseAndExpression();
    while (!isAtEnd() && currentToken().value == "OR") {
        std::string op = currentToken().value;
        advance();
        auto right = parseAndExpression();
        auto binExpr = std::make_unique<BinaryExpression>();
        binExpr->op = op;
        binExpr->left = std::move(left);
        binExpr->right = std::move(right);
        left = std::move(binExpr);
    }
    return left;
}

std::unique_ptr<ASTNode> Parser::parseAndExpression() {
    auto left = parseComparisonExpression();
    while (!isAtEnd() && currentToken().value == "AND") {
        std::string op = currentToken().value;
        advance();
        auto right = parseComparisonExpression();
        auto binExpr = std::make_unique<BinaryExpression>();
        binExpr->op = op;
        binExpr->left = std::move(left);
        binExpr->right = std::move(right);
        left = std::move(binExpr);
    }
    return left;
}

std::unique_ptr<ASTNode> Parser::parseComparisonExpression() {
    auto left = parseTerm();
    while (!isAtEnd() && (currentToken().value == "=" || currentToken().value == ">" || currentToken().value == "<" || currentToken().value == ">=" || currentToken().value == "<=" || currentToken().value == "!=")) {
        std::string op = currentToken().value;
        advance();
        auto right = parseTerm();
        auto binExpr = std::make_unique<BinaryExpression>();
        binExpr->op = op;
        binExpr->left = std::move(left);
        binExpr->right = std::move(right);
        left = std::move(binExpr);
    }
    return left;
}

std::unique_ptr<ASTNode> Parser::parseTerm() {
    auto left = parseFactor();
    while (!isAtEnd() && (currentToken().value == "+" || currentToken().value == "-")) {
        std::string op = currentToken().value;
        advance();
        auto right = parseFactor();
        auto binExpr = std::make_unique<BinaryExpression>();
        binExpr->op = op;
        binExpr->left = std::move(left);
        binExpr->right = std::move(right);
        left = std::move(binExpr);
    }
    return left;
}

std::unique_ptr<ASTNode> Parser::parseFactor() {
    auto left = parsePrimary();
    while (!isAtEnd() && (currentToken().value == "*" || currentToken().value == "/")) {
        std::string op = currentToken().value;
        advance();
        auto right = parsePrimary();
        auto binExpr = std::make_unique<BinaryExpression>();
        binExpr->op = op;
        binExpr->left = std::move(left);
        binExpr->right = std::move(right);
        left = std::move(binExpr);
    }
    return left;
}

std::unique_ptr<ASTNode> Parser::parsePrimary() {
    if (!isAtEnd() && currentToken().value == "(") {
        eat("(");
        auto expr = parseExpression();
        eat(")");
        return expr;
    }

    if (!isAtEnd() && currentToken().type == TokenType::IDENTIFIER && !isAtEnd() && peekToken().value == ".") {
        std::unique_ptr<ColumnReference> colRef = std::make_unique<ColumnReference>();
        colRef->tableName = currentToken().value;
        eat(TokenType::IDENTIFIER);
        eat(".");
        colRef->columnName = currentToken().value;
        eat(TokenType::IDENTIFIER);
        return colRef;
    }

    if (!isAtEnd() && (currentToken().type == TokenType::STRING_LITERAL ||
        currentToken().type == TokenType::INTEGER_LITERAL ||
        currentToken().type == TokenType::DOUBLE_LITERAL)) {

        std::unique_ptr<Literal> lit = std::make_unique<Literal>();
        lit->value = currentToken().value;
        // 修复：将 TokenType 转换为字符串，再赋值给 lit->type
        lit->type = pcsql::tokenTypeToString(currentToken().type);
        advance();
        return lit;
    }

    if (!isAtEnd() && currentToken().type == TokenType::IDENTIFIER) {
        std::unique_ptr<ColumnReference> colRef = std::make_unique<ColumnReference>();
        colRef->columnName = currentToken().value;
        advance();
        return colRef;
    }

    std::stringstream ss;
    ss << "Unexpected token in expression: '" << currentToken().value << "' at line " << currentToken().line << ", column " << currentToken().column;
    throw std::runtime_error(ss.str());
}

// ---------------------- 语句解析 ----------------------

std::unique_ptr<ASTNode> Parser::parseSelectStatement() {
    std::cout << "Parsing SELECT statement..." << std::endl;
    auto selectStmt = std::make_unique<SelectStatement>();

    eat("SELECT");
    selectStmt->columns = parseSelectList();

    eat("FROM");
    selectStmt->fromTable = currentToken().value;
    eat(TokenType::IDENTIFIER);

    // 解析连续的 JOIN 子句
    std::unique_ptr<JoinClause> currentJoin;
    while (!isAtEnd() && (currentToken().value == "JOIN" || currentToken().value == "LEFT" || currentToken().value == "RIGHT")) {
        currentJoin = parseOptionalJoin();
        if (!selectStmt->joinClause) {
            selectStmt->joinClause = std::move(currentJoin);
        }
        else {
            JoinClause* lastJoin = selectStmt->joinClause.get();
            while (lastJoin->nextJoin) {
                lastJoin = lastJoin->nextJoin.get();
            }
            lastJoin->nextJoin = std::move(currentJoin);
        }
    }

    selectStmt->whereCondition = parseOptionalWhere();
    eat(TokenType::SEMICOLON);

    return selectStmt;
}

std::unique_ptr<ASTNode> Parser::parseOptionalWhere() {
    if (!isAtEnd() && currentToken().value == "WHERE") {
        eat("WHERE");
        return parseExpression();
    }
    return nullptr;
}

std::unique_ptr<JoinClause> Parser::parseOptionalJoin() {
    if (isAtEnd() || (currentToken().value != "JOIN" && currentToken().value != "LEFT" && currentToken().value != "RIGHT")) {
        return nullptr;
    }

    std::unique_ptr<JoinClause> join = std::make_unique<JoinClause>();
    std::string join_type_str = currentToken().value;

    if (join_type_str == "JOIN") {
        join->type = JoinType::INNER;
        eat("JOIN");
    } else if (join_type_str == "LEFT") {
        join->type = JoinType::LEFT;
        eat("LEFT");
        eat("JOIN");
    } else if (join_type_str == "RIGHT") {
        join->type = JoinType::RIGHT;
        eat("RIGHT");
        eat("JOIN");
    }
    // TODO: 可以继续添加对 FULL OUTER JOIN 的支持

    join->rightTable = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("ON");
    join->onCondition = parseExpression();

    return join;
}

std::vector<std::string> Parser::parseSelectList() {
    std::vector<std::string> columns;
    while (!isAtEnd() && currentToken().value != "FROM") {
        if (currentToken().value == "*") {
            columns.push_back("*");
            advance();
            break;
        }
        columns.push_back(currentToken().value);
        advance();
        if (!isAtEnd() && currentToken().value == ",") {
            eat(",");
        }
        else {
            break;
        }
    }
    return columns;
}

std::unique_ptr<ASTNode> Parser::parseCreateTableStatement() {
    std::cout << "Parsing CREATE TABLE statement..." << std::endl;
    auto createTableStmt = std::make_unique<CreateTableStatement>();
    eat("CREATE");
    eat("TABLE");
    createTableStmt->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("(");
    createTableStmt->columnDefinitions = parseColumnDefinitionList();
    eat(")");
    eat(TokenType::SEMICOLON);
    return createTableStmt;
}

std::vector<ColumnDefinition> Parser::parseColumnDefinitionList() {
    std::vector<ColumnDefinition> definitions;
    while (currentToken().value != ")") {
        definitions.push_back(parseColumnDefinition());
        if (currentToken().value == ",") {
            eat(",");
        } else if (currentToken().value != ")") {
            throw std::runtime_error("Expected ',' or ')' after column definition.");
        }
    }
    return definitions;
}

// ColumnDefinition Parser::parseColumnDefinition() {
//     ColumnDefinition def;
//     def.name = currentToken().value;
//     eat(TokenType::IDENTIFIER);
//     def.type = currentToken().value;
//     eat(TokenType::IDENTIFIER);
//     while (currentToken().value != "," && currentToken().value != ")") {
//         def.constraints.push_back(currentToken().value);
//         advance();
//     }
//     return def;
// }
ColumnDefinition Parser::parseColumnDefinition() {
    ColumnDefinition def;

    // 列名
    def.name = currentToken().value;
    eat(TokenType::IDENTIFIER);

    // 类型（可能是 KEYWORD，比如 INT / VARCHAR）
    def.type = currentToken().value;
    if (currentToken().type == TokenType::IDENTIFIER || currentToken().type == TokenType::KEYWORD) {
        advance();
    } else {
        throw std::runtime_error("Expected type name after column '" + def.name + "'");
    }

    // 处理类型参数，比如 VARCHAR(20)
    if (currentToken().value == "(") {
        std::string typeWithParam = def.type;
        typeWithParam += "(";
        eat("(");
        typeWithParam += currentToken().value;
        eat(TokenType::INTEGER_LITERAL);  // 长度
        eat(")");
        typeWithParam += ")";
        def.type = typeWithParam;
    }

    // 处理约束
    while (currentToken().value != "," && currentToken().value != ")") {
        def.constraints.push_back(currentToken().value);
        advance();
    }

    return def;
}


std::unique_ptr<ASTNode> Parser::parseInsertStatement() {
    std::cout << "Parsing INSERT statement..." << std::endl;
    auto insertStmt = std::make_unique<InsertStatement>();
    eat("INSERT");
    eat("INTO");
    insertStmt->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("VALUES");
    eat("(");
    insertStmt->values = parseInsertValues();
    eat(")");
    eat(TokenType::SEMICOLON);
    return insertStmt;
}

std::vector<std::string> Parser::parseInsertValues() {
    std::vector<std::string> values;
    while (currentToken().value != ")") {
        values.push_back(currentToken().value);
        advance();
        if (currentToken().value == ",") {
            eat(",");
        }
    }
    return values;
}

std::unique_ptr<ASTNode> Parser::parseDeleteStatement() {
    std::cout << "Parsing DELETE statement..." << std::endl;
    auto deleteStmt = std::make_unique<DeleteStatement>();
    eat("DELETE");
    eat("FROM");
    deleteStmt->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    deleteStmt->whereCondition = parseOptionalWhere();
    eat(TokenType::SEMICOLON);
    return deleteStmt;
}

std::unique_ptr<ASTNode> Parser::parseUpdateStatement() {
    std::cout << "Parsing UPDATE statement..." << std::endl;
    auto updateStmt = std::make_unique<UpdateStatement>();
    eat("UPDATE");
    updateStmt->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("SET");
    updateStmt->assignments = parseSetClause();
    updateStmt->whereCondition = parseOptionalWhere();
    eat(TokenType::SEMICOLON);
    return updateStmt;
}

std::map<std::string, std::string> Parser::parseSetClause() {
    std::map<std::string, std::string> assignments;
    while (true) {
        std::string column = currentToken().value;
        eat(TokenType::IDENTIFIER);
        eat("=");
        std::string value = currentToken().value;
        advance();
        assignments[column] = value;
        if (currentToken().value == ",") {
            eat(",");
        } else {
            break;
        }
    }
    return assignments;
}

std::unique_ptr<ASTNode> Parser::parseCreateIndexStatement() {
    std::cout << "Parsing CREATE INDEX statement..." << std::endl;
    eat("CREATE");
    eat("INDEX");
    std::string indexName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("ON");
    std::string tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("(");
    std::string columnName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat(")");
    eat(TokenType::SEMICOLON);
    auto createIndexStmt = std::make_unique<CreateIndexStatement>();
    createIndexStmt->indexName = indexName;
    createIndexStmt->tableName = tableName;
    createIndexStmt->columnName = columnName;
    return createIndexStmt;
}
