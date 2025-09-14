#include "compiler/parser.h"
#include <stdexcept>
#include <iostream>
#include <sstream>
// removed: #include "compiler/catalog.h"
// removed: #include "compiler/semantic_analyzer.h"
// removed: #include "compiler/ir_generator.h"

// ===== AST 调试打印辅助函数 =====
namespace {
    std::string indent(int level) { return std::string(level * 2, ' '); }

    void printASTNode(const ASTNode* node, int level = 0) {
        if (!node) {
            std::cout << indent(level) << "<null ASTNode>" << std::endl;
            return;
        }
        if (auto sel = dynamic_cast<const SelectStatement*>(node)) {
            std::cout << indent(level) << "SelectStatement" << std::endl;
            std::cout << indent(level+1) << "columns: ";
            for (size_t i = 0; i < sel->columns.size(); ++i) {
                std::cout << sel->columns[i] << (i + 1 < sel->columns.size() ? ", " : "");
            }
            std::cout << std::endl;
            std::cout << indent(level+1) << "from: " << sel->fromTable << std::endl;
            if (sel->whereClause) {
                std::cout << indent(level+1) << "where:" << std::endl;
                printASTNode(sel->whereClause.get(), level + 2);
            }
            return;
        }
        if (auto ct = dynamic_cast<const CreateTableStatement*>(node)) {
            std::cout << indent(level) << "CreateTableStatement" << std::endl;
            std::cout << indent(level+1) << "table: " << ct->tableName << std::endl;
            std::cout << indent(level+1) << "columns:" << std::endl;
            for (const auto& col : ct->columns) {
                std::cout << indent(level+2) << col.name << " : " << col.type;
                if (!col.constraints.empty()) {
                    std::cout << " [";
                    for (size_t i = 0; i < col.constraints.size(); ++i) {
                        std::cout << col.constraints[i] << (i + 1 < col.constraints.size() ? ", " : "");
                    }
                    std::cout << "]";
                }
                std::cout << std::endl;
            }
            return;
        }
        if (auto ins = dynamic_cast<const InsertStatement*>(node)) {
            std::cout << indent(level) << "InsertStatement" << std::endl;
            std::cout << indent(level+1) << "table: " << ins->tableName << std::endl;
            std::cout << indent(level+1) << "values: ";
            for (size_t i = 0; i < ins->values.size(); ++i) {
                std::cout << ins->values[i] << (i + 1 < ins->values.size() ? ", " : "");
            }
            std::cout << std::endl;
            return;
        }
        if (auto del = dynamic_cast<const DeleteStatement*>(node)) {
            std::cout << indent(level) << "DeleteStatement" << std::endl;
            std::cout << indent(level+1) << "table: " << del->tableName << std::endl;
            if (del->whereClause) {
                std::cout << indent(level+1) << "where:" << std::endl;
                printASTNode(del->whereClause.get(), level + 2);
            }
            return;
        }
        if (auto upd = dynamic_cast<const UpdateStatement*>(node)) {
            std::cout << indent(level) << "UpdateStatement" << std::endl;
            std::cout << indent(level+1) << "table: " << upd->tableName << std::endl;
            std::cout << indent(level+1) << "set:" << std::endl;
            for (const auto& kv : upd->assignments) {
                std::cout << indent(level+2) << kv.first << " = " << kv.second << std::endl;
            }
            if (upd->whereClause) {
                std::cout << indent(level+1) << "where:" << std::endl;
                printASTNode(upd->whereClause.get(), level + 2);
            }
            return;
        }
        if (auto where = dynamic_cast<const WhereClause*>(node)) {
            std::cout << indent(level) << "WhereClause" << std::endl;
            std::cout << indent(level+1) << "condition: " << where->condition << std::endl;
            return;
        }
        std::cout << indent(level) << "<Unknown ASTNode type>" << std::endl;
    }
}
// ===== AST 调试打印辅助函数结束 =====

// Parser 类实现
Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens), pos_(0) {
    if (tokens_.empty() || tokens_.back().type != TokenType::END_OF_FILE) {
        throw std::runtime_error("Lexical analysis did not return EOF token, cannot proceed with parsing.");
    }
}

// 获取当前 Token
const Token& Parser::currentToken() const {
    if (pos_ >= tokens_.size()) {
        throw std::runtime_error("Unexpected end of input during parsing.");
    }
    return tokens_[pos_];
}

// 查看下一个 Token
const Token& Parser::peekNextToken() const {
    if (pos_ + 1 >= tokens_.size()) {
        throw std::runtime_error("Cannot peek next token, input has ended.");
    }
    return tokens_[pos_ + 1];
}

// 前进到下一个 Token
void Parser::advance() {
    pos_++;
}

// 匹配并消耗一个特定值的 Token
void Parser::eat(const std::string& expectedValue) {
    if (currentToken().value != expectedValue) {
        std::stringstream ss;
        ss << "Syntax error: At line " << currentToken().line << ", column " << currentToken().column
           << ", expected '" << expectedValue << "' but got '" << currentToken().value << "'.";
        throw std::runtime_error(ss.str());
    }
    advance();
}

// 匹配并消耗一个特定类型的 Token
void Parser::eat(TokenType expectedType) {
    if (currentToken().type != expectedType) {
        std::stringstream ss;
        ss << "Syntax error: At line " << currentToken().line << ", column " << currentToken().column
           << ", expected type " << static_cast<int>(expectedType) << " but got type "
           << static_cast<int>(currentToken().type) << " ('" << currentToken().value << "').";
        throw std::runtime_error(ss.str());
    }
    advance();
}

// 报告错误
void Parser::reportError(const std::string& message) const {
    std::stringstream ss;
    ss << message << " (at line " << currentToken().line << ", column " << currentToken().column << ").";
    throw std::runtime_error(ss.str());
}

// 主解析函数
std::unique_ptr<ASTNode> Parser::parse() {
    std::cout << "Starting syntax analysis..." << std::endl;
    if (currentToken().type == TokenType::END_OF_FILE) {
        reportError("Empty input");
    }

    std::unique_ptr<ASTNode> ast;

    if (currentToken().value == "SELECT") {
        ast = parseSelectStatement();
    } else if (currentToken().value == "CREATE") {
        ast = parseCreateTableStatement();
    } else if (currentToken().value == "INSERT") {
        ast = parseInsertStatement();
    } else if (currentToken().value == "DELETE") {
        ast = parseDeleteStatement();
    } else if (currentToken().value == "UPDATE") {
        ast = parseUpdateStatement();
    } else {
        reportError("Unknown SQL statement type");
    }
    
    std::cout << "Syntax analysis completed." << std::endl;

    // 打印 AST（语法分析的输出）
    try {
        std::cout << "\nAST dump (from parser):" << std::endl;
        printASTNode(ast.get(), 0);
    } catch (...) {
        // 打印不应影响主流程
    }

    // 仅返回 AST，后续的语义分析与 IR 生成由 Compiler 负责
    return ast;
}

// SELECT 语句解析函数
std::unique_ptr<ASTNode> Parser::parseSelectStatement() {
    std::cout << "Parsing SELECT statement..." << std::endl;
    auto node = std::make_unique<SelectStatement>();
    
    eat("SELECT");
    node->columns = parseSelectList();
    eat("FROM");
    node->tableTokenIndex = pos_;
    node->fromTable = currentToken().value;
    eat(TokenType::IDENTIFIER);
    node->whereClause = parseOptionalWhere();
    eat(";");
    
    return node;
}

// CREATE TABLE 语句解析函数
std::unique_ptr<ASTNode> Parser::parseCreateTableStatement() {
    std::cout << "Parsing CREATE TABLE statement..." << std::endl;
    auto node = std::make_unique<CreateTableStatement>();
    
    eat("CREATE");
    eat("TABLE");
    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("(");
    node->columns = parseColumnDefinitionList();
    eat(")");
    eat(";");
    
    return node;
}

// INSERT 语句解析函数
std::unique_ptr<ASTNode> Parser::parseInsertStatement() {
    std::cout << "Parsing INSERT statement..." << std::endl;
    auto node = std::make_unique<InsertStatement>();
    
    eat("INSERT");
    eat("INTO");
    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("VALUES");
    eat("(");
    node->values = parseValueList();
    eat(")");
    eat(";");

    return node;
}

// DELETE 语句解析函数
std::unique_ptr<ASTNode> Parser::parseDeleteStatement() {
    std::cout << "Parsing DELETE statement..." << std::endl;
    auto node = std::make_unique<DeleteStatement>();

    eat("DELETE");
    eat("FROM");
    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    node->whereClause = parseOptionalWhere();
    eat(";");

    return node;
}

// UPDATE 语句解析函数
std::unique_ptr<ASTNode> Parser::parseUpdateStatement() {
    std::cout << "Parsing UPDATE statement..." << std::endl;
    auto node = std::make_unique<UpdateStatement>();

    eat("UPDATE");
    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("SET");
    node->assignments = parseSetClause();
    node->whereClause = parseOptionalWhere();
    eat(";");

    return node;
}

// 解析 SELECT 列表
std::vector<std::string> Parser::parseSelectList() {
    std::vector<std::string> cols;
    while (true) {
        cols.emplace_back(currentToken().value);
        eat(TokenType::IDENTIFIER);
        if (currentToken().value == ",") {
            eat(",");
        } else {
            break;
        }
    }
    return cols;
}

std::unique_ptr<WhereClause> Parser::parseOptionalWhere() {
    if (currentToken().value == "WHERE") {
        eat("WHERE");
        auto where = std::make_unique<WhereClause>();
        where->tokenIndex = pos_;
        where->condition = parseCondition(where->tokenIndex);
        return where;
    }
    return nullptr;
}

std::string Parser::parseCondition(size_t& tokenIndex) {
    // 简化：WHERE 条件到下一个分号前的所有内容
    std::ostringstream cond;
    while (currentToken().value != ";") {
        cond << currentToken().value << " ";
        tokenIndex = pos_;
        advance();
    }
    return cond.str();
}

std::vector<ColumnDefinition> Parser::parseColumnDefinitionList() {
    std::vector<ColumnDefinition> columns;
    columns.push_back(parseColumnDefinition());
    while (currentToken().value == ",") {
        eat(",");
        columns.push_back(parseColumnDefinition());
    }
    return columns;
}

ColumnDefinition Parser::parseColumnDefinition() {
    ColumnDefinition col;
    col.name = currentToken().value;
    eat(TokenType::IDENTIFIER);
    col.type = parseDataType();
    col.constraints = parseColumnConstraints();
    return col;
}

std::string Parser::parseDataType() {
    // 简化：类型是一个标识符
    std::string type = currentToken().value;
    eat(TokenType::IDENTIFIER);
    return type;
}

std::vector<std::string> Parser::parseColumnConstraints() {
    // 简化：可选的约束列表（如 PRIMARY, KEY, NOT, NULL 等）直到遇到 "," 或 ")" 结束
    std::vector<std::string> constraints;
    while (currentToken().value != "," && currentToken().value != ")") {
        constraints.push_back(currentToken().value);
        advance();
    }
    return constraints;
}

std::vector<std::string> Parser::parseValueList() {
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