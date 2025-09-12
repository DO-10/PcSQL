#include "compiler/parser.h"
#include <stdexcept>
#include <iostream>
#include <sstream>
#include "compiler/catalog.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"

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

    // 语法分析成功后，启动语义分析
    Catalog catalog;
    SemanticAnalyzer semanticAnalyzer(catalog);
    try {
        semanticAnalyzer.analyze(ast, tokens_);
    } catch (const std::runtime_error& e) {
        throw e;
    }
    
    // 语义分析成功后，启动 IR 生成
    IRGenerator irGenerator;
    std::vector<Quadruplet> quadruplets = irGenerator.generate(ast);
    
    // 打印四元式
    std::cout << "\nStarting IR generation (Quadruplets)..." << std::endl;
    for (const auto& quad : quadruplets) {
        std::cout << "(" << quad.op << ", " << quad.arg1 << ", " << quad.arg2 << ", " << quad.result << ")" << std::endl;
    }
    std::cout << "IR generation completed." << std::endl;
    
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

// 子句解析函数
std::vector<std::string> Parser::parseSelectList() {
    std::vector<std::string> columns;
    columns.push_back(currentToken().value);
    eat(TokenType::IDENTIFIER);

    while (currentToken().value == ",") {
        advance();
        columns.push_back(currentToken().value);
        eat(TokenType::IDENTIFIER);
    }
    
    return columns;
}

std::unique_ptr<WhereClause> Parser::parseOptionalWhere() {
    if (currentToken().value == "WHERE") {
        auto node = std::make_unique<WhereClause>();
        advance();
        size_t conditionTokenIndex;
        node->condition = parseCondition(conditionTokenIndex);
        node->tokenIndex = conditionTokenIndex;
        return node;
    }
    return nullptr;
}

std::string Parser::parseCondition(size_t& tokenIndex) {
    std::stringstream ss;
    
    tokenIndex = pos_;
    ss << currentToken().value;
    eat(TokenType::IDENTIFIER);
    
    ss << " " << currentToken().value;
    eat(TokenType::OPERATOR);
    
    ss << " " << currentToken().value;
    eat(currentToken().type);
    
    return ss.str();
}

std::vector<ColumnDefinition> Parser::parseColumnDefinitionList() {
    std::vector<ColumnDefinition> columns;
    columns.push_back(parseColumnDefinition());

    while (currentToken().value == ",") {
        advance();
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
    std::string dataType = currentToken().value;
    eat(TokenType::IDENTIFIER);
    return dataType;
}

std::vector<std::string> Parser::parseColumnConstraints() {
    std::vector<std::string> constraints;
    while (currentToken().value == "PRIMARY" || currentToken().value == "UNIQUE" || currentToken().value == "NOT") {
        if (currentToken().value == "PRIMARY") {
            std::stringstream ss;
            ss << currentToken().value;
            eat("PRIMARY");
            ss << " " << currentToken().value;
            eat("KEY");
            constraints.push_back(ss.str());
        } else if (currentToken().value == "UNIQUE") {
            constraints.push_back(currentToken().value);
            eat("UNIQUE");
        } else if (currentToken().value == "NOT") {
            std::stringstream ss;
            ss << currentToken().value;
            eat("NOT");
            ss << " " << currentToken().value;
            eat("NULL");
            constraints.push_back(ss.str());
        } else {
            break;
        }
    }
    return constraints;
}

std::vector<std::string> Parser::parseValueList() {
    std::vector<std::string> values;
    values.push_back(currentToken().value);
    eat(currentToken().type);

    while (currentToken().value == ",") {
        advance();
        values.push_back(currentToken().value);
        eat(currentToken().type);
    }
    
    return values;
}

std::map<std::string, std::string> Parser::parseSetClause() {
    std::map<std::string, std::string> assignments;
    std::string column = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("=");
    std::string value = currentToken().value;
    eat(currentToken().type);

    assignments[column] = value;

    while (currentToken().value == ",") {
        advance();
        column = currentToken().value;
        eat(TokenType::IDENTIFIER);
        eat("=");
        value = currentToken().value;
        eat(currentToken().type);
        assignments[column] = value;
    }

    return assignments;
}