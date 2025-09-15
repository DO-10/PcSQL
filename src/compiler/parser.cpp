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
    // 确保 Token 序列以 EOF 结束
    if (tokens_.empty() || tokens_.back().type != TokenType::END_OF_FILE) {
        throw std::runtime_error("Lexical analysis did not return EOF token, cannot proceed with parsing.");
    }
}

// 获取当前 Token
const Token& Parser::currentToken() const {
    // 检查是否超出 Token 范围
    if (pos_ >= tokens_.size()) {
        throw std::runtime_error("Unexpected end of input during parsing.");
    }
    return tokens_[pos_];
}

// 查看下一个 Token（不消耗）
const Token& Parser::peekNextToken() const {
    if (pos_ + 1 >= tokens_.size()) {
        throw std::runtime_error("Cannot peek next token, input has ended.");
    }
    return tokens_[pos_ + 1];
}

// 移动到下一个 Token
void Parser::advance() {
    pos_++;
}

// 匹配并消耗特定值的 Token
void Parser::eat(const std::string& expectedValue) {
    if (currentToken().value != expectedValue) {
        // 构建详细的错误信息（包含行列位置）
        std::stringstream ss;
        ss << "Syntax error: At line " << currentToken().line << ", column " << currentToken().column
           << ", expected '" << expectedValue << "' but got '" << currentToken().value << "'.";
        throw std::runtime_error(ss.str());
    }
    advance();
}

// 匹配并消耗特定类型的 Token
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

// 报告错误信息（包含位置）
// 重载的 reportError 函数
void Parser::reportError(const std::string& message, size_t position) const {
    if (position < tokens_.size()) {
        const Token& errorToken = tokens_[position];
        std::stringstream ss;
        ss << "Parser Error: " << message << " at line " << errorToken.line 
           << ", column " << errorToken.column << ".";
        throw std::runtime_error(ss.str());
    } else {
        std::stringstream ss;
        ss << "Parser Error: " << message << " at the end of input.";
        throw std::runtime_error(ss.str());
    }
}

// 原始的 reportError 函数
void Parser::reportError(const std::string& message) const {
    // 调用新实现的函数，传入当前位置
    reportError(message, pos_);
}

// 主解析函数（入口点）
std::unique_ptr<ASTNode> Parser::parse() {
    std::cout << "Starting syntax analysis..." << std::endl;
    
    // 检查空输入
    if (currentToken().type == TokenType::END_OF_FILE) {
        reportError("Empty input");
    }

    std::unique_ptr<ASTNode> ast;

    // 根据首个 Token 分发到不同的 SQL 语句解析器
     if (currentToken().value == "SELECT") {
        ast = parseSelectStatement();
    } else if (currentToken().value == "INSERT") {
        ast = parseInsertStatement();
    } else if (currentToken().value == "DELETE") {
        ast = parseDeleteStatement();
    }else if (currentToken().value == "UPDATE") {
        ast = parseUpdateStatement();    //  添加 UPDATE 支持 
    }else if (currentToken().value == "CREATE") {
        if (peekNextToken().value == "TABLE") {
            ast = parseCreateTableStatement();
        } else if (peekNextToken().value == "INDEX") {
            ast = parseCreateIndexStatement();
        } else {
            reportError("Unsupported CREATE statement type");
        }
    } else {
        reportError("Unsupported SQL statement");
    }

     // 【修改】处理语句末尾可选的分号
    if (currentToken().value == ";") {
        eat(";");
    }

    // 此时，如果所有内容都被正确解析，下一个 token 应该就是 EOF
    eat(TokenType::END_OF_FILE);
    
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

//  SQL 语句解析实现 

// SELECT 语句解析
std::unique_ptr<ASTNode> Parser::parseSelectStatement() {
    std::cout << "Parsing SELECT statement..." << std::endl;
    std::unique_ptr<SelectStatement> selectStmt = std::make_unique<SelectStatement>();

    // 1. 匹配 SELECT 关键字
    eat("SELECT");

    // 2. 解析选择列表
    if (currentToken().value == "*") {
        selectStmt->selectAll = true;
        eat("*");
    } else {
        selectStmt->columns = parseSelectList();
    }

    // 3. 匹配 FROM 关键字
    eat("FROM");

    // 4. 解析表名
    selectStmt->fromTable = currentToken().value;
    selectStmt->tableTokenIndex = pos_;
    eat(TokenType::IDENTIFIER);

    // 5. 解析可选的 WHERE 子句
    selectStmt->whereClause = parseOptionalWhere();
    
    // 【修改开始】
    // 6. 检查并消耗可选的分号（;）
    if (currentToken().value == ";") {
        advance(); // 消耗分号
    }
    
    // 7. 确保当前位置是文件末尾
    if (currentToken().type != TokenType::END_OF_FILE) {
        reportError("Syntax error: Expected end of file after statement.", pos_);
    }
    // 【修改结束】

    return selectStmt;
}
// CREATE TABLE 语句解析
std::unique_ptr<ASTNode> Parser::parseCreateTableStatement() {
    std::cout << "Parsing CREATE TABLE statement..." << std::endl;
    eat("CREATE");
    eat("TABLE");

    std::string tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);

    eat("("); // 吃掉左括号

    std::vector<ColumnDefinition> columns;
    while (currentToken().value != ")") {
        std::string name = currentToken().value;
        eat(TokenType::IDENTIFIER);

        std::string type = currentToken().value;
        eat(TokenType::KEYWORD);

       // 【修改】新增：处理 CHAR 或 VARCHAR 后的长度
size_t length = 0; // 默认长度为0
// 直接检查类型字符串，不再依赖已移除的 Catalog 类
if (type == "VARCHAR" || type == "CHAR") {
    eat("(");
    if (currentToken().type != TokenType::NUMBER) {
        reportError("Expected a number for CHAR/VARCHAR length.", pos_);
    }
    length = std::stoul(currentToken().value);
    eat(TokenType::NUMBER);
    eat(")");
}
        std::vector<std::string> constraints;
        while (currentToken().value != "," && currentToken().value != ")") {
            // 这里可以处理 PRIMARY KEY, UNIQUE, NOT NULL 等约束
            // 简化处理：将所有后续的关键字作为约束
            constraints.push_back(currentToken().value);
            eat(TokenType::KEYWORD);
        }

        // 【修改】使用新的列表初始化，包含四个参数
        columns.push_back({name, type, length, constraints});

        if (currentToken().value == ",") {
            eat(",");
        }
    }

    eat(")"); // 吃掉右括号

    // 检查 CREATE TABLE 语句末尾的分号
    if (currentToken().value == ";") {
        eat(";");
    }

    auto createTableNode = std::make_unique<CreateTableStatement>();
    createTableNode->tableName = tableName;
    createTableNode->columns = columns;

    return createTableNode;
}

// INSERT 语句解析
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
    node->values = parseValueList();    // 解析值列表
    eat(")");
    eat(";");

    return node;
}

// DELETE 语句解析
std::unique_ptr<ASTNode> Parser::parseDeleteStatement() {
    std::cout << "Parsing DELETE statement..." << std::endl;
    auto node = std::make_unique<DeleteStatement>();

    eat("DELETE");
    eat("FROM");
    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    node->whereClause = parseOptionalWhere();    // 解析可选 WHERE 子句
    eat(";");

    return node;
}

// UPDATE 语句解析
std::unique_ptr<ASTNode> Parser::parseUpdateStatement() {
    std::cout << "Parsing UPDATE statement..." << std::endl;
    auto node = std::make_unique<UpdateStatement>();

    eat("UPDATE");
    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat("SET");
    node->assignments = parseSetClause();    // 解析 SET 子句
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

// 解析可选的 WHERE 子句
std::unique_ptr<WhereClause> Parser::parseOptionalWhere() {
    if (currentToken().value == "WHERE") {
        eat("WHERE");
        auto where = std::make_unique<WhereClause>();
        where->tokenIndex = pos_;
        where->condition = parseCondition(where->tokenIndex);
        return where;
    }
    return nullptr;    // 无 WHERE 子句
}

// 解析条件表达式（简化版）
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
    std::string colName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    
    std::string colType = currentToken().value;
    eat(TokenType::KEYWORD); // 消耗类型关键字
    
    size_t colLength = 0;
    // 检查是否为带长度的类型，例如 VARCHAR(10) 或 CHAR(10)
    if ((colType == "CHAR" || colType == "VARCHAR") && currentToken().value == "(") {
        eat("(");
        if (currentToken().type != TokenType::NUMBER) {
            reportError("Expected a number for column type length", pos_);
        }
        try {
            colLength = std::stoul(currentToken().value);
        } catch (const std::exception& e) {
            reportError("Invalid number for column type length: " + std::string(e.what()), pos_);
        }
        eat(TokenType::NUMBER);
        eat(")");
    }
    
    // 解析约束
    std::vector<std::string> constraints = parseColumnConstraints();
    
    // 创建并返回 ColumnDefinition 结构体
return ColumnDefinition(colName, colType, colLength, constraints);
}

// 解析数据类型 (已在 parseColumnDefinition 中处理，此函数不再需要)
std::string Parser::parseDataType() {
    // 简化：类型是一个标识符
    std::string type = currentToken().value;
    eat(TokenType::IDENTIFIER);
    return type;
}


// 解析列约束（PRIMARY KEY, UNIQUE, NOT NULL）
std::vector<std::string> Parser::parseColumnConstraints() {
    // 简化：可选的约束列表（如 PRIMARY, KEY, NOT, NULL 等）直到遇到 "," 或 ")" 结束
    std::vector<std::string> constraints;
    while (currentToken().value != "," && currentToken().value != ")") {
        constraints.push_back(currentToken().value);
        advance();
    }
    return constraints;
}

// 解析值列表（INSERT）
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

// 解析 SET 子句（UPDATE）
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

//解析create index语句
std::unique_ptr<ASTNode> Parser::parseCreateIndexStatement() {
    std::cout << "Parsing CREATE INDEX statement..." << std::endl;

    // 吃掉 CREATE INDEX
    eat("CREATE");
    eat("INDEX");

    // 索引名
    std::string indexName = currentToken().value;
    eat(TokenType::IDENTIFIER);

    // 关键字 ON
    eat("ON");

    // 表名
    std::string tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);

    // 列名
    eat("(");
    std::string columnName = currentToken().value;
    eat(TokenType::IDENTIFIER);
    eat(")");

    // 结束分号
    eat(";");

    return std::make_unique<CreateIndexStatement>(indexName, tableName, columnName);
}