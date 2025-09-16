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
        // Added: DropTableStatement debug print
        if (auto drop = dynamic_cast<const DropTableStatement*>(node)) {
            std::cout << indent(level) << "DropTableStatement" << std::endl;
            std::cout << indent(level+1) << "table: " << drop->tableName << std::endl;
            std::cout << indent(level+1) << "ifExists: " << (drop->ifExists ? "true" : "false") << std::endl;
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
        throw std::runtime_error("[语法, (line 0, column 0), Lexical analysis did not return EOF token]");
    }
}

// 获取当前 Token
const Token& Parser::currentToken() const {
    // 检查是否超出 Token 范围
    if (pos_ >= tokens_.size()) {
        throw std::runtime_error("[语法, (line 0, column 0), Unexpected end of input during parsing]");
    }
    return tokens_[pos_];
}

// 查看下一个 Token（不消耗）
const Token& Parser::peekNextToken() const {
    if (pos_ + 1 >= tokens_.size()) {
        throw std::runtime_error("[语法, (line 0, column 0), Cannot peek next token, input has ended]");
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
        std::stringstream ss;
        ss << "Expected '" << expectedValue << "' but got '" << currentToken().value << "'";
        reportError(ss.str(), pos_);
    }
    advance();
}

// 匹配并消耗特定类型的 Token
void Parser::eat(TokenType expectedType) {
    if (currentToken().type != expectedType) {
        std::stringstream ss;
        ss << "Expected token type " << static_cast<int>(expectedType)
           << " but got type " << static_cast<int>(currentToken().type)
           << " ('" << currentToken().value << "')";
        reportError(ss.str(), pos_);
    }
    advance();
}

// 报告错误信息（包含位置）
// 重载的 reportError 函数
void Parser::reportError(const std::string& message, size_t position) const {
    if (position < tokens_.size()) {
        const Token& errorToken = tokens_[position];
        std::stringstream ss;
        ss << "[语法, (line " << errorToken.line << ", column " << errorToken.column << "), " << message << "]";
        throw std::runtime_error(ss.str());
    } else {
        std::stringstream ss;
        ss << "[语法, (line 0, column 0), " << message << "]";
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
    } else if (currentToken().value == "DROP") {
        if (peekNextToken().value == "TABLE") {
            ast = parseDropTableStatement();
        } else {
            reportError("Unsupported DROP statement type");
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

    // 记录表名 token 位置，便于后续错误定位
    size_t tableTokIdx = pos_;
    std::string tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);

    eat("("); // 吃掉左括号

    // 支持列定义与表级约束（PRIMARY KEY/UNIQUE (col)）混合出现
    std::vector<ColumnDefinition> columns;
    auto applyConstraintToColumn = [&](const std::string& targetCol, const std::vector<std::string>& consTokens) {
        // 查找列
        int idx = -1;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == targetCol) { idx = static_cast<int>(i); break; }
        }
        if (idx < 0) {
            reportError("Referenced column '" + targetCol + "' in table-level constraint not found", pos_);
        }
        // 直接把约束 token 追加到该列
        columns[idx].constraints.insert(columns[idx].constraints.end(), consTokens.begin(), consTokens.end());
    };

    while (currentToken().value != ")") {
        bool handled_table_level = false;
        // 仅当匹配到 PRIMARY KEY ( ... ) 模式时，才按表级约束处理
        if (currentToken().value == "PRIMARY") {
            const Token& t1 = peekNextToken();
            if (t1.value == "KEY") {
                // 检查再下一个是否是 "("
                if (pos_ + 2 < tokens_.size() && tokens_[pos_ + 2].value == "(") {
                    eat("PRIMARY");
                    eat("KEY");
                    eat("(");
                    std::string colName = currentToken().value;
                    eat(TokenType::IDENTIFIER);
                    eat(")");
                    applyConstraintToColumn(colName, std::vector<std::string>{"PRIMARY", "KEY"});
                    handled_table_level = true;
                }
            }
        }
        // 仅当匹配到 UNIQUE ( ... ) 模式时，才按表级约束处理
        if (!handled_table_level && currentToken().value == "UNIQUE") {
            // 下一个必须是 "("
            if (peekNextToken().value == "(") {
                eat("UNIQUE");
                eat("(");
                std::string colName = currentToken().value;
                eat(TokenType::IDENTIFIER);
                eat(")");
                applyConstraintToColumn(colName, std::vector<std::string>{"UNIQUE"});
                handled_table_level = true;
            }
        }

        if (!handled_table_level) {
            // 列定义（包含列级 PRIMARY KEY/UNIQUE/NOT NULL/DEFAULT 等）
            columns.push_back(parseColumnDefinition());
        }

        if (currentToken().value == ",") {
            eat(",");
        }
    }

    eat(")"); // 吃掉右括号

    // 检查 CREATE TABLE 语句末尾的分号（可选，parse() 也会处理一次）
    if (currentToken().value == ";") {
        eat(";");
    }

    auto createTableNode = std::make_unique<CreateTableStatement>();
    createTableNode->tableName = tableName;
    createTableNode->columns = columns;
    createTableNode->tableTokenIndex = tableTokIdx;

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

// 解析条件表达式（改进版：保留字符串字面量引号，便于执行阶段稳健解析）
std::string Parser::parseCondition(size_t& tokenIndex) {
    std::ostringstream cond;
    bool first = true;
    while (currentToken().value != ";") {
        const Token& t = currentToken();
        if (!first) cond << " ";
        if (t.type == TokenType::STRING) {
            // 词法器已去掉引号，这里补回引号以避免值中包含空格/运算符时解析出错
            cond << "'" << t.value << "'";
        } else {
            cond << t.value;
        }
        tokenIndex = pos_;
        advance();
        first = false;
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
    // 放宽类型 token 的限制：既可以是关键字也可以是标识符（如 TIMESTAMP）
    if (currentToken().type == TokenType::KEYWORD || currentToken().type == TokenType::IDENTIFIER) {
        advance();
    } else {
        reportError("Expected a type name (keyword or identifier)", pos_);
    }
    
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
    
    // 解析约束（直到遇到","或")"），支持 KEYWORD/IDENTIFIER/NUMBER/STRING 等 token
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


// 解析列约束（PRIMARY KEY, UNIQUE, NOT NULL, DEFAULT, AUTO_INCREMENT 等原样收集）
std::vector<std::string> Parser::parseColumnConstraints() {
    std::vector<std::string> constraints;
    while (currentToken().value != "," && currentToken().value != ")") {
        // 接受关键字、标识符、数字、字符串以及部分运算符（例如 CURRENT_TIMESTAMP 可能被识别为 IDENTIFIER）
        if (currentToken().type == TokenType::KEYWORD ||
            currentToken().type == TokenType::IDENTIFIER ||
            currentToken().type == TokenType::NUMBER ||
            currentToken().type == TokenType::STRING) {
            constraints.push_back(currentToken().value);
            advance();
        } else {
            // 遇到无法识别为约束的 token，报错以提示
            reportError("Unexpected token in column constraints: '" + currentToken().value + "'", pos_);
        }
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
// 在文件末尾附近添加 DROP TABLE 解析实现（与其它语句实现相邻）
std::unique_ptr<ASTNode> Parser::parseDropTableStatement() {
    std::cout << "Parsing DROP TABLE statement..." << std::endl;
    auto node = std::make_unique<DropTableStatement>();

    eat("DROP");
    eat("TABLE");

    // 可选的 IF EXISTS
    if (currentToken().value == "IF" && peekNextToken().value == "EXISTS") {
        eat("IF");
        eat("EXISTS");
        node->ifExists = true;
    }

    node->tableTokenIndex = pos_;
    node->tableName = currentToken().value;
    eat(TokenType::IDENTIFIER);

    // 结束分号
    eat(";");

    return node;
}