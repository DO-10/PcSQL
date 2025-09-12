#ifndef COMPILER_PARSER_PARSER_H
#define COMPILER_PARSER_PARSER_H

#include <string>
#include <vector>
#include <memory>
#include <map>
#include "compiler/lexer.h"

// 抽象语法树节点基类
struct ASTNode {
    virtual ~ASTNode() = default;
};

// SELECT 语句节点
struct SelectStatement : public ASTNode {
    std::vector<std::string> columns;
    std::string fromTable;
    std::unique_ptr<ASTNode> whereClause;
    size_t tableTokenIndex;
};

// CREATE TABLE 语句节点
struct ColumnDefinition {
    std::string name;
    std::string type;
    std::vector<std::string> constraints;
};

struct CreateTableStatement : public ASTNode {
    std::string tableName;
    std::vector<ColumnDefinition> columns;
    size_t tableTokenIndex;
};

// INSERT 语句节点
struct InsertStatement : public ASTNode {
    std::string tableName;
    std::vector<std::string> values;
    size_t tableTokenIndex;
};

// DELETE 语句节点
struct DeleteStatement : public ASTNode {
    std::string tableName;
    std::unique_ptr<ASTNode> whereClause;
    size_t tableTokenIndex;
};

// UPDATE 语句节点
struct UpdateStatement : public ASTNode {
    std::string tableName;
    std::map<std::string, std::string> assignments;
    std::unique_ptr<ASTNode> whereClause;
    size_t tableTokenIndex;
};

// WHERE 子句节点
struct WhereClause : public ASTNode {
    std::string condition;
    size_t tokenIndex;
};

// Parser 类声明
class Parser {
public:
    Parser(const std::vector<Token>& tokens);
    
    std::unique_ptr<ASTNode> parse();

private:
    const std::vector<Token>& tokens_;
    size_t pos_;
    
    // 辅助方法
    const Token& currentToken() const;
    const Token& peekNextToken() const;
    void advance();
    void eat(const std::string& expectedValue);
    void eat(TokenType expectedType);
    void reportError(const std::string& message) const;
    
    // 语句解析函数 (对应顶级规则)
    std::unique_ptr<ASTNode> parseSelectStatement();
    std::unique_ptr<ASTNode> parseCreateTableStatement();
    std::unique_ptr<ASTNode> parseInsertStatement();
    std::unique_ptr<ASTNode> parseDeleteStatement();
    std::unique_ptr<ASTNode> parseUpdateStatement();

    // 子句解析函数
    std::vector<std::string> parseSelectList();
    std::unique_ptr<WhereClause> parseOptionalWhere();
    std::string parseCondition(size_t& tokenIndex);
    std::vector<ColumnDefinition> parseColumnDefinitionList();
    ColumnDefinition parseColumnDefinition();
    std::string parseDataType();
    std::vector<std::string> parseColumnConstraints();
    std::vector<std::string> parseValueList();
    std::map<std::string, std::string> parseSetClause();
};

#endif // COMPILER_PARSER_PARSER_H