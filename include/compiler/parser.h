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

// 新增：表示二元表达式的 AST 节点
struct BinaryExpression : public ASTNode {
    std::string op;
    std::unique_ptr<ASTNode> left;
    std::unique_ptr<ASTNode> right;
};

// 新增：表示列引用的 AST 节点，如 `table.column` 或 `column`
struct ColumnReference : public ASTNode {
    std::string tableName;
    std::string columnName;
};

// 新增：表示字面量的 AST 节点，如 `123`, `'abc'`, `1.23`
struct Literal : public ASTNode {
    std::string value;
    std::string type; // INT, DOUBLE, STRING
};

// 新增：连接类型
enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL
};

// 新增：表示 JOIN 子句的 AST 节点
struct JoinClause : public ASTNode {
    JoinType type; // 新增：连接类型
    std::string rightTable;
    std::unique_ptr<ASTNode> onCondition;
    std::unique_ptr<JoinClause> nextJoin; // 用于支持多重 JOIN
};

// ---------------------- 现有 AST 节点 ----------------------

struct ColumnDefinition {
    std::string name;
    std::string type;
    std::vector<std::string> constraints;
};

struct SelectStatement : public ASTNode {
    std::vector<std::string> columns;
    std::string fromTable;
    std::unique_ptr<JoinClause> joinClause;
    std::unique_ptr<ASTNode> whereCondition;
};

struct CreateTableStatement : public ASTNode {
    std::string tableName;
    std::vector<ColumnDefinition> columnDefinitions;
};

struct InsertStatement : public ASTNode {
    std::string tableName;
    std::vector<std::string> values;
};

struct DeleteStatement : public ASTNode {
    std::string tableName;
    std::unique_ptr<ASTNode> whereCondition;
};

struct UpdateStatement : public ASTNode {
    std::string tableName;
    std::map<std::string, std::string> assignments;
    std::unique_ptr<ASTNode> whereCondition;
};

struct CreateIndexStatement : public ASTNode {
    std::string indexName;
    std::string tableName;
    std::string columnName;
};

// ---------------------- Parser 类 ----------------------

class Parser {
public:
    Parser(const std::vector<Token>& tokens);
    std::unique_ptr<ASTNode> parse();

private:
    const std::vector<Token>& tokens_;
    size_t current_token_index_;

    // 辅助函数
    void advance();
    void eat(const std::string& value);
    void eat(TokenType type);
    const Token& currentToken() const;
    const Token& peekToken() const;
    bool isAtEnd() const;

    // 表达式解析器方法
    std::unique_ptr<ASTNode> parseExpression();
    std::unique_ptr<ASTNode> parseAndExpression();
    std::unique_ptr<ASTNode> parseComparisonExpression();
    std::unique_ptr<ASTNode> parseTerm();
    std::unique_ptr<ASTNode> parseFactor();
    std::unique_ptr<ASTNode> parsePrimary();

    // 语句解析方法
    std::unique_ptr<ASTNode> parseSelectStatement();
    std::unique_ptr<ASTNode> parseCreateTableStatement();
    std::unique_ptr<ASTNode> parseInsertStatement();
    std::unique_ptr<ASTNode> parseDeleteStatement();
    std::unique_ptr<ASTNode> parseUpdateStatement();
    std::unique_ptr<ASTNode> parseCreateIndexStatement();

    // 子句解析方法
    std::vector<std::string> parseSelectList();
    std::unique_ptr<JoinClause> parseOptionalJoin();
    std::unique_ptr<ASTNode> parseOptionalWhere();
    std::vector<ColumnDefinition> parseColumnDefinitionList();
    ColumnDefinition parseColumnDefinition();
    std::vector<std::string> parseInsertValues();
    std::map<std::string, std::string> parseSetClause();
};

#endif // COMPILER_PARSER_PARSER_H