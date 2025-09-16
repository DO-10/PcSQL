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

struct SelectStatement : public ASTNode {
    std::vector<std::string> columns;  // 选择的列
    std::string fromTable;             // 来源表
    std::unique_ptr<ASTNode> whereClause; // WHERE 子句
    size_t tableTokenIndex;            // 表名在 token 流中的位置

    bool selectAll = false;                // 新增：是否选择了所有列（*）    
};

// 代表 CREATE INDEX 语句的 AST 节点
struct CreateIndexStatement : public ASTNode {
    std::string indexName;
    std::string tableName;
    std::string columnName;
    
    CreateIndexStatement(const std::string& index, const std::string& table, const std::string& column)
        : indexName(index), tableName(table), columnName(column) {}
};

// 修改后的 ColumnDefinition
struct ColumnDefinition {
    std::string name;
    std::string type;
    size_t length = 0; // 新增：用于存储 CHAR 或 VARCHAR 的长度
    std::vector<std::string> constraints;

    // 显式构造函数，用于支持列表初始化
    ColumnDefinition(const std::string& name, const std::string& type, size_t len, const std::vector<std::string>& cons)
        : name(name), type(type), length(len), constraints(cons) {}

    // 默认构造函数
    ColumnDefinition() = default;
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

// DROP TABLE 语句节点
struct DropTableStatement : public ASTNode {
    std::string tableName;
    bool ifExists{false};
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

    void reportError(const std::string& message, size_t position) const;
    // 原始函数保留，以兼容其他调用
    void reportError(const std::string& message) const;
    
    
    // 语句解析函数 (对应顶级规则)
    std::unique_ptr<ASTNode> parseSelectStatement();
    std::unique_ptr<ASTNode> parseCreateTableStatement();
    std::unique_ptr<ASTNode> parseInsertStatement();
    std::unique_ptr<ASTNode> parseDeleteStatement();
    std::unique_ptr<ASTNode> parseUpdateStatement();
    
     //处理 CREATE INDEX 语句的解析逻辑
    std::unique_ptr<ASTNode> parseCreateIndexStatement();

    // 新增：处理 DROP TABLE 语句
    std::unique_ptr<ASTNode> parseDropTableStatement();

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