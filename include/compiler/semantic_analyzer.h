#ifndef COMPILER_SEMANTIC_SEMANTIC_ANALYZER_H
#define COMPILER_SEMANTIC_SEMANTIC_ANALYZER_H

#include <memory>
#include <string>
#include <stdexcept>
#include "compiler/catalog.h"
#include "compiler/parser.h"

// 语义分析器类
class SemanticAnalyzer {
public:
    SemanticAnalyzer(Catalog& catalog);
    void analyze(const std::unique_ptr<ASTNode>& ast, const std::vector<Token>& tokens);

private:
    Catalog& catalog_;
    
    // 访问者模式：访问不同类型的 AST 节点
    void visit(SelectStatement* node, const std::vector<Token>& tokens);
    void visit(CreateTableStatement* node, const std::vector<Token>& tokens);
    void visit(InsertStatement* node, const std::vector<Token>& tokens);
    void visit(DeleteStatement* node, const std::vector<Token>& tokens);
    void visit(UpdateStatement* node, const std::vector<Token>& tokens);

     // 新增：访问 CREATE INDEX 语句
    void visit(CreateIndexStatement* node, const std::vector<Token>& tokens);

    // 辅助检查函数
    void checkValueType(const std::string& value, DataType expectedType);
    void checkColumnExistence(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Token>& tokens);
    void checkWhereClause(WhereClause* whereClause, const std::string& tableName, const std::vector<Token>& tokens);
    
    // 报告错误
    void reportError(const std::string& message, size_t tokenIndex, const std::vector<Token>& tokens);
};

#endif // COMPILER_SEMANTIC_SEMANTIC_ANALYZER_H
