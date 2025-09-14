#pragma once
#include <memory>
#include <vector>
#include <string>

#include "compiler/parser.h" // brings in ASTNode, statements, and Token via lexer.h
#include "system_catalog/types.hpp"
#include "storage/storage_engine.hpp"

class SemanticAnalyzer {
public:
    explicit SemanticAnalyzer(pcsql::StorageEngine& storage) : storage_(&storage) {}

    void analyze(const std::unique_ptr<ASTNode>& ast, const std::vector<Token>& tokens);

    // 访问不同类型的 AST 节点
    void visit(SelectStatement* node, const std::vector<Token>& tokens);
    void visit(CreateTableStatement* node, const std::vector<Token>& tokens);
    void visit(InsertStatement* node, const std::vector<Token>& tokens);
    void visit(DeleteStatement* node, const std::vector<Token>& tokens);
    void visit(UpdateStatement* node, const std::vector<Token>& tokens);

private:
    void reportError(const std::string& message, size_t tokenIndex, const std::vector<Token>& tokens);
    void checkValueType(const std::string& value, DataType expectedType);
    void checkColumnExistence(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Token>& tokens);
    void checkWhereClause(WhereClause* whereClause, const std::string& tableName, const std::vector<Token>& tokens);

    // 查询系统表/缓存的辅助函数
    bool tableExists(const std::string& tableName) const;
    TableSchema loadSchemaFromSys(const std::string& tableName) const;
    static std::string to_lower(std::string s);

private:
    pcsql::StorageEngine* storage_ {nullptr};
};
