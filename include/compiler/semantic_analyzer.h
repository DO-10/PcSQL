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

    void visit(CreateIndexStatement* node, const std::vector<Token>& tokens);

private:
    void reportError(const std::string& message, size_t tokenIndex, const std::vector<Token>& tokens);
    void checkValueType(const std::string& value, DataType expectedType);
    void checkColumnExistence(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<Token>& tokens);
    void checkWhereClause(WhereClause* whereClause, const std::string& tableName, const std::vector<Token>& tokens);

    bool isValidDataType(const std::string& type) const;

    // 查询系统表/缓存的辅助函数
    bool tableExists(const std::string& tableName) const;
    TableSchema loadSchemaFromSys(const std::string& tableName) const;
    static std::string to_lower(std::string s);

    // 约束检查相关辅助
    struct ConstraintFlags { bool not_null=false; bool unique=false; bool primary=false; };
    static ConstraintFlags parseConstraintFlags(const std::vector<std::string>& cons);
    void checkConstraintsOnInsert(const std::string& tableName,
                                  const TableSchema& schema,
                                  const std::vector<std::string>& values,
                                  size_t tokenIndex,
                                  const std::vector<Token>& tokens);
    void checkConstraintsOnUpdate(UpdateStatement* node,
                                  const TableSchema& schema,
                                  const std::vector<Token>& tokens);

    // 简单 WHERE 谓词解析与评估（为 UPDATE 唯一性检查准备）
    static bool parse_simple_condition(const std::string& cond, std::string& col, std::string& op, std::string& val);
    static bool compare_typed(DataType type, const std::string& left, const std::string& op, const std::string& right);

private:
    pcsql::StorageEngine* storage_ {nullptr};
};
