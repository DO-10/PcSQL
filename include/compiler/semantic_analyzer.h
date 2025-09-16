#pragma once
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <map>

#include "compiler/parser.h" // 引入 ASTNode, statements, and Token
#include "storage/storage_engine.hpp"
#include "system_catalog/types.hpp"


// 用于在语义分析过程中传递上下文
struct SemanticContext {
    // 存储所有参与查询的表名及其模式
    std::unordered_map<std::string, TableSchema> schemas;
    
    // 存储所有可用列的完整名称（如 users.id）
    std::unordered_map<std::string, std::string> full_column_names;
    
    // 存储存在歧义的列名（用于快速查找）
    std::unordered_set<std::string> ambiguous_columns;
};

class SemanticAnalyzer {
public:
    explicit SemanticAnalyzer(pcsql::StorageEngine& storage) : storage_(&storage) {}

    void analyze(const std::unique_ptr<ASTNode>& ast, const std::vector<Token>& tokens);
    
private:
    // 访问不同类型的 AST 节点
    void visit(SelectStatement* node, const std::vector<Token>& tokens);
    void visit(CreateTableStatement* node, const std::vector<Token>& tokens);
    void visit(InsertStatement* node, const std::vector<Token>& tokens);
    void visit(DeleteStatement* node, const std::vector<Token>& tokens);
    void visit(UpdateStatement* node, const std::vector<Token>& tokens);
    void visit(CreateIndexStatement* node, const std::vector<Token>& tokens);

    // 递归验证表达式
    void validateExpression(const ASTNode* expr, 
                            const SemanticContext& ctx, 
                            const std::vector<Token>& tokens) const;

    // 验证 SELECT 列表中的列
    void validateSelectColumns(const std::vector<std::string>& columns, 
                               const SemanticContext& ctx, 
                               const std::vector<Token>& tokens) const;

    // 辅助函数
    void reportError(const std::string& message, size_t tokenIndex, const std::vector<Token>& tokens) const;
    void reportErrorWithToken(const std::string& message, const Token& token) const;
    size_t findTokenIndex(const std::string& value, const std::vector<Token>& tokens, size_t start_pos = 0) const;

    // 查询系统表/缓存的辅助函数
    bool tableExists(const std::string& tableName) const;
    TableSchema loadSchemaFromSys(const std::string& tableName) const;
    static std::string to_lower(std::string s);
    static std::string tokenTypeToString(TokenType type);
    static bool is_null_literal(const std::string& v);

    // 新增: 验证数据类型
    bool isValidDataType(const std::string& type) const;

    // 约束检查相关辅助
    struct ConstraintFlags { bool not_null=false; bool unique=false; bool primary=false; };
    static ConstraintFlags parseConstraintFlags(const std::vector<std::string>& cons);
    void checkConstraintsOnInsert(const std::string& tableName,
                                  const TableSchema& schema,
                                  const std::vector<std::string>& values,
                                  const std::vector<Token>& tokens);
    void checkConstraintsOnUpdate(UpdateStatement* node,
                                  const TableSchema& schema,
                                  const std::vector<Token>& tokens);
    
    // 简单 WHERE 谓词解析与评估（为 UPDATE 唯一性检查准备）
    static bool parse_simple_condition(const std::string& cond, std::string& col, std::string& op, std::string& val);
    static bool compare_typed(DataType type, const std::string& left, const std::string& op, const std::string& right);
    
    private:
    pcsql::StorageEngine* storage_;
};