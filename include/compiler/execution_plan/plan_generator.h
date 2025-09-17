#pragma once
#include <memory>
#include <vector>
#include <string>
#include "compiler/parser.h"
#include "storage/storage_engine.hpp"

// 执行计划节点基类
class PlanNode {
public:
    enum class NodeType {
        CreateTable,
        CreateIndex,
        Insert,
        SeqScan,
        Filter,
        Project,
        Delete,
        Update,
        DropTable
    };

    explicit PlanNode(NodeType type) : type_(type) {}
    virtual ~PlanNode() = default;
    
    NodeType getType() const { return type_; }
    void addChild(std::shared_ptr<PlanNode> child) { children_.push_back(child); }
    const std::vector<std::shared_ptr<PlanNode>>& getChildren() const { return children_; }

private:
    NodeType type_;
    std::vector<std::shared_ptr<PlanNode>> children_;
};

// 具体算子实现
class CreateTableNode : public PlanNode {
public:
    CreateTableNode(const std::string& tableName, 
                    const std::vector<ColumnDefinition>& columns,
                    size_t tokenIndex)
        : PlanNode(NodeType::CreateTable), 
          tableName_(tableName), 
          columns_(columns),
          tokenIndex_(tokenIndex) {}
    
    const std::string& getTableName() const { return tableName_; }
    const std::vector<ColumnDefinition>& getColumns() const { return columns_; }
    size_t getTokenIndex() const { return tokenIndex_; }

private:
    std::string tableName_;
    std::vector<ColumnDefinition> columns_;
    size_t tokenIndex_;
};

class CreateIndexNode : public PlanNode {
public:
    CreateIndexNode(const std::string& indexName,
                    const std::string& tableName,
                    const std::string& columnName)
        : PlanNode(NodeType::CreateIndex),
          indexName_(indexName),
          tableName_(tableName),
          columnName_(columnName) {}
    
    const std::string& getIndexName() const { return indexName_; }
    const std::string& getTableName() const { return tableName_; }
    const std::string& getColumnName() const { return columnName_; }

private:
    std::string indexName_;
    std::string tableName_;
    std::string columnName_;
};

class InsertNode : public PlanNode {
public:
    InsertNode(const std::string& tableName,
               const std::vector<std::string>& values,
               size_t tokenIndex)
        : PlanNode(NodeType::Insert),
          tableName_(tableName),
          values_(values),
          tokenIndex_(tokenIndex) {}
    
    const std::string& getTableName() const { return tableName_; }
    const std::vector<std::string>& getValues() const { return values_; }
    size_t getTokenIndex() const { return tokenIndex_; }

private:
    std::string tableName_;
    std::vector<std::string> values_;
    size_t tokenIndex_;
};

class SeqScanNode : public PlanNode {
public:
    SeqScanNode(const std::string& tableName,
                const std::vector<std::string>& columns,
                const std::string& filterCondition,
                size_t tokenIndex)
        : PlanNode(NodeType::SeqScan),
          tableName_(tableName),
          columns_(columns),
          filterCondition_(filterCondition),
          tokenIndex_(tokenIndex) {}
    
    const std::string& getTableName() const { return tableName_; }
    const std::vector<std::string>& getColumns() const { return columns_; }
    const std::string& getFilterCondition() const { return filterCondition_; }
    size_t getTokenIndex() const { return tokenIndex_; }

private:
    std::string tableName_;
    std::vector<std::string> columns_;
    std::string filterCondition_;
    size_t tokenIndex_;
};

class FilterNode : public PlanNode {
public:
    explicit FilterNode(const std::string& condition)
        : PlanNode(NodeType::Filter), condition_(condition) {}
    
    const std::string& getCondition() const { return condition_; }

private:
    std::string condition_;
};

class ProjectNode : public PlanNode {
public:
    explicit ProjectNode(const std::vector<std::string>& columns)
        : PlanNode(NodeType::Project), columns_(columns) {}
    
    const std::vector<std::string>& getColumns() const { return columns_; }

private:
    std::vector<std::string> columns_;
};

class DeleteNode : public PlanNode {
public:
    DeleteNode(const std::string& tableName,
               const std::string& condition,
               size_t tokenIndex)
        : PlanNode(NodeType::Delete),
          tableName_(tableName),
          condition_(condition),
          tokenIndex_(tokenIndex) {}
    
    const std::string& getTableName() const { return tableName_; }
    const std::string& getCondition() const { return condition_; }
    size_t getTokenIndex() const { return tokenIndex_; }

private:
    std::string tableName_;
    std::string condition_;
    size_t tokenIndex_;
};

class UpdateNode : public PlanNode {
public:
    UpdateNode(const std::string& tableName,
               const std::map<std::string, std::string>& assignments,
               const std::string& condition,
               size_t tokenIndex)
        : PlanNode(NodeType::Update),
          tableName_(tableName),
          assignments_(assignments),
          condition_(condition),
          tokenIndex_(tokenIndex) {}
    
    const std::string& getTableName() const { return tableName_; }
    const std::map<std::string, std::string>& getAssignments() const { return assignments_; }
    const std::string& getCondition() const { return condition_; }
    size_t getTokenIndex() const { return tokenIndex_; }

private:
    std::string tableName_;
    std::map<std::string, std::string> assignments_;
    std::string condition_;
    size_t tokenIndex_;
};

class DropTableNode : public PlanNode {
public:
    DropTableNode(const std::string& tableName, 
                  bool ifExists,
                  size_t tokenIndex)
        : PlanNode(NodeType::DropTable),
          tableName_(tableName),
          ifExists_(ifExists),
          tokenIndex_(tokenIndex) {}
    
    const std::string& getTableName() const { return tableName_; }
    bool ifExists() const { return ifExists_; }
    size_t getTokenIndex() const { return tokenIndex_; }

private:
    std::string tableName_;
    bool ifExists_;
    size_t tokenIndex_;
};

// 执行计划生成器
class PlanGenerator {
public:
    explicit PlanGenerator(pcsql::StorageEngine& storage) : storage_(&storage) {}
    
    std::shared_ptr<PlanNode> generate(const std::unique_ptr<ASTNode>& ast);

private:
    std::shared_ptr<PlanNode> generateSelect(SelectStatement* node);
    std::shared_ptr<PlanNode> generateCreateTable(CreateTableStatement* node);
    std::shared_ptr<PlanNode> generateInsert(InsertStatement* node);
    std::shared_ptr<PlanNode> generateDelete(DeleteStatement* node);
    std::shared_ptr<PlanNode> generateUpdate(UpdateStatement* node);
    std::shared_ptr<PlanNode> generateCreateIndex(CreateIndexStatement* node);
    std::shared_ptr<PlanNode> generateDropTable(DropTableStatement* node);

    void reportError(const std::string& message, size_t tokenIndex);

    pcsql::StorageEngine* storage_;
};

