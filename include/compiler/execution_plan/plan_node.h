#pragma once
#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// 执行计划节点基类
class PlanNode {
public:
    virtual ~PlanNode() = default;
    virtual std::string toString(int indent = 0) const = 0;
    virtual json toJSON() const = 0;
    virtual std::string toSExpression() const = 0;
};

// 具体算子节点
class CreateTableNode : public PlanNode {
public:
    std::string tableName;
    std::vector<std::tuple<std::string, std::string, size_t>> columns; // name, type, length
    
    CreateTableNode(const std::string& name, const std::vector<std::tuple<std::string, std::string, size_t>>& cols)
        : tableName(name), columns(cols) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class InsertNode : public PlanNode {
public:
    std::string tableName;
    std::vector<std::string> values;
    
    InsertNode(const std::string& name, const std::vector<std::string>& vals)
        : tableName(name), values(vals) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class SeqScanNode : public PlanNode {
public:
    std::string tableName;
    std::unique_ptr<PlanNode> child;
    
    SeqScanNode(const std::string& name, std::unique_ptr<PlanNode> child = nullptr)
        : tableName(name), child(std::move(child)) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class FilterNode : public PlanNode {
public:
    std::string condition;
    std::unique_ptr<PlanNode> child;
    
    FilterNode(const std::string& cond, std::unique_ptr<PlanNode> child)
        : condition(cond), child(std::move(child)) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class ProjectNode : public PlanNode {
public:
    std::vector<std::string> columns;
    bool selectAll;
    std::unique_ptr<PlanNode> child;
    
    ProjectNode(const std::vector<std::string>& cols, bool all, std::unique_ptr<PlanNode> child)
        : columns(cols), selectAll(all), child(std::move(child)) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class DeleteNode : public PlanNode {
public:
    std::string tableName;
    std::unique_ptr<PlanNode> child;
    
    DeleteNode(const std::string& name, std::unique_ptr<PlanNode> child)
        : tableName(name), child(std::move(child)) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class UpdateNode : public PlanNode {
public:
    std::string tableName;
    std::vector<std::pair<std::string, std::string>> assignments;
    std::unique_ptr<PlanNode> child;
    
    UpdateNode(const std::string& name, const std::vector<std::pair<std::string, std::string>>& assigns, std::unique_ptr<PlanNode> child)
        : tableName(name), assignments(assigns), child(std::move(child)) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};

class CreateIndexNode : public PlanNode {
public:
    std::string indexName;
    std::string tableName;
    std::string columnName;
    
    CreateIndexNode(const std::string& idxName, const std::string& tblName, const std::string& colName)
        : indexName(idxName), tableName(tblName), columnName(colName) {}
    
    std::string toString(int indent = 0) const override;
    json toJSON() const override;
    std::string toSExpression() const override;
};
