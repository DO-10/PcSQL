#include "execution_plan/plan_generator.h"
#include <iostream>

std::unique_ptr<PlanNode> PlanGenerator::generate(const std::unique_ptr<ASTNode>& ast) {
    if (!ast) {
        throw std::runtime_error("AST is null");
    }
    
    if (auto select = dynamic_cast<SelectStatement*>(ast.get())) {
        return visit(select);
    } else if (auto createTable = dynamic_cast<CreateTableStatement*>(ast.get())) {
        return visit(createTable);
    } else if (auto insert = dynamic_cast<InsertStatement*>(ast.get())) {
        return visit(insert);
    } else if (auto del = dynamic_cast<DeleteStatement*>(ast.get())) {
        return visit(del);
    } else if (auto update = dynamic_cast<UpdateStatement*>(ast.get())) {
        return visit(update);
    } else if (auto createIndex = dynamic_cast<CreateIndexStatement*>(ast.get())) {
        return visit(createIndex);
    }
    
    throw std::runtime_error("Unsupported AST node type");
}

std::unique_ptr<PlanNode> PlanGenerator::visit(const SelectStatement* node) {
    // 创建扫描节点
    auto scan = std::make_unique<SeqScanNode>(node->fromTable);
    
    // 添加过滤节点（如果有WHERE子句）
    std::unique_ptr<PlanNode> topNode = std::move(scan);
    if (node->whereClause) {
        topNode = std::make_unique<FilterNode>(node->whereClause->condition, std::move(topNode));
    }
    
    // 添加投影节点
    return std::make_unique<ProjectNode>(node->columns, node->selectAll, std::move(topNode));
}

std::unique_ptr<PlanNode> PlanGenerator::visit(const CreateTableStatement* node) {
    std::vector<std::tuple<std::string, std::string, size_t>> columns;
    for (const auto& col : node->columns) {
        columns.emplace_back(col.name, col.type, col.length);
    }
    return std::make_unique<CreateTableNode>(node->tableName, columns);
}

std::unique_ptr<PlanNode> PlanGenerator::visit(const InsertStatement* node) {
    return std::make_unique<InsertNode>(node->tableName, node->values);
}

std::unique_ptr<PlanNode> PlanGenerator::visit(const DeleteStatement* node) {
    // 创建扫描节点
    auto scan = std::make_unique<SeqScanNode>(node->tableName);
    
    // 添加过滤节点（如果有WHERE子句）
    std::unique_ptr<PlanNode> topNode = std::move(scan);
    if (node->whereClause) {
        topNode = std::make_unique<FilterNode>(node->whereClause->condition, std::move(topNode));
    }
    
    // 添加删除节点
    return std::make_unique<DeleteNode>(node->tableName, std::move(topNode));
}

std::unique_ptr<PlanNode> PlanGenerator::visit(const UpdateStatement* node) {
    // 创建扫描节点
    auto scan = std::make_unique<SeqScanNode>(node->tableName);
    
    // 添加过滤节点（如果有WHERE子句）
    std::unique_ptr<PlanNode> topNode = std::move(scan);
    if (node->whereClause) {
        topNode = std::make_unique<FilterNode>(node->whereClause->condition, std::move(topNode));
    }
    
    // 添加更新节点
    return std::make_unique<UpdateNode>(node->tableName, node->assignments, std::move(topNode));
}

std::unique_ptr<PlanNode> PlanGenerator::visit(const CreateIndexStatement* node) {
    return std::make_unique<CreateIndexNode>(node->indexName, node->tableName, node->columnName);
}

