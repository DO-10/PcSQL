#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>
#include "compiler/ir_generator.h"

// 计划节点基类
// 所有具体的执行计划节点都将继承自此基类
struct PlanNode {
    enum class PlanNodeType {
        CREATE_TABLE,
        CREATE_INDEX,
        INSERT,
        SELECT,
        UPDATE,
        DELETE,
        DROP_TABLE,
    };

    PlanNodeType type;
    virtual ~PlanNode() = default;
};

// CREATE TABLE 语句的执行计划节点
struct CreateTablePlanNode : public PlanNode {
    std::string tableName;
    std::vector<ColumnDefinition> columns;
    CreateTablePlanNode(const std::string& table, const std::vector<ColumnDefinition>& cols)
        : tableName(table), columns(cols) {
        type = PlanNodeType::CREATE_TABLE;
    }
};

// CREATE INDEX 语句的执行计划节点
struct CreateIndexPlanNode : public PlanNode {
    std::string indexName;
    std::string tableName;
    std::string columnName;

    CreateIndexPlanNode(const std::string& index, const std::string& table, const std::string& column)
        : indexName(index), tableName(table), columnName(column) {
        type = PlanNodeType::CREATE_INDEX;
    }
};

// INSERT 语句的执行计划节点
struct InsertPlanNode : public PlanNode {
    std::string tableName;
    std::vector<std::string> values;
    InsertPlanNode(const std::string& table, const std::vector<std::string>& vals)
        : tableName(table), values(vals) {
        type = PlanNodeType::INSERT;
    }
};

// SELECT 语句的执行计划节点
struct SelectPlanNode : public PlanNode {
    std::string tableName;
    std::vector<std::string> columns;
    std::string whereCondition;
    SelectPlanNode(const std::string& table, const std::vector<std::string>& cols, const std::string& where)
        : tableName(table), columns(cols), whereCondition(where) {
        type = PlanNodeType::SELECT;
    }
};

// UPDATE 语句的执行计划节点
struct UpdatePlanNode : public PlanNode {
    std::string tableName;
    std::string whereCondition;
    std::map<std::string, std::string> assignments;
    UpdatePlanNode(const std::string& table, const std::string& where, const std::map<std::string, std::string>& assignmentsMap)
        : tableName(table), whereCondition(where), assignments(assignmentsMap) {
        type = PlanNodeType::UPDATE;
    }
};

// DELETE 语句的执行计划节点
struct DeletePlanNode : public PlanNode {
    std::string tableName;
    std::string whereCondition;
    DeletePlanNode(const std::string& table, const std::string& where)
        : tableName(table), whereCondition(where) {
        type = PlanNodeType::DELETE;
    }
};

// DROP TABLE 语句的执行计划节点
struct DropTablePlanNode : public PlanNode {
    std::string tableName;
    bool ifExists;
    DropTablePlanNode(const std::string& table, bool if_exists)
        : tableName(table), ifExists(if_exists) {
        type = PlanNodeType::DROP_TABLE;
    }
};

// 执行计划生成器类
class ExecutionPlanGenerator {
public:
    std::unique_ptr<PlanNode> generate(const std::vector<Quadruplet>& ir);
};