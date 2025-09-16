#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include "compiler/ir_generator.h"

// 计划节点基类
// 所有具体的执行计划节点都将继承自此基类
struct PlanNode {
    enum class PlanNodeType {
        CREATE_TABLE,
        CREATE_INDEX,
        INSERT,
        SELECT,      // 兼容旧类型（仍保留）
        UPDATE,
        DELETE,
        DROP_TABLE,
        // 新增三种逻辑算子
        SEQ_SCAN,
        FILTER,
        PROJECT,
    };

    PlanNodeType type;
    virtual ~PlanNode() = default;

    // 统一序列化接口：JSON 和 S 表达式
    virtual std::string to_json() const = 0;
    virtual std::string to_sexpr() const = 0;
};

// 工具：将列向量序列化为 JSON 数组字符串
inline std::string json_array(const std::vector<std::string>& arr) {
    std::ostringstream os; os << "[";
    for (size_t i = 0; i < arr.size(); ++i) {
        os << "\"" << arr[i] << "\"";
        if (i + 1 < arr.size()) os << ",";
    }
    os << "]"; return os.str();
}

// CREATE TABLE 语句的执行计划节点
struct CreateTablePlanNode : public PlanNode {
    std::string tableName;
    std::vector<ColumnDefinition> columns;
    CreateTablePlanNode(const std::string& table, const std::vector<ColumnDefinition>& cols)
        : tableName(table), columns(cols) {
        type = PlanNodeType::CREATE_TABLE;
    }
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"CreateTable\",\"table\":\"" << tableName << "\",\"columns\":[";
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& c = columns[i];
            os << "{\"name\":\"" << c.name << "\",\"type\":\"" << c.type << "\",\"length\":" << c.length << "}";
            if (i + 1 < columns.size()) os << ",";
        }
        os << "]}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(CreateTable " << tableName << " (";
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& c = columns[i];
            os << "(col " << c.name << " " << c.type << " " << c.length << ")";
            if (i + 1 < columns.size()) os << " ";
        }
        os << "))"; return os.str();
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
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"CreateIndex\",\"index\":\"" << indexName
                                   << "\",\"table\":\"" << tableName << "\",\"column\":\"" << columnName << "\"}";
        return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(CreateIndex " << indexName << " " << tableName << " " << columnName << ")"; return os.str();
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
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"Insert\",\"table\":\"" << tableName << "\",\"values\":" << json_array(values) << "}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(Insert " << tableName << " (";
        for (size_t i = 0; i < values.size(); ++i) { os << values[i]; if (i + 1 < values.size()) os << " "; }
        os << "))"; return os.str();
    }
};

// 兼容旧的 SELECT 计划节点（保留）
struct SelectPlanNode : public PlanNode {
    std::string tableName;
    std::vector<std::string> columns;
    std::string whereCondition;
    SelectPlanNode(const std::string& table, const std::vector<std::string>& cols, const std::string& where)
        : tableName(table), columns(cols), whereCondition(where) {
        type = PlanNodeType::SELECT;
    }
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"Select(legacy)\",\"table\":\"" << tableName
                                   << "\",\"columns\":" << json_array(columns) << ",\"where\":\"" << whereCondition << "\"}";
        return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(Select-legacy " << tableName << " (";
        for (size_t i = 0; i < columns.size(); ++i) { os << columns[i]; if (i + 1 < columns.size()) os << " "; }
        os << ") " << whereCondition << ")"; return os.str();
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
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"Update\",\"table\":\"" << tableName << "\",\"where\":\"" << whereCondition << "\",\"assignments\":{";
        size_t i = 0; for (const auto& kv : assignments) { os << "\"" << kv.first << "\":\"" << kv.second << "\""; if (++i < assignments.size()) os << ","; }
        os << "}}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(Update " << tableName << " (";
        size_t i = 0; for (const auto& kv : assignments) { os << "(= " << kv.first << " " << kv.second << ")"; if (++i < assignments.size()) os << " "; }
        os << ") " << whereCondition << ")"; return os.str();
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
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"Delete\",\"table\":\"" << tableName << "\",\"where\":\"" << whereCondition << "\"}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(Delete " << tableName << " " << whereCondition << ")"; return os.str();
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
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"DropTable\",\"table\":\"" << tableName << "\",\"ifExists\":" << (ifExists?"true":"false") << "}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(DropTable " << tableName << (ifExists?" IF-EXISTS":"") << ")"; return os.str();
    }
};

// 新增：顺序扫描
struct SeqScanPlanNode : public PlanNode {
    std::string tableName;
    explicit SeqScanPlanNode(const std::string& table) : tableName(table) {
        type = PlanNodeType::SEQ_SCAN;
    }
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"SeqScan\",\"table\":\"" << tableName << "\"}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(SeqScan " << tableName << ")"; return os.str();
    }
};

// 新增：过滤
struct FilterPlanNode : public PlanNode {
    std::string predicate;
    std::unique_ptr<PlanNode> input;
    FilterPlanNode(const std::string& pred, std::unique_ptr<PlanNode> child)
        : predicate(pred), input(std::move(child)) {
        type = PlanNodeType::FILTER;
    }
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"Filter\",\"predicate\":\"" << predicate << "\",\"input\":" << (input? input->to_json():"null") << "}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(Filter \"" << predicate << "\" " << (input? input->to_sexpr():"null") << ")"; return os.str();
    }
};

// 新增：投影
struct ProjectPlanNode : public PlanNode {
    std::vector<std::string> columns;
    std::unique_ptr<PlanNode> input;
    ProjectPlanNode(const std::vector<std::string>& cols, std::unique_ptr<PlanNode> child)
        : columns(cols), input(std::move(child)) {
        type = PlanNodeType::PROJECT;
    }
    std::string to_json() const override {
        std::ostringstream os; os << "{\"type\":\"Project\",\"columns\":" << json_array(columns) << ",\"input\":" << (input? input->to_json():"null") << "}"; return os.str();
    }
    std::string to_sexpr() const override {
        std::ostringstream os; os << "(Project (";
        for (size_t i = 0; i < columns.size(); ++i) { os << columns[i]; if (i + 1 < columns.size()) os << " "; }
        os << ") " << (input? input->to_sexpr():"null") << ")"; return os.str();
    }
};

// 执行计划生成器类
class ExecutionPlanGenerator {
public:
    std::unique_ptr<PlanNode> generate(const std::vector<Quadruplet>& ir);
};