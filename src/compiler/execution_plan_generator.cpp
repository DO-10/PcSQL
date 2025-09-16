#include "compiler/execution_plan_generator.h"
#include "compiler/ir_generator.h"
#include "compiler/parser.h" 
#include <stdexcept>
#include <iostream>
#include <vector>

std::unique_ptr<PlanNode> ExecutionPlanGenerator::generate(const std::vector<Quadruplet>& ir) {
    if (ir.empty()) {
        throw std::runtime_error("[执行计划, (line 0, column 0), Intermediate code is empty]");
    }

    std::cout << "Starting execution plan generation..." << std::endl;

    // Robustly detect the root operation (IR may start with COMPARE when WHERE exists)
    auto find_op = [&](const std::string& op) -> int {
        for (size_t i = 0; i < ir.size(); ++i) if (ir[i].op == op) return static_cast<int>(i);
        return -1;
    };

    int idx = -1;
    std::string root;
    // Priority: single-statement IR contains exactly one of these root ops
    const char* roots[] = {"CREATE_TABLE","CREATE_INDEX","INSERT_INTO","SELECT_FROM","UPDATE","DELETE_FROM","DROP_TABLE"};
    for (const char* r : roots) {
        int t = find_op(r);
        if (t >= 0) { idx = t; root = r; break; }
    }

    if (idx < 0) {
        // No known root op found; report first op to aid debugging
        std::string op0 = ir.front().op;
        throw std::runtime_error(std::string("[执行计划, (line 0, column 0), Unsupported IR operation '") + op0 + "']");
    }

    const auto& firstQuad = ir[static_cast<size_t>(idx)];

    if (root == "CREATE_TABLE") {
        std::vector<ColumnDefinition> columns;
        for (size_t i = 0; i < ir.size(); ++i) {
            const auto& q = ir[i];
            if (q.op == "COLUMN_DEF") {
                std::string name = q.arg1;
                std::string type = q.arg2;
                size_t length = (q.result != "NULL" && !q.result.empty()) ? std::stoul(q.result) : 0;
                columns.push_back({name, type, length, {}});
            }
        }
        return std::make_unique<CreateTablePlanNode>(firstQuad.arg1, columns);
    } else if (root == "CREATE_INDEX") {
        return std::make_unique<CreateIndexPlanNode>(firstQuad.arg1, firstQuad.arg2, firstQuad.result);
    } else if (root == "INSERT_INTO") {
        std::vector<std::string> values;
        for (size_t i = 0; i < ir.size(); ++i) {
            const auto& q = ir[i];
            if (q.op == "VALUE") {
                values.push_back(q.arg1);
            }
        }
        return std::make_unique<InsertPlanNode>(firstQuad.arg1, values);
    } else if (root == "SELECT_FROM") {
        // 新版：构建 SeqScan -> (Filter?) -> Project 计划
        const std::string table = firstQuad.arg1;
        bool select_all = false;
        std::vector<std::string> cols;
        std::string predicate;
        for (const auto& q : ir) {
            if (q.op == "SELECT_ALL") {
                select_all = true;
            } else if (q.op == "SELECT_COLUMN") {
                cols.push_back(q.arg1);
            } else if (q.op == "COMPARE") {
                predicate = q.arg1; // WHERE 原始条件表达式
            }
        }
        if (cols.empty()) {
            // 若未指定列，且存在 SELECT_ALL 或未显式列，则使用"*"
            cols.push_back("*");
        }

        std::unique_ptr<PlanNode> plan = std::make_unique<SeqScanPlanNode>(table);
        if (!predicate.empty() && predicate != "NULL") {
            plan = std::make_unique<FilterPlanNode>(predicate, std::move(plan));
        }
        plan = std::make_unique<ProjectPlanNode>(cols, std::move(plan));
        return plan;
    } else if (root == "UPDATE") {
        std::map<std::string, std::string> assignments;
        std::string whereCondition = "NULL"; // 默认无 WHERE 条件

        for (const auto& q : ir) {
            if (q.op == "SET_ASSIGN") {
                assignments[q.arg1] = q.arg2;
            } else if (q.op == "COMPARE") {
                whereCondition = q.arg1;
            }
        }
        return std::make_unique<UpdatePlanNode>(firstQuad.arg1, whereCondition, assignments);
    } else if (root == "DELETE_FROM") {
        std::string whereCondition = "NULL";
        for (const auto& q : ir) {
            if (q.op == "COMPARE") {
                whereCondition = q.arg1;
            }
        }
        return std::make_unique<DeletePlanNode>(firstQuad.arg1, whereCondition);
    } else if (root == "DROP_TABLE") {
        bool if_exists = (firstQuad.result == "1");
        return std::make_unique<DropTablePlanNode>(firstQuad.arg1, if_exists);
    }

    throw std::runtime_error(std::string("[执行计划, (line 0, column 0), Unsupported IR operation '") + firstQuad.op + "']");
}