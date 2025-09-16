#include "compiler/execution_plan_generator.h"
#include "compiler/ir_generator.h"
#include "compiler/parser.h" 
#include <stdexcept>
#include <iostream>
#include <vector>

std::unique_ptr<PlanNode> ExecutionPlanGenerator::generate(const std::vector<Quadruplet>& ir) {
    if (ir.empty()) {
        throw std::runtime_error("Execution Plan Generation Error: Intermediate code is empty.");
    }

    std::cout << "Starting execution plan generation..." << std::endl;
    const auto& firstQuad = ir[0];

    if (firstQuad.op == "CREATE_TABLE") {
        std::vector<ColumnDefinition> columns;
        for (size_t i = 1; i < ir.size(); ++i) {
            const auto& q = ir[i];
            if (q.op == "COLUMN_DEF") {
                std::string name = q.arg1;
                std::string type = q.arg2;
                // 从四元式中读取长度信息，并将其转换为 size_t
                size_t length = (q.result != "NULL" && !q.result.empty()) ? std::stoul(q.result) : 0;
                
                // 直接使用列表初始化
                // columns.push_back({name, type, length, {}}); 
                //cols.emplace_back(colDef.name, colDef.type, std::vector<std::string>{}); // 传递一个空的 vector
            }
        }
        return std::make_unique<CreateTablePlanNode>(firstQuad.arg1, columns);
    } else if (firstQuad.op == "CREATE_INDEX") {
        return std::make_unique<CreateIndexPlanNode>(firstQuad.arg1, firstQuad.arg2, firstQuad.result);
    } else if (firstQuad.op == "INSERT_INTO") {
        std::vector<std::string> values;
        for (size_t i = 1; i < ir.size(); ++i) {
            const auto& q = ir[i];
            if (q.op == "VALUE") {
                values.push_back(q.arg1);
            }
        }
        return std::make_unique<InsertPlanNode>(firstQuad.arg1, values);
    } else if (firstQuad.op == "SELECT_FROM") {
        std::vector<std::string> cols;
        std::string whereCondition = "NULL"; // 默认无 WHERE 条件
        for (const auto& q : ir) {
            // 【修改】检查 SELECT_ALL 四元式
            if (q.op == "SELECT_ALL") {
                cols.push_back("*");
            } else if (q.op == "SELECT_COLUMN") {
                cols.push_back(q.arg1);
            } else if (q.op == "WHERE_CLAUSE") {
                whereCondition = q.arg1;
            }
        }
        
        // 确保 plan node 总是有一个列列表
        if (cols.empty()) {
            cols.push_back("NULL");
        }
        
        return std::make_unique<SelectPlanNode>(firstQuad.arg1, cols, whereCondition);
    } else if (firstQuad.op == "UPDATE") {
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
    } else if (firstQuad.op == "DELETE_FROM") {
        std::string whereCondition = "NULL";
        for (const auto& q : ir) {
            if (q.op == "COMPARE") {
                whereCondition = q.arg1;
            }
        }
        return std::make_unique<DeletePlanNode>(firstQuad.arg1, whereCondition);
    }

    throw std::runtime_error("Execution Plan Generation Error: Unsupported IR operation '" + firstQuad.op + "'.");
}