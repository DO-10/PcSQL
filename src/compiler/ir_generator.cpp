#include "compiler/ir_generator.h"
#include <iostream>
#include <sstream>

IRGenerator::IRGenerator() : temp_counter_(0) {}

std::vector<Quadruplet> IRGenerator::generate(const std::unique_ptr<ASTNode>& ast) {
    quadruplets_.clear(); // 清空上次的结果
    
    if (auto selectStmt = dynamic_cast<SelectStatement*>(ast.get())) {
        visit(selectStmt);
    } else if (auto createTableStmt = dynamic_cast<CreateTableStatement*>(ast.get())) {
        visit(createTableStmt);
    } else if (auto insertStmt = dynamic_cast<InsertStatement*>(ast.get())) {
        visit(insertStmt);
    } else if (auto deleteStmt = dynamic_cast<DeleteStatement*>(ast.get())) {
        visit(deleteStmt);
    } else if (auto updateStmt = dynamic_cast<UpdateStatement*>(ast.get())) {
        visit(updateStmt);
    }
    
    return quadruplets_;
}

std::string IRGenerator::newTemp() {
    std::stringstream ss;
    ss << "T" << temp_counter_++;
    return ss.str();
}

void IRGenerator::visit(SelectStatement* node) {
    // 处理 WHERE 子句
    std::string whereResult = "NULL";
    if (node->whereClause) {
        WhereClause* whereClause = dynamic_cast<WhereClause*>(node->whereClause.get());
        std::string temp = newTemp();
        quadruplets_.push_back({"COMPARE", whereClause->condition, "NULL", temp});
        whereResult = temp;
    }
    
    // 生成 SELECT 指令
    std::string columnsList;
    for (const auto& col : node->columns) {
        columnsList += col + ",";
    }
    if (!columnsList.empty()) {
        columnsList.pop_back();
    }

    quadruplets_.push_back({"SELECT", columnsList, node->fromTable, whereResult});
}

void IRGenerator::visit(CreateTableStatement* node) {
    quadruplets_.push_back({"CREATE_TABLE", node->tableName, "NULL", "NULL"});
    for (const auto& col : node->columns) {
        std::string constraintsStr;
        for (const auto& cons : col.constraints) {
            constraintsStr += cons + " ";
        }
        quadruplets_.push_back({"ADD_COLUMN", col.name, col.type, constraintsStr});
    }
}

void IRGenerator::visit(InsertStatement* node) {
    quadruplets_.push_back({"INSERT_INTO", node->tableName, "NULL", "NULL"});
    for (const auto& value : node->values) {
        quadruplets_.push_back({"VALUE", value, "NULL", newTemp()});
    }
}

void IRGenerator::visit(DeleteStatement* node) {
    std::string whereResult = "NULL";
    if (node->whereClause) {
        WhereClause* whereClause = dynamic_cast<WhereClause*>(node->whereClause.get());
        std::string temp = newTemp();
        quadruplets_.push_back({"COMPARE", whereClause->condition, "NULL", temp});
        whereResult = temp;
    }
    quadruplets_.push_back({"DELETE_FROM", node->tableName, whereResult, "NULL"});
}

void IRGenerator::visit(UpdateStatement* node) {
    std::string whereResult = "NULL";
    if (node->whereClause) {
        WhereClause* whereClause = dynamic_cast<WhereClause*>(node->whereClause.get());
        std::string temp = newTemp();
        quadruplets_.push_back({"COMPARE", whereClause->condition, "NULL", temp});
        whereResult = temp;
    }

    quadruplets_.push_back({"UPDATE", node->tableName, whereResult, "NULL"});
    for (const auto& assignment : node->assignments) {
        quadruplets_.push_back({"SET_ASSIGN", assignment.first, assignment.second, "NULL"});
    }
}