#include "compiler/ir_generator.h"
#include <iostream>
#include <sstream>

IRGenerator::IRGenerator() : temp_counter_(0) {}

std::vector<Quadruplet> IRGenerator::generate(const std::unique_ptr<ASTNode>& ast) {
    quadruplets_.clear(); 
    
    if (auto createIndexStmt = dynamic_cast<CreateIndexStatement*>(ast.get())) {
        visit(createIndexStmt);
    } else if (auto selectStmt = dynamic_cast<SelectStatement*>(ast.get())) {
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
    std::string whereResult = "NULL";
    if (node->whereClause) {
        WhereClause* whereClause = dynamic_cast<WhereClause*>(node->whereClause.get());
        std::string temp = newTemp();
        quadruplets_.push_back({"COMPARE", whereClause->condition, "NULL", temp});
        whereResult = temp;
    }

    quadruplets_.push_back({"SELECT_FROM", node->fromTable, whereResult, "NULL"});

    // if (node->columns.empty() || node->columns[0] == "*") {
    //     quadruplets_.push_back({"SELECT_COLUMN", "*", "NULL", "NULL"});
    // } else {
    //     for (const auto& column : node->columns) {
    //         quadruplets_.push_back({"SELECT_COLUMN", column, "NULL", "NULL"});
    //     }
    // }
    if (node->selectAll) {
        quadruplets_.push_back({"SELECT_ALL", "NULL", "NULL", "NULL"});
    } else {
        // 为每一列生成一个四元式
        for (const auto& column : node->columns) {
            quadruplets_.push_back({"SELECT_COLUMN", column, "NULL", "NULL"});
        }
    }
}

void IRGenerator::visit(CreateTableStatement* node) {
    quadruplets_.push_back({"CREATE_TABLE", node->tableName, "NULL", "NULL"});
    for (const auto& colDef : node->columns) {
        // 【修改】在四元式中包含 name, type 和 length
        // 原本的 constraints 无法通过简单的四元式表达，我们只专注于修复 char(n) bug
        quadruplets_.push_back({"COLUMN_DEF", colDef.name, colDef.type, std::to_string(colDef.length)});
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

// 新增：访问 CREATE INDEX 语句
void IRGenerator::visit(CreateIndexStatement* node) {
    std::cout << "Generating IR for CREATE INDEX statement..." << std::endl;
    quadruplets_.push_back({"CREATE_INDEX", node->indexName, node->tableName, node->columnName});
}