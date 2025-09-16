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
    if (node->whereCondition) { // 修正: SelectStatement 使用 whereCondition
        //  (这里的逻辑需要根据您的表达式AST来具体实现，暂时简化)
        //  一个简化的例子是直接使用表达式字符串
        //  BinaryExpression* bin_expr = dynamic_cast<BinaryExpression*>(node->whereCondition.get());
        //  string condition_str = ... 从 bin_expr 构造出字符串 ...
        std::string temp = newTemp();
        // [FIXED] 使用 emplace_back
        quadruplets_.emplace_back("WHERE_CLAUSE", "condition_placeholder", "NULL", temp); 
        whereResult = temp;
    }

    // [FIXED] 使用 emplace_back
    quadruplets_.emplace_back("SELECT_FROM", node->fromTable, whereResult, "NULL");

    // if (node->columns.empty() || node->columns[0] == "*") {
    //    quadruplets_.emplace_back("SELECT_COLUMN", "*", "NULL", "NULL");
    // }
    bool selectAll = false;
    for(const auto& col : node->columns){
        if (col == "*"){
            selectAll = true;
            break;
        }
    }
    
    if (selectAll) {
        // [FIXED] 使用 emplace_back
        quadruplets_.emplace_back("SELECT_ALL", "NULL", "NULL", "NULL");
    } else {
        // 为每一列生成一个四元式
        for (const auto& column : node->columns) {
            // [FIXED] 使用 emplace_back
            quadruplets_.emplace_back("SELECT_COLUMN", column, "NULL", "NULL");
        }
    }
}

void IRGenerator::visit(CreateTableStatement* node) {
    // [FIXED] 使用 emplace_back
    quadruplets_.emplace_back("CREATE_TABLE", node->tableName, "NULL", "NULL");
    for (const auto& colDef : node->columnDefinitions) {
        // [FIXED] 使用 emplace_back, 假设 colDef 有 length 成员
        // std::to_string(colDef.length)
        quadruplets_.emplace_back("COLUMN_DEF", colDef.name, colDef.type, "0"); 
    }
}

void IRGenerator::visit(InsertStatement* node) {
    // [FIXED] 使用 emplace_back
    quadruplets_.emplace_back("INSERT_INTO", node->tableName, "NULL", "NULL");
    for (const auto& value : node->values) {
        // [FIXED] 使用 emplace_back
        quadruplets_.emplace_back("VALUE", value, "NULL", newTemp());
    }
}

void IRGenerator::visit(DeleteStatement* node) {
    std::string whereResult = "NULL";
    if (node->whereCondition) {
        std::string temp = newTemp();
        // [FIXED] 使用 emplace_back (同样，条件部分是简化的)
        quadruplets_.emplace_back("WHERE_CLAUSE", "condition_placeholder", "NULL", temp);
        whereResult = temp;
    }
    // [FIXED] 使用 emplace_back
    quadruplets_.emplace_back("DELETE_FROM", node->tableName, whereResult, "NULL");
}

void IRGenerator::visit(UpdateStatement* node) {
    std::string whereResult = "NULL";
    if (node->whereCondition) {
        std::string temp = newTemp();
        // [FIXED] 使用 emplace_back (同样，条件部分是简化的)
        quadruplets_.emplace_back("WHERE_CLAUSE", "condition_placeholder", "NULL", temp);
        whereResult = temp;
    }

    // [FIXED] 使用 emplace_back
    quadruplets_.emplace_back("UPDATE", node->tableName, whereResult, "NULL");
    for (const auto& assignment : node->assignments) {
        // [FIXED] 使用 emplace_back
        quadruplets_.emplace_back("SET_ASSIGN", assignment.first, assignment.second, "NULL");
    }
}

// 新增：访问 CREATE INDEX 语句
void IRGenerator::visit(CreateIndexStatement* node) {
    std::cout << "Generating IR for CREATE INDEX statement..." << std::endl;
    // [FIXED] 使用 emplace_back
    quadruplets_.emplace_back("CREATE_INDEX", node->indexName, node->tableName, node->columnName);
}