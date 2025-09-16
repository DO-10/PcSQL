#pragma once
#include "execution_plan/plan_node.h"
#include "compiler/ast.h"
#include <memory>

class PlanGenerator {
public:
    std::unique_ptr<PlanNode> generate(const std::unique_ptr<ASTNode>& ast);
    
private:
    std::unique_ptr<PlanNode> visit(const SelectStatement* node);
    std::unique_ptr<PlanNode> visit(const CreateTableStatement* node);
    std::unique_ptr<PlanNode> visit(const InsertStatement* node);
    std::unique_ptr<PlanNode> visit(const DeleteStatement* node);
    std::unique_ptr<PlanNode> visit(const UpdateStatement* node);
    std::unique_ptr<PlanNode> visit(const CreateIndexStatement* node);
};
