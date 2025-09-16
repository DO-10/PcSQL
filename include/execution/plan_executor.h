#ifndef PLAN_EXECUTOR_H
#define PLAN_EXECUTOR_H

#include <memory>
#include "execution_plan/plan_node.h"
#include "execution_engine.h"

namespace pcsql {

class PlanExecutor {
public:
    PlanExecutor(StorageEngine& storage);
    
    std::unique_ptr<Operator> buildOperator(const PlanNode* node);
    
    std::vector<std::vector<std::string>> execute(const PlanNode* root);

private:
    StorageEngine& storage_;
};

} // namespace pcsql

#endif // PLAN_EXECUTOR_H