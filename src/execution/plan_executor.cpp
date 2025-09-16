#include "plan_executor.h"
#include "execution_engine.h"

namespace pcsql {

PlanExecutor::PlanExecutor(StorageEngine& storage)
    : storage_(storage) {}

std::unique_ptr<Operator> PlanExecutor::buildOperator(const PlanNode* node) {
    if (auto scan = dynamic_cast<const SeqScanNode*>(node)) {
        return std::make_unique<SeqScanOp>(storage_, scan->tableName);
    }
    else if (auto filter = dynamic_cast<const FilterNode*>(node)) {
        auto child = buildOperator(filter->child.get());
        return std::make_unique<FilterOp>(std::move(child), filter->condition);
    }
    else if (auto project = dynamic_cast<const ProjectNode*>(node)) {
        auto child = buildOperator(project->child.get());
        return std::make_unique<ProjectOp>(std::move(child), project->columns);
    }
    else if (auto createTable = dynamic_cast<const CreateTableNode*>(node)) {
        return std::make_unique<CreateTableOp>(storage_, createTable->tableName, createTable->columns);
    }
    else if (auto insert = dynamic_cast<const InsertNode*>(node)) {
        return std::make_unique<InsertOp>(storage_, insert->tableName, insert->values);
    }
    else if (auto del = dynamic_cast<const DeleteNode*>(node)) {
        auto child = buildOperator(del->child.get());
        return std::make_unique<DeleteOp>(storage_, del->tableName, std::move(child));
    }
    else if (auto update = dynamic_cast<const UpdateNode*>(node)) {
        auto child = buildOperator(update->child.get());
        return std::make_unique<UpdateOp>(storage_, update->tableName, update->assignments, std::move(child));
    }
    else if (auto createIndex = dynamic_cast<const CreateIndexNode*>(node)) {
        return std::make_unique<CreateIndexOp>(storage_, createIndex->indexName, 
                                              createIndex->tableName, createIndex->columnName);
    }
    
    throw std::runtime_error("Unsupported plan node type");
}

std::vector<std::vector<std::string>> PlanExecutor::execute(const PlanNode* root) {
    ExecutionEngine engine(storage_);
    auto op = buildOperator(root);
    return engine.execute(std::move(op));
}

} // namespace pcsql