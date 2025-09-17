#pragma once
#include <string>
#include <memory>
#include "plan_generator.h"

class PlanPrinter {
public:
    static std::string getPlanAsTree(const std::shared_ptr<PlanNode>& root);
    static std::string getPlanAsJSON(const std::shared_ptr<PlanNode>& root);
    static std::string getPlanAsSExpression(const std::shared_ptr<PlanNode>& root);

private:
    static void treeHelper(const std::shared_ptr<PlanNode>& node, std::string& result, int depth);
    static void jsonHelper(const std::shared_ptr<PlanNode>& node, std::string& result);
    static void sexprHelper(const std::shared_ptr<PlanNode>& node, std::string& result);
    
    static std::string escapeJSON(const std::string& input);
};


