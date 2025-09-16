#pragma once
#include "execution_plan/plan_node.h"
#include <string>

class   {
public:
    static std::string toString(const PlanNode* node);
    static std::string toJSON(const PlanNode* node);
    static std::string toSExpression(const PlanNode* node);
    
private:
    static std::string indent(int level);
    static void serializeToJSON(const PlanNode* node, json& j);
    static void serializeToSExpression(const PlanNode* node, std::string& expr);
};

