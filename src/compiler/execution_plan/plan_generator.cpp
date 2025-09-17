#include <compiler/execution_plan/plan_printer.h>
#include <sstream>
#include <iomanip>
#include <cctype>

std::string PlanPrinter::getPlanAsTree(const std::shared_ptr<PlanNode>& root) {
    std::string result;
    treeHelper(root, result, 0);
    return result;
}

std::string PlanPrinter::getPlanAsJSON(const std::shared_ptr<PlanNode>& root) {
    std::string result = "{";
    jsonHelper(root, result);
    result += "\n}";
    return result;
}

std::string PlanPrinter::getPlanAsSExpression(const std::shared_ptr<PlanNode>& root) {
    std::string result;
    sexprHelper(root, result);
    return result;
}

void PlanPrinter::treeHelper(const std::shared_ptr<PlanNode>& node, std::string& result, int depth) {
    if (!node) return;
    
    // 添加缩进
    result += std::string(depth * 2, ' ');
    
    // 根据节点类型添加描述
    switch (node->getType()) {
        case PlanNode::NodeType::CreateTable:
            result += "CreateTable";
            break;
        case PlanNode::NodeType::CreateIndex:
            result += "CreateIndex";
            break;
        case PlanNode::NodeType::Insert:
            result += "Insert";
            break;
        case PlanNode::NodeType::SeqScan:
            result += "SeqScan";
            break;
        case PlanNode::NodeType::Filter:
            result += "Filter";
            break;
        case PlanNode::NodeType::Project:
            result += "Project";
            break;
        case PlanNode::NodeType::Delete:
            result += "Delete";
            break;
        case PlanNode::NodeType::Update:
            result += "Update";
            break;
        case PlanNode::NodeType::DropTable:
            result += "DropTable";
            break;
    }
    
    // 添加节点特定信息
    switch (node->getType()) {
        case PlanNode::NodeType::CreateTable: {
            auto n = std::static_pointer_cast<CreateTableNode>(node);
            result += " (table=" + n->getTableName() + ")";
            break;
        }
        case PlanNode::NodeType::CreateIndex: {
            auto n = std::static_pointer_cast<CreateIndexNode>(node);
            result += " (index=" + n->getIndexName() + " on " + 
                      n->getTableName() + "." + n->getColumnName() + ")";
            break;
        }
        case PlanNode::NodeType::Insert: {
            auto n = std::static_pointer_cast<InsertNode>(node);
            result += " (table=" + n->getTableName() + ")";
            break;
        }
        case PlanNode::NodeType::SeqScan: {
            auto n = std::static_pointer_cast<SeqScanNode>(node);
            result += " (table=" + n->getTableName() + ")";
            break;
        }
        case PlanNode::NodeType::Filter: {
            auto n = std::static_pointer_cast<FilterNode>(node);
            result += " (condition=" + n->getCondition() + ")";
            break;
        }
        case PlanNode::NodeType::Project: {
            auto n = std::static_pointer_cast<ProjectNode>(node);
            std::string columns;
            for (const auto& col : n->getColumns()) {
                if (!columns.empty()) columns += ", ";
                columns += col;
            }
            result += " (columns=" + columns + ")";
            break;
        }
        case PlanNode::NodeType::Delete: {
            auto n = std::static_pointer_cast<DeleteNode>(node);
            result += " (table=" + n->getTableName() + ")";
            break;
        }
        case PlanNode::NodeType::Update: {
            auto n = std::static_pointer_cast<UpdateNode>(node);
            result += " (table=" + n->getTableName() + ")";
            break;
        }
        case PlanNode::NodeType::DropTable: {
            auto n = std::static_pointer_cast<DropTableNode>(node);
            result += " (table=" + n->getTableName() + ")";
            break;
        }
    }
    
    result += "\n";
    
    // 递归处理子节点
    for (const auto& child : node->getChildren()) {
        treeHelper(child, result, depth + 1);
    }
}

void PlanPrinter::jsonHelper(const std::shared_ptr<PlanNode>& node, std::string& result) {
    if (!node) return;
    
    result += "\n  \"node\": {";
    result += "\n    \"type\": \"";
    
    switch (node->getType()) {
        case PlanNode::NodeType::CreateTable: 
            result += "CreateTable\",";
            break;
        case PlanNode::NodeType::CreateIndex: 
            result += "CreateIndex\",";
            break;
        case PlanNode::NodeType::Insert: 
            result += "Insert\",";
            break;
        case PlanNode::NodeType::SeqScan: 
            result += "SeqScan\",";
            break;
        case PlanNode::NodeType::Filter: 
            result += "Filter\",";
            break;
        case PlanNode::NodeType::Project: 
            result += "Project\",";
            break;
        case PlanNode::NodeType::Delete: 
            result += "Delete\",";
            break;
        case PlanNode::NodeType::Update: 
            result += "Update\",";
            break;
        case PlanNode::NodeType::DropTable: 
            result += "DropTable\",";
            break;
    }
    
    // 添加节点特定信息
    switch (node->getType()) {
        case PlanNode::NodeType::CreateTable: {
            auto n = std::static_pointer_cast<CreateTableNode>(node);
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\"";
            break;
        }
        case PlanNode::NodeType::CreateIndex: {
            auto n = std::static_pointer_cast<CreateIndexNode>(node);
            result += "\n    \"index\": \"" + escapeJSON(n->getIndexName()) + "\",";
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\",";
            result += "\n    \"column\": \"" + escapeJSON(n->getColumnName()) + "\"";
            break;
        }
        case PlanNode::NodeType::Insert: {
            auto n = std::static_pointer_cast<InsertNode>(node);
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\"";
            break;
        }
        case PlanNode::NodeType::SeqScan: {
            auto n = std::static_pointer_cast<SeqScanNode>(node);
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\",";
            result += "\n    \"filter\": \"" + escapeJSON(n->getFilterCondition()) + "\"";
            break;
        }
        case PlanNode::NodeType::Filter: {
            auto n = std::static_pointer_cast<FilterNode>(node);
            result += "\n    \"condition\": \"" + escapeJSON(n->getCondition()) + "\"";
            break;
        }
        case PlanNode::NodeType::Project: {
            auto n = std::static_pointer_cast<ProjectNode>(node);
            result += "\n    \"columns\": [";
            bool first = true;
            for (const auto& col : n->getColumns()) {
                if (!first) result += ",";
                result += "\n      \"" + escapeJSON(col) + "\"";
                first = false;
            }
            result += "\n    ]";
            break;
        }
        case PlanNode::NodeType::Delete: {
            auto n = std::static_pointer_cast<DeleteNode>(node);
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\",";
            result += "\n    \"condition\": \"" + escapeJSON(n->getCondition()) + "\"";
            break;
        }
        case PlanNode::NodeType::Update: {
            auto n = std::static_pointer_cast<UpdateNode>(node);
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\",";
            result += "\n    \"assignments\": {";
            bool first = true;
            for (const auto& [col, val] : n->getAssignments()) {
                if (!first) result += ",";
                result += "\n      \"" + escapeJSON(col) + "\": \"" + escapeJSON(val) + "\"";
                first = false;
            }
            result += "\n    },";
            result += "\n    \"condition\": \"" + escapeJSON(n->getCondition()) + "\"";
            break;
        }
        case PlanNode::NodeType::DropTable: {
            auto n = std::static_pointer_cast<DropTableNode>(node);
            result += "\n    \"table\": \"" + escapeJSON(n->getTableName()) + "\",";
            result += "\n    \"ifExists\": " + std::string(n->ifExists() ? "true" : "false");
            break;
        }
    }
    
    // 处理子节点
    if (!node->getChildren().empty()) {
        result += ",\n    \"children\": [";
        bool firstChild = true;
        for (const auto& child : node->getChildren()) {
            if (!firstChild) result += ",";
            jsonHelper(child, result);
            firstChild = false;
        }
        result += "\n    ]";
    }
    
    result += "\n  }";
}

void PlanPrinter::sexprHelper(const std::shared_ptr<PlanNode>& node, std::string& result) {
    if (!node) return;
    
    result += "(";
    
    switch (node->getType()) {
        case PlanNode::NodeType::CreateTable: 
            result += "CreateTable";
            break;
        case PlanNode::NodeType::CreateIndex: 
            result += "CreateIndex";
            break;
        case PlanNode::NodeType::Insert: 
            result += "Insert";
            break;
        case PlanNode::NodeType::SeqScan: 
            result += "SeqScan";
            break;
        case PlanNode::NodeType::Filter: 
            result += "Filter";
            break;
        case PlanNode::NodeType::Project: 
            result += "Project";
            break;
        case PlanNode::NodeType::Delete: 
            result += "Delete";
            break;
        case PlanNode::NodeType::Update: 
            result += "Update";
            break;
        case PlanNode::NodeType::DropTable: 
            result += "DropTable";
            break;
    }
    
    // 添加节点特定信息
    switch (node->getType()) {
        case PlanNode::NodeType::CreateTable: {
            auto n = std::static_pointer_cast<CreateTableNode>(node);
            result += " :table \"" + n->getTableName() + "\"";
            break;
        }
        case PlanNode::NodeType::CreateIndex: {
            auto n = std::static_pointer_cast<CreateIndexNode>(node);
            result += " :index \"" + n->getIndexName() + "\"";
            result += " :table \"" + n->getTableName() + "\"";
            result += " :column \"" + n->getColumnName() + "\"";
            break;
        }
        case PlanNode::NodeType::Insert: {
            auto n = std::static_pointer_cast<InsertNode>(node);
            result += " :table \"" + n->getTableName() + "\"";
            break;
        }
        case PlanNode::NodeType::SeqScan: {
            auto n = std::static_pointer_cast<SeqScanNode>(node);
            result += " :table \"" + n->getTableName() + "\"";
            if (!n->getFilterCondition().empty()) {
                result += " :filter \"" + n->getFilterCondition() + "\"";
            }
            break;
        }
        case PlanNode::NodeType::Filter: {
            auto n = std::static_pointer_cast<FilterNode>(node);
            result += " :condition \"" + n->getCondition() + "\"";
            break;
        }
        case PlanNode::NodeType::Project: {
            auto n = std::static_pointer_cast<ProjectNode>(node);
            result += " :columns (";
            for (const auto& col : n->getColumns()) {
                result += " \"" + col + "\"";
            }
            result += " )";
            break;
        }
        case PlanNode::NodeType::Delete: {
            auto n = std::static_pointer_cast<DeleteNode>(node);
            result += " :table \"" + n->getTableName() + "\"";
            if (!n->getCondition().empty()) {
                result += " :condition \"" + n->getCondition() + "\"";
            }
            break;
        }
        case PlanNode::NodeType::Update: {
            auto n = std::static_pointer_cast<UpdateNode>(node);
            result += " :table \"" + n->getTableName() + "\"";
            result += " :assignments (";
            for (const auto& [col, val] : n->getAssignments()) {
                result += " (\"" + col + "\" . \"" + val + "\")";
            }
            result += " )";
            if (!n->getCondition().empty()) {
                result += " :condition \"" + n->getCondition() + "\"";
            }
            break;
        }
        case PlanNode::NodeType::DropTable: {
            auto n = std::static_pointer_cast<DropTableNode>(node);
            result += " :table \"" + n->getTableName() + "\"";
            result += " :ifExists " + std::string(n->ifExists() ? "true" : "false");
            break;
        }
    }
    
    // 处理子节点
    for (const auto& child : node->getChildren()) {
        result += " ";
        sexprHelper(child, result);
    }
    
    result += ")";
}

std::string PlanPrinter::escapeJSON(const std::string& input) {
    std::ostringstream ss;
    for (auto c : input) {
        switch (c) {
            case '"': ss << "\\\""; break;
            case '\\': ss << "\\\\"; break;
            case '\b': ss << "\\b"; break;
            case '\f': ss << "\\f"; break;
            case '\n': ss << "\\n"; break;
            case '\r': ss << "\\r"; break;
            case '\t': ss << "\\t"; break;
            default:
                if (c >= 0 && c <= 0x1F) {
                    ss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
                } else {
                    ss << c;
                }
        }
    }
    return ss.str();
}