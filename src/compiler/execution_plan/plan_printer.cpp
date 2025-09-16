#include "plan_printer.h"
#include <sstream>
#include <algorithm>

std::string ExecutionPlanPrinter::print(const Operator* plan, OutputFormat format) {
    switch (format) {
        case OutputFormat::TREE: return printTree(plan);
        case OutputFormat::JSON: return printJSON(plan);
        case OutputFormat::S_EXPRESSION: return printSExpression(plan);
        default: return "Unsupported format";
    }
}

// 树形结构输出
std::string ExecutionPlanPrinter::printTree(const Operator* plan, int indent) {
    std::string result(indent, ' ');
    result += plan->toString() + "\n";
    
    for (const auto& child : plan->children) {
        result += printTree(child.get(), indent + 2);
    }
    
    return result;
}

// JSON格式输出
std::string ExecutionPlanPrinter::printJSON(const Operator* plan) {
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"operator\": \"" << plan->toString() << "\",\n";
    
    // 添加特定算子的属性
    if (auto scan = dynamic_cast<const SeqScanOp*>(plan)) {
        oss << "  \"table\": \"" << escapeJson(scan->table_name) << "\",\n";
    } else if (auto filter = dynamic_cast<const FilterOp*>(plan)) {
        oss << "  \"condition\": \"" << escapeJson(filter->condition->toString()) << "\",\n";
    } else if (auto project = dynamic_cast<const ProjectOp*>(plan)) {
        oss << "  \"columns\": [";
        for (size_t i = 0; i < project->columns.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << "\"" << escapeJson(project->columns[i]->toString()) << "\"";
        }
        oss << "],\n";
    }
    
    // 处理子节点
    if (!plan->children.empty()) {
        oss << "  \"children\": [\n";
        for (size_t i = 0; i < plan->children.size(); ++i) {
            if (i > 0) oss << ",\n";
            std::string childJson = printJSON(plan->children[i].get());
            // 缩进处理
            std::istringstream iss(childJson);
            std::string line;
            while (std::getline(iss, line)) {
                oss << "    " << line << "\n";
            }
        }
        oss << "  ]\n";
    } else {
        oss << "  \"children\": []\n";
    }
    
    oss << "}";
    return oss.str();
}

// S表达式输出
std::string ExecutionPlanPrinter::printSExpression(const Operator* plan) {
    std::ostringstream oss;
    oss << "(" << plan->toString();
    
    // 添加特定算子的属性
    if (auto scan = dynamic_cast<const SeqScanOp*>(plan)) {
        oss << " (table " << scan->table_name << ")";
    } else if (auto filter = dynamic_cast<const FilterOp*>(plan)) {
        oss << " (condition " << filter->condition->toString() << ")";
    } else if (auto project = dynamic_cast<const ProjectOp*>(plan)) {
        oss << " (columns";
        for (const auto& col : project->columns) {
            oss << " " << col->toString();
        }
        oss << ")";
    }
    
    // 处理子节点
    for (const auto& child : plan->children) {
        oss << " " << printSExpression(child.get());
    }
    
    oss << ")";
    return oss.str();
}

std::string ExecutionPlanPrinter::escapeJson(const std::string& str) const {
    std::string result;
    for (char c : str) {
        switch (c) {
            case '"': result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\b': result += "\\b"; break;
            case '\f': result += "\\f"; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            default:
                if ('\x00' <= c && c <= '\x1f') {
                    std::ostringstream oss;
                    oss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
                    result += oss.str();
                } else {
                    result += c;
                }
        }
    }
    return result;
}
