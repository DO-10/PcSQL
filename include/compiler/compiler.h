#pragma once
#include <memory>
#include <string>
#include <vector>
#include <iostream>

#include "compiler/lexer.h"
#include "compiler/parser.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"
#include "compiler/execution_plan_generator.h" // 提供 PlanNode 完整定义
#include "storage/storage_engine.hpp"

// 编译器对外接口：提供从 SQL 文本到编译结果（tokens/AST/IR/Plan）的统一入口
class Compiler {
public:
    Compiler() = default;

    struct CompiledUnit {
        std::vector<Token> tokens;
        std::unique_ptr<ASTNode> ast;
        std::vector<Quadruplet> ir;
        // 新增：执行计划与序列化
        std::unique_ptr<PlanNode> plan;       // 逻辑计划根
        std::string plan_json;                 // 计划的 JSON 序列化
        std::string plan_sexpr;                // 计划的 S 表达式序列化
    };

    // 将 SQL 编译为中间结果（需要访问存储引擎以读取系统目录）
    CompiledUnit compile(const std::string& sql, pcsql::StorageEngine& storage);
};