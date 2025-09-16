#pragma once
#include <memory>
#include <string>
#include <vector>
#include <iostream>

#include "compiler/lexer.h"
#include "compiler/parser.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"
#include "storage/storage_engine.hpp"
#include "compiler/ast.h"
#include "execution_plan/plan_generator.h"
#include "execution_plan/plan_serializer.h"

// 编译器对外接口：提供从 SQL 文本到编译结果（tokens/AST/IR）的统一入口
class Compiler {
public:
    Compiler() = default;

    struct CompiledUnit {
        std::vector<Token> tokens;
        std::unique_ptr<ASTNode> ast;
        std::vector<Quadruplet> ir;
    };

    // 将 SQL 编译为中间结果（需要访问存储引擎以读取系统目录）
    CompiledUnit compile(const std::string& sql, pcsql::StorageEngine& storage);

    // 获取执行计划的三种格式
    std::string getPlanAsTree(const std::string& sql, pcsql::StorageEngine& storage);
    std::string getPlanAsJSON(const std::string& sql, pcsql::StorageEngine& storage);
    std::string getPlanAsSExpression(const std::string& sql, pcsql::StorageEngine& storage);
};
