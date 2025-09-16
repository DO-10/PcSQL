#include "compiler/compiler.h"
#include <stdexcept>
#include <iostream>
#include "compiler/execution_plan_generator.h"

Compiler::CompiledUnit Compiler::compile(const std::string& sql, pcsql::StorageEngine& storage) {
    // 1) 词法
    Lexer lexer(sql);
    std::vector<Token> tokens = lexer.tokenize();

    // 2) 语法
    Parser parser(tokens);
    std::unique_ptr<ASTNode> ast = parser.parse();

    // 3) 语义：直接访问系统目录（通过 StorageEngine 的系统表）
    SemanticAnalyzer sema(storage);
    sema.analyze(ast, tokens);

    // 4) IR 生成
    IRGenerator irg;
    std::vector<Quadruplet> ir = irg.generate(ast);

    // 5) 执行计划生成（新增）
    ExecutionPlanGenerator epg;
    std::unique_ptr<PlanNode> plan = epg.generate(ir);

    // 填充编译单元
    CompiledUnit unit;
    unit.tokens = std::move(tokens);
    unit.ast = std::move(ast);
    unit.ir = std::move(ir);
    unit.plan = std::move(plan);
    if (unit.plan) {
        unit.plan_json = unit.plan->to_json();
        unit.plan_sexpr = unit.plan->to_sexpr();
        // 可选：在编译阶段打印，便于调试
        std::cout << "Plan(JSON): " << unit.plan_json << std::endl;
        std::cout << "Plan(S-Expr): " << unit.plan_sexpr << std::endl;
    }
    return unit;
}