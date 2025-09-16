#include "compiler/compiler.h"
#include <stdexcept>
#include <iostream>

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

    CompiledUnit unit;
    unit.tokens = std::move(tokens);
    unit.ast = std::move(ast);
    unit.ir = std::move(ir);
    return unit;

    // 5) 执行计划生成
    std::string Compiler::getPlanAsTree(const std::string& sql, pcsql::StorageEngine& storage) {
        auto unit = compile(sql, storage);
        return PlanSerializer::toString(unit.plan.get());
    }

    std::string Compiler::getPlanAsJSON(const std::string& sql, pcsql::StorageEngine& storage) {
        auto unit = compile(sql, storage);
        return PlanSerializer::toJSON(unit.plan.get());
    }

    std::string Compiler::getPlanAsSExpression(const std::string& sql, pcsql::StorageEngine& storage) {
        auto unit = compile(sql, storage);
        return PlanSerializer::toSExpression(unit.plan.get());
    }
}