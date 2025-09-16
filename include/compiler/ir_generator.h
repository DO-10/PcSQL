#ifndef COMPILER_SEMANTIC_IR_GENERATOR_H
#define COMPILER_SEMANTIC_IR_GENERATOR_H

#include <vector>
#include <string>
#include <memory>
#include <utility> // For std::move
#include "compiler/parser.h"

// 四元式结构
struct Quadruplet {
    std::string op;
    std::string arg1;
    std::string arg2;
    std::string result;

    // [FIX] 添加一个构造函数，解决 emplace_back 的问题
    Quadruplet(std::string o, std::string a1, std::string a2, std::string res)
        : op(std::move(o)), arg1(std::move(a1)), arg2(std::move(a2)), result(std::move(res)) {}
};

// IR 生成器类
class IRGenerator {
public:
    IRGenerator();
    std::vector<Quadruplet> generate(const std::unique_ptr<ASTNode>& ast);

private:
    std::vector<Quadruplet> quadruplets_;
    int temp_counter_;

    // 访问者模式：为不同类型的 AST 节点生成 IR
    void visit(SelectStatement* node);
    void visit(CreateTableStatement* node);
    void visit(InsertStatement* node);
    void visit(DeleteStatement* node);
    void visit(UpdateStatement* node);
    void visit(CreateIndexStatement* node);
    
    // 辅助函数
    std::string newTemp(); // 创建一个新的临时变量
};

#endif // COMPILER_SEMANTIC_IR_GENERATOR_H