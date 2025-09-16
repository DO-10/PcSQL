#ifndef COMPILER_SEMANTIC_IR_GENERATOR_H
#define COMPILER_SEMANTIC_IR_GENERATOR_H

#include <vector>
#include <string>
#include <memory>
#include "compiler/parser.h"

// 四元式结构
struct Quadruplet {
    std::string op;
    std::string arg1;
    std::string arg2;
    std::string result;
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
    void visit(DropTableStatement* node);

    // 新增：访问 CREATE INDEX 语句
    void visit(CreateIndexStatement* node);
    
    // 辅助函数
    std::string newTemp(); // 创建一个新的临时变量
};

#endif // COMPILER_SEMANTIC_IR_GENERATOR_H