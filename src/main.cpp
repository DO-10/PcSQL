#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include "compiler/lexer.h"
#include "compiler/parser.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"
#include "compiler/execution_plan_generator.h"
#include "compiler/catalog.h"

// 辅助函数：将 Token 类型转换为字符串（方便调试）
std::string tokenTypeToString(TokenType type) {
    switch (type) {
        case TokenType::END_OF_FILE: return "END_OF_FILE";
        case TokenType::KEYWORD:     return "KEYWORD";
        case TokenType::IDENTIFIER:  return "IDENTIFIER";
        case TokenType::NUMBER:      return "NUMBER";
        case TokenType::STRING:      return "STRING";
        case TokenType::OPERATOR:    return "OPERATOR";
        case TokenType::DELIMITER:   return "DELIMITER";
        default:                     return "UNKNOWN";
    }
}

// 辅助函数：打印 Token
void printTokens(const std::vector<Token>& tokens) {
    std::cout << "--- Tokens ---" << std::endl;
    for (const auto& token : tokens) {
        std::cout << "Type: " << tokenTypeToString(token.type) << ", Value: '" << token.value << "'" << std::endl;
    }
    std::cout << "--------------" << std::endl;
}

// 辅助函数：打印四元式
void printIR(const std::vector<Quadruplet>& ir) {
    std::cout << "--- Intermediate Representation (Quadruplets) ---" << std::endl;
    for (const auto& q : ir) {
        std::cout << "(" << q.op << ", " << q.arg1 << ", " << q.arg2 << ", " << q.result << ")" << std::endl;
    }
    std::cout << "-------------------------------------------------" << std::endl;
}

// 测试函数：运行整个编译和计划生成流程
void test_sql_pipeline(const std::string& sql_statement, Catalog& catalog) {
    std::cout << "=================================================" << std::endl;
    std::cout << "Testing SQL: \"" << sql_statement << "\"" << std::endl;
    try {
        // 1. 词法分析
        Lexer lexer(sql_statement);
        std::vector<Token> tokens = lexer.tokenize();
        printTokens(tokens);

        // 2. 语法分析 (AST 生成)
        Parser parser(tokens);
        std::unique_ptr<ASTNode> ast = parser.parse();

        // 3. 语义分析
        SemanticAnalyzer analyzer(catalog);
        analyzer.analyze(ast, tokens);

        // 4. IR 生成
        IRGenerator ir_generator;
        std::vector<Quadruplet> ir = ir_generator.generate(ast);
        printIR(ir);

        // 5. 执行计划生成
        ExecutionPlanGenerator plan_generator;
        std::unique_ptr<PlanNode> plan = plan_generator.generate(ir);

        // 模拟执行计划
        switch (plan->type) {
            case PlanNode::PlanNodeType::CREATE_TABLE:
                std::cout << "Execution Plan: CREATE TABLE" << std::endl;
                break;
            case PlanNode::PlanNodeType::CREATE_INDEX:
                std::cout << "Execution Plan: CREATE INDEX" << std::endl;
                break;
            case PlanNode::PlanNodeType::INSERT:
                std::cout << "Execution Plan: INSERT" << std::endl;
                break;
            case PlanNode::PlanNodeType::SELECT:
                std::cout << "Execution Plan: SELECT" << std::endl;
                break;
            case PlanNode::PlanNodeType::UPDATE:
                std::cout << "Execution Plan: UPDATE" << std::endl;
                break;
            case PlanNode::PlanNodeType::DELETE:
                std::cout << "Execution Plan: DELETE" << std::endl;
                break;
        }
        std::cout << "Test passed!" << std::endl;

    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        std::cerr << "Test failed!" << std::endl;
    }
    std::cout << "=================================================" << std::endl << std::endl;
}

int main() {
    Catalog catalog;

    // 1. 测试 CREATE TABLE 语句
    test_sql_pipeline("CREATE TABLE students (id INT, name VARCHAR(50), grade DOUBLE, city CHAR(10));", catalog);

    // 2. 测试 INSERT INTO 语句
    test_sql_pipeline("INSERT INTO users VALUES ('1', 'Alice', 'alice@example.com', '30');", catalog);
    
    // 3. 测试 SELECT 语句
    test_sql_pipeline("SELECT username, age FROM users WHERE age > 25;", catalog);
    
    // 【新增】测试 SELECT * 语句
    test_sql_pipeline("SELECT * FROM users;", catalog);

    // 4. 测试 UPDATE 语句
    test_sql_pipeline("UPDATE users SET age = 31 WHERE username = 'Alice';", catalog);

    // 5. 测试 DELETE FROM 语句
    test_sql_pipeline("DELETE FROM employees WHERE id = 101;", catalog);

    // 6. 测试 CREATE INDEX 语句
    test_sql_pipeline("CREATE INDEX idx_name ON employees (name);", catalog);

    return 0;
}