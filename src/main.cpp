#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>

#include "storage/storage_engine.hpp"
#include "compiler/lexer.h"
#include "compiler/parser.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"
#include "compiler/catalog.h"

using namespace pcsql;

// 这是一个函数，用于封装和测试整个SQL编译过程
void test_sql_compiler(const std::string& sql_query) {
    std::cout << "\n--- Testing SQL Compiler with Query: \"" << sql_query << "\" ---\n";
    try {
        // 1. 词法分析
        Lexer lexer(sql_query);
        std::vector<Token> tokens = lexer.tokenize();
        std::cout << "Tokenization successful! Tokens count: " << tokens.size() << "\n";

        // 2. 语法分析
        Parser parser(tokens);
        std::unique_ptr<ASTNode> ast = parser.parse();
        std::cout << "Parsing successful! AST generated.\n";
        
        // 3. 语义分析
        Catalog catalog; // 创建一个目录实例
        SemanticAnalyzer semantic_analyzer(catalog);
        semantic_analyzer.analyze(ast, tokens);
        std::cout << "Semantic analysis successful!\n";

        // 4. 中间代码生成
        IRGenerator ir_generator;
        std::vector<Quadruplet> ir_code = ir_generator.generate(ast);
        std::cout << "IR generation successful! Quadruplets:\n";
        for (const auto& q : ir_code) {
            std::cout << "  (" << q.op << ", " << q.arg1 << ", " << q.arg2 << ", " << q.result << ")\n";
        }

    } catch (const std::runtime_error& e) {
        std::cerr << "Compilation failed: " << e.what() << "\n";
    }
    std::cout << "--- End of SQL Compiler Test ---\n";
}

int main() {
std::cout << "--- Testing Storage Engine ---\n";
    StorageEngine engine{"./storage_data", 4, Policy::LRU, true};

    // 为了演示可重复运行：若已存在同名表则先删除（并回收页）
    engine.drop_table_by_name("users");

    // 演示：创建表并为表分配两页
    auto tid = engine.create_table("users");
    std::cout << "Created table 'users' with id: " << tid << "\n";
    auto p1 = engine.allocate_table_page(tid);
    auto p2 = engine.allocate_table_page(tid);
    std::cout << "Allocated pages for table users: " << p1 << ", " << p2 << "\n";

    // 写入第一页
    {
        Page& page = engine.get_page(p1);
        const char* msg = "hello users page1";
        const size_t msg_len = std::strlen(msg);
        
        // 增加边界检查，并复制时包含空终止符
        if (msg_len + 1 <= page.data.size()) {
            std::memcpy(page.data.data(), msg, msg_len + 1);
            std::cout << "Successfully wrote to page " << p1 << "\n";
        } else {
            std::cerr << "Error: Page buffer is too small to write message.\n";
        }
        engine.unpin_page(p1, true);
    }

    // 再次读取并打印
    {
        Page& page = engine.get_page(p1);
        // 读取时使用字符串构造函数，直到遇到空终止符
        std::string read_msg(page.data.data());
        std::cout << "Read page " << p1 << ": " << read_msg << "\n";
        engine.unpin_page(p1, false);
    }

    // 使用 RecordManager 接口插入若干记录
    auto rid1 = engine.insert_record(tid, "alice");
    auto rid2 = engine.insert_record(tid, "bob");
    auto rid3 = engine.insert_record(tid, "charlie");
    std::cout << "Inserted RIDs: (" << rid1.page_id << "," << rid1.slot_id << ") "
              << "(" << rid2.page_id << "," << rid2.slot_id << ") "
              << "(" << rid3.page_id << "," << rid3.slot_id << ")\n";

    // 读取与更新
    std::string out;
    if (engine.read_record(rid2, out)) {
        std::cout << "Read rid2: " << out << "\n";
    }
    bool up_ok = engine.update_record(rid2, "bobby");
    std::cout << "Update rid2 -> bobby: " << (up_ok ? "OK" : "FAIL") << "\n";
    if (engine.read_record(rid2, out)) {
        std::cout << "Read rid2 after update: " << out << "\n";
    }

    // 删除
    bool del_ok = engine.delete_record(rid1);
    std::cout << "Delete rid1: " << (del_ok ? "OK" : "FAIL") << "\n";

    // 扫描
    auto rows = engine.scan_table(tid);
    std::cout << "Scan table users -> " << rows.size() << " rows\n";
    for (auto& [rid, bytes] : rows) {
        std::cout << "  (" << rid.page_id << "," << rid.slot_id << ") => " << bytes << "\n";
    }

    // 遍历表页
    const auto& pages = engine.get_table_pages(tid);
    std::cout << "Table 'users' pages (" << pages.size() << "): ";
    for (auto pid : pages) std::cout << pid << ' ';
    std::cout << "\n";

    engine.flush_all();
    const auto& st = engine.stats();
    std::cout << "Stats - hit:" << st.hits << " miss:" << st.misses << " evict:" << st.evictions << " flush:" << st.flushes << "\n";
std::cout << "--- End of Storage Engine Test ---\n";

    // 调用 SQL 编译器测试函数
    test_sql_compiler("SELECT id, username FROM users WHERE age > 25;");
    test_sql_compiler("CREATE TABLE my_table (id INT PRIMARY KEY, name VARCHAR(20));");
    test_sql_compiler("DELETE FROM products WHERE price > 100.0;");

    return 0;
}