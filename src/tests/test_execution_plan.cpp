#include "compiler/compiler.h"
#include "storage/storage_engine.h"
#include <iostream>
#include <cassert>

void testCreateTablePlan() {
    pcsql::StorageEngine storage;
    Compiler compiler;
    
    std::string sql = "CREATE TABLE users (id INT, name VARCHAR(50));";
    auto plan = compiler.compile(sql, storage).plan;
    
    // 验证执行计划类型
    assert(dynamic_cast<CreateTableNode*>(plan.get()) != nullptr);
    
    // 验证输出格式
    std::cout << "=== CREATE TABLE Plan ===\n";
    std::cout << "Tree Format:\n" << compiler.getPlanAsTree(sql, storage) << "\n";
    std::cout << "JSON Format:\n" << compiler.getPlanAsJSON(sql, storage) << "\n";
    std::cout << "S-Expression Format:\n" << compiler.getPlanAsSExpression(sql, storage) << "\n";
}

void testSelectPlan() {
    pcsql::StorageEngine storage;
    Compiler compiler;
    
    // 先创建表
    compiler.compile("CREATE TABLE users (id INT, name VARCHAR(50));", storage);
    
    std::string sql = "SELECT id, name FROM users WHERE id > 10;";
    auto plan = compiler.compile(sql, storage).plan;
    
    // 验证执行计划结构
    auto project = dynamic_cast<ProjectNode*>(plan.get());
    assert(project != nullptr);
    assert(project->columns.size() == 2);
    assert(project->columns[0] == "id");
    assert(project->columns[1] == "name");
    
    auto filter = dynamic_cast<FilterNode*>(project->child.get());
    assert(filter != nullptr);
    assert(filter->condition == "id > 10");
    
    auto scan = dynamic_cast<SeqScanNode*>(filter->child.get());
    assert(scan != nullptr);
    assert(scan->tableName == "users");
    
    // 验证输出格式
    std::cout << "=== SELECT Plan ===\n";
    std::cout << "Tree Format:\n" << compiler.getPlanAsTree(sql, storage) << "\n";
    std::cout << "JSON Format:\n" << compiler.getPlanAsJSON(sql, storage) << "\n";
    std::cout << "S-Expression Format:\n" << compiler.getPlanAsSExpression(sql, storage) << "\n";
}

void testInsertPlan() {
    pcsql::StorageEngine storage;
    Compiler compiler;
    
    // 先创建表
    compiler.compile("CREATE TABLE users (id INT, name VARCHAR(50));", storage);
    
    std::string sql = "INSERT INTO users VALUES (1, 'Alice');";
    auto plan = compiler.compile(sql, storage).plan;
    
    // 验证执行计划类型
    auto insert = dynamic_cast<InsertNode*>(plan.get());
    assert(insert != nullptr);
    assert(insert->tableName == "users");
    assert(insert->values.size() == 2);
    assert(insert->values[0] == "1");
    assert(insert->values[1] == "'Alice'");
    
    // 验证输出格式
    std::cout << "=== INSERT Plan ===\n";
    std::cout << "Tree Format:\n" << compiler.getPlanAsTree(sql, storage) << "\n";
    std::cout << "JSON Format:\n" << compiler.getPlanAsJSON(sql, storage) << "\n";
    std::cout << "S-Expression Format:\n" << compiler.getPlanAsSExpression(sql, storage) << "\n";
}

void testDeletePlan() {
    pcsql::StorageEngine storage;
    Compiler compiler;
    
    // 先创建表
    compiler.compile("CREATE TABLE users (id INT, name VARCHAR(50));", storage);
    
    std::string sql = "DELETE FROM users WHERE id = 1;";
    auto plan = compiler.compile(sql, storage).plan;
    
    // 验证执行计划结构
    auto del = dynamic_cast<DeleteNode*>(plan.get());
    assert(del != nullptr);
    assert(del->tableName == "users");
    
    auto filter = dynamic_cast<FilterNode*>(del->child.get());
    assert(filter != nullptr);
    assert(filter->condition == "id = 1");
    
    auto scan = dynamic_cast<SeqScanNode*>(filter->child.get());
    assert(scan != nullptr);
    assert(scan->tableName == "users");
    
    // 验证输出格式
    std::cout << "=== DELETE Plan ===\n";
    std::cout << "Tree Format:\n" << compiler.getPlanAsTree(sql, storage) << "\n";
    std::cout << "JSON Format:\n" << compiler.getPlanAsJSON(sql, storage) << "\n";
    std::cout << "S-Expression Format:\n" << compiler.getPlanAsSExpression(sql, storage) << "\n";
}

void testUpdatePlan() {
    pcsql::StorageEngine storage;
    Compiler compiler;
    
    // 先创建表
    compiler.compile("CREATE TABLE users (id INT, name VARCHAR(50));", storage);
    
    std::string sql = "UPDATE users SET name = 'Bob' WHERE id = 1;";
    auto plan = compiler.compile(sql, storage).plan;
    
    // 验证执行计划结构
    auto update = dynamic_cast<UpdateNode*>(plan.get());
    assert(update != nullptr);
    assert(update->tableName == "users");
    assert(update->assignments.size() == 1);
    assert(update->assignments[0].first == "name");
    assert(update->assignments[0].second == "'Bob'");
    
    auto filter = dynamic_cast<FilterNode*>(update->child.get());
    assert(filter != nullptr);
    assert(filter->condition == "id = 1");
    
    auto scan = dynamic_cast<SeqScanNode*>(filter->child.get());
    assert(scan != nullptr);
    assert(scan->tableName == "users");
    
    // 验证输出格式
    std::cout << "=== UPDATE Plan ===\n";
    std::cout << "Tree Format:\n" << compiler.getPlanAsTree(sql, storage) << "\n";
    std::cout << "JSON Format:\n" << compiler.getPlanAsJSON(sql, storage) << "\n";
    std::cout << "S-Expression Format:\n" << compiler.getPlanAsSExpression(sql, storage) << "\n";
}

void testCreateIndexPlan() {
    pcsql::StorageEngine storage;
    Compiler compiler;
    
    // 先创建表
    compiler.compile("CREATE TABLE users (id INT, name VARCHAR(50));", storage);
    
    std::string sql = "CREATE INDEX idx_user_id ON users(id);";
    auto plan = compiler.compile(sql, storage).plan;
    
    // 验证执行计划类型
    auto createIndex = dynamic_cast<CreateIndexNode*>(plan.get());
    assert(createIndex != nullptr);
    assert(createIndex->indexName == "idx_user_id");
    assert(createIndex->tableName == "users");
    assert(createIndex->columnName == "id");
    
    // 验证输出格式
    std::cout << "=== CREATE INDEX Plan ===\n";
    std::cout << "Tree Format:\n" << compiler.getPlanAsTree(sql, storage) << "\n";
    std::cout << "JSON Format:\n" << compiler.getPlanAsJSON(sql, storage) << "\n";
    std::cout << "S-Expression Format:\n" << compiler.getPlanAsSExpression(sql, storage) << "\n";
}

int main() {
    try {
        testCreateTablePlan();
        testSelectPlan();
        testInsertPlan();
        testDeletePlan();
        testUpdatePlan();
        testCreateIndexPlan();
        
        std::cout << "\nAll tests passed successfully!\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
}
