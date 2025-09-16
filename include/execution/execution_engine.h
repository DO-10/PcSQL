#pragma once
#include <memory>
#include <string>
#include <vector>

#include "compiler/lexer.h"
#include "compiler/parser.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"
#include "system_catalog/types.hpp"
#include "storage/storage_engine.hpp"
#include "compiler/compiler.h"

// 执行引擎：
// - 接收 AST/IR，驱动存储层执行
// - 不再负责词法/语法/语义/IR 生成（改由 Compiler 完成）

class ExecutionEngine {
public:
    explicit ExecutionEngine(pcsql::StorageEngine& storage)
        : storage_(storage) {}

    // 接收编译结果执行
    std::string execute(const Compiler::CompiledUnit& unit);

    // 新增：直接返回 SELECT 的原始行（record-string，使用 '|' 分隔字段）供服务器封包
    std::vector<std::pair<pcsql::RID, std::string>> selectRows(SelectStatement* stmt);

private:
    // 语句分派
    std::string handleSelect(SelectStatement* stmt);
    std::string handleCreate(CreateTableStatement* stmt);
    std::string handleCreateIndex(CreateIndexStatement* stmt);
    std::string handleInsert(InsertStatement* stmt);
    std::string handleDelete(DeleteStatement* stmt);
    std::string handleUpdate(UpdateStatement* stmt);
    // 新增：DROP TABLE
    std::string handleDropTable(DropTableStatement* stmt);

    // 便捷：把表按行扫描转为二维文本
    static std::string format_rows(const std::vector<std::pair<pcsql::RID, std::string>>& rows);

    // 复用：构建 SELECT 结果行，并可选输出调试信息（索引范围等）。
    bool buildSelectRows(SelectStatement* stmt,
                         std::vector<std::pair<pcsql::RID, std::string>>& rows_out,
                         std::string* debug_out);

private:
    pcsql::StorageEngine& storage_;
};