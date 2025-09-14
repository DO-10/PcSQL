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

private:
    // 语句分派
    std::string handleSelect(SelectStatement* stmt);
    std::string handleCreate(CreateTableStatement* stmt);
    std::string handleInsert(InsertStatement* stmt);
    std::string handleDelete(DeleteStatement* stmt);
    std::string handleUpdate(UpdateStatement* stmt);

    // 便捷：把表按行扫描转为二维文本
    static std::string format_rows(const std::vector<std::pair<pcsql::RID, std::string>>& rows);

private:
    pcsql::StorageEngine& storage_;
};