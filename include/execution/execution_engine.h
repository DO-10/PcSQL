#ifndef EXECUTION_ENGINE_H
#define EXECUTION_ENGINE_H

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include "storage_engine.h"

namespace pcsql {

// 算子基类
class Operator {
public:
    virtual ~Operator() = default;
    virtual void open() = 0;
    virtual bool next() = 0;
    virtual void close() = 0;
    virtual std::vector<std::string> getOutput() const = 0;
};

// 顺序扫描算子
class SeqScanOp : public Operator {
public:
    SeqScanOp(StorageEngine& storage, const std::string& table_name);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    StorageEngine& storage_;
    std::string table_name_;
    int table_id_;
    std::vector<std::pair<RID, std::string>> records_;
    size_t current_index_;
};

// 过滤算子
class FilterOp : public Operator {
public:
    FilterOp(std::unique_ptr<Operator> child, const std::string& condition);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    std::unique_ptr<Operator> child_;
    std::string condition_;
    bool satisfiesCondition(const std::vector<std::string>& record) const;
};

// 投影算子
class ProjectOp : public Operator {
public:
    ProjectOp(std::unique_ptr<Operator> child, const std::vector<std::string>& columns);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    std::unique_ptr<Operator> child_;
    std::vector<std::string> columns_;
    std::vector<std::string> current_output_;
};

// 创建表算子
class CreateTableOp : public Operator {
public:
    CreateTableOp(StorageEngine& storage, const std::string& table_name, 
                 const std::vector<std::tuple<std::string, std::string, size_t>>& columns);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    StorageEngine& storage_;
    std::string table_name_;
    std::vector<std::tuple<std::string, std::string, size_t>> columns_;
    bool executed_;
};

// 插入算子
class InsertOp : public Operator {
public:
    InsertOp(StorageEngine& storage, const std::string& table_name, 
            const std::vector<std::string>& values);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    StorageEngine& storage_;
    std::string table_name_;
    std::vector<std::string> values_;
    bool executed_;
};

// 删除算子
class DeleteOp : public Operator {
public:
    DeleteOp(StorageEngine& storage, const std::string& table_name, 
            std::unique_ptr<Operator> child);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    StorageEngine& storage_;
    std::string table_name_;
    std::unique_ptr<Operator> child_;
    RID current_rid_;
};

// 更新算子
class UpdateOp : public Operator {
public:
    UpdateOp(StorageEngine& storage, const std::string& table_name, 
            const std::vector<std::pair<std::string, std::string>>& assignments,
            std::unique_ptr<Operator> child);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    StorageEngine& storage_;
    std::string table_name_;
    std::vector<std::pair<std::string, std::string>> assignments_;
    std::unique_ptr<Operator> child_;
    RID current_rid_;
};

// 创建索引算子
class CreateIndexOp : public Operator {
public:
    CreateIndexOp(StorageEngine& storage, const std::string& index_name, 
                 const std::string& table_name, const std::string& column_name);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::string> getOutput() const override;

private:
    StorageEngine& storage_;
    std::string index_name_;
    std::string table_name_;
    std::string column_name_;
    bool executed_;
};

// 执行引擎
class ExecutionEngine {
public:
    explicit ExecutionEngine(StorageEngine& storage);
    
    // 执行查询计划
    std::vector<std::vector<std::string>> execute(std::unique_ptr<Operator> root);
    
    // 获取影响行数
    int getRowsAffected() const { return rows_affected_; }

private:
    StorageEngine& storage_;
    int rows_affected_ = 0;
};

} // namespace pcsql

#endif // EXECUTION_ENGINE_H