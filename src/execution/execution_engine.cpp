#include "execution_engine.h"
#include <sstream>
#include <stdexcept>
#include <cctype>
#include <algorithm>

namespace pcsql {

// SeqScanOp 实现
SeqScanOp::SeqScanOp(StorageEngine& storage, const std::string& table_name)
    : storage_(storage), table_name_(table_name) {}

void SeqScanOp::open() {
    table_id_ = storage_.get_table_id(table_name_);
    if (table_id_ < 0) {
        throw std::runtime_error("Table not found: " + table_name_);
    }
    records_ = storage_.scan_table(table_id_);
    current_index_ = 0;
}

bool SeqScanOp::next() {
    if (current_index_ < records_.size()) {
        current_index_++;
        return true;
    }
    return false;
}

void SeqScanOp::close() {
    records_.clear();
}

std::vector<std::string> SeqScanOp::getOutput() const {
    if (current_index_ == 0 || current_index_ > records_.size()) {
        return {};
    }
    
    const auto& record = records_[current_index_ - 1].second;
    std::vector<std::string> fields;
    std::istringstream iss(record);
    std::string field;
    while (std::getline(iss, field, '|')) {
        fields.push_back(field);
    }
    return fields;
}

// FilterOp 实现
FilterOp::FilterOp(std::unique_ptr<Operator> child, const std::string& condition)
    : child_(std::move(child)), condition_(condition) {}

void FilterOp::open() {
    child_->open();
}

bool FilterOp::next() {
    while (child_->next()) {
        auto record = child_->getOutput();
        if (satisfiesCondition(record)) {
            return true;
        }
    }
    return false;
}

void FilterOp::close() {
    child_->close();
}

std::vector<std::string> FilterOp::getOutput() const {
    return child_->getOutput();
}

bool FilterOp::satisfiesCondition(const std::vector<std::string>& record) const {
    // 简化的条件评估 - 实际实现需要解析条件表达式
    // 这里只实现简单的等值比较
    size_t pos = condition_.find('=');
    if (pos == std::string::npos) {
        return false;
    }
    
    std::string column = condition_.substr(0, pos);
    std::string value = condition_.substr(pos + 1);
    
    // 去除空格
    column.erase(std::remove_if(column.begin(), column.end(), ::isspace), column.end());
    value.erase(std::remove_if(value.begin(), value.end(), ::isspace), value.end());
    
    // 在实际系统中，需要知道列的位置
    // 这里假设第一列是ID
    if (column == "id" && !record.empty()) {
        return record[0] == value;
    }
    
    return false;
}

// ProjectOp 实现
ProjectOp::ProjectOp(std::unique_ptr<Operator> child, const std::vector<std::string>& columns)
    : child_(std::move(child)), columns_(columns) {}

void ProjectOp::open() {
    child_->open();
}

bool ProjectOp::next() {
    if (child_->next()) {
        auto record = child_->getOutput();
        current_output_.clear();
        
        // 在实际系统中，需要根据列名找到对应的位置
        // 这里简化处理，只返回请求的列
        for (const auto& col : columns_) {
            // 假设列名与位置对应
            size_t index = std::stoul(col);
            if (index < record.size()) {
                current_output_.push_back(record[index]);
            }
        }
        return true;
    }
    return false;
}

void ProjectOp::close() {
    child_->close();
}

std::vector<std::string> ProjectOp::getOutput() const {
    return current_output_;
}

// CreateTableOp 实现
CreateTableOp::CreateTableOp(StorageEngine& storage, const std::string& table_name, 
                            const std::vector<std::tuple<std::string, std::string, size_t>>& columns)
    : storage_(storage), table_name_(table_name), columns_(columns), executed_(false) {}

void CreateTableOp::open() {
    // 转换列定义
    std::vector<ColumnMetadata> col_meta;
    for (const auto& col : columns_) {
        ColumnMetadata cm;
        cm.name = std::get<0>(col);
        cm.type = stringToDataType(std::get<1>(col));
        cm.length = std::get<2>(col);
        col_meta.push_back(cm);
    }
    
    // 创建表
    storage_.create_table(table_name_, col_meta);
    executed_ = true;
}

bool CreateTableOp::next() {
    if (!executed_) {
        executed_ = true;
        return true;
    }
    return false;
}

void CreateTableOp::close() {}

std::vector<std::string> CreateTableOp::getOutput() const {
    return {"Table created: " + table_name_};
}

// InsertOp 实现
InsertOp::InsertOp(StorageEngine& storage, const std::string& table_name, 
                  const std::vector<std::string>& values)
    : storage_(storage), table_name_(table_name), values_(values), executed_(false) {}

void InsertOp::open() {
    int table_id = storage_.get_table_id(table_name_);
    if (table_id < 0) {
        throw std::runtime_error("Table not found: " + table_name_);
    }
    
    // 构建记录数据
    std::ostringstream record_data;
    for (size_t i = 0; i < values_.size(); ++i) {
        if (i > 0) record_data << "|";
        record_data << values_[i];
    }
    
    // 插入记录
    storage_.insert_record(table_id, record_data.str());
    executed_ = true;
}

bool InsertOp::next() {
    if (!executed_) {
        executed_ = true;
        return true;
    }
    return false;
}

void InsertOp::close() {}

std::vector<std::string> InsertOp::getOutput() const {
    return {"1 row inserted into " + table_name_};
}

// DeleteOp 实现
DeleteOp::DeleteOp(StorageEngine& storage, const std::string& table_name, 
                  std::unique_ptr<Operator> child)
    : storage_(storage), table_name_(table_name), child_(std::move(child)) {}

void DeleteOp::open() {
    child_->open();
}

bool DeleteOp::next() {
    if (child_->next()) {
        // 在实际系统中，需要从子算子获取RID
        // 这里简化处理，假设子算子返回RID
        auto output = child_->getOutput();
        if (output.size() >= 2) {
            RID rid;
            rid.page_id = std::stoul(output[0]);
            rid.slot_id = std::stoul(output[1]);
            storage_.delete_record(rid);
            return true;
        }
    }
    return false;
}

void DeleteOp::close() {
    child_->close();
}

std::vector<std::string> DeleteOp::getOutput() const {
    return {"Record deleted"};
}

// UpdateOp 实现
UpdateOp::UpdateOp(StorageEngine& storage, const std::string& table_name, 
                  const std::vector<std::pair<std::string, std::string>>& assignments,
                  std::unique_ptr<Operator> child)
    : storage_(storage), table_name_(table_name), 
      assignments_(assignments), child_(std::move(child)) {}

void UpdateOp::open() {
    child_->open();
}

bool UpdateOp::next() {
    if (child_->next()) {
        // 在实际系统中，需要从子算子获取RID和当前值
        // 这里简化处理，假设子算子返回RID
        auto output = child_->getOutput();
        if (output.size() >= 2) {
            RID rid;
            rid.page_id = std::stoul(output[0]);
            rid.slot_id = std::stoul(output[1]);
            
            // 读取当前记录
            std::string current_data;
            if (storage_.read_record(rid, current_data)) {
                // 解析当前记录
                std::vector<std::string> fields;
                std::istringstream iss(current_data);
                std::string field;
                while (std::getline(iss, field, '|')) {
                    fields.push_back(field);
                }
                
                // 应用更新
                for (const auto& assign : assignments_) {
                    // 在实际系统中，需要知道列的位置
                    // 这里简化处理，只更新第一列
                    if (assign.first == "id" && !fields.empty()) {
                        fields[0] = assign.second;
                    }
                }
                
                // 构建新记录
                std::ostringstream new_data;
                for (size_t i = 0; i < fields.size(); ++i) {
                    if (i > 0) new_data << "|";
                    new_data << fields[i];
                }
                
                // 更新记录
                storage_.update_record(rid, new_data.str());
                return true;
            }
        }
    }
    return false;
}

void UpdateOp::close() {
    child_->close();
}

std::vector<std::string> UpdateOp::getOutput() const {
    return {"Record updated"};
}

// CreateIndexOp 实现
CreateIndexOp::CreateIndexOp(StorageEngine& storage, const std::string& index_name, 
                            const std::string& table_name, const std::string& column_name)
    : storage_(storage), index_name_(index_name), 
      table_name_(table_name), column_name_(column_name), executed_(false) {}

void CreateIndexOp::open() {
    // 在实际系统中，这里会创建索引
    // 简化处理，只打印信息
    std::cout << "Creating index " << index_name_ 
              << " on " << table_name_ << "(" << column_name_ << ")" << std::endl;
    executed_ = true;
}

bool CreateIndexOp::next() {
    if (!executed_) {
        executed_ = true;
        return true;
    }
    return false;
}

void CreateIndexOp::close() {}

std::vector<std::string> CreateIndexOp::getOutput() const {
    return {"Index created: " + index_name_};
}

// ExecutionEngine 实现
ExecutionEngine::ExecutionEngine(StorageEngine& storage) 
    : storage_(storage) {}

std::vector<std::vector<std::string>> ExecutionEngine::execute(std::unique_ptr<Operator> root) {
    std::vector<std::vector<std::string>> results;
    rows_affected_ = 0;
    
    root->open();
    while (root->next()) {
        results.push_back(root->getOutput());
        rows_affected_++;
    }
    root->close();
    
    return results;
}

} // namespace pcsql