#include "compiler/catalog.h"
#include <algorithm>
#include <iostream>
#include <stdexcept>

// 辅助函数：将字符串转换为大写
std::string Catalog::toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return result;
}

// 构造函数
Catalog::Catalog() {
    initializeBuiltinSchemas(); // 初始化预设表结构
}

// 初始化内置表模式
void Catalog::initializeBuiltinSchemas() {
    TableSchema usersSchema;
    usersSchema.columns = {
        {"ID", DataType::INT, 0, {"PRIMARY KEY"}},
        {"USERNAME", DataType::VARCHAR, 255, {"UNIQUE", "NOT NULL"}},
        {"EMAIL", DataType::VARCHAR, 255, {"UNIQUE"}},
        {"AGE", DataType::INT, 0, {}}
    };
    for (const auto& col : usersSchema.columns) {
        usersSchema.columnTypes[toUpper(col.name)] = col.type;
    }
    schemas_[toUpper("users")] = usersSchema;

    // === 创建 'employees' 表模式 ===
    TableSchema employeesSchema;
    employeesSchema.columns = {
        {"id", DataType::INT, 0, {"PRIMARY KEY"}},
        {"name", DataType::VARCHAR, 255, {"NOT NULL"}},
        {"salary", DataType::DOUBLE, 0, {}}
    };
    for (const auto& col : employeesSchema.columns) {
        employeesSchema.columnTypes[toUpper(col.name)] = col.type;
    }
    schemas_[toUpper("employees")] = employeesSchema;

    std::cout << "Built-in schemas initialized." << std::endl;
}

// 检查表是否存在
bool Catalog::tableExists(const std::string& tableName) const {
    return schemas_.count(toUpper(tableName)) > 0;
}

// 检查列是否存在
bool Catalog::columnExists(const std::string& tableName, const std::string& columnName) const {
    auto t = toUpper(tableName);
    auto c = toUpper(columnName);
    if (!tableExists(t)) return false;
    const auto& schema = schemas_.at(t);
    return schema.columnTypes.count(c) > 0;
}

// 获取列的数据类型
DataType Catalog::getColumnType(const std::string& tableName, const std::string& columnName) const {
    if (!columnExists(tableName, columnName)) {
        return DataType::UNKNOWN;
    }
    return schemas_.at(toUpper(tableName)).columnTypes.at(toUpper(columnName));
}

// 获取表的列数
size_t Catalog::getColumnCount(const std::string& tableName) const {
    if (!tableExists(tableName)) return 0;
    return schemas_.at(toUpper(tableName)).columns.size();
}

// 获取表模式
const TableSchema& Catalog::getTableSchema(const std::string& tableName) const {
    if (!tableExists(tableName)) {
        throw std::runtime_error("Table '" + tableName + "' does not exist.");
    }
    return schemas_.at(toUpper(tableName));
}

// 添加新表
void Catalog::addTable(const std::string& tableName, const std::vector<ColumnMetadata>& columns) {
    auto t = toUpper(tableName);
    if (tableExists(t)) {
        throw std::runtime_error("Table '" + t + "' already exists.");
    }

    TableSchema newSchema;
    // 这里的逻辑没有问题，直接将解析器传来的带有长度信息的 ColumnMetadata 拷贝过来
    newSchema.columns = columns;
    for (const auto& col : columns) {
        newSchema.columnTypes[toUpper(col.name)] = col.type;
    }
    schemas_[t] = newSchema;
    std::cout << "Table '" << t << "' added successfully." << std::endl;
}

// 索引管理方法
bool Catalog::indexExists(const std::string& indexName) const {
    return indices_.count(toUpper(indexName)) > 0;
}

void Catalog::addIndex(const std::string& indexName,
                      const std::string& tableName,
                      const std::string& columnName) {
    auto idx = toUpper(indexName);
    auto t = toUpper(tableName);
    auto c = toUpper(columnName);

    if (indexExists(idx)) {
        std::cerr << "Error: Index '" << idx << "' already exists." << std::endl;
        return;
    }

    std::cout << "Adding index '" << idx
              << "' on table '" << t
              << "' column '" << c << "' to catalog." << std::endl;

    indices_[idx] = {t, c};
}

// 数据类型字符串转枚举
DataType Catalog::stringToDataType(const std::string& typeStr) {
    // 【修改】直接调用 toUpper，不再需要 Catalog::
    std::string upperStr = toUpper(typeStr);
    
    if (upperStr == "INT") return DataType::INT;
    if (upperStr == "DOUBLE") return DataType::DOUBLE;

    // 处理带括号的类型，如 VARCHAR(10) 和 CHAR(10)
    size_t pos = upperStr.find('(');
    std::string baseType = (pos == std::string::npos) ? upperStr : upperStr.substr(0, pos);

    if (baseType == "VARCHAR") return DataType::VARCHAR;
    if (baseType == "CHAR") return DataType::CHAR;

    return DataType::UNKNOWN;
}