#ifndef COMPILER_SEMANTIC_CATALOG_H
#define COMPILER_SEMANTIC_CATALOG_H

#include <string>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <algorithm> // 包含 std::transform
#include <cctype>    // 包含 std::toupper


// 枚举：数据类型
enum class DataType {
    INT,
    VARCHAR,
    DOUBLE,
    BOOLEAN,
    UNKNOWN
};

// 将字符串转换为 DataType 枚举
inline DataType stringToDataType(const std::string& typeStr) {
    std::string upperStr = typeStr;
    
    // 使用 Lambda 表达式解决重载歧义
    std::transform(upperStr.begin(), upperStr.end(), upperStr.begin(), 
                   [](unsigned char c){ return std::toupper(c); });
                   
    if (upperStr == "INT") return DataType::INT;
    if (upperStr == "VARCHAR") return DataType::VARCHAR;
    if (upperStr == "DOUBLE") return DataType::DOUBLE;
    if (upperStr == "BOOLEAN") return DataType::BOOLEAN;
    return DataType::UNKNOWN;
}


// 列的元数据
struct ColumnMetadata {
    std::string name;
    DataType type;
    std::vector<std::string> constraints;
};

// 表的模式
struct TableSchema {
    std::vector<ColumnMetadata> columns;
    std::unordered_map<std::string, DataType> columnTypes;
};

// 模式目录类，用于存储数据库元数据
class Catalog {
public:
    Catalog();

    // 检查表是否存在
    bool tableExists(const std::string& tableName) const;

    // 检查表中列是否存在
    bool columnExists(const std::string& tableName, const std::string& columnName) const;

    // 获取表模式
    const TableSchema& getTableSchema(const std::string& tableName) const;

    // 获取列的数据类型
    DataType getColumnType(const std::string& tableName, const std::string& columnName) const;
    
    // 获取表的列数
    size_t getColumnCount(const std::string& tableName) const;

    // 添加新表
    void addTable(const std::string& tableName, const std::vector<ColumnMetadata>& columns);

private:
    // 使用 unordered_map 存储表名到表模式的映射
    std::unordered_map<std::string, TableSchema> schemas_;

    // 辅助函数：初始化预设模式
    void initializeBuiltinSchemas();
};

#endif // COMPILER_SEMANTIC_CATALOG_H
