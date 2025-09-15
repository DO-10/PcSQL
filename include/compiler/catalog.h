#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

// 数据类型枚举
enum class DataType {
    INT,
    DOUBLE,
    VARCHAR,
    CHAR,
    UNKNOWN
};

// 枚举转字符串（可选，方便调试用）
std::string dataTypeToString(DataType type);

// 列元数据
struct ColumnMetadata {
    std::string name;
    DataType type;
    size_t length = 0;
    std::vector<std::string> constraints;
};

// 表模式
struct TableSchema {
    std::vector<ColumnMetadata> columns;
    std::unordered_map<std::string, DataType> columnTypes;
};

// 索引信息
struct IndexMetadata {
    std::string tableName;
    std::string columnName;
};

// 目录（元数据管理）
class Catalog {
public:
    Catalog();

    // 表管理
    bool tableExists(const std::string& tableName) const;
    bool columnExists(const std::string& tableName, const std::string& columnName) const;
    DataType getColumnType(const std::string& tableName, const std::string& columnName) const;
    size_t getColumnCount(const std::string& tableName) const;
    const TableSchema& getTableSchema(const std::string& tableName) const;
    void addTable(const std::string& tableName, const std::vector<ColumnMetadata>& columns);
    
    // 【修改】将 stringToDataType 移动到 Catalog 类中
    static DataType stringToDataType(const std::string& typeStr);

    // 索引管理
    bool indexExists(const std::string& indexName) const;
    void addIndex(const std::string& indexName,
                  const std::string& tableName,
                  const std::string& columnName);

private:
    std::unordered_map<std::string, TableSchema> schemas_;
    std::unordered_map<std::string, IndexMetadata> indices_;
    
    static std::string toUpper(const std::string& str);
    
    void initializeBuiltinSchemas();
};