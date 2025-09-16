#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cctype>

// Shared schema types for system catalog and compiler components
// DataType definition
enum class DataType {
    INT,
    VARCHAR,
    DOUBLE,
    BOOLEAN,
    TIMESTAMP,
    UNKNOWN
};

// Helper: convert string to DataType (case-insensitive)
inline DataType stringToDataType(const std::string& typeStr) {
    std::string upperStr = typeStr;
    std::transform(upperStr.begin(), upperStr.end(), upperStr.begin(),
                   [](unsigned char c){ return static_cast<char>(std::toupper(c)); });
    if (upperStr == "INT") return DataType::INT;
    if (upperStr == "VARCHAR") return DataType::VARCHAR;
    if (upperStr == "DOUBLE") return DataType::DOUBLE;
    if (upperStr == "BOOLEAN") return DataType::BOOLEAN;
    if (upperStr == "TIMESTAMP") return DataType::TIMESTAMP;
    return DataType::UNKNOWN;
}

// Column metadata
struct ColumnMetadata {
    std::string name;
    DataType type;
    std::vector<std::string> constraints;
    std::size_t length = 0; // 新增：VARCHAR/CHAR 长度（0 表示未指定）
};

// Table schema
struct TableSchema {
    std::vector<ColumnMetadata> columns;
    std::unordered_map<std::string, DataType> columnTypes; // lower-case column name -> type
};