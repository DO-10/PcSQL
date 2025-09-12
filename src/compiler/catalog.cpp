#include "compiler/catalog.h"
#include <algorithm>

Catalog::Catalog() {
    initializeBuiltinSchemas();
}

void Catalog::initializeBuiltinSchemas() {
    // 模拟一个预设的 'users' 表
    TableSchema usersSchema;
    usersSchema.columns = {
        {"id", DataType::INT, {"PRIMARY KEY"}},
        {"username", DataType::VARCHAR, {"UNIQUE", "NOT NULL"}},
        {"email", DataType::VARCHAR, {"UNIQUE"}},
        {"age", DataType::INT, {}}
    };
    for (const auto& col : usersSchema.columns) {
        usersSchema.columnTypes[col.name] = col.type;
    }
    schemas_["users"] = usersSchema;

    // 模拟一个预设的 'products' 表
    TableSchema productsSchema;
    productsSchema.columns = {
        {"product_id", DataType::INT, {"PRIMARY KEY"}},
        {"name", DataType::VARCHAR, {"NOT NULL"}},
        {"price", DataType::DOUBLE, {}}
    };
    for (const auto& col : productsSchema.columns) {
        productsSchema.columnTypes[col.name] = col.type;
    }
    schemas_["products"] = productsSchema;
}

bool Catalog::tableExists(const std::string& tableName) const {
    return schemas_.count(tableName) > 0;
}

bool Catalog::columnExists(const std::string& tableName, const std::string& columnName) const {
    if (!tableExists(tableName)) return false;
    const auto& schema = schemas_.at(tableName);
    return schema.columnTypes.count(columnName) > 0;
}

DataType Catalog::getColumnType(const std::string& tableName, const std::string& columnName) const {
    if (!columnExists(tableName, columnName)) {
        return DataType::UNKNOWN;
    }
    return schemas_.at(tableName).columnTypes.at(columnName);
}

size_t Catalog::getColumnCount(const std::string& tableName) const {
    if (!tableExists(tableName)) return 0;
    return schemas_.at(tableName).columns.size();
}

const TableSchema& Catalog::getTableSchema(const std::string& tableName) const {
    if (!tableExists(tableName)) {
        throw std::runtime_error("Table '" + tableName + "' does not exist.");
    }
    return schemas_.at(tableName);
}

void Catalog::addTable(const std::string& tableName, const std::vector<ColumnMetadata>& columns) {
    if (tableExists(tableName)) {
        throw std::runtime_error("Table '" + tableName + "' already exists.");
    }
    TableSchema newSchema;
    newSchema.columns = columns;
    for (const auto& col : columns) {
        newSchema.columnTypes[col.name] = col.type;
    }
    schemas_[tableName] = newSchema;
}
