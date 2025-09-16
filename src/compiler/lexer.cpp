#include "compiler/lexer.h"
#include <cctype>
#include <stdexcept>
#include <iostream>
#include <sstream> // 用于构建错误消息

// 1. 静态成员变量的初始化
const std::unordered_set<std::string> Lexer::keywords_ = {
    "SELECT", "FROM", "WHERE", "CREATE", "TABLE", "INSERT",
    "INTO", "VALUES", "UPDATE", "DELETE", "SET", "JOIN",
    "ON", "AS", "AND", "OR", "NOT", "LIKE", "IN", "BETWEEN",
    "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET",
    "DISTINCT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
    "UNIQUE", "INDEX", "CHECK", "DEFAULT", "NULL", "IS",
    // 新增：将数据类型/常量/属性作为关键字，避免被识别为标识符
    "INT", "DOUBLE", "VARCHAR", "CHAR",
    "TIMESTAMP", "AUTO_INCREMENT", "CURRENT_TIMESTAMP",
    // 新增：DROP / IF / EXISTS 支持
    "DROP", "IF", "EXISTS"
};

// 2. 构造函数
// 初始化行号和列号
Lexer::Lexer(const std::string& text) : text_(text), pos_(0), current_line_(1), current_column_(1) {}

// 3. tokenize() 方法的实现
std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    Token current_token = getNextToken();
    while (current_token.type != TokenType::END_OF_FILE) {
        tokens.push_back(current_token);
        current_token = getNextToken();
    }
    tokens.push_back(current_token);

    // 调试输出：打印所有词法单元
    try {
        std::cout << "[Lexer] Tokens (" << tokens.size() << "):" << std::endl;
        for (size_t i = 0; i < tokens.size(); ++i) {
            const Token& t = tokens[i];
            const char* type_str = "";
            switch (t.type) {
                case TokenType::END_OF_FILE: type_str = "END_OF_FILE"; break;
                case TokenType::KEYWORD:     type_str = "KEYWORD"; break;
                case TokenType::IDENTIFIER:  type_str = "IDENTIFIER"; break;
                case TokenType::NUMBER:      type_str = "NUMBER"; break;
                case TokenType::STRING:      type_str = "STRING"; break;
                case TokenType::OPERATOR:    type_str = "OPERATOR"; break;
                case TokenType::DELIMITER:   type_str = "DELIMITER"; break;
                default:                     type_str = "UNKNOWN"; break;
            }
            std::cout << "  [" << i << "] "
                      << type_str << "('" << t.value << "')"
                      << " at " << t.line << ":" << t.column
                      << std::endl;
        }
    } catch (...) {
        // 打印不应影响主流程
    }

    return tokens;
}

// 4. 其余私有方法的实现
char Lexer::currentChar() const {
    return (pos_ < text_.length()) ? text_[pos_] : '\0';
}

// 3.1 advance()：在前进时更新行号和列号
void Lexer::advance() {
    if (pos_ < text_.length()) {
        // 如果遇到换行符，行号加1，列号重置
        if (text_[pos_] == '\n') {
            current_line_++;
            current_column_ = 1;
        } else {
            current_column_++;
        }
        pos_++;
    }
}

// 3.2 skipWhitespace()：在跳过空白符时也更新位置
void Lexer::skipWhitespace() {
    while (pos_ < text_.length() && isspace(currentChar())) {
        advance();
    }
}

// 3.3 getNextToken()：在生成Token时记录位置
Token Lexer::getNextToken() {
    skipWhitespace();
    
    size_t start_line = current_line_;
    size_t start_column = current_column_;
    
    if (pos_ >= text_.length()) {
        return {TokenType::END_OF_FILE, "EOF", start_line, start_column};
    }
    
    char current_char = currentChar();
    
    if (isalpha(current_char) || current_char == '_') {
        return getIdentifierOrKeyword();
    }
    
    if (isdigit(current_char)) {
        return getNumber();
    }
    
    if (current_char == '\'') {
        return getString();
    }
    
    // 运算符和分隔符处理，创建Token时传入位置信息
    switch (current_char) {
        case '+': advance(); return {TokenType::OPERATOR, "+", start_line, start_column};
        case '-': advance(); return {TokenType::OPERATOR, "-", start_line, start_column};
        case '*': advance(); return {TokenType::OPERATOR, "*", start_line, start_column};
        case '/': advance(); return {TokenType::OPERATOR, "/", start_line, start_column};
        case '%': advance(); return {TokenType::OPERATOR, "%", start_line, start_column};
        case '^': advance(); return {TokenType::OPERATOR, "^", start_line, start_column};
        
        case '=':
            advance();
            return {TokenType::OPERATOR, "=", start_line, start_column};
        
        case '<':
            advance();
            if (currentChar() == '=') { // <=
                advance();
                return {TokenType::OPERATOR, "<=", start_line, start_column};
            } else if (currentChar() == '>') { // <>
                advance();
                return {TokenType::OPERATOR, "<>", start_line, start_column};
            }
            return {TokenType::OPERATOR, "<", start_line, start_column};
            
        case '>':
            advance();
            if (currentChar() == '=') { // >=
                advance();
                return {TokenType::OPERATOR, ">=", start_line, start_column};
            }
            return {TokenType::OPERATOR, ">", start_line, start_column};
            
        case '!':
            advance();
            if (currentChar() == '=') { // !=
                advance();
                return {TokenType::OPERATOR, "!=", start_line, start_column};
            }
            // 抛出带有位置信息的错误
            {
                std::stringstream ss;
                ss << "Unknown operator: " << current_char << " at line " << start_line << ", column " << start_column;
                throw std::runtime_error(ss.str());
            }
            
        case ',': advance(); return {TokenType::DELIMITER, ",", start_line, start_column};
        case ';': advance(); return {TokenType::DELIMITER, ";", start_line, start_column};
        case '(': advance(); return {TokenType::DELIMITER, "(", start_line, start_column};
        case ')': advance(); return {TokenType::DELIMITER, ")", start_line, start_column};
        case '[': advance(); return {TokenType::DELIMITER, "[", start_line, start_column};
        case ']': advance(); return {TokenType::DELIMITER, "]", start_line, start_column};
        case '{': advance(); return {TokenType::DELIMITER, "{", start_line, start_column};
        case '}': advance(); return {TokenType::DELIMITER, "}", start_line, start_column};
        case '.': advance(); return {TokenType::DELIMITER, ".", start_line, start_column};
        case ':': advance(); return {TokenType::DELIMITER, ":", start_line, start_column};

        default:
            // 抛出带有位置信息的错误
            {
                std::stringstream ss;
                ss << "Unknown character: '" << current_char << "' at line " << start_line << ", column " << start_column;
                throw std::runtime_error(ss.str());
            }
    }
}

// 3.4 getIdentifierOrKeyword()：创建Token时记录位置
Token Lexer::getIdentifierOrKeyword() {
    size_t start_line = current_line_;
    size_t start_column = current_column_;
    std::string result;
    while (pos_ < text_.length() && (isalnum(currentChar()) || currentChar() == '_')) {
        result += toupper(currentChar());
        advance();
    }
    if (keywords_.find(result) != keywords_.end()) {
        return {TokenType::KEYWORD, result, start_line, start_column};
    }
    return {TokenType::IDENTIFIER, result, start_line, start_column};
}

// 3.5 getNumber()：创建Token时记录位置
Token Lexer::getNumber() {
    size_t start_line = current_line_;
    size_t start_column = current_column_;
    std::string result;
    // 整数部分
    while (pos_ < text_.length() && isdigit(currentChar())) {
        result += currentChar();
        advance();
    }
    
    // 处理小数部分
    if (currentChar() == '.') {
        result += '.';
        advance();
        while (pos_ < text_.length() && isdigit(currentChar())) {
            result += currentChar();
            advance();
        }
    }
    
    return {TokenType::NUMBER, result, start_line, start_column};
}

// 3.6 getString()：创建Token时记录位置和抛出错误
Token Lexer::getString() {
    size_t start_line = current_line_;
    size_t start_column = current_column_;
    advance(); // 跳过起始的单引号
    std::string result;
    while (pos_ < text_.length() && currentChar() != '\'') {
        // 处理转义字符
        if (currentChar() == '\\') {
            advance(); // 跳过反斜杠
            if (pos_ >= text_.length()) {
                std::stringstream ss;
                ss << "Unterminated escape sequence at line " << start_line << ", column " << start_column;
                throw std::runtime_error(ss.str());
            }
            char esc = currentChar();
            switch (esc) {
                case '\\': result.push_back('\\'); break;
                case '\'': result.push_back('\''); break;
                case 'n': result.push_back('\n'); break;
                case 't': result.push_back('\t'); break;
                default: result.push_back(esc); break;
            }
            advance();
        } else {
            result.push_back(currentChar());
            advance();
        }
    }
    if (pos_ >= text_.length()) {
        std::stringstream ss;
        ss << "Unterminated string literal at line " << start_line << ", column " << start_column;
        throw std::runtime_error(ss.str());
    }
    advance(); // 跳过结束的单引号
    return {TokenType::STRING, result, start_line, start_column};
}
