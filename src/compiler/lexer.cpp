#include "compiler/lexer.h"
#include <cctype>
#include <stdexcept>
#include <iostream>
#include <sstream> // 用于构建错误消息
#include <algorithm>
#include <cctype> // for ::toupper

// 1. 静态成员变量的初始化
const std::unordered_set<std::string> Lexer::keywords_ = {
    "SELECT", "FROM", "WHERE", "CREATE", "TABLE", "INSERT",
    "INTO", "VALUES", "UPDATE", "DELETE", "SET", "JOIN",
    "ON", "AS", "AND", "OR", "NOT", "LIKE", "IN", "BETWEEN",
    "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET",
    "DISTINCT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
    "UNIQUE", "INDEX", "CHECK", "DEFAULT", "NULL", "IS",
    "INT", "DOUBLE", "VARCHAR", "CHAR", "TIMESTAMP", "AUTO_INCREMENT",
    "CURRENT_TIMESTAMP"
};

const std::unordered_set<char> Lexer::punctuation_ = {
    '(', ')', ',', '.'
};

const std::unordered_set<std::string> Lexer::operators_ = {
    "=", ">", "<", ">=", "<=", "!=", "+", "-", "*", "/"
};

// 2. 构造函数
Lexer::Lexer(const std::string& text) : text_(text), pos_(0), current_line_(1), current_column_(1) {}

// 3. 辅助函数
char Lexer::currentChar() const {
    if (pos_ < text_.length()) {
        return text_[pos_];
    }
    return '\0';
}

void Lexer::advance() {
    if (pos_ < text_.length()) {
        if (currentChar() == '\n') {
            current_line_++;
            current_column_ = 1;
        } else {
            current_column_++;
        }
        pos_++;
    }
}

void Lexer::skipWhitespace() {
    while (pos_ < text_.length() && isspace(currentChar())) {
        advance();
    }
}

// 4. tokenize() 方法的实现
std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    Token current_token = getNextToken();
    while (current_token.type != TokenType::END_OF_FILE) {
        tokens.push_back(current_token);
        current_token = getNextToken();
    }
    tokens.push_back(current_token); // 添加 EOF token
    return tokens;
}

// 5. 核心：获取下一个 Token
Token Lexer::getNextToken() {
    skipWhitespace();
    
    if (pos_ >= text_.length()) {
        return {TokenType::END_OF_FILE, "", current_line_, current_column_};
    }

    char c = currentChar();
    if (isalpha(c)) {
        return getIdentifierOrKeyword();
    }
    if (isdigit(c)) {
        return getNumber();
    }
    if (c == '\'') {
        return getString();
    }
    if (c == ';') {
        advance();
        return {TokenType::SEMICOLON, ";", current_line_, current_column_ - 1};
    }
    if (punctuation_.count(c) || operators_.count(std::string(1, c)) || (c == '>' && text_.length() > pos_ + 1 && text_[pos_ + 1] == '=') || (c == '<' && text_.length() > pos_ + 1 && text_[pos_ + 1] == '=')) {
        return getPunctuationOrOperator();
    }
    
    std::stringstream ss;
    ss << "Unrecognized character: '" << c << "' at line " << current_line_ << ", column " << current_column_;
    throw std::runtime_error(ss.str());
}

Token Lexer::getIdentifierOrKeyword() {
    size_t start_col = current_column_;
    std::string value;
    while (pos_ < text_.length() && (isalnum(currentChar()) || currentChar() == '_')) {
        value += currentChar();
        advance();
    }
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    if (keywords_.count(upper_value)) {
        return {TokenType::KEYWORD, upper_value, current_line_, start_col};
    }
    return {TokenType::IDENTIFIER, value, current_line_, start_col};
}

Token Lexer::getNumber() {
    size_t start_col = current_column_;
    std::string value;
    bool has_decimal = false;
    while (pos_ < text_.length() && (isdigit(currentChar()) || currentChar() == '.')) {
        if (currentChar() == '.') {
            if (has_decimal) {
                // 如果已经有小数点，则不能再有
                break;
            }
            has_decimal = true;
        }
        value += currentChar();
        advance();
    }
    
    if (has_decimal) {
        return {TokenType::DOUBLE_LITERAL, value, current_line_, start_col};
    }
    return {TokenType::INTEGER_LITERAL, value, current_line_, start_col};
}

Token Lexer::getString() {
    size_t start_col = current_column_;
    advance(); // 跳过起始的单引号
    std::string result;
    while (pos_ < text_.length() && currentChar() != '\'') {
        if (currentChar() == '\\') {
            advance(); // 跳过反斜杠
            if (pos_ >= text_.length()) {
                std::stringstream ss;
                ss << "Unterminated escape sequence at line " << current_line_ << ", column " << start_col;
                throw std::runtime_error(ss.str());
            }
            switch (currentChar()) {
                case 'n': result += '\n'; break;
                case 't': result += '\t'; break;
                case 'r': result += '\r'; break;
                case '\'': result += '\''; break;
                case '\\': result += '\\'; break;
                default:
                    std::stringstream ss;
                    ss << "Invalid escape sequence: \\" << currentChar() << " at line " << current_line_ << ", column " << current_column_;
                    throw std::runtime_error(ss.str());
            }
            advance();
        } else {
            result += currentChar();
            advance();
        }
    }
    
    if (pos_ >= text_.length() || currentChar() != '\'') {
        std::stringstream ss;
        ss << "Unterminated string literal at line " << current_line_ << ", column " << start_col;
        throw std::runtime_error(ss.str());
    }
    advance(); // 跳过结束的单引号
    return {TokenType::STRING_LITERAL, result, current_line_, start_col};
}

Token Lexer::getPunctuationOrOperator() {
    size_t start_col = current_column_;
    std::string value;
    
    // 优先匹配双字符操作符
    if (pos_ + 1 < text_.length()) {
        std::string two_char_op = text_.substr(pos_, 2);
        if (operators_.count(two_char_op)) {
            value = two_char_op;
            advance();
            advance();
            return {TokenType::OPERATOR, value, current_line_, start_col};
        }
    }
    
    // 匹配单字符符号
    value = currentChar();
    if (operators_.count(value)) {
        advance();
        return {TokenType::OPERATOR, value, current_line_, start_col};
    }
    if (punctuation_.count(currentChar())) {
        advance();
        return {TokenType::PUNCTUATION, value, current_line_, start_col};
    }

    std::stringstream ss;
    ss << "Unrecognized operator or punctuation: '" << currentChar() << "' at line " << current_line_ << ", column " << current_column_;
    throw std::runtime_error(ss.str());
}