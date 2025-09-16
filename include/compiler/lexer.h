#ifndef COMPILER_LEXER_LEXER_H
#define COMPILER_LEXER_LEXER_H

#include <string>
#include <vector>
#include <unordered_set>

// 1. Token 类型和结构
enum class TokenType {
    IDENTIFIER,
    KEYWORD,
    INTEGER_LITERAL,
    DOUBLE_LITERAL,
    STRING_LITERAL,
    PUNCTUATION, // 用于像 ( ) , 等符号
    OPERATOR,    // 用于像 = > < >= 等操作符
    SEMICOLON,   // 特殊处理分号
    END_OF_FILE,
};

struct Token {
    TokenType type;
    std::string value;
    size_t line;    // 添加行号
    size_t column;  // 添加列号
};

// 新增：将 TokenType 转换为字符串，方便调试和报错
inline std::string tokenTypeToString(TokenType type) {
    switch (type) {
        case TokenType::IDENTIFIER: return "IDENTIFIER";
        case TokenType::KEYWORD: return "KEYWORD";
        case TokenType::INTEGER_LITERAL: return "INTEGER_LITERAL";
        case TokenType::DOUBLE_LITERAL: return "DOUBLE_LITERAL";
        case TokenType::STRING_LITERAL: return "STRING_LITERAL";
        case TokenType::PUNCTUATION: return "PUNCTUATION";
        case TokenType::OPERATOR: return "OPERATOR";
        case TokenType::SEMICOLON: return "SEMICOLON";
        case TokenType::END_OF_FILE: return "END_OF_FILE";
        default: return "UNKNOWN";
    }
}

// 2. Lexer 类的声明
class Lexer {
public:
    Lexer(const std::string& text);
    std::vector<Token> tokenize();

private:
    std::string text_;
    size_t pos_;
    size_t current_line_;   // 追踪当前行号
    size_t current_column_; // 追踪当前列号
    
    // 关键字集合
    const static std::unordered_set<std::string> keywords_;
    const static std::unordered_set<char> punctuation_;
    const static std::unordered_set<std::string> operators_;

    char currentChar() const;
    void advance();
    void skipWhitespace();
    
    Token getNextToken();
    Token getIdentifierOrKeyword();
    Token getNumber();
    Token getString();
    Token getPunctuationOrOperator();
};

#endif // COMPILER_LEXER_LEXER_H