#ifndef COMPILER_LEXER_LEXER_H
#define COMPILER_LEXER_LEXER_H

#include <string>
#include <vector>
#include <unordered_set>

// 1. Token 类型和结构
enum class TokenType {
    END_OF_FILE,
    KEYWORD,
    IDENTIFIER,
    NUMBER,
    STRING,
    OPERATOR,
    DELIMITER
};

struct Token {
    TokenType type;
    std::string value;
    size_t line;    // 添加行号
    size_t column;  // 添加列号
};

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

    char currentChar() const;
    void advance();
    void skipWhitespace();
    
    Token getNextToken();
    Token getIdentifierOrKeyword();
    Token getNumber();
    Token getString();
};

#endif // COMPILER_LEXER_LEXER_H