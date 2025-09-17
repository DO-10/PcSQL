#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <map>
#include <algorithm>
#include <fstream>
// #include <windows.h>
#include <filesystem>

#include "storage/storage_engine.hpp"
#include "compiler/lexer.h"
#include "compiler/parser.h"
#include "compiler/semantic_analyzer.h"
#include "compiler/ir_generator.h"
#include "system_catalog/types.hpp"
#include "execution/execution_engine.h"
#include "execution/execution_engine.h"
#include "compiler/compiler.h"

using namespace pcsql;

// 常量定义
const std::string ADMIN_USERNAME = "admin";
const std::string ADMIN_PASSWORD = "admin123";
const std::string VERSION = "1.0.0";

const std::string USER_FILE = "users.dat";

// 用户结构体
struct User {
    std::string username;
    std::string password;
};

// 用户管理类
class UserManager {
private:
    std::map<std::string, User> users;

    void loadUsers() {
        std::ifstream file(USER_FILE);
        if (file.is_open()) {
            std::cout << "Loading users from " << USER_FILE << "..." << std::endl;
            std::string line;
            while (std::getline(file, line)) {
                size_t sep = line.find(':');
                if (sep != std::string::npos) {
                    std::string username = line.substr(0, sep);
                    std::string password = line.substr(sep + 1);
                    users[username] = {username, password};
                    std::cout << "Loaded user: " << username << std::endl;
                }
            }
            file.close();
        } else {
            std::cout << "User file not found. Creating new user database..." << std::endl;
        }
        
        // 如果文件为空或不存在，添加默认管理员
        if (users.empty()) {
            std::cout << "Adding default admin user..." << std::endl;
            users[ADMIN_USERNAME] = {ADMIN_USERNAME, ADMIN_PASSWORD};
            saveUsers();
        }
    }

    void saveUsers() {
        std::ofstream file(USER_FILE);
        if (file.is_open()) {
            std::cout << "Saving users to " << USER_FILE << "..." << std::endl;
            for (const auto& pair : users) {
                file << pair.first << ":" << pair.second.password << std::endl;
                std::cout << "Saved user: " << pair.first << std::endl;
            }
            file.close();
            std::cout << "User database saved successfully." << std::endl;
        } else {
            std::cout << "ERROR: Could not save user database!" << std::endl;
        }
    }

public:
    UserManager() {
        std::cout << "Initializing User Manager..." << std::endl;
        loadUsers();
        std::cout << "User Manager initialized with " << users.size() << " users." << std::endl;
         std::cout << "Current path: " << std::filesystem::current_path() << std::endl;
    }

    bool createUser(const std::string& username, const std::string& password) {
        if (userExists(username)) {
            std::cout << "ERROR: User '" << username << "' already exists." << std::endl;
            return false;
        }

        if (username.empty() || password.empty()) {
            std::cout << "ERROR: Username and password cannot be empty." << std::endl;
            return false;
        }

        users[username] = {username, password};
        saveUsers();
        std::cout << "Query OK, 1 row affected" << std::endl;
        return true;
    }

    bool changePassword(const std::string& username, const std::string& newPassword) {
        if (!userExists(username)) {
            std::cout << "ERROR: User '" << username << "' does not exist." << std::endl;
            return false;
        }

        if (newPassword.empty()) {
            std::cout << "ERROR: New password cannot be empty." << std::endl;
            return false;
        }

        users[username].password = newPassword;
        saveUsers();
        std::cout << "Query OK, 1 row affected" << std::endl;
        return true;
    }

    bool authenticate(const std::string& username, const std::string& password) {
        auto it = users.find(username);
        if (it != users.end() && it->second.password == password) {
            return true;
        }
        return false;
    }

    bool userExists(const std::string& username) {
        return users.find(username) != users.end();
    }

    void printUsers() {
    // 使用当前数据库名称或默认名称
    std::string dbName = "system"; // 可以根据需要修改这个名称
    
    // 计算最大用户名长度
    size_t maxUsernameLength = dbName.length() + 12; // "Users_in_".length() + dbName.length()
    for (const auto& pair : users) {
        if (pair.first.length() > maxUsernameLength) {
            maxUsernameLength = pair.first.length();
        }
    }
    
    // 构建表头
    std::string header = "| Users_in_" + dbName;
    header.append(maxUsernameLength - (dbName.length() + 10) + 1, ' '); // 填充空格对齐
    header += "|";
    
    // 构建分隔线
    std::string separator = "+-";
    separator.append(maxUsernameLength + 2, '-');
    separator += "+";
    
    // 输出表格
    std::cout << separator << std::endl;
    std::cout << header << std::endl;
    std::cout << separator << std::endl;
    
    for (const auto& pair : users) {
        std::cout << "| " << pair.first;
        // 填充空格使所有行对齐
        size_t padding = maxUsernameLength - pair.first.length() + 1;
        std::cout << std::string(padding, ' ') << "|" << std::endl;
    }
    
    std::cout << separator << std::endl;
    std::cout << users.size() << " rows in set" << std::endl;
}

    void logout() {
        std::cout << "Logout successful." << std::endl;
    }
};

// 数据库系统类
class DatabaseSystem {
private:
    UserManager userManager;
    bool isLoggedIn;
    std::string currentUser;
    StorageEngine storageEngine;

    void showWelcome() {
        std::cout << "==========================================" << std::endl;
        std::cout << "Welcome to the Mini Database System " << VERSION << std::endl;
        std::cout << "==========================================" << std::endl;
    }

    void showHelp() {
        std::cout << "\nAvailable commands:" << std::endl;
        std::cout << "  CREATE USER 'username' IDENTIFIED BY 'password';" << std::endl;
        std::cout << "  ALTER USER 'username' IDENTIFIED BY 'newpassword';" << std::endl;
        std::cout << "  SHOW USERS;" << std::endl;
        std::cout << "  LOGOUT;" << std::endl;
        std::cout << "  EXIT;" << std::endl;
        std::cout << "  QUIT;" << std::endl;
        std::cout << "  HELP;" << std::endl;
        std::cout << "  SQL;          - Enter SQL mode for database operations" << std::endl;
        std::cout << std::endl;
    }

    void showUserHelp() {
        std::cout << "\nAvailable user commands:" << std::endl;
        std::cout << "  ALTER USER 'username' IDENTIFIED BY 'newpassword';" << std::endl;
        std::cout << "  LOGOUT;" << std::endl;
        std::cout << "  EXIT;" << std::endl;
        std::cout << "  HELP;" << std::endl;
        std::cout << "  SQL;          - Enter SQL mode for database operations" << std::endl;
        std::cout << std::endl;
    }

    // 这是一个函数，用于封装和测试整个SQL编译过程
    void test_sql_compiler(const std::string& sql_query) {
        std::cout << "\n--- Testing SQL Compiler with Query: \"" << sql_query << "\" ---\n";
        try {
            // 1. 词法分析
            Lexer lexer(sql_query);
            std::vector<Token> tokens = lexer.tokenize();
            std::cout << "Tokenization successful! Tokens count: " << tokens.size() << "\n";

            // 2. 语法分析
            Parser parser(tokens);
            std::unique_ptr<ASTNode> ast = parser.parse();
            std::cout << "Parsing successful! AST generated.\n";
            
            // 3. 语义分析
            Catalog catalog; // 创建一个目录实例
            SemanticAnalyzer semantic_analyzer(catalog);
            semantic_analyzer.analyze(ast, tokens);
            std::cout << "Semantic analysis successful!\n";

            // 4. 中间代码生成
            IRGenerator ir_generator;
            std::vector<Quadruplet> ir_code = ir_generator.generate(ast);
            std::cout << "IR generation successful! Quadruplets:\n";
            for (const auto& q : ir_code) {
                std::cout << "  (" << q.op << ", " << q.arg1 << ", " << q.arg2 << ", " << q.result << ")\n";
            }

        } catch (const std::runtime_error& e) {
            std::cerr << "Compilation failed: " << e.what() << "\n";
        }
        std::cout << "--- End of SQL Compiler Test ---\n";
    }

    void handleSQLInput(const std::string& sql) {
        // 调用 SQL 编译器测试函数
        test_sql_compiler(sql);
    }

    bool handleLogin() {
        std::string username, password;
        
        std::cout << "Enter username: ";
        std::getline(std::cin, username);
        
        std::cout << "Enter password: ";
        std::getline(std::cin, password);

        if (userManager.authenticate(username, password)) {
            currentUser = username;
            isLoggedIn = true;
            std::cout << "Login successful! Welcome, " << username << "!" << std::endl;
            std::cout << "==========================================" << std::endl;
            return true;
        } else {
            std::cout << "ERROR: Access denied for user '" << username << "'" << std::endl;
            return false;
        }
    }

    void handleLogout() {
        userManager.logout();
        currentUser = "";
        isLoggedIn = false;
        std::cout << "You have been logged out." << std::endl;
        std::cout << "==========================================" << std::endl;
    }

    void createUserCommand(const std::string& input) {
        size_t userPos = input.find("USER");
        if (userPos == std::string::npos) {
            std::cout << "ERROR: Syntax error near 'USER'" << std::endl;
            return;
        }

        size_t identifiedPos = input.find("IDENTIFIED BY");
        if (identifiedPos == std::string::npos) {
            std::cout << "ERROR: Syntax error near 'IDENTIFIED BY'" << std::endl;
            return;
        }

        // 提取用户名
        size_t userStart = input.find('\'', userPos);
        size_t userEnd = input.find('\'', userStart + 1);
        if (userStart == std::string::npos || userEnd == std::string::npos) {
            std::cout << "ERROR: Invalid username format" << std::endl;
            return;
        }
        std::string username = input.substr(userStart + 1, userEnd - userStart - 1);

        // 提取密码
        size_t passStart = input.find('\'', identifiedPos);
        size_t passEnd = input.find('\'', passStart + 1);
        if (passStart == std::string::npos || passEnd == std::string::npos) {
            std::cout << "ERROR: Invalid password format" << std::endl;
            return;
        }
        std::string password = input.substr(passStart + 1, passEnd - passStart - 1);

        userManager.createUser(username, password);
    }

    void alterUserCommand(const std::string& input) {
        size_t userPos = input.find("USER");
        if (userPos == std::string::npos) {
            std::cout << "ERROR: Syntax error near 'USER'" << std::endl;
            return;
        }

        size_t identifiedPos = input.find("IDENTIFIED BY");
        if (identifiedPos == std::string::npos) {
            std::cout << "ERROR: Syntax error near 'IDENTIFIED BY'" << std::endl;
            return;
        }

        // 提取用户名
        size_t userStart = input.find('\'', userPos);
        size_t userEnd = input.find('\'', userStart + 1);
        if (userStart == std::string::npos || userEnd == std::string::npos) {
            std::cout << "ERROR: Invalid username format" << std::endl;
            return;
        }
        std::string username = input.substr(userStart + 1, userEnd - userStart - 1);

        // 提取新密码
        size_t passStart = input.find('\'', identifiedPos);
        size_t passEnd = input.find('\'', passStart + 1);
        if (passStart == std::string::npos || passEnd == std::string::npos) {
            std::cout << "ERROR: Invalid password format" << std::endl;
            return;
        }
        std::string newPassword = input.substr(passStart + 1, passEnd - passStart - 1);

        // 权限检查：只有管理员或用户自己可以修改密码
        if (currentUser != ADMIN_USERNAME && currentUser != username) {
            std::cout << "ERROR: Only admin or the user themselves can change passwords." << std::endl;
            return;
        }

        userManager.changePassword(username, newPassword);
    }

    void handleCommand(const std::string& command) {
        std::string upperCmd = command;
        std::transform(upperCmd.begin(), upperCmd.end(), upperCmd.begin(), ::toupper);

        if (upperCmd.find("CREATE USER") == 0) {
            if (currentUser == ADMIN_USERNAME) {
                createUserCommand(command);
            } else {
                std::cout << "ERROR: Only admin can create users." << std::endl;
            }
        } else if (upperCmd.find("ALTER USER") == 0) {
            alterUserCommand(command);
        } else if (upperCmd == "SHOW USERS;") {
            if (currentUser == ADMIN_USERNAME) {
                userManager.printUsers();
            } else {
                std::cout << "ERROR: Only admin can view users." << std::endl;
            }
        } else if (upperCmd == "LOGOUT;") {
            handleLogout();
            while (!isLoggedIn) {
                if (!handleLogin()) {
                    std::cout << "Please try again." << std::endl;
                }
            }
        } else if (upperCmd == "HELP;" || upperCmd == "\\H") {
            if (currentUser == ADMIN_USERNAME) {
                showHelp();
            } else {
                showUserHelp();
            }
        } else if (upperCmd == "EXIT;" || upperCmd == "QUIT;") {
            std::cout << "Bye" << std::endl;
            exit(0);
        } else if (upperCmd == "SQL;") {
            std::cout << "Entering SQL mode. Type 'EXIT;' to return to command mode." << std::endl;
            std::string sql;
            while (true) {
                std::cout << "SQL> ";
                std::getline(std::cin, sql);
                
                size_t start = sql.find_first_not_of(" \t");
                if (start == std::string::npos) continue;
                size_t end = sql.find_last_not_of(" \t");
                sql = sql.substr(start, end - start + 1);
                
                if (sql.empty()) continue;
                
                std::string upperSql = sql;
                std::transform(upperSql.begin(), upperSql.end(), upperSql.begin(), ::toupper);
                
                if (upperSql == "EXIT;") {
                    std::cout << "Exiting SQL mode." << std::endl;
                    break;
                }
                
                handleSQLInput(sql);
            }
        } else {
            handleSQLInput(command);
        }
    }

    void testStorageEngine() {
        std::cout << "--- Testing Storage Engine ---\n";
        StorageEngine engine{"./storage_data", 4, Policy::LRU, true};

        // 演示：创建表并为表分配两页
        auto tid = engine.create_table("users");
        std::cout << "Created table 'users' with id: " << tid << "\n";
        auto p1 = engine.allocate_table_page(tid);
        auto p2 = engine.allocate_table_page(tid);
        std::cout << "Allocated pages for table users: " << p1 << ", " << p2 << "\n";

        // 写入第一页
        {
            Page& page = engine.get_page(p1);
            const char* msg = "hello users page1";
            const size_t msg_len = std::strlen(msg);
            
            // 增加边界检查，并复制时包含空终止符
            if (msg_len + 1 <= page.data.size()) {
                std::memcpy(page.data.data(), msg, msg_len + 1);
                std::cout << "Successfully wrote to page " << p1 << "\n";
            } else {
                std::cerr << "Error: Page buffer is too small to write message.\n";
            }
            engine.unpin_page(p1, true);
        }

        // 再次读取并打印
        {
            Page& page = engine.get_page(p1);
            // 读取时使用字符串构造函数，直到遇到空终止符
            std::string read_msg(page.data.data());
            std::cout << "Read page " << p1 << ": " << read_msg << "\n";
            engine.unpin_page(p1, false);
        }


        
        // 遍历表页
        const auto& pages = engine.get_table_pages(tid);
        std::cout << "Table 'users' pages (" << pages.size() << "): ";
        for (auto pid : pages) std::cout << pid << ' ';
        std::cout << "\n";

        engine.flush_all();
        const auto& st = engine.stats();
        std::cout << "Stats - hit:" << st.hits << " miss:" << st.misses << " evict:" << st.evictions << " flush:" << st.flushes << "\n";
        std::cout << "--- End of Storage Engine Test ---\n";
    }

public:
    // DatabaseSystem() : isLoggedIn(false), currentUser(""), storageEngine("./storage_data", 64, Policy::LRU, true) {
    //     SetConsoleOutputCP(65001);
    // }
    
 // 构造函数：初始化状态为未登录
    DatabaseSystem() 
        : isLoggedIn(false), 
          currentUser(""), 
          storageEngine("./storage_data", 64, Policy::LRU, true) 
    {
        // SetConsoleOutputCP(65001); // 如果有中文输出可取消注释
    }

    void run() {
       
        // 测试存储引擎
        testStorageEngine();


        showWelcome();

        while (!isLoggedIn) {
            if (!handleLogin()) {
                std::cout << "Please try again." << std::endl;
            }
        }

        if (currentUser == ADMIN_USERNAME) {
            showHelp();
        } else {
            showUserHelp();
        }

        std::string input;
        while (true) {
            std::cout << PROMPT;
            std::getline(std::cin, input);

            // 去除首尾空白字符
            size_t start = input.find_first_not_of(" \t");
            if (start == std::string::npos) continue;
            size_t end = input.find_last_not_of(" \t");
            input = input.substr(start, end - start + 1);

            if (input.empty()) continue;

            // 确保命令以分号结尾
            if (input.back() != ';') {
                std::cout << "ERROR: Commands must end with a semicolon (;)" << std::endl;
                continue;
            }

            handleCommand(input);
        }
    }
};

int main() {
    try {
        DatabaseSystem system;
        system.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}

    