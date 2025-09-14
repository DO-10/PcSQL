#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <map>
#include <algorithm>
#include <fstream>
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
const std::string PROMPT = "pcsql> ";
const std::string USER_FILE = "users.dat";

// 简单的分隔函数，按指定分隔符拆分字符串
static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out; std::string cur; std::istringstream iss(s);
    while (std::getline(iss, cur, delim)) out.push_back(cur);
    return out;
}

// 用户结构体
struct User {
    std::string username;
    std::string password;
};

// 用户管理类（仅文件读写与基本校验）
class UserManager {
private:
    StorageEngine* storage_ {nullptr};

    int sys_users_tid() {
        int tid = storage_->get_table_id("sys_users");
        if (tid < 0) {
            // 理论上 StorageEngine 启动时会自举创建，这里兜底
            std::vector<ColumnMetadata> cols = {
                {"user", DataType::VARCHAR, {}},
                {"password", DataType::VARCHAR, {}}
            };
            storage_->create_table("sys_users", cols);
            tid = storage_->get_table_id("sys_users");
        }
        return tid;
    }

    void ensureDefaultAdmin() {
        int tid = sys_users_tid();
        auto rows = storage_->scan_table(tid);
        bool has_admin = false;
        for (const auto& kv : rows) {
            auto fields = split(kv.second, '|');
            if (fields.size() >= 2 && fields[0] == ADMIN_USERNAME) { has_admin = true; break; }
        }
        if (!has_admin) {
            std::string row = ADMIN_USERNAME + std::string("|") + ADMIN_PASSWORD;
            (void)storage_->insert_record(tid, row);
        }
    }

public:
    explicit UserManager(StorageEngine& storage) : storage_(&storage) { ensureDefaultAdmin(); }

    bool createUser(const std::string& username, const std::string& password) {
        if (username.empty() || password.empty()) {
            std::cout << "ERROR: Username and password cannot be empty." << std::endl;
            return false;
        }
        int tid = sys_users_tid();
        auto rows = storage_->scan_table(tid);
        for (const auto& kv : rows) {
            auto fields = split(kv.second, '|');
            if (fields.size() >= 2 && fields[0] == username) {
                std::cout << "ERROR: User '" << username << "' already exists." << std::endl;
                return false;
            }
        }
        std::string row = username + std::string("|") + password;
        (void)storage_->insert_record(tid, row);
        std::cout << "Query OK, 1 row affected" << std::endl;
        return true;
    }

    bool changePassword(const std::string& username, const std::string& newPassword) {
        if (newPassword.empty()) {
            std::cout << "ERROR: New password cannot be empty." << std::endl;
            return false;
        }
        int tid = sys_users_tid();
        auto rows = storage_->scan_table(tid);
        for (const auto& kv : rows) {
            auto fields = split(kv.second, '|');
            if (fields.size() >= 2 && fields[0] == username) {
                std::string updated = username + std::string("|") + newPassword;
                (void)storage_->update_record(kv.first, updated);
                std::cout << "Query OK, 1 row affected" << std::endl;
                return true;
            }
        }
        std::cout << "ERROR: User '" << username << "' does not exist." << std::endl;
        return false;
    }

    bool authenticate(const std::string& username, const std::string& password) {
        int tid = sys_users_tid();
        auto rows = storage_->scan_table(tid);
        for (const auto& kv : rows) {
            auto fields = split(kv.second, '|');
            if (fields.size() >= 2 && fields[0] == username && fields[1] == password) return true;
        }
        return false;
    }

    bool userExists(const std::string& username) {
        int tid = sys_users_tid();
        auto rows = storage_->scan_table(tid);
        for (const auto& kv : rows) {
            auto fields = split(kv.second, '|');
            if (fields.size() >= 2 && fields[0] == username) return true;
        }
        return false;
    }

    void printUsers() {
        int tid = sys_users_tid();
        auto rows = storage_->scan_table(tid);
        std::cout << "Users in system:" << std::endl;
        for (const auto& kv : rows) {
            auto fields = split(kv.second, '|');
            if (fields.size() >= 1) std::cout << " - " << fields[0] << std::endl;
        }
    }

    void logout() { std::cout << "Logout successful." << std::endl; }
};

// 数据库系统类（精简、规整缩进与空白）
class DatabaseSystem {
private:
    StorageEngine storageEngine;
    ExecutionEngine executionEngine;
    UserManager userManager;
    bool isLoggedIn;
    std::string currentUser;

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

    // 处理SQL输入
    void handleSQLInput(const std::string& sql) {
        try {
            // 先调用编译器，完成词法/语法/语义与IR生成
            Compiler compiler;
            auto unit = compiler.compile(sql, storageEngine);

            //把编译结果交给执行引擎
            std::string out = executionEngine.execute(unit);
            if (!out.empty()) {
                std::cout << out << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Execution failed: " << e.what() << std::endl;
        }
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
        currentUser.clear();
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
        if (upperCmd.rfind("CREATE USER", 0) == 0) {
            createUserCommand(command);
        } else if (upperCmd.rfind("ALTER USER", 0) == 0) {
            alterUserCommand(command);
        } else if (upperCmd == "SHOW USERS;") {
            userManager.printUsers();
        } else if (upperCmd == "LOGOUT;") {
            handleLogout();
        } else if (upperCmd == "HELP;") {
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

public:
    DatabaseSystem()
        : storageEngine("./storage_data", 64, Policy::LRU, true),
          executionEngine(storageEngine),
          userManager(storageEngine),
          isLoggedIn(false),
          currentUser("") {}

    void run() {
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
            std::cout << PROMPT;//输出提示词
            std::getline(std::cin, input);
            size_t start = input.find_first_not_of(" \t");
            if (start == std::string::npos) continue;
            size_t end = input.find_last_not_of(" \t");
            input = input.substr(start, end - start + 1);
            if (input.empty()) continue;
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