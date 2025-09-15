cmake_minimum_required(VERSION 3.14)
project(PcSQL CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 添加此编译选项以解决编码警告 (warning C4819)
add_compile_options(/utf-8)

# 递归查找所有源代码文件，包括存储和编译器部分
file(GLOB_RECURSE ALL_SOURCES
    ${PROJECT_SOURCE_DIR}/src/storage/*.cpp
    ${PROJECT_SOURCE_DIR}/src/compiler/*.cpp
    ${PROJECT_SOURCE_DIR}/src/main.cpp
)

# 创建一个名为 pcsql_demo 的可执行文件，将所有源文件都编译进来
add_executable(pcsql_demo ${ALL_SOURCES})

# 添加头文件搜索路径，以便编译器找到你的头文件
# 新增: 将 include 目录添加到搜索路径，以解决头文件找不到的问题
target_include_directories(pcsql_demo PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

# 如果你的项目未来需要多线程，请取消注释以下两行
# find_package(Threads REQUIRED)
# target_link_libraries(pcsql_demo Threads::Threads)
