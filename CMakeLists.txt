cmake_minimum_required(VERSION 3.14)
project(PcSQL CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Use UTF-8 on MSVC only
if(MSVC)
  add_compile_options(/utf-8)
endif()

# Collect sources
file(GLOB_RECURSE STORAGE_SOURCES CONFIGURE_DEPENDS
  ${PROJECT_SOURCE_DIR}/src/storage/*.cpp
)
file(GLOB_RECURSE COMPILER_SOURCES CONFIGURE_DEPENDS
  ${PROJECT_SOURCE_DIR}/src/compiler/*.cpp
)
file(GLOB_RECURSE EXECUTION_SOURCES CONFIGURE_DEPENDS
  ${PROJECT_SOURCE_DIR}/src/execution/*.cpp
)
# System catalog sources
file(GLOB_RECURSE SYSTEM_CATALOG_SOURCES CONFIGURE_DEPENDS
  ${PROJECT_SOURCE_DIR}/src/system_catalog/*.cpp
)

# Core library
add_library(pcsql_storage STATIC
  ${STORAGE_SOURCES}
  ${COMPILER_SOURCES}
  ${EXECUTION_SOURCES}
  ${SYSTEM_CATALOG_SOURCES}
)

target_include_directories(pcsql_storage PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

# Demo executable
add_executable(storage_demo ${PROJECT_SOURCE_DIR}/src/main.cpp)
target_link_libraries(storage_demo pcsql_storage)

# Test executables
add_executable(storage_basic_tests ${PROJECT_SOURCE_DIR}/src/tests/basic_tests.cpp)
add_executable(storage_stress_tests ${PROJECT_SOURCE_DIR}/src/tests/stress_tests.cpp)
add_executable(bptree_tests ${PROJECT_SOURCE_DIR}/src/tests/bptree_tests.cpp)

target_link_libraries(storage_basic_tests pcsql_storage)
target_link_libraries(storage_stress_tests pcsql_storage)
target_link_libraries(bptree_tests pcsql_storage)

# CTest integration
include(CTest)
enable_testing()
add_test(NAME storage_basic_tests COMMAND storage_basic_tests)
add_test(NAME storage_stress_tests COMMAND storage_stress_tests)
add_test(NAME bptree_tests COMMAND bptree_tests)

# Server executable for MySQL-compatible protocol
add_executable(pcsqld
    src/server/mysql_server.cpp
)

target_include_directories(pcsqld PRIVATE include)

target_link_libraries(pcsqld PRIVATE pcsql_storage)