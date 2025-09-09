cmake_minimum_required(VERSION 3.14)
project(PcSQL_Storage CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${PROJECT_SOURCE_DIR}/include)

file(GLOB_RECURSE STORAGE_SOURCES
    ${PROJECT_SOURCE_DIR}/src/storage/*.cpp
)

add_library(pcsql_storage ${STORAGE_SOURCES})
# headers are included via include_directories

add_executable(storage_demo ${PROJECT_SOURCE_DIR}/src/main.cpp)

# If pthread needed later, link it here
# find_package(Threads REQUIRED)
# target_link_libraries(pcsql_storage Threads::Threads)

target_link_libraries(storage_demo pcsql_storage)