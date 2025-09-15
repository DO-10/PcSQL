# PcSQL Storage Engine (C++)

一个极简教学用页式存储引擎，纯 C++ 实现，包含：磁盘页管理（DiskManager）、缓冲池（BufferManager，支持 LRU/FIFO）、表管理（TableManager，表到页集合映射）、以及统一封装（StorageEngine）。内置演示程序 main.cpp。

## 特性
- 固定页大小 4KB（PAGE_SIZE = 4096）
- 磁盘文件自动扩容，简单文本元数据（next_page_id、空闲页列表）
- 缓冲池替换策略：LRU / FIFO（构造时选择）
- 命中/未命中/淘汰/刷写统计（Stats：hits、misses、evictions、flushes）
- 表管理：创建/删除表、为表分配页、查询表页集合（文本持久化）

## 目录结构
```
PcSQL/
├── CMakeLists.txt
├── include/
│   ├── compiler/
│   │   ├── ir_generator.h
│   │   ├── lexer.h
│   │   ├── parser.h
│   │   ├── semantic_analyzer.h
│   │   └── catalog.h
│   └── storage/
│       ├── buffer_manager.hpp
│       ├── common.hpp
│       ├── disk_manager.hpp
│       ├── storage_engine.hpp
│       └── table_manager.hpp
└── src/
    ├── compiler/
│   │   ├── catalog.cpp
│   │   ├── ir_generator.cpp
│   │   ├── lexer.cpp
│   │   ├── parser.cpp
│   │   └── semantic_analyzer.cpp
    ├── main.cpp
    └── storage/
        ├── buffer_manager.cpp
        ├── disk_manager.cpp
        └── table_manager.cpp
```

运行期数据默认写入工作目录（或示例里使用的 ./storage_data）：
- data.db：页数据文件
- meta.json：磁盘元数据（纯文本，两行：next_page_id 与空闲页列表）
- tables.meta：表目录元数据（第一行 next_table_id；后续每行：table_id table_name page_id...）

## 快速开始

### 依赖
- g++/clang++（支持 C++17）
- 可选：CMake（3.14+）

### 方式一：直接用 g++ 构建（无需 CMake）
```bash
# 在项目根目录执行
mkdir -p build
g++ -std=c++17 -O2 -Wall -Iinclude src/storage/*.cpp src/main.cpp -o build/storage_demo
./build/storage_demo
```

### 方式二：使用 CMake
```bash
cmake -S . -B build
cmake --build build -j
./build/storage_demo
```

## 使用说明（要点）
- 选择替换策略：构造 StorageEngine 或 BufferManager 时指定 Policy::LRU 或 Policy::FIFO。
  - 例：`StorageEngine engine{"./storage_data", 4, Policy::LRU, true};`
- 页读写：`get_page(pid)` 获取页（会 pin），`unpin_page(pid, dirty)` 解 pin 并标记是否脏页，`flush_page/flush_all()` 刷盘。
- 表操作：`create_table(name)`、`drop_table_by_id/name`、`allocate_table_page(table_id)`、`get_table_pages(table_id)`。
- 统计：`engine.stats()` 返回 `Stats{hits, misses, evictions, flushes}`。

演示程序 main.cpp 会：
1) 创建表 users 并分配两页
2) 对第一页写入并再次读取
3) 打印表页列表与缓存统计

示例输出（不同环境略有差异）：
```
Created table 'users' with id: 0
Allocated pages for table users: 3, 4
MISS load page 3 into frame 3
HIT page 3 -> frame 3
Read page 3: hello users page1
Table 'users' pages (2): 3 4 
FLUSH page 3
Stats - hit:1 miss:1 evict:0 flush:1
```

## 数据与忽略项
仓库已包含 .gitignore，默认忽略：
- 构建产物与中间文件：build/、CMakeFiles/、CMakeCache.txt、cmake_install.cmake、Makefile、storage_demo、*.o、*.a、*.so 等
- 运行期数据：storage_data/
- IDE/编辑器：.idea/、.vscode/、*.swp、.DS_Store

如果需要提交示例数据，请手动将对应文件从忽略列表移除或改名到非忽略路径。

## 设计小结
- DiskManager：管理页 ID 分配/回收，二进制数据文件 + 文本元数据；
- BufferManager：维护页帧、pin 计数与替换队列；LRU 命中会移除并在 unpin 时更新队尾，FIFO 不改变顺序；
- TableManager：表名/ID 与页集合的双向映射与持久化；
- StorageEngine：统一出入口，便于后续对接 SQL 层。

## 后续工作（可选）
- 记录管理器（RecordManager）：槽式页、增删改查、顺序扫描
- drop_table 释放并回收其所属页到空闲列表
- 基础单测与压力测试