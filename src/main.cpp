#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "storage/storage_engine.hpp"

using namespace pcsql;

int main() {
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
        std::memcpy(page.data.data(), msg, std::strlen(msg));
        engine.unpin_page(p1, true);
    }

    // 再次读取并打印
    {
        Page& page = engine.get_page(p1);
        std::cout << "Read page " << p1 << ": " << std::string(page.data.data(), 18) << "\n";
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
    return 0;
}