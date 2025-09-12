#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "storage/storage_engine.hpp"

using namespace pcsql;

int main() {
    StorageEngine engine{"./storage_data", 4, Policy::LRU, true};

    // 为了演示可重复运行：若已存在同名表则先删除（并回收页）
    engine.drop_table_by_name("users");

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

    // 使用 RecordManager 接口插入若干记录
    auto rid1 = engine.insert_record(tid, "alice");
    auto rid2 = engine.insert_record(tid, "bob");
    auto rid3 = engine.insert_record(tid, "charlie");
    std::cout << "Inserted RIDs: (" << rid1.page_id << "," << rid1.slot_id << ") "
              << "(" << rid2.page_id << "," << rid2.slot_id << ") "
              << "(" << rid3.page_id << "," << rid3.slot_id << ")\n";

    // 读取与更新
    std::string out;
    if (engine.read_record(rid2, out)) {
        std::cout << "Read rid2: " << out << "\n";
    }
    bool up_ok = engine.update_record(rid2, "bobby");
    std::cout << "Update rid2 -> bobby: " << (up_ok ? "OK" : "FAIL") << "\n";
    if (engine.read_record(rid2, out)) {
        std::cout << "Read rid2 after update: " << out << "\n";
    }

    // 删除
    bool del_ok = engine.delete_record(rid1);
    std::cout << "Delete rid1: " << (del_ok ? "OK" : "FAIL") << "\n";

    // 扫描
    auto rows = engine.scan_table(tid);
    std::cout << "Scan table users -> " << rows.size() << " rows\n";
    for (auto& [rid, bytes] : rows) {
        std::cout << "  (" << rid.page_id << "," << rid.slot_id << ") => " << bytes << "\n";
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