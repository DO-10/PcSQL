#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "storage/storage_engine.hpp"
#include "compiler/compiler.h"
#include "execution/execution_engine.h"

using namespace pcsql;

static void clean_dir(const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

int main() {
    const std::string base = "./storage_testdata";
    clean_dir(base);

    // 1) 基础：创建表、分配页、RecordManager 基本CRUD
    {
        StorageEngine eng(base, 2, Policy::LRU, false);
        auto tid = eng.create_table("t");
        auto p1 = eng.allocate_table_page(tid);
        auto p2 = eng.allocate_table_page(tid);
        assert(p1 != p2);

        auto r1 = eng.insert_record(tid, "A");
        auto r2 = eng.insert_record(tid, "BB");
        std::string out;
        assert(eng.read_record(r1, out) && out == "A");
        assert(eng.update_record(r2, "BBBB"));
        assert(eng.read_record(r2, out) && out == "BBBB");
        assert(eng.delete_record(r1));
        auto rows = eng.scan_table(tid);
        assert(rows.size() == 1);
        eng.flush_all();
    }

    // 2) 回收验证：drop_table 后 allocate_page 应复用其中一个页
    {
        StorageEngine eng(base, 2, Policy::LRU, false);
        auto tid = eng.get_table_id("t");
        // t 表来自上一节，确保存在
        assert(tid >= 0);
        const auto& pages_before = eng.get_table_pages(tid);
        assert(!pages_before.empty());
        // 删除表并回收页
        assert(eng.drop_table_by_name("t"));
        // 分配新页应从空闲列表取，等于 pages_before 的最后一个
        auto new_pid = eng.allocate_page();
        assert(new_pid == pages_before.back());
        eng.flush_all();
    }

    // 3) BufferManager 命中/替换（容量=1，形成 miss-hit-miss）
    {
        StorageEngine eng(base, 1, Policy::LRU, false);
        auto pA = eng.allocate_page();
        auto pB = eng.allocate_page();
        // 访问 A -> miss; 访问 B -> miss+驱逐A; 再访问 A -> miss
        eng.get_page(pA); eng.unpin_page(pA, false);
        eng.get_page(pB); eng.unpin_page(pB, false);
        eng.get_page(pA); eng.unpin_page(pA, false);
        const auto& st = eng.stats();
        assert(st.misses >= 3);
        eng.flush_all();
    }

    // 4) SQL 集成：通过 Compiler+ExecutionEngine 执行 DROP TABLE
    {
        StorageEngine eng(base, 2, Policy::LRU, false);
        // 先用存储层创建一张表，供 SQL DROP 测试
        eng.create_table("sqlt");
        assert(eng.get_table_id("sqlt") >= 0);

        Compiler comp;
        ExecutionEngine exec(eng);

        // 普通 DROP TABLE，应成功删除
        auto unit1 = comp.compile("DROP TABLE sqlt;", eng);
        std::string out1 = exec.execute(unit1);
        assert(out1.find("DROP TABLE OK") != std::string::npos);
        assert(eng.get_table_id("sqlt") < 0);

        // IF EXISTS 版本：表已不存在，应跳过且不报错
        auto unit2 = comp.compile("DROP TABLE IF EXISTS sqlt;", eng);
        std::string out2 = exec.execute(unit2);
        // 输出可能是 skipped 文案
        assert(out2.find("skipped") != std::string::npos);
        eng.flush_all();
    }

    std::cout << "All basic tests passed.\n";
    return 0;
}