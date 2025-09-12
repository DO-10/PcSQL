#include <cassert>
#include <iostream>
#include <random>
#include <vector>

#include "storage/disk_manager.hpp"
#include "storage/buffer_manager.hpp"
#include "storage/record_manager.hpp"
#include "storage/bplus_tree.hpp"

using namespace pcsql;

int main() {
    const std::string base = "bptree_testdata";
    // cleanup
    std::filesystem::remove_all(base);
    DiskManager disk(base);
    BufferManager buf(disk, 8, Policy::LRU, false);
    TableManager tables(base);
    RecordManager rm(disk, buf, tables);

    // create table with few pages and insert records, then index them
    int32_t tid = tables.create_table("idx");
    // Default B+Tree uses int64_t keys; here keep it for baseline
    BPlusTree idx(disk, buf);
    idx.create();

    // Example: string key index
    using StrKey = FixedString<32>;
    using StrBpt = BPlusTreeT<StrKey>;
    StrBpt str_index(disk, buf);
    str_index.create();

    std::vector<RID> rids;
    rids.reserve(200);
    for (int i = 0; i < 200; ++i) {
        std::string v = "val" + std::to_string(i);
        RID rid = rm.insert(tid, v);
        rids.push_back(rid);
        bool ok2 = idx.insert(i, rid);
        assert(ok2);
    }

    // point queries
    for (int i = 0; i < 200; ++i) {
        RID got;
        bool f = idx.search(i, got);
        assert(f);
        assert(got.page_id == rids[i].page_id && got.slot_id == rids[i].slot_id);
    }

    // duplicates should fail
    {
        bool dup = idx.insert(42, rids[42]);
        assert(!dup);
    }

    // range query
    auto res = idx.range(50, 149);
    assert(res.size() == 100);
    for (int i = 0; i < 100; ++i) {
        assert(res[i].first == 50 + i);
    }

    // ---- String-key B+Tree tests ----
    {
        // Insert 120 string keys: key0000 .. key0119
        std::vector<std::pair<StrKey, RID>> kvs;
        kvs.reserve(120);
        for (int i = 0; i < 120; ++i) {
            char bufk[32];
            std::snprintf(bufk, sizeof(bufk), "key%04d", i);
            StrKey k{std::string(bufk)};
            std::string v = std::string("val_") + bufk;
            RID rid = rm.insert(tid, v);
            bool ok = str_index.insert(k, rid);
            assert(ok);
            kvs.emplace_back(k, rid);
        }
        // point queries on 0..119
        for (int i = 0; i < 120; ++i) {
            char bufk[32];
            std::snprintf(bufk, sizeof(bufk), "key%04d", i);
            StrKey k{std::string(bufk)};
            RID got{};
            bool found = str_index.search(k, got);
            assert(found);
            assert(got.page_id == kvs[i].second.page_id && got.slot_id == kvs[i].second.slot_id);
        }
        // duplicate rejects
        {
            char bufk[32];
            std::snprintf(bufk, sizeof(bufk), "key%04d", 42);
            StrKey k{std::string(bufk)};
            bool dup = str_index.insert(k, kvs[42].second);
            assert(!dup);
        }
        // range from key0030 .. key0079 inclusive -> 50 items
        StrKey low{std::string("key0030")};
        StrKey high{std::string("key0079")};
        auto r2 = str_index.range(low, high);
        assert(r2.size() == 50);
        for (int i = 0; i < 50; ++i) {
            char bufk[32];
            std::snprintf(bufk, sizeof(bufk), "key%04d", 30 + i);
            StrKey expected{std::string(bufk)};
            // compare memory
            assert(!(r2[i].first < expected) && !(expected < r2[i].first));
        }
    }

    std::cout << "B+Tree tests passed.\n";
    return 0;
}