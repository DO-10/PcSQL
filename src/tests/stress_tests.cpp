#include <cassert>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/storage_engine.hpp"

using namespace pcsql;

static void clean_dir(const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

static uint64_t rid_key(const RID& r) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(r.page_id)) << 32) |
           static_cast<uint32_t>(r.slot_id);
}

static std::string gen_string(std::mt19937& rng, size_t len) {
    static const char alphabet[] = "abcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<int> pick(0, 25);
    std::string s;
    s.resize(len);
    for (size_t i = 0; i < len; ++i) s[i] = alphabet[pick(rng)];
    return s;
}

int main() {
    const std::string base = "./storage_stressdata";
    clean_dir(base);

    StorageEngine eng(base, /*frames*/ 8, Policy::LRU, /*verbose*/ false);

    // 创建表并预分配一些数据页，避免插入初期频繁扩容带来的干扰
    auto tid = eng.create_table("s");
    const int pre_pages = 16;
    for (int i = 0; i < pre_pages; ++i) eng.allocate_table_page(tid);

    std::mt19937 rng(123456);
    std::uniform_int_distribution<int> op_pick(0, 99);
    std::uniform_int_distribution<int> len_pick(1, 64);

    std::vector<RID> rids;
    rids.reserve(10000);
    std::unordered_map<uint64_t, std::string> kv; // RID -> value

    const int ops = 3000; // 控制运行时长
    for (int i = 0; i < ops; ++i) {
        int p = op_pick(rng);
        if (p < 50) {
            // 50% 插入
            std::string v = gen_string(rng, static_cast<size_t>(len_pick(rng)));
            RID r = eng.insert_record(tid, v);
            rids.push_back(r);
            kv[rid_key(r)] = v;
        } else if (p < 70) {
            // 20% 更新（不增长长度，确保总能成功）
            if (rids.empty()) continue;
            std::uniform_int_distribution<size_t> idx_pick(0, rids.size() - 1);
            RID r = rids[idx_pick(rng)];
            auto it = kv.find(rid_key(r));
            if (it == kv.end()) continue; // 可能已被删除
            size_t L = it->second.size();
            if (L == 0) L = 1;
            std::string nv = gen_string(rng, L);
            bool ok = eng.update_record(r, nv);
            if (ok) it->second = nv;
        } else if (p < 90) {
            // 20% 读取
            if (rids.empty()) continue;
            std::uniform_int_distribution<size_t> idx_pick(0, rids.size() - 1);
            RID r = rids[idx_pick(rng)];
            auto it = kv.find(rid_key(r));
            if (it == kv.end()) continue;
            std::string out;
            bool ok = eng.read_record(r, out);
            if (ok) assert(out == it->second);
        } else {
            // 10% 删除
            if (rids.empty()) continue;
            std::uniform_int_distribution<size_t> idx_pick(0, rids.size() - 1);
            size_t idx = idx_pick(rng);
            RID r = rids[idx];
            bool ok = eng.delete_record(r);
            if (ok) kv.erase(rid_key(r));
        }
    }

    // 最终一致性校验：全表扫描应与内存映射一致
    auto rows = eng.scan_table(tid);
    std::unordered_map<uint64_t, std::string> scan_map;
    scan_map.reserve(rows.size());
    for (auto& pr : rows) {
        scan_map[rid_key(pr.first)] = pr.second;
    }
    assert(scan_map.size() == kv.size());
    for (auto& kvp : kv) {
        auto it = scan_map.find(kvp.first);
        assert(it != scan_map.end());
        assert(it->second == kvp.second);
    }

    eng.flush_all();
    std::cout << "All stress tests passed.\n";
    return 0;
}