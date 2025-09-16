#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>

#include "storage/storage_engine.hpp"

using namespace pcsql;

// ... existing code ...

int main() {
    try {
        const std::string base = "./storage_benchmark_data";
        // 为基准测试降低日志噪声，关闭 BufferManager 的详细日志
        StorageEngine eng(base, 8, Policy::LRU, /*log=*/false);
        // 为了避免索引内部调试输出干扰计时，这里关闭索引跟踪
        eng.set_index_trace(false);

        // 如果表已存在则删除（幂等）
        if (eng.get_table_id("t_noidx") >= 0) { (void)eng.drop_table_by_name("t_noidx"); }
        if (eng.get_table_id("t_idx") >= 0)   { (void)eng.drop_table_by_name("t_idx"); }

        // 定义表结构：id INT, name VARCHAR(64)
        std::vector<ColumnMetadata> cols = {
            ColumnMetadata{"id", DataType::INT, {}, 0},
            ColumnMetadata{"name", DataType::VARCHAR, {}, 64}
        };

        int tid_noidx = eng.create_table("t_noidx", cols);
        int tid_idx   = eng.create_table("t_idx", cols);

        // 写入 1000 行：id = 1..1000, name = name{id}
        for (int i = 1; i <= 1000; ++i) {
            std::ostringstream os; os << i << '|' << "name" << i;
            (void)eng.insert_record(tid_noidx, os.str());
            (void)eng.insert_record(tid_idx,   os.str());
        }

        // 在 t_idx(id) 上创建索引
        eng.create_index("idx_t_idx_id", "t_idx", "id", /*unique=*/true);

        // 待检索键值
        const int target_id = 777;

        // 1) 无索引表：全表扫描查找 id=777
        auto t1 = std::chrono::steady_clock::now();
        auto rows_noidx = eng.scan_table(tid_noidx);
        std::string found_noidx;
        for (const auto& kv : rows_noidx) {
            const std::string& row = kv.second;
            // 解析第一列 id
            std::size_t pos_bar = row.find('|');
            std::string s_id = (pos_bar == std::string::npos) ? row : row.substr(0, pos_bar);
            int id = 0; try { id = std::stoi(s_id); } catch (...) { continue; }
            if (id == target_id) { found_noidx = row; break; }
        }
        auto t2 = std::chrono::steady_clock::now();
        auto dur_scan_us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

        // 2) 有索引表：使用索引等值查找 id=777
        auto t3 = std::chrono::steady_clock::now();
        auto rows_idx = eng.index_select_eq_int(tid_idx, /*column_index=*/0, /*key=*/target_id);
        auto t4 = std::chrono::steady_clock::now();
        auto dur_index_us = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();

        std::cout << "===== 基准结果 =====\n";
        std::cout << "数据量: 1000 行, 目标: id=" << target_id << "\n";
        std::cout << "[无索引] 全表扫描耗时: " << dur_scan_us << " us, 结果: " << (found_noidx.empty()?"<未找到>":found_noidx) << "\n";
        std::cout << "[有索引] 索引等值查找耗时: " << dur_index_us << " us, 结果: "
                  << (rows_idx.empty()?"<未找到>":rows_idx.front().second) << "\n";

        // 打印缓冲池统计，便于观察 I/O 行为差异
        eng.flush_all();
        const auto& st = eng.stats();
        std::cout << "\n[Buffer Stats] hits=" << st.hits
                  << ", misses=" << st.misses
                  << ", evictions=" << st.evictions
                  << ", flushes=" << st.flushes << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[Benchmark Error] " << e.what() << std::endl;
        return 1;
    }
}

