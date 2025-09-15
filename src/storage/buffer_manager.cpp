#include "storage/buffer_manager.hpp"

#include <stdexcept>
#include <cstring>

namespace pcsql {

BufferManager::BufferManager(DiskManager& disk, std::size_t capacity, Policy policy, bool enable_logging)
    : disk_(disk), capacity_(capacity), policy_(policy), enable_logging_(enable_logging) {
    if (capacity_ == 0) throw std::invalid_argument("capacity must be > 0");
    frames_.resize(capacity_);
    used_.assign(capacity_, false);
    for (std::size_t i = 0; i < capacity_; ++i) free_list_.push_back(i);
}

Page& BufferManager::get_page(std::uint32_t page_id) {
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {//如果没找到：it 等于 page_table_.end()
        // hit
        stats_.hits++;//命中数+1
        std::size_t idx = it->second;
        Frame& f = frames_[idx];
        f.pin_count++;
        // LRU: 从替换队列移除，稍后在unpin时按最近使用重新入队；
        // FIFO: 保持原入队顺序，不做改变
        if (policy_ == Policy::LRU) {
            auto itpos = repl_pos_.find(idx);
            if (itpos != repl_pos_.end()) {
                repl_list_.erase(itpos->second);
                repl_pos_.erase(itpos);
            }
        }
        log("HIT page " + std::to_string(page_id) + " -> frame " + std::to_string(idx));
        return f.page;
    }

    stats_.misses++;//未命中
    // Need a free frame or evict one
    std::size_t idx;//被选frame索引
    if (!free_list_.empty()) {//有空闲frame
        idx = free_list_.back();
        free_list_.pop_back();
    } else {
        idx = pick_victim();//选择一个frame
    }

    Frame& f = frames_[idx];
    if (used_[idx]) {
        // evict existing page
        if (f.dirty) {
            disk_.write_page(f.page.page_id, f.page.data.data());//若脏页，写回磁盘
            stats_.flushes++;
            log("FLUSH dirty page " + std::to_string(f.page.page_id) + " before eviction");
        }
        log("EVICT page " + std::to_string(f.page.page_id) + " from frame " + std::to_string(idx));
        page_table_.erase(f.page.page_id);//删除 page_table_ 中的旧映射
        stats_.evictions++;
        // also remove from replacement structures if exists
        auto itpos = repl_pos_.find(idx);
        if (itpos != repl_pos_.end()) {
            repl_list_.erase(itpos->second);
            repl_pos_.erase(itpos);
        }//删除替换队列中的旧映射
    }

    // load from disk
    f.page.page_id = page_id;
    disk_.read_page(page_id, f.page.data.data());
    f.dirty = false;
    f.pin_count = 1; // pinned by caller
    used_[idx] = true;//填充used_
    page_table_[page_id] = idx;//填充page_table_
    log("MISS load page " + std::to_string(page_id) + " into frame " + std::to_string(idx));
    return f.page;
}

void BufferManager::unpin_page(std::uint32_t page_id, bool dirty) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) throw std::out_of_range("page not in buffer");
    Frame& f = frames_[it->second];
    if (f.pin_count == 0) throw std::logic_error("unpin on already unpinned page");
    f.pin_count--;
    f.dirty = f.dirty || dirty;
    if (f.pin_count == 0) {
        on_unpinned(it->second);
    }
}

void BufferManager::on_unpinned(std::size_t frame_idx) {
    // 加入替换队列队尾；如已存在则先移除再加入（实现LRU“最近使用”的效果）。
    auto itpos = repl_pos_.find(frame_idx);
    if (itpos != repl_pos_.end()) {
        // 只有在LRU下才更新位置；FIFO不改变顺序
        if (policy_ == Policy::LRU) {
            repl_list_.erase(itpos->second);
            repl_pos_.erase(itpos);
        } else {
            return; // FIFO 已在队列中则不重复入队
        }
    }
    repl_list_.push_back(frame_idx);
    repl_pos_[frame_idx] = std::prev(repl_list_.end());
}

std::size_t BufferManager::pick_victim() {
    // Victim from front of list; ensure available
    if (repl_list_.empty()) throw std::runtime_error("No frame available for eviction (all pinned)");
    std::size_t idx = repl_list_.front();
    repl_list_.pop_front();
    repl_pos_.erase(idx);
    return idx;
}

void BufferManager::flush_page(std::uint32_t page_id) {
    //写回单页
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return; // not in buffer
    Frame& f = frames_[it->second];
    if (f.dirty) {
        disk_.write_page(page_id, f.page.data.data());
        f.dirty = false;
        stats_.flushes++;
        log("FLUSH page " + std::to_string(page_id));
    }
}

void BufferManager::flush_all() {
    for (std::size_t i = 0; i < frames_.size(); ++i) {
        Frame& f = frames_[i];
        if (used_[i] && f.dirty) {
            disk_.write_page(f.page.page_id, f.page.data.data());
            f.dirty = false;
            stats_.flushes++;
            log("FLUSH page " + std::to_string(f.page.page_id));
        }
    }
}

} // namespace pcsql