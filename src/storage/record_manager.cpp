#include "storage/record_manager.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace pcsql {

void RecordManager::compact(Page& page) {
    /*将槽放入live中排序，更新槽的位置，将分散的槽变紧凑
    不改变槽（slot）在槽表中的索引位置，只改变每个槽记录的 off；因此外部对某个 slot_id 的引用在 compact 后仍然是有效的*/
    ensure_initialized(page);//确保 page 的 header/metadata 已正确初始化
    auto& h = header(page);//取得页头引用 h，用于读写 slot_count、free_off 等字段

    // Collect live slots
    std::vector<std::pair<std::uint16_t, Slot>> live;
    live.reserve(h.slot_count);
    for (std::uint16_t i = 0; i < h.slot_count; ++i) {
        Slot* s = slot_at(page, i);//定位某个槽在内存中的位置，s指向该槽
        if (s->off >= 0 && s->len > 0) live.emplace_back(i, *s);
    }
    // Sort by offset increasing 将活记录按数据在页面中当前的偏移 off 从小到大排序。
    std::sort(live.begin(), live.end(), [](auto& a, auto& b){ return a.second.off < b.second.off; });

    // Move records to be tightly packed after Header
    std::uint16_t off = static_cast<std::uint16_t>(sizeof(Header));
    for (auto& [idx, s] : live) {
        if (static_cast<std::uint16_t>(s.off) != off) {
            // move
            std::memmove(page.data.data() + off, page.data.data() + s.off, s.len);
            Slot* cur = slot_at(page, idx);
            cur->off = static_cast<std::int16_t>(off);
        }
        off = static_cast<std::uint16_t>(off + s.len);
    }
    h.free_off = off;
    // 对单页进行碎片整理（compact/defragment）。目的是把页内所有“存活记录”紧凑地移动到页头（Header 之后），释放连续的空闲空间放在末尾。
}

RID RecordManager::insert(std::int32_t table_id, const char* data, std::size_t size) {
    //插入一条记录到指定表 table_id，返回 RID（页 id + 槽 id）
    if (size > UINT16_MAX) throw std::invalid_argument("record too large");
    // Find a page in table with enough free space, or allocate a new page
    const auto& pages = tables_.get_table_pages(table_id);//获取该表已分配的页面列表
    for (auto pid : pages) {
        Page& page = buffer_.get_page(pid);
        ensure_initialized(page);//确保页元数据存在
        std::size_t need = sizeof(Slot) + size;//计算需要的空间
        if (free_space(page) >= need) {//空闲空间足够
            auto& h = header(page);//获取header
            Slot* s = slot_at(page, h.slot_count);//s指向第一个空槽
            // Place record at h.free_off
            std::uint16_t rec_off = h.free_off;
            std::memcpy(page.data.data() + rec_off, data, size);
            s->off = static_cast<std::int16_t>(rec_off);
            s->len = static_cast<std::uint16_t>(size);
            RID rid{pid, h.slot_count};
            h.slot_count += 1;
            h.free_off = static_cast<std::uint16_t>(rec_off + size);
            buffer_.unpin_page(pid, true);
            tables_.save();//持久化表元数据变更
            return rid;
        }
        //空间不够
        buffer_.unpin_page(pid, false);
    }
    // No space found; allocate a new page for the table
    std::uint32_t new_pid = tables_.allocate_table_page(table_id, disk_);//给表分配新页

    Page& page = buffer_.get_page(new_pid);//？？？
    ensure_initialized(page);
    auto& h = header(page);
    Slot* s = slot_at(page, h.slot_count);

    std::uint16_t rec_off = h.free_off;
    std::memcpy(page.data.data() + rec_off, data, size);
    s->off = static_cast<std::int16_t>(rec_off);
    s->len = static_cast<std::uint16_t>(size);
    RID rid{new_pid, h.slot_count};
    h.slot_count += 1;
    h.free_off = static_cast<std::uint16_t>(rec_off + size);
    buffer_.unpin_page(new_pid, true);
    tables_.save();
    return rid;
}

bool RecordManager::read(const RID& rid, std::string& out) {
    Page& page = buffer_.get_page(rid.page_id);
    ensure_initialized(page);
    const auto& h = header(page);
    if (rid.slot_id >= h.slot_count) { buffer_.unpin_page(rid.page_id, false); return false; }
    const Slot* s = slot_at(page, rid.slot_id);
    //获取槽指针
    if (s->off < 0 || s->len == 0) { buffer_.unpin_page(rid.page_id, false); return false; }
    out.assign(page.data.data() + s->off, page.data.data() + s->off + s->len);
    buffer_.unpin_page(rid.page_id, false);
    return true;
}

bool RecordManager::update(const RID& rid, const char* data, std::size_t size) {
    //更新有问题待处理
    if (size > UINT16_MAX) return false;
    Page& page = buffer_.get_page(rid.page_id);
    ensure_initialized(page);
    auto& h = header(page);
    if (rid.slot_id >= h.slot_count) { buffer_.unpin_page(rid.page_id, false); return false; }
    Slot* s = slot_at(page, rid.slot_id);
    if (s->off < 0 || s->len == 0) { buffer_.unpin_page(rid.page_id, false); return false; }
    if (size <= s->len) {//如果新数据大小 size 小于等于现有记录长度 s->len，直接覆盖现有记录开始位置
        std::memcpy(page.data.data() + s->off, data, size);
        s->len = static_cast<std::uint16_t>(size);
        buffer_.unpin_page(rid.page_id, true);
        return true;
    }

    // 2) 若记录位于数据区尾部，且有足够连续空闲，则原地扩展
    if (static_cast<std::uint16_t>(s->off + s->len) == h.free_off) {
        std::size_t need_extra = size - s->len;
        if (free_space(page) >= need_extra) {
            std::memcpy(page.data.data() + s->off, data, size);
            s->len = static_cast<std::uint16_t>(size);
            h.free_off = static_cast<std::uint16_t>(s->off + s->len);
            buffer_.unpin_page(rid.page_id, true);
            return true;
        }
    }

    // 3) 无法原地扩展：执行一次压缩，获得连续尾部空闲，然后将记录搬迁到页尾
    compact(page);
    auto& h2 = header(page);
    Slot* s2 = slot_at(page, rid.slot_id);

    if (free_space(page) >= size) {
        std::uint16_t new_off = h2.free_off;
        std::memcpy(page.data.data() + new_off, data, size);
        s2->off = static_cast<std::int16_t>(new_off);
        s2->len = static_cast<std::uint16_t>(size);
        h2.free_off = static_cast<std::uint16_t>(new_off + size);
        buffer_.unpin_page(rid.page_id, true);
        return true;
    }

    // 4) 仍然不足：失败，交由上层做删除+重新插入策略
    buffer_.unpin_page(rid.page_id, false);
    return false;
}

bool RecordManager::erase(const RID& rid) {
    //需要注意槽表随时间增长会导致页尾槽区膨胀，可能需要一个专门机制回收空槽索引
    //删除不完备，需要补充
    Page& page = buffer_.get_page(rid.page_id);
    ensure_initialized(page);
    auto& h = header(page);
    if (rid.slot_id >= h.slot_count) { buffer_.unpin_page(rid.page_id, false); return false; }
    Slot* s = slot_at(page, rid.slot_id);
    if (s->off < 0 || s->len == 0) { buffer_.unpin_page(rid.page_id, false); return false; }
    s->off = -1; s->len = 0; // tombstone 标记为无效
    // optional: compact if lots of garbage; here simple heuristic
    if (free_space(page) < PAGE_SIZE / 4) {
        compact(page);
    }
    buffer_.unpin_page(rid.page_id, true);
    return true;
}

std::vector<std::pair<RID, std::string>> RecordManager::scan(std::int32_t table_id) {
    std::vector<std::pair<RID, std::string>> out;
    const auto& pages = tables_.get_table_pages(table_id);
    for (auto pid : pages) {
        Page& page = buffer_.get_page(pid);
        ensure_initialized(page);
        const auto& h = header(page);
        for (std::uint16_t i = 0; i < h.slot_count; ++i) {
            const Slot* s = slot_at(page, i);
            if (s->off >= 0 && s->len > 0) {
                out.emplace_back(RID{pid, i}, std::string(page.data.data() + s->off, s->len));
            }
        }
        buffer_.unpin_page(pid, false);
    }
    return out;//读取一个表的全部内容
}

} // namespace pcsql