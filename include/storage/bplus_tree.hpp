#pragma once
#include <cstdint>
#include <vector>
#include <utility>
#include <limits>
#include <type_traits>
#include <cstring>
#include <string>
#include <functional>

#include "storage/buffer_manager.hpp"
#include "storage/disk_manager.hpp"
#include "storage/record_manager.hpp" // for RID

namespace pcsql {

// Helper: fixed-size string key
template <std::size_t N>
struct FixedString {
    char data[N]{};
    FixedString() = default;
    explicit FixedString(const std::string& s) {
        std::size_t m = s.size() < N ? s.size() : N;
        std::memcpy(data, s.data(), m);
        if (m < N) std::memset(data + m, 0, N - m);
    }
};

template <std::size_t N>
inline bool operator<(const FixedString<N>& a, const FixedString<N>& b) {
    return std::memcmp(a.data, b.data, N) < 0;
}

template <typename Key, typename Comparator = std::less<Key>>
class BPlusTreeT {
    static_assert(std::is_trivially_copyable<Key>::value, "Key must be trivially copyable");
public:
    explicit BPlusTreeT(DiskManager& disk, BufferManager& buffer)
        : disk_(disk), buffer_(buffer) {}

    // Create a new empty B+Tree and return the root page id (persist this in your catalog)
    std::uint32_t create();

    // Open an existing tree from root page id
    void open(std::uint32_t root_id) { root_ = root_id; }

    std::uint32_t root() const { return root_; }

    // Insert unique key -> rid. Returns false if key already exists.
    bool insert(const Key& key, const RID& rid);

    // Exact search
    bool search(const Key& key, RID& out) const;

    // Range [low, high]
    std::vector<std::pair<Key, RID>> range(const Key& low, const Key& high) const;

    // Erase by key (unique). Returns false if key not exists.
    bool erase(const Key& key);

private:
    struct NodeHdr {
        std::uint8_t is_leaf{1};
        std::uint8_t reserved{0};
        std::uint16_t count{0};
        std::uint32_t parent{std::numeric_limits<std::uint32_t>::max()};
        std::uint32_t next{std::numeric_limits<std::uint32_t>::max()}; // leaf sibling
        std::uint32_t leftmost{std::numeric_limits<std::uint32_t>::max()}; // internal only
    }; // 16 bytes

    struct LeafEntry { // sizeof(Key) + 8 bytes
        Key key;
        std::uint32_t page_id;
        std::uint16_t slot_id;
        std::uint16_t pad{0};
    };

    struct InterEntry { // sizeof(Key) + 8 bytes
        Key key;
        std::uint32_t child; // right child pointer
        std::uint32_t pad{0};
    };

    static constexpr std::size_t HEADER_SZ = sizeof(NodeHdr);
    static constexpr std::size_t LEAF_ENTRY_SZ = sizeof(LeafEntry);
    static constexpr std::size_t INTER_ENTRY_SZ = sizeof(InterEntry);
    static constexpr std::size_t LEAF_CAP() { return (PAGE_SIZE - HEADER_SZ) / LEAF_ENTRY_SZ; }
    static constexpr std::size_t INTER_CAP() { return (PAGE_SIZE - HEADER_SZ) / INTER_ENTRY_SZ; }

    static NodeHdr& hdr(Page& p) { return *reinterpret_cast<NodeHdr*>(p.data.data()); }
    static const NodeHdr& hdr(const Page& p) { return *reinterpret_cast<const NodeHdr*>(p.data.data()); }

    static LeafEntry* leaf_entries(Page& p) { return reinterpret_cast<LeafEntry*>(p.data.data() + HEADER_SZ); }
    static const LeafEntry* leaf_entries(const Page& p) { return reinterpret_cast<const LeafEntry*>(p.data.data() + HEADER_SZ); }

    static InterEntry* inter_entries(Page& p) { return reinterpret_cast<InterEntry*>(p.data.data() + HEADER_SZ); }
    static const InterEntry* inter_entries(const Page& p) { return reinterpret_cast<const InterEntry*>(p.data.data() + HEADER_SZ); }

    // helpers
    std::uint32_t find_leaf(const Key& key) const;
    bool insert_in_leaf(Page& leaf, std::uint32_t leaf_id, const Key& key, const RID& rid);
    void split_leaf_and_insert(Page& leaf, std::uint32_t leaf_id, const Key& key, const RID& rid);

    void insert_in_parent(std::uint32_t left_id, const Key& key, std::uint32_t right_id);
    bool insert_in_internal(Page& page, std::uint32_t pid, const Key& key, std::uint32_t right_id);
    void split_internal_and_insert(Page& page, std::uint32_t pid, const Key& key, std::uint32_t right_id);

    int leaf_lower_bound(const Page& leaf, const Key& key) const;
    int inter_child_index(const Page& inter, const Key& key) const;

    inline bool eq(const Key& a, const Key& b) const { return !comp_(a, b) && !comp_(b, a); }

    // deletion helpers
    int find_child_slot(const Page& parent, std::uint32_t child_id) const;
    void remove_child_at(Page& parent, std::uint32_t parent_id, int child_slot);

private:
    DiskManager& disk_;
    BufferManager& buffer_;
    std::uint32_t root_{std::numeric_limits<std::uint32_t>::max()};
    Comparator comp_{};
};

// ============ implementation ============

template <typename Key, typename Comparator>
std::uint32_t BPlusTreeT<Key, Comparator>::create() {//创建B+树
    std::uint32_t root = disk_.allocate_page();//磁盘分配页，返回页id，->root
    Page& p = buffer_.get_page(root);//向buffer索要root页
    auto& h = hdr(p);
    h.is_leaf = 1; h.reserved = 0; h.count = 0; h.parent = std::numeric_limits<std::uint32_t>::max();
    h.next = std::numeric_limits<std::uint32_t>::max(); h.leftmost = std::numeric_limits<std::uint32_t>::max();
    buffer_.unpin_page(root, true);//脏页写回
    root_ = root;//全局变量root_为根索引
    return root_;
}

template <typename Key, typename Comparator>
std::uint32_t BPlusTreeT<Key, Comparator>::find_leaf(const Key& key) const {
    //找到key所在的子叶
    std::uint32_t pid = root_;
    while (true) {
        Page& p = const_cast<BufferManager&>(buffer_).get_page(pid);
        const auto& h = hdr(p);
        if (h.is_leaf) {
            const_cast<BufferManager&>(buffer_).unpin_page(pid, false);
            return pid;
        }
        int idx = inter_child_index(p, key);
        std::uint32_t child;
        if (idx < 0) {
            child = h.leftmost;
        } else {
            child = inter_entries(p)[idx].child;
        }
        const_cast<BufferManager&>(buffer_).unpin_page(pid, false);
        pid = child;
    }
}

template <typename Key, typename Comparator>
int BPlusTreeT<Key, Comparator>::leaf_lower_bound(const Page& leaf, const Key& key) const {
    const auto& h = hdr(leaf);
    const LeafEntry* es = leaf_entries(leaf);
    int l = 0, r = static_cast<int>(h.count);
    while (l < r) {
        int m = (l + r) / 2;
        if (comp_(es[m].key, key)) l = m + 1; else r = m;
    }
    return l;
}

template <typename Key, typename Comparator>
int BPlusTreeT<Key, Comparator>::inter_child_index(const Page& inter, const Key& key) const {
    const auto& h = hdr(inter);
    const InterEntry* es = inter_entries(inter);
    int l = 0, r = static_cast<int>(h.count);//h.count = 内部节点里有多少个 InterEntry
    // find rightmost i where es[i].key <= key
    while (l < r) {//我们要在 [0, h.count) 之间二分查找 l左端，r右端， m中间检测点
        int m = (l + r) / 2;
        if (!comp_(key, es[m].key)) l = m + 1; else r = m; // !(key < es[m]) -> es[m] <= key !comp_(key, es[m].key) 等价于 key >= es[m].key
    }
    return l - 1; // -1 means go to leftmost l 最终会停在 第一个 es[i].key > key 的位置
}

template <typename Key, typename Comparator>
bool BPlusTreeT<Key, Comparator>::search(const Key& key, RID& out) const {
    if (root_ == std::numeric_limits<std::uint32_t>::max()) return false;
    std::uint32_t leaf_id = find_leaf(key);
    Page& p = const_cast<BufferManager&>(buffer_).get_page(leaf_id);
    const auto& h = hdr(p);
    int i = leaf_lower_bound(p, key);
    bool ok = (i < h.count) && eq(leaf_entries(p)[i].key, key);
    if (ok) {
        const auto& e = leaf_entries(p)[i];
        out.page_id = e.page_id;
        out.slot_id = e.slot_id;
    }
    const_cast<BufferManager&>(buffer_).unpin_page(leaf_id, false);
    return ok;
}

template <typename Key, typename Comparator>
std::vector<std::pair<Key, RID>> BPlusTreeT<Key, Comparator>::range(const Key& low, const Key& high) const {
    std::vector<std::pair<Key, RID>> res;
    if (root_ == std::numeric_limits<std::uint32_t>::max()) return res;
    std::uint32_t leaf_id = find_leaf(low);
    while (leaf_id != std::numeric_limits<std::uint32_t>::max()) {
        Page& p = const_cast<BufferManager&>(buffer_).get_page(leaf_id);
        const auto& h = hdr(p);
        const LeafEntry* es = leaf_entries(p);
        for (int i = leaf_lower_bound(p, low); i < h.count; ++i) {
            if (comp_(high, es[i].key)) { break; }
            if (comp_(es[i].key, low)) continue;
            RID r{es[i].page_id, es[i].slot_id};
            res.emplace_back(es[i].key, r);
        }
        // cache next and stop condition BEFORE unpin to avoid accessing invalidated memory
        std::uint32_t next = h.next;
        bool stop_after = false;
        if (h.count == 0) {
            stop_after = true;
        } else {
            const Key& last_key = es[h.count - 1].key;
            stop_after = comp_(high, last_key); // last_key > high
        }
        const_cast<BufferManager&>(buffer_).unpin_page(leaf_id, false);
        if (stop_after) break;
        leaf_id = next;
    }
    return res;
}

template <typename Key, typename Comparator>
bool BPlusTreeT<Key, Comparator>::insert(const Key& key, const RID& rid) {
    if (root_ == std::numeric_limits<std::uint32_t>::max()) create();
    std::uint32_t leaf_id = find_leaf(key);
    Page& leaf = buffer_.get_page(leaf_id);
    if (insert_in_leaf(leaf, leaf_id, key, rid)) {//最简单的情况，直接插入
        buffer_.unpin_page(leaf_id, true);
        return true;
    }
    // Not inserted: either duplicate or leaf full. Check duplicate first.
    {
        auto& h = hdr(leaf);
        int pos = leaf_lower_bound(leaf, key);
        const LeafEntry* es = leaf_entries(leaf);
        if (pos < h.count && eq(es[pos].key, key)) {//检查重复键
            buffer_.unpin_page(leaf_id, false);
            return false;
        }
    }
    // Need to split
    split_leaf_and_insert(leaf, leaf_id, key, rid);
    buffer_.unpin_page(leaf_id, true);
    return true;
}

template <typename Key, typename Comparator>
bool BPlusTreeT<Key, Comparator>::insert_in_leaf(Page& leaf, std::uint32_t /*leaf_id*/, const Key& key, const RID& rid) {//页，页号，搜索码，记录位置
    auto& h = hdr(leaf);
    LeafEntry* es = leaf_entries(leaf);//获取头和表项
    int pos = leaf_lower_bound(leaf, key);//找到第一个大于等于 key 的位置
    if (pos < h.count && eq(es[pos].key, key)) return false; // unique 这里要求搜索码唯一
    if (h.count < LEAF_CAP()) {//还可以添加项
        // shift right
        for (int i = static_cast<int>(h.count); i > pos; --i) es[i] = es[i - 1];//从后往前移动，腾出 pos 位置
        es[pos].key = key; es[pos].page_id = rid.page_id; es[pos].slot_id = rid.slot_id;//插入
        h.count++;//增加项数
        return true;
    }
    return false;
}

template <typename Key, typename Comparator>
void BPlusTreeT<Key, Comparator>::split_leaf_and_insert(Page& leaf, std::uint32_t leaf_id, const Key& key, const RID& rid) {
    auto& h = hdr(leaf);
    LeafEntry* es = leaf_entries(leaf);

    // create new right sibling
    std::uint32_t right_id = disk_.allocate_page();//分配新页
    Page& right = buffer_.get_page(right_id);//从缓存中获取该页
    auto& hr = hdr(right);
    hr.is_leaf = 1; hr.count = 0; hr.parent = h.parent; hr.next = h.next;//初始化新页 叶结点， 无entity, 父节点与原节点相同，插入

    // move half to right with new key inserted
    int total = static_cast<int>(h.count) + 1;
    int mid = total / 2;
    // temp array
    std::vector<LeafEntry> tmp(total);//将原叶中的所有项放到了数组里，后移后插入
    int pos = leaf_lower_bound(leaf, key);//找到第一个大于等于 key 的位置
    int i = 0;
    for (; i < static_cast<int>(h.count); ++i) tmp[i] = es[i];
    // insert new into tmp
    for (int k = total - 1; k > pos; --k) tmp[k] = tmp[k - 1];
    tmp[pos] = LeafEntry{key, rid.page_id, rid.slot_id, 0};

    // left keep [0, mid), right keep [mid, total)
    h.count = static_cast<std::uint16_t>(mid);
    for (int k = 0; k < mid; ++k) es[k] = tmp[k];//一半放旧叶

    LeafEntry* rs = leaf_entries(right);
    hr.count = static_cast<std::uint16_t>(total - mid);
    for (int k = 0; k < total - mid; ++k) rs[k] = tmp[mid + k];//一半放新叶

    hr.next = h.next; h.next = right_id;

    // promote split key = first key in right
    Key sep = rs[0].key;//最小搜索码值
    buffer_.unpin_page(right_id, true);

    insert_in_parent(leaf_id, sep, right_id);
}

template <typename Key, typename Comparator>
bool BPlusTreeT<Key, Comparator>::insert_in_internal(Page& page, std::uint32_t /*pid*/, const Key& key, std::uint32_t right_id) {
    auto& h = hdr(page);
    InterEntry* es = inter_entries(page);
    int pos = inter_child_index(page, key) + 1; // insert to the right of idx
    if (h.count < INTER_CAP()) {
        for (int i = static_cast<int>(h.count); i > pos; --i) es[i] = es[i - 1];
        es[pos].key = key; es[pos].child = right_id;
        h.count++;
        return true;
    }
    return false;
}

template <typename Key, typename Comparator>
//分裂内部节点并插入
void BPlusTreeT<Key, Comparator>::split_internal_and_insert(Page& page, std::uint32_t pid, const Key& key, std::uint32_t right_id) {
    //数据结构：
    // children 是存储页索引的顺序表
    auto& h = hdr(page);//page指向中间页
    InterEntry* es = inter_entries(page);

    // Build arrays: children size = h.count + 1, keys size = h.count
    std::vector<std::uint32_t> children(h.count + 1);
    std::vector<Key> keys(h.count);
    children[0] = h.leftmost;
    for (int i = 0; i < h.count; ++i) {
        keys[i] = es[i].key;
        children[i + 1] = es[i].child;
    }

    // position to insert new key/right child
    int pos = inter_child_index(page, key) + 1; // insert right of idx => affects children[pos+1] 这里的key是右页中最小的，pos是插入的位置
    // insert into keys and children
    keys.insert(keys.begin() + pos, key);
    children.insert(children.begin() + pos + 1, right_id);

    int total = static_cast<int>(keys.size());
    int mid = total / 2; // promote keys[mid]
    Key sep = keys[mid];//中间键

    // Left node keeps keys[0..mid-1], children[0..mid]
    h.count = static_cast<std::uint16_t>(mid);//数量count = mid
    h.leftmost = children[0];
    for (int i = 0; i < h.count; ++i) { es[i].key = keys[i]; es[i].child = children[i + 1]; }

    // Create right internal node
    std::uint32_t right_pid = disk_.allocate_page();
    Page& right = buffer_.get_page(right_pid);
    auto& hr = hdr(right);
    hr.is_leaf = 0;
    hr.count = static_cast<std::uint16_t>(total - mid - 1);
    hr.parent = h.parent;
    hr.leftmost = children[mid + 1]; // first child on the right side (should be mid+1)
    InterEntry* ers = inter_entries(right);
    for (int i = 0; i < hr.count; ++i) { ers[i].key = keys[mid + 1 + i]; ers[i].child = children[mid + 1 + i + 1]; }

    // Update parent pointer of all children moved to the right internal node
    //更新右叶
    for (int cidx = mid + 1; cidx <= total; ++cidx) {
        std::uint32_t child_id = children[cidx];
        Page& chp = buffer_.get_page(child_id);
        hdr(chp).parent = right_pid;
        buffer_.unpin_page(child_id, true);
    }

    buffer_.unpin_page(right_pid, true);

    // link new right into parent
    insert_in_parent(pid, sep, right_pid);
}

template <typename Key, typename Comparator>
void BPlusTreeT<Key, Comparator>::insert_in_parent(std::uint32_t left_id, const Key& key, std::uint32_t right_id) {
    // if left is root
    if (left_id == root_) {//如果左节点是根节点
        std::uint32_t new_root = disk_.allocate_page();//重新分配根页
        Page& p = buffer_.get_page(new_root);//
        auto& h = hdr(p);
        h.is_leaf = 0; h.count = 0; h.parent = std::numeric_limits<std::uint32_t>::max();
        h.leftmost = left_id; h.next = std::numeric_limits<std::uint32_t>::max();

        InterEntry* es = inter_entries(p);
        es[0].key = key; es[0].child = right_id; h.count = 1;
        buffer_.unpin_page(new_root, true);

        // update children parent for left and right
        Page& l = buffer_.get_page(left_id); hdr(l).parent = new_root; buffer_.unpin_page(left_id, true);
        Page& r = buffer_.get_page(right_id); hdr(r).parent = new_root; buffer_.unpin_page(right_id, true);

        root_ = new_root;
        return;
    }

    // get parent id from left node (temporary pin and immediately unpin to avoid extra pin leak)
    Page& lpage = buffer_.get_page(left_id);
    std::uint32_t parent_id = hdr(lpage).parent;
    buffer_.unpin_page(left_id, false);

    // ensure right child's parent points to parent
    {
        Page& r = buffer_.get_page(right_id);
        hdr(r).parent = parent_id;
        buffer_.unpin_page(right_id, true);
    }

    // insert into parent
    Page& parent = buffer_.get_page(parent_id);
    bool ok = insert_in_internal(parent, parent_id, key, right_id);
    if (ok) {
        buffer_.unpin_page(parent_id, true); // 已直接插入父页，标记为脏并释放
    } else {
        // 不要在分裂前释放父页引用，保持 pin 状态，避免使用悬空引用
        split_internal_and_insert(parent, parent_id, key, right_id);
        buffer_.unpin_page(parent_id, true); // 分裂完成后再释放父页
    }
}

// Default alias for backward compatibility: int64 keys
using BPlusTree = BPlusTreeT<std::int64_t, std::less<std::int64_t>>;

// } // namespace pcsql  // moved to file end

template <typename Key, typename Comparator>
bool BPlusTreeT<Key, Comparator>::erase(const Key& key) {
    if (root_ == std::numeric_limits<std::uint32_t>::max()) return false;
    std::uint32_t leaf_id = find_leaf(key);
    Page& leaf = buffer_.get_page(leaf_id);
    auto& lh = hdr(leaf);
    if (!lh.is_leaf) { buffer_.unpin_page(leaf_id, false); return false; }
    LeafEntry* les = leaf_entries(leaf);
    int pos = leaf_lower_bound(leaf, key);
    if (pos >= lh.count || !eq(les[pos].key, key)) {
        buffer_.unpin_page(leaf_id, false);
        return false;
    }

    // delete entry by shift-left
    for (int i = pos + 1; i < lh.count; ++i) les[i - 1] = les[i];
    lh.count--;

    if (lh.count > 0) {
        // if first key changed, update parent's separator
        if (pos == 0) {
            std::uint32_t parent_id = lh.parent;
            if (parent_id != std::numeric_limits<std::uint32_t>::max()) {
                Page& parent = buffer_.get_page(parent_id);
                int slot = find_child_slot(parent, leaf_id);
                if (slot > 0) { // separator key is at slot-1
                    InterEntry* es = inter_entries(parent);
                    es[slot - 1].key = les[0].key;
                    buffer_.unpin_page(parent_id, true);
                } else {
                    buffer_.unpin_page(parent_id, false);
                }
            }
        }
        buffer_.unpin_page(leaf_id, true);
        return true;
    }

    // leaf becomes empty
    if (leaf_id == root_) {
        // keep an empty leaf as root (simpler)
        buffer_.unpin_page(leaf_id, true);
        return true;
    }

    // Need to remove this empty leaf and fix parent/leaf-links
    std::uint32_t parent_id = lh.parent;
    std::uint32_t next_id = lh.next;

    // Pin parent to compute siblings and adjust
    Page& parent = buffer_.get_page(parent_id);
    auto& ph = hdr(parent);
    // build children array to locate slot and siblings
    int child_slot = find_child_slot(parent, leaf_id);
    // compute left sibling id if any
    std::uint32_t left_id = std::numeric_limits<std::uint32_t>::max();
    if (child_slot > 0) {
        if (child_slot - 1 == 0) {
            left_id = ph.leftmost;
        } else {
            left_id = inter_entries(parent)[child_slot - 2].child;
        }
    }

    // update left sibling's next to skip removed leaf
    if (left_id != std::numeric_limits<std::uint32_t>::max()) {
        Page& leftp = buffer_.get_page(left_id);
        hdr(leftp).next = next_id;
        buffer_.unpin_page(left_id, true);
    }

    // Unpin the leaf before freeing it
    buffer_.unpin_page(leaf_id, false);
    disk_.free_page(leaf_id);

    // remove this child from parent and possibly shrink root
    remove_child_at(parent, parent_id, child_slot);
    // remove_child_at handles unpin of parent and possible root change

    return true;
}

template <typename Key, typename Comparator>
void BPlusTreeT<Key, Comparator>::remove_child_at(Page& parent, std::uint32_t parent_id, int child_slot) {
    auto& h = hdr(parent);
    InterEntry* es = inter_entries(parent);

    // Build children array lazily when needed to fetch right sibling for k==0
    if (child_slot < 0 || child_slot > h.count) {
        buffer_.unpin_page(parent_id, false);
        return;
    }

    if (child_slot == 0) {
        // remove leftmost child => update leftmost to previous children[1]
        // children[1] is es[0].child when count>0
        std::uint32_t new_leftmost = (h.count > 0) ? es[0].child : std::numeric_limits<std::uint32_t>::max();
        h.leftmost = new_leftmost;
        // remove key at index 0 by shifting
        for (int i = 1; i < h.count; ++i) es[i - 1] = es[i];
        if (h.count > 0) h.count--;
    } else {
        // remove key at index child_slot-1 by shifting left
        for (int i = child_slot; i < h.count; ++i) es[i - 1] = es[i];
        h.count--;
    }

    // If parent is root and becomes empty, shrink tree height
    if (parent_id == root_ && h.count == 0) {
        std::uint32_t new_root = h.leftmost;
        buffer_.unpin_page(parent_id, true);
        if (new_root != std::numeric_limits<std::uint32_t>::max()) {
            Page& nr = buffer_.get_page(new_root);
            hdr(nr).parent = std::numeric_limits<std::uint32_t>::max();
            buffer_.unpin_page(new_root, true);
        }
        disk_.free_page(parent_id);
        root_ = new_root;
        return;
    }

    buffer_.unpin_page(parent_id, true);
}

// find index k in parent's children array where children[k] == child_id
// children[0] = leftmost; children[i+1] = es[i].child
// returns -1 if not found
template <typename Key, typename Comparator>
int BPlusTreeT<Key, Comparator>::find_child_slot(const Page& parent, std::uint32_t child_id) const {
    const auto& h = hdr(parent);
    if (h.leftmost == child_id) return 0;
    const InterEntry* es = inter_entries(parent);
    for (int i = 0; i < h.count; ++i) {
        if (es[i].child == child_id) return i + 1;
    }
    return -1;
}

} // namespace pcsql