#include "storage/table_manager.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "storage/disk_manager.hpp"

namespace pcsql {

static bool file_exists(const std::filesystem::path& p) {
    std::error_code ec;
    return std::filesystem::exists(p, ec);
}

TableManager::TableManager(const std::string& base_dir, const std::string& tables_file)
    : base_dir_(std::filesystem::absolute(base_dir)),
      file_path_(base_dir_ / tables_file) {
    init_file();
    load();
}

void TableManager::init_file() {
    std::filesystem::create_directories(base_dir_);
    if (!file_exists(file_path_)) {
        std::ofstream ofs(file_path_);
        if (!ofs) throw std::runtime_error("Failed to create tables meta file");
        ofs << 0 << "\n"; // next_table_id
    }
}

void TableManager::load() {
    id_to_name_.clear();
    name_to_id_.clear();
    table_pages_.clear();

    std::ifstream ifs(file_path_);
    if (!ifs) throw std::runtime_error("Failed to open tables meta file");

    // line 1: next_table_id
    ifs >> next_table_id_;
    std::string dummy;
    std::getline(ifs, dummy); // consume remainder of line

    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        std::int32_t tid; std::string name;
        if (!(iss >> tid >> name)) continue;
        id_to_name_[tid] = name;
        name_to_id_[name] = tid;
        std::vector<std::uint32_t> pages;
        std::uint32_t pid;
        while (iss >> pid) pages.push_back(pid);
        table_pages_[tid] = std::move(pages);
    }
}

void TableManager::save() const {
    std::ofstream ofs(file_path_);
    if (!ofs) throw std::runtime_error("Failed to write tables meta file");
    ofs << next_table_id_ << "\n";
    for (const auto& [tid, name] : id_to_name_) {
        ofs << tid << ' ' << name;
        auto it = table_pages_.find(tid);
        if (it != table_pages_.end()) {
            for (auto pid : it->second) ofs << ' ' << pid;
        }
        ofs << '\n';
    }
}

std::int32_t TableManager::create_table(const std::string& name) {
    if (name_to_id_.count(name)) throw std::invalid_argument("[TableManager] table exists");//如果表名已存在（name→id 索引中有记录），抛 invalid_argument（不允许重复表名）。
    std::int32_t tid = next_table_id_++;//分配当前 next_table_id_ 给新表 tid，然后自增 next_table_id_（为下一个新表准备）
    id_to_name_[tid] = name;//分配当前 next_table_id_ 给新表 tid，然后自增 next_table_id_（为下一个新表准备）
    name_to_id_[name] = tid;//将新表名 name 映射到 tid
    table_pages_[tid] = {};//初始化该表的页列表为空向量。
    save();//持久化变更到元数据文件（此处移除，改由后续记录写入时统一持久化）
    std::cout << "[TableManager] create_table: " << tid << " " << name << std::endl;
    return tid;//返回新表的 tid
}

bool TableManager::drop_table_by_id(std::int32_t table_id, DiskManager& disk) {
    auto itn = id_to_name_.find(table_id);
    if (itn == id_to_name_.end()) return false;
    // 回收所有页
    auto itp = table_pages_.find(table_id);
    if (itp != table_pages_.end()) {
        for (auto pid : itp->second) disk.free_page(pid);
    }//如果 table_pages_ 中有此 table_id 的页列表，则遍历每个 pid 并调用 disk.free_page(pid) 回收页面资源
    // 再删除目录项
    std::string name = itn->second;
    id_to_name_.erase(itn);
    name_to_id_.erase(name);
    table_pages_.erase(table_id);
    save();
    return true;
}

bool TableManager::drop_table_by_name(const std::string& name, DiskManager& disk) {
    auto it = name_to_id_.find(name);
    if (it == name_to_id_.end()) return false;
    return drop_table_by_id(it->second, disk);
}

std::int32_t TableManager::get_table_id(const std::string& name) const {
    auto it = name_to_id_.find(name);
    if (it == name_to_id_.end()) return -1;
    std::cout << "[TableManager] get_table_id: " << name << "--->" << it->second << std::endl;
    return it->second;
}

std::string TableManager::get_table_name(std::int32_t table_id) const {
    auto it = id_to_name_.find(table_id);
    if (it == id_to_name_.end()) return {};
    return it->second;
}

std::uint32_t TableManager::allocate_table_page(std::int32_t table_id, DiskManager& disk) {
    auto it = id_to_name_.find(table_id);//首先确认 table_id 有对应的表（在 id_to_name_ 中）；如果不存在抛 invalid_argument
    if (it == id_to_name_.end()) throw std::invalid_argument("invalid table id");
    std::uint32_t pid = disk.allocate_page();//向 DiskManager 请求分配一个新页 pid = disk.allocate_page()（具体实现由 DiskManager 决定，可能是返回空闲页号）。
    table_pages_[table_id].push_back(pid);//将新页 id pid 添加到该表的页列表 table_pages_[table_id]（如果以前没有该键，operator[] 会创建一个空向量）。
    save();
    return pid;
}

const std::vector<std::uint32_t>& TableManager::get_table_pages(std::int32_t table_id) const {
    static const std::vector<std::uint32_t> empty;
    auto it = table_pages_.find(table_id);
    if (it == table_pages_.end()) return empty;
    return it->second;
    /*返回指定 table_id 的页 id 向量的常量引用
    如果表没有页列表或表不存在，返回一个静态的空向量引用 empty（避免返回对临时对象的引用）。
    使用 const& 避免拷贝，便于调用端只读访问。*/
}

} // namespace pcsql