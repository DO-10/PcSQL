#include "storage/table_manager.hpp"

#include <fstream>
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
    if (name_to_id_.count(name)) throw std::invalid_argument("table exists");
    std::int32_t tid = next_table_id_++;
    id_to_name_[tid] = name;
    name_to_id_[name] = tid;
    table_pages_[tid] = {};
    save();
    return tid;
}

bool TableManager::drop_table_by_id(std::int32_t table_id) {
    auto itn = id_to_name_.find(table_id);
    if (itn == id_to_name_.end()) return false;
    std::string name = itn->second;
    id_to_name_.erase(itn);
    name_to_id_.erase(name);
    table_pages_.erase(table_id);
    save();
    return true;
}

bool TableManager::drop_table_by_name(const std::string& name) {
    auto it = name_to_id_.find(name);
    if (it == name_to_id_.end()) return false;
    return drop_table_by_id(it->second);
}

std::int32_t TableManager::get_table_id(const std::string& name) const {
    auto it = name_to_id_.find(name);
    if (it == name_to_id_.end()) return -1;
    return it->second;
}

std::string TableManager::get_table_name(std::int32_t table_id) const {
    auto it = id_to_name_.find(table_id);
    if (it == id_to_name_.end()) return {};
    return it->second;
}

std::uint32_t TableManager::allocate_table_page(std::int32_t table_id, DiskManager& disk) {
    auto it = id_to_name_.find(table_id);
    if (it == id_to_name_.end()) throw std::invalid_argument("invalid table id");
    std::uint32_t pid = disk.allocate_page();
    table_pages_[table_id].push_back(pid);
    save();
    return pid;
}

const std::vector<std::uint32_t>& TableManager::get_table_pages(std::int32_t table_id) const {
    static const std::vector<std::uint32_t> empty;
    auto it = table_pages_.find(table_id);
    if (it == table_pages_.end()) return empty;
    return it->second;
}

} // namespace pcsql