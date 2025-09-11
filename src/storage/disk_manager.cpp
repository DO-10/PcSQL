#include "storage/disk_manager.hpp"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

namespace pcsql {

static bool file_exists(const std::filesystem::path& p) {
    struct stat st{};
    // return ::stat(p.c_str(), &st) == 0;
    return ::stat(p.string().c_str(), &st) == 0;
    }

DiskManager::DiskManager(const std::string& base_dir,
                         const std::string& db_file,
                         const std::string& meta_file)
    : base_dir_(std::filesystem::absolute(base_dir)),
      db_path_(base_dir_ / db_file),
      meta_path_(base_dir_ / meta_file) {
    init_files();
    load_meta();
}

void DiskManager::init_files() {
    std::filesystem::create_directories(base_dir_);
    if (!file_exists(db_path_)) {
        std::ofstream ofs(db_path_, std::ios::binary);
        if (!ofs) throw std::runtime_error("Failed to create db file");
    }
    if (!file_exists(meta_path_)) {
        // Text meta format:
        // line 1: next_page_id\n
        // line 2: space-separated free list (may be empty)\n
        std::ofstream ofs(meta_path_);
        if (!ofs) throw std::runtime_error("Failed to create meta file");
        ofs << 0 << "\n";
        ofs << "\n"; // empty free list
    }
}

void DiskManager::load_meta() {
    std::ifstream ifs(meta_path_);
    if (!ifs) throw std::runtime_error("Failed to open meta file");
    std::string line;
    // line 1: next_page_id
    if (!std::getline(ifs, line)) throw std::runtime_error("Invalid meta file: missing next_page_id");
    {
        std::istringstream iss(line);
        iss >> next_page_id_;
        if (!iss) next_page_id_ = 0;
    }
    // line 2: free list
    free_list_.clear();
    if (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::uint32_t v;
        while (iss >> v) free_list_.push_back(v);
    }
}

void DiskManager::save_meta() const {
    std::ofstream ofs(meta_path_);
    if (!ofs) throw std::runtime_error("Failed to write meta file");
    ofs << next_page_id_ << "\n";
    for (std::size_t i = 0; i < free_list_.size(); ++i) {
        ofs << free_list_[i];
        if (i + 1 < free_list_.size()) ofs << ' ';
    }
    ofs << "\n";
}

void DiskManager::ensure_file_size_for(std::uint32_t page_id) {
    std::ifstream ifs(db_path_, std::ios::binary | std::ios::ate);
    std::uint64_t size = 0;
    if (ifs) size = static_cast<std::uint64_t>(ifs.tellg());
    std::uint64_t required = (static_cast<std::uint64_t>(page_id) + 1) * PAGE_SIZE;
    if (size < required) {
        std::ofstream ofs(db_path_, std::ios::binary | std::ios::app);
        if (!ofs) throw std::runtime_error("Failed to expand db file");
        std::uint64_t to_write = required - size;
        // write zeros
        const std::size_t chunk_size = 4096;
        char zeros[chunk_size];
        std::memset(zeros, 0, chunk_size);
        while (to_write > 0) {
            std::size_t chunk = static_cast<std::size_t>(std::min<std::uint64_t>(to_write, chunk_size));
            ofs.write(zeros, static_cast<std::streamsize>(chunk));
            to_write -= chunk;
        }
    }
}

std::uint32_t DiskManager::allocate_page() {
    std::uint32_t page_id;
    if (!free_list_.empty()) {
        page_id = free_list_.back();
        free_list_.pop_back();
    } else {
        page_id = next_page_id_++;
        ensure_file_size_for(page_id);
    }
    save_meta();
    return page_id;
}

void DiskManager::free_page(std::uint32_t page_id) {
    free_list_.push_back(page_id);
    save_meta();
}

void DiskManager::read_page(std::uint32_t page_id, void* out_buffer, std::size_t size) {
    if (size != PAGE_SIZE) throw std::invalid_argument("size must be PAGE_SIZE");
    std::ifstream ifs(db_path_, std::ios::binary);
    if (!ifs) throw std::runtime_error("Failed to open db file for read");

    std::uint64_t offset = static_cast<std::uint64_t>(page_id) * PAGE_SIZE;
    ifs.seekg(0, std::ios::end);
    std::uint64_t file_size = static_cast<std::uint64_t>(ifs.tellg());
    if (offset + PAGE_SIZE > file_size) throw std::out_of_range("Page does not exist");
    ifs.seekg(offset);
    ifs.read(reinterpret_cast<char*>(out_buffer), static_cast<std::streamsize>(PAGE_SIZE));
    if (ifs.gcount() != static_cast<std::streamsize>(PAGE_SIZE))
        throw std::runtime_error("Short read");
}

void DiskManager::write_page(std::uint32_t page_id, const void* buffer, std::size_t size) {
    if (size != PAGE_SIZE) throw std::invalid_argument("size must be PAGE_SIZE");
    ensure_file_size_for(page_id);
    std::fstream fs(db_path_, std::ios::binary | std::ios::in | std::ios::out);
    if (!fs) throw std::runtime_error("Failed to open db file for write");
    std::uint64_t offset = static_cast<std::uint64_t>(page_id) * PAGE_SIZE;
    fs.seekp(static_cast<std::streamoff>(offset));
    fs.write(reinterpret_cast<const char*>(buffer), static_cast<std::streamsize>(PAGE_SIZE));
    fs.flush();
}

} // namespace pcsql