#include "storage/disk_manager.hpp"                              //引入头文件 disk_manager.hpp，里面声明了 DiskManager 类

#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <array>

namespace pcsql {                                                //定义命名空间 pcsql，避免名字冲突

static bool file_exists(const std::filesystem::path& p) {        //检查p路径文件是否存在，存在返回0
    struct stat st{};
    // return ::stat(p.c_str(), &st) == 0;
    return ::stat(p.string().c_str(), &st) == 0;
    }

/*
base_dir_: 存储目录，转为绝对路径。
db_path_: 数据文件路径。
meta_path_: 元数据文件路径。
init_files(): 若文件不存在则创建。
load_meta(): 读取元数据（下一页 ID 和空闲页列表）。*/
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
    std::filesystem::create_directories(base_dir_);//确保数据库目录存在（没有则创建）。
    if (!file_exists(db_path_)) {
        std::ofstream ofs(db_path_, std::ios::binary);
        if (!ofs) throw std::runtime_error("Failed to create db file");
    }//如果数据库文件不存在，就新建一个空文件
    if (!file_exists(meta_path_)) {
        // Text meta format:
        // line 1: next_page_id\n
        // line 2: space-separated free list (may be empty)\n
        std::ofstream ofs(meta_path_);
        if (!ofs) throw std::runtime_error("Failed to create meta file");
        ofs << 0 << "\n";
        ofs << "\n"; // empty free list
    }// 如果元数据文件不存在，就新建：
     // 第一行写 0（下一页 ID 初始为 0）。
     // 第二行空行（空闲页列表为空）。
}

void DiskManager::load_meta() {
    std::ifstream ifs(meta_path_);
    if (!ifs) throw std::runtime_error("Failed to open meta file");//打开元数据文件，失败抛异常。
    std::string line;
    // line 1: next_page_id
    if (!std::getline(ifs, line)) throw std::runtime_error("Invalid meta file: missing next_page_id");//读取第一行（下一页 ID）
    {
        std::istringstream iss(line);
        iss >> next_page_id_;
        if (!iss) next_page_id_ = 0;
    }//从字符串中解析 next_page_id_，若失败则设为 0
    // line 2: free list
    free_list_.clear();
    if (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::uint32_t v;
        while (iss >> v) free_list_.push_back(v);
    }//读取第二行（空闲页 ID 列表），解析并存入 free_list_
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

    /*
    保存元数据
    1.以输出模式打开元数据文件
    2.写入下一页 ID
    3.写入空闲页列表
    */
}

void DiskManager::ensure_file_size_for(std::uint32_t page_id) {
    std::ifstream ifs(db_path_, std::ios::binary | std::ios::ate);
    std::uint64_t size = 0;
    if (ifs) size = static_cast<std::uint64_t>(ifs.tellg());//打开数据库文件，获取当前大小
    std::uint64_t required = (static_cast<std::uint64_t>(page_id) + 1) * PAGE_SIZE;//计算需要的最小文件大小（能容纳到 page_id 这一页）。
    if (size < required) {
        std::ofstream ofs(db_path_, std::ios::binary | std::ios::app);
        if (!ofs) throw std::runtime_error("Failed to expand db file");
        std::uint64_t to_write = required - size;//需要扩展to_write大小 如果文件不够大，追加写入零字节补齐。
        // write zeros
        const std::size_t chunk_size = 4096;
        char zeros[chunk_size];
        std::memset(zeros, 0, chunk_size);
        while (to_write > 0) {
            std::size_t chunk = static_cast<std::size_t>(std::min<std::uint64_t>(to_write, chunk_size));//取to_write 和 4096的最小值
            ofs.write(zeros, static_cast<std::streamsize>(chunk));
            to_write -= chunk;//写完减去
        }//分块写入 0（初始化新扩展部分），避免一次写太大内存。
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
    // 始终将页内容清零，避免复用空闲页时残留数据
    std::array<char, PAGE_SIZE> zeros{}; // value-initialized to 0
    write_page(page_id, zeros.data(), PAGE_SIZE);
    save_meta();
    return page_id;

    /*分配新页：
    如果有空闲页（之前释放的），就复用。
    否则，分配新页（增加 next_page_id_，并扩展文件）。
    保存元数据。
    返回新页 ID。*/
}

//释放页
void DiskManager::free_page(std::uint32_t page_id) {
    free_list_.push_back(page_id);//在 vector 的末尾添加一个元素
    save_meta();//保存新的元数据
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
    /*
    读页
    参数：page_id 目标页号, out_buffer, 接受数据的缓冲区, size大小
    1.size? 大小必须与PAGE_SIZE相等
    2.以二进制读模式打开文件
    3.计算偏移量，确定文件的写入位置。offset = page_id * PAGE_SIZE
    4.检查页是否越界
    5.定位到页起始位置
    6.读取数据
    7.检查是否读取完整
    */
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
    /*
    写页（大致理解）
    参数：page_id 目标页号, buffer 指向内存中页数据的指针, 缓冲区大小
    1.size? 大小必须与PAGE_SIZE相等
    2.确保文件足够大，若不足则扩展。
    3.以二进制读写模式打开文件
    4.计算偏移量，确定文件的写入位置。offset = page_id * PAGE_SIZE
    5.定位写入位置
    6.写入数据到输出流缓冲区
    7.刷新缓冲区，确保数据写入磁盘。
    */
}

} // namespace pcsql