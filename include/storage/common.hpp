#pragma once
#include <cstddef>
#include <string>

namespace pcsql {

constexpr std::size_t PAGE_SIZE = 4096; // 4KB

enum class Policy {
    LRU,
    FIFO
};

inline const char* to_string(Policy p) {
    switch (p) {
        case Policy::LRU: return "LRU";
        case Policy::FIFO: return "FIFO";
    }
    return "UNKNOWN";
}

struct Stats {
    std::size_t hits{0};
    std::size_t misses{0};
    std::size_t evictions{0};
    std::size_t flushes{0};
};

} // namespace pcsql