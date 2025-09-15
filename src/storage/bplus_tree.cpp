#include "storage/bplus_tree.hpp"
#include <cstdint>
#include <functional>

namespace pcsql {

// Explicit instantiation for the default int64_t-key B+Tree used across the storage library
// All template method definitions are provided in the header.
template class BPlusTreeT<std::int64_t, std::less<std::int64_t>>;

} // namespace pcsql