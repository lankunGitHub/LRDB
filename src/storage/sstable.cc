// SSTable存储引擎实现

#include "lrdb/storage/sstable.h"
#include "lrdb/core/coding.h"
#include "lrdb/storage/memtable.h"
#include "lrdb/util/error_handler.h"
#include "lrdb/util/hash.h"
#include <algorithm>
#include <cmath>

// 性能优化常量
static constexpr size_t kDefaultIndexEntryReserve = 1000;
static constexpr size_t kDefaultOffsetCacheReserve = 100;
static constexpr size_t kBloomFilterOverheadBytes = 64;
static constexpr size_t kBlockHeaderCacheSize = 64;

namespace lrdb {

// ============================================================================
// 内部工具函数 - 性能优化
// ============================================================================

// 高效字符串连接，避免多次内存分配
inline std::string JoinPath(const std::string &dir, const std::string &file) {
  std::string result;
  result.reserve(dir.size() + file.size() + 1);
  result = dir;
  if (!dir.empty() && dir.back() != '/') {
    result += '/';
  }
  result += file;
  return result;
}

// 快速检查文件扩展名
inline bool HasSSTExtension(const std::string &filename) {
  return filename.size() > 4 &&
         filename.compare(filename.size() - 4, 4, ".sst") == 0;
}

// 高效的字节大小格式化（避免浮点运算）
inline std::string FormatBytes(uint64_t bytes) {
  if (bytes < 1024)
    return std::to_string(bytes) + "B";
  if (bytes < 1024 * 1024)
    return std::to_string(bytes / 1024) + "KB";
  if (bytes < 1024 * 1024 * 1024)
    return std::to_string(bytes / (1024 * 1024)) + "MB";
  return std::to_string(bytes / (1024 * 1024 * 1024)) + "GB";
}

// 统一错误处理宏
#define RETURN_IF_ERROR(s)                                                     \
  do {                                                                         \
    const Status &_status = (s);                                               \
    if (!_status.ok()) {                                                       \
      HANDLE_ERROR_WITH_RECOVERY(_status, __FUNCTION__, "SSTable");            \
      return _status;                                                          \
    }                                                                          \
  } while (0)

// 带上下文的错误包装
inline Status WrapError(const Status &s, const std::string &context) {
  if (s.ok())
    return s;
  return Status::IOError(context + ": " + s.ToString());
}

// ============================================================================
// SSTableMeta 实现
// ============================================================================

std::string SSTableMeta::Encode() const {
  std::string result;

  // 文件信息
  coding::PutLengthPrefixedSlice(&result, Slice(filename));
  coding::PutVarint64(&result, file_number);
  coding::PutVarint64(&result, file_size);
  coding::PutFixed32(&result, static_cast<uint32_t>(level));

  // 键范围
  coding::PutLengthPrefixedSlice(&result, Slice(smallest_key));
  coding::PutLengthPrefixedSlice(&result, Slice(largest_key));

  // 统计信息
  coding::PutVarint64(&result, num_entries);
  coding::PutVarint64(&result, num_deletions);
  coding::PutVarint64(&result, raw_key_size);
  coding::PutVarint64(&result, raw_value_size);

  // 时间戳
  coding::PutVarint64(&result, creation_time);
  coding::PutVarint64(&result, oldest_key_time);

  return result;
}

bool SSTableMeta::Decode(const Slice &data) {
  Slice input = data;

  // 文件信息
  Slice filename_slice;
  if (!coding::GetLengthPrefixedSlice(&input, &filename_slice) ||
      !coding::GetVarint64(&input, &file_number) ||
      !coding::GetVarint64(&input, &file_size)) {
    return false;
  }
  filename = filename_slice.ToString();

  // level使用定长4字节编码，与Encode保持一致
  if (input.size() < 4) {
    return false;
  }
  level = static_cast<SSTableLevel>(coding::DecodeFixed32(input));
  input.remove_prefix(4);

  // 键范围
  Slice smallest_slice, largest_slice;
  if (!coding::GetLengthPrefixedSlice(&input, &smallest_slice) ||
      !coding::GetLengthPrefixedSlice(&input, &largest_slice)) {
    return false;
  }
  smallest_key = smallest_slice.ToString();
  largest_key = largest_slice.ToString();

  // 统计信息
  if (!coding::GetVarint64(&input, &num_entries) ||
      !coding::GetVarint64(&input, &num_deletions) ||
      !coding::GetVarint64(&input, &raw_key_size) ||
      !coding::GetVarint64(&input, &raw_value_size)) {
    return false;
  }

  // 时间戳
  if (!coding::GetVarint64(&input, &creation_time) ||
      !coding::GetVarint64(&input, &oldest_key_time)) {
    return false;
  }

  return true;
}

bool SSTableMeta::KeyInRange(const Slice &key,
                             const Comparator *comparator) const {
  if (smallest_key.empty() || largest_key.empty()) {
    return true; // 范围未设置，认为包含所有键
  }

  return comparator->Compare(key, Slice(smallest_key)) >= 0 &&
         comparator->Compare(key, Slice(largest_key)) <= 0;
}

bool SSTableMeta::OverlapsWith(const SSTableMeta &other,
                               const Comparator *comparator) const {
  if (smallest_key.empty() || largest_key.empty() ||
      other.smallest_key.empty() || other.largest_key.empty()) {
    return true; // 如果范围未设置，认为重叠
  }

  // 检查是否有重叠：not (a.max < b.min or b.max < a.min)
  return !(
      comparator->Compare(Slice(largest_key), Slice(other.smallest_key)) < 0 ||
      comparator->Compare(Slice(other.largest_key), Slice(smallest_key)) < 0);
}

double SSTableMeta::CompressionRatio() const {
  if (raw_key_size + raw_value_size == 0)
    return 1.0;
  return static_cast<double>(file_size) / (raw_key_size + raw_value_size);
}

// ============================================================================
// BlockHeader 实现
// ============================================================================

std::string BlockHeader::Encode() const {
  std::string result;
  result.reserve(kHeaderSize);

  coding::PutFixed32(&result, static_cast<uint32_t>(type));
  coding::PutFixed32(&result, size);
  coding::PutFixed32(&result, crc32);
  coding::PutFixed32(&result, compression);

  return result;
}

bool BlockHeader::Decode(const Slice &data) {
  if (data.size() < kHeaderSize) {
    return false;
  }

  const char *ptr = data.data();
  type = static_cast<BlockType>(coding::DecodeFixed32(ptr));
  ptr += 4;
  size = coding::DecodeFixed32(ptr);
  ptr += 4;
  crc32 = coding::DecodeFixed32(ptr);
  ptr += 4;
  compression = coding::DecodeFixed32(ptr);

  return true;
}

// ============================================================================
// IndexEntry 实现
// ============================================================================

std::string IndexEntry::Encode() const {
  std::string result;

  coding::PutLengthPrefixedSlice(&result, Slice(key));
  coding::PutVarint64(&result, block_offset);
  coding::PutFixed32(&result, block_size);
  coding::PutFixed32(&result, first_key_offset);

  return result;
}

bool IndexEntry::Decode(const Slice &data) {
  Slice input = data;

  Slice key_slice;
  if (!coding::GetLengthPrefixedSlice(&input, &key_slice) ||
      !coding::GetVarint64(&input, &block_offset)) {
    return false;
  }

  // block_size和first_key_offset使用定长4字节编码，与Encode保持一致
  if (input.size() < 8) {
    return false;
  }
  block_size = coding::DecodeFixed32(input);
  input.remove_prefix(4);
  first_key_offset = coding::DecodeFixed32(input);
  input.remove_prefix(4);

  key = key_slice.ToString();
  return true;
}

// ============================================================================
// SSTableReader 实现
// ============================================================================

// SSTableIterator 实现
class SSTableIterator : public SSTableReader::Iterator {
public:
  SSTableIterator(SSTableReader *reader, uint64_t snapshot)
      : reader_(reader), snapshot_(snapshot), valid_(false),
        current_block_(-1) {}

  bool Valid() const override {
    return valid_ && current_block_ >= 0 &&
           current_block_ < static_cast<int>(reader_->index_.size());
  }

  void SeekToFirst() override {
    if (reader_->index_.empty()) {
      valid_ = false;
      return;
    }

    current_block_ = 0;
    LoadCurrentBlock();
    if (block_data_.empty()) {
      valid_ = false;
      return;
    }

    // 解析第一个键值对
    ParseCurrentEntry();
  }

  void SeekToLast() override {
    if (reader_->index_.empty()) {
      valid_ = false;
      return;
    }

    current_block_ = static_cast<int>(reader_->index_.size()) - 1;
    LoadCurrentBlock();
    if (block_data_.empty()) {
      valid_ = false;
      return;
    }

    // 找到最后一个键值对
    SeekToLastInBlock();
  }

  void Seek(const Slice &target) override {
    // 使用二分查找定位到正确的数据块
    int block_index = reader_->FindIndexEntry(target);
    if (block_index < 0) {
      valid_ = false;
      return;
    }

    current_block_ = block_index;
    LoadCurrentBlock();
    if (block_data_.empty()) {
      valid_ = false;
      return;
    }

    // 在块内查找目标键
    SeekInBlock(target);
  }

  void Next() override {
    if (!Valid())
      return;

    // 移动到块内下一个位置
    if (MoveToNextInBlock()) {
      return; // 成功移动到下一个位置
    }

    // 当前块已结束，移动到下一个块
    current_block_++;
    if (current_block_ >= static_cast<int>(reader_->index_.size())) {
      valid_ = false;
      return;
    }

    LoadCurrentBlock();
    if (block_data_.empty()) {
      valid_ = false;
      return;
    }

    ParseCurrentEntry();
  }

  void Prev() override {
    if (!Valid())
      return;

    // 移动到块内前一个位置
    if (MoveToPrevInBlock()) {
      return; // 成功移动到前一个位置
    }

    // 当前块已开始，移动到前一个块
    current_block_--;
    if (current_block_ < 0) {
      valid_ = false;
      return;
    }

    LoadCurrentBlock();
    if (block_data_.empty()) {
      valid_ = false;
      return;
    }

    SeekToLastInBlock();
  }

  Slice key() const override { return Valid() ? current_key_ : Slice(); }

  Slice value() const override { return Valid() ? current_value_ : Slice(); }

  Status status() const override { return status_; }

private:
  void LoadCurrentBlock() {
    if (current_block_ < 0 ||
        current_block_ >= static_cast<int>(reader_->index_.size())) {
      status_ = Status::InvalidArgument("Invalid block index");
      return;
    }

    const IndexEntry &entry = reader_->index_[current_block_];
    status_ = reader_->ReadDataBlock(entry.block_offset, entry.block_size,
                                     &block_data_);
    if (!status_.ok()) {
      valid_ = false;
      return;
    }

    block_offset_ = 0;
    // 清空偏移量缓存，新块需要重新构建
    offset_cache_.clear();
  }

  void ParseCurrentEntry() {
    if (block_offset_ >= block_data_.size()) {
      valid_ = false;
      return;
    }

    Slice input(block_data_.data() + block_offset_,
                block_data_.size() - block_offset_);

    // 解析键
    Slice key_slice;
    if (!coding::GetLengthPrefixedSlice(&input, &key_slice)) {
      valid_ = false;
      status_ = Status::Corruption("Failed to parse key");
      return;
    }

    // 解析值
    Slice value_slice;
    if (!coding::GetLengthPrefixedSlice(&input, &value_slice)) {
      valid_ = false;
      status_ = Status::Corruption("Failed to parse value");
      return;
    }

    current_key_ = key_slice;
    current_value_ = value_slice;
    valid_ = true;

    // 更新偏移量
    size_t consumed = (block_data_.size() - block_offset_) - input.size();
    block_offset_ += consumed;
  }

  bool MoveToNextInBlock() {
    size_t old_offset = block_offset_;
    ParseCurrentEntry();
    return valid_ && block_offset_ > old_offset;
  }

  bool MoveToPrevInBlock() {
    if (block_offset_ == 0)
      return false;

    // 如果还没有构建偏移量缓存，则构建它
    if (offset_cache_.empty()) {
      BuildOffsetCache();
    }

    // 在偏移量缓存中找到当前位置的前一个
    auto it = std::lower_bound(offset_cache_.begin(), offset_cache_.end(),
                               block_offset_);

    if (it == offset_cache_.begin()) {
      // 已经在第一个位置
      return false;
    }

    // 移动到前一个位置
    --it;
    block_offset_ = *it;
    ParseCurrentEntry();
    return valid_;
  }

  void BuildOffsetCache() {
    offset_cache_.clear();
    offset_cache_.reserve(kDefaultOffsetCacheReserve);

    const char *data = block_data_.data();
    const size_t data_size = block_data_.size();
    size_t offset = 0;

    while (offset < data_size) {
      offset_cache_.push_back(offset);

      // 解析记录格式：[key_length][key_data][value_length][value_data]
      if (offset + 4 > data_size)
        break;

      uint32_t key_length = coding::DecodeFixed32(data + offset);
      offset += 4;

      if (offset + key_length + 4 > data_size)
        break;
      offset += key_length; // 跳过键

      uint32_t value_length = coding::DecodeFixed32(data + offset);
      offset += 4;

      if (offset + value_length > data_size)
        break;
      offset += value_length; // 跳过值
    }

    // 紧缩内存：如果实际条目数小于预估，释放多余空间
    if (offset_cache_.capacity() > offset_cache_.size() * 2) {
      offset_cache_.shrink_to_fit();
    }
  }

  void SeekInBlock(const Slice &target) {
    block_offset_ = 0;

    while (block_offset_ < block_data_.size()) {
      ParseCurrentEntry();
      if (!valid_)
        return;

      int cmp = reader_->comparator_->Compare(current_key_, target);
      if (cmp >= 0) {
        return; // 找到目标或第一个大于目标的键
      }

      if (!MoveToNextInBlock()) {
        valid_ = false;
        return;
      }
    }

    valid_ = false;
  }

  void SeekToLastInBlock() {
    block_offset_ = 0;
    size_t last_valid_offset = 0;

    while (block_offset_ < block_data_.size()) {
      last_valid_offset = block_offset_;
      if (!MoveToNextInBlock())
        break;
    }

    block_offset_ = last_valid_offset;
    ParseCurrentEntry();
  }

private:
  SSTableReader *reader_;
  uint64_t snapshot_;
  bool valid_;
  int current_block_;

  std::string block_data_;
  size_t block_offset_;

  Slice current_key_;
  Slice current_value_;
  Status status_;

  // 偏移量缓存，用于高效的向后导航
  std::vector<size_t> offset_cache_;
};

SSTableReader::SSTableReader()
    : env_(nullptr), comparator_(nullptr), opened_(false),
      index_block_offset_(0), index_block_size_(0), bloom_block_offset_(0),
      bloom_block_size_(0) {
  // 注册error_handler回调
  ErrorRecoveryManager::Instance().RegisterCleanupCallback(
      "SSTable",
      [this](const std::string &component,
             const std::string &operation) -> Status {
        return this->HandleErrorRecovery(component, operation);
      });

  // 注册文件损坏错误的特定恢复回调
  ErrorRecoveryManager::Instance().RegisterRecoveryCallback(
      ErrorType::CorruptionError, [this](const ErrorReport &error) -> Status {
        return this->HandleCorruptionError(error);
      });

  LOG_DEBUG << "SSTableReader initialized with error recovery callbacks";
}

SSTableReader::~SSTableReader() = default;

Status SSTableReader::Open(const std::string &filename, Env *env,
                           const Comparator *comparator) {
  filename_ = filename;
  env_ = env;
  comparator_ = comparator;

  // 打开文件
  Status s = env->NewRandomAccessFile(filename, &file_);
  if (!s.ok()) {
    return s;
  }

  // 读取文件大小
  uint64_t file_size;
  s = env->GetFileSize(filename, &file_size);
  if (!s.ok()) {
    return s;
  }

  if (file_size <
      BlockHeader::kHeaderSize * 2) { // 至少需要一个数据块和一个索引块
    return Status::Corruption("SSTable file too small");
  }

  // 存储文件大小到meta_
  meta_.file_size = file_size;

  // 读取元数据
  s = ReadMeta();
  if (!s.ok()) {
    return s;
  }

  // 读取索引
  s = ReadIndex();
  if (!s.ok()) {
    return s;
  }

  // 读取Bloom过滤器
  s = ReadBloomFilter();
  if (!s.ok()) {
    return s; // Bloom过滤器可选，失败不影响整体功能
  }

  opened_ = true;
  return Status::OK();
}

Status SSTableReader::Get(const Slice &key, std::string *value, bool *found,
                          uint64_t snapshot) {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (!opened_) {
    return Status::InvalidArgument("SSTable not opened");
  }

  *found = false;

  // 首先检查键范围
  if (!meta_.KeyInRange(key, comparator_)) {
    return Status::OK();
  }

  // 检查Bloom过滤器
  if (bloom_filter_ && !bloom_filter_->MayContain(key)) {
    return Status::OK();
  }

  // 使用二分查找定位数据块
  int block_index = FindIndexEntry(key);
  if (block_index < 0) {
    return Status::OK();
  }

  // 读取数据块
  const IndexEntry &entry = index_[block_index];
  std::string block_data;
  Status s = ReadDataBlock(entry.block_offset, entry.block_size, &block_data);
  RETURN_IF_ERROR(s);

  // 在块内查找键。同一用户键可能存在多个版本，取对快照可见的最大序列号版本。
  Slice input(block_data);
  bool best_found = false;
  uint64_t best_sequence = 0;
  std::string best_value;
  bool best_is_delete = false;

  while (!input.empty()) {
    // 解析存储的InternalKey
    Slice stored_internal_key;
    if (!coding::GetLengthPrefixedSlice(&input, &stored_internal_key)) {
      Status corruption_error =
          Status::Corruption("Failed to parse key in data block");
      RETURN_IF_ERROR(corruption_error);
    }

    // 解析值
    Slice stored_value;
    if (!coding::GetLengthPrefixedSlice(&input, &stored_value)) {
      Status corruption_error =
          Status::Corruption("Failed to parse value in data block");
      RETURN_IF_ERROR(corruption_error);
    }

    // 解析InternalKey得到用户键、序列号、操作类型
    Slice user_key;
    uint64_t sequence;
    uint8_t type;
    if (!coding::ParseInternalKey(stored_internal_key, &user_key, &sequence,
                                  &type)) {
      // 跳过无效的键
      continue;
    }

    // 比较用户键
    int cmp = comparator_->Compare(user_key, key);
    if (cmp == 0) {
      // 匹配的用户键：只记录对快照可见且序列号最大的版本
      if ((snapshot == 0 || sequence <= snapshot) && sequence >= best_sequence) {
        best_found = true;
        best_sequence = sequence;
        best_is_delete = (type == static_cast<uint8_t>(ValueType::kDeletion));
        best_value = stored_value.ToString();
      }
    } else if (cmp > 0) {
      // 已经超过目标键，没有找到更多版本
      break;
    }
    // cmp < 0，继续查找
  }

  if (best_found) {
    if (best_is_delete) {
      *found = false; // 最新可见版本是删除标记
    } else {
      *found = true;
      if (value) {
        *value = best_value;
      }
    }
  }

  return Status::OK();
}

std::unique_ptr<SSTableReader::Iterator>
SSTableReader::NewIterator(uint64_t snapshot) {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (!opened_) {
    return nullptr;
  }

  return std::make_unique<SSTableIterator>(this, snapshot);
}

bool SSTableReader::MayContainKey(const Slice &key) {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (!opened_) {
    return false;
  }

  // 快速检查：先检查键范围，避免不必要的Bloom过滤器查询
  if (!meta_.KeyInRange(key, comparator_)) {
    return false;
  }

  // Bloom过滤器查询
  if (bloom_filter_) {
    // 如果Bloom过滤器说不存在，则一定不存在
    // 如果说可能存在，则需要进一步查询实际数据
    return bloom_filter_->MayContain(key);
  }

  // 没有Bloom过滤器时，基于键范围的保守返回
  return true;
}

bool SSTableReader::KeyInRange(const Slice &key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return meta_.KeyInRange(key, comparator_);
}

Status SSTableReader::VerifyChecksum() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (!opened_) {
    return Status::InvalidArgument("SSTable not opened");
  }

  // 验证所有数据块的校验和
  for (const IndexEntry &entry : index_) {
    std::string block_data;
    Status s = ReadDataBlock(entry.block_offset, entry.block_size, &block_data);
    if (!s.ok()) {
      return s;
    }

    // 读取块头部并验证校验和
    if (entry.block_size < BlockHeader::kHeaderSize) {
      return Status::Corruption("Block too small");
    }

    Slice header_slice;
    std::string header_scratch(BlockHeader::kHeaderSize, '\0');
    s = file_->Read(entry.block_offset, BlockHeader::kHeaderSize, &header_slice,
                    &header_scratch[0]);
    if (!s.ok()) {
      return s;
    }

    BlockHeader header;
    if (!header.Decode(header_slice)) {
      return Status::Corruption("Failed to decode block header");
    }

    uint32_t actual_crc = hash_util::CRC32(
        block_data.data() + BlockHeader::kHeaderSize, header.size);
    if (actual_crc != header.crc32) {
      return Status::Corruption("Block checksum mismatch");
    }
  }

  return Status::OK();
}

size_t SSTableReader::ApproximateMemoryUsage() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  size_t usage = sizeof(*this);
  usage += filename_.size();
  usage += index_.size() * sizeof(IndexEntry);
  for (const IndexEntry &entry : index_) {
    usage += entry.key.size();
  }

  if (bloom_filter_) {
    // 准确计算Bloom过滤器内存使用
    usage += bloom_filter_->GetBitArraySize() / 8; // 位数组大小
    usage += sizeof(BloomFilterReader);            // 读取器对象大小
    usage += kBloomFilterOverheadBytes;            // 其他开销（使用常量）
  }

  return usage;
}

SSTableReader::BloomFilterStats SSTableReader::GetBloomFilterStats() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  BloomFilterStats stats;

  if (bloom_filter_) {
    stats.has_bloom_filter = true;
    stats.bit_array_size = bloom_filter_->GetBitArraySize();
    stats.hash_function_count = bloom_filter_->GetHashFunctionCount();
    stats.bloom_memory_usage = stats.bit_array_size / 8 +
                               sizeof(BloomFilterReader) +
                               kBloomFilterOverheadBytes;

    // 基于位数组大小和哈希函数数量估算假阳性率
    // 假设元素数量约等于SSTable中的键数量
    if (meta_.num_entries > 0) {
      double m = static_cast<double>(stats.bit_array_size);      // 位数组大小
      double k = static_cast<double>(stats.hash_function_count); // 哈希函数数量
      double n = static_cast<double>(meta_.num_entries);         // 元素数量

      // 假阳性率公式: (1 - e^(-kn/m))^k
      stats.estimated_false_positive_rate =
          std::pow(1.0 - std::exp(-k * n / m), k);
    } else {
      stats.estimated_false_positive_rate = 0.0;
    }
  } else {
    stats.has_bloom_filter = false;
    stats.bloom_memory_usage = 0;
    stats.bit_array_size = 0;
    stats.hash_function_count = 0;
    stats.estimated_false_positive_rate = 0.0;
  }

  return stats;
}

Status SSTableReader::PrefetchRange(const Slice &begin, const Slice &end) {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (!opened_) {
    return Status::InvalidArgument("SSTable not opened");
  }

  // 找到范围内的所有块并预取
  for (const IndexEntry &entry : index_) {
    if (comparator_->Compare(Slice(entry.key), begin) >= 0 &&
        comparator_->Compare(Slice(entry.key), end) <= 0) {

      // 预取数据块（优化后的同步预取）
      std::string block_data;
      block_data.reserve(entry.block_size); // 预分配内存
      Status s =
          ReadDataBlock(entry.block_offset, entry.block_size, &block_data);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return Status::OK();
}

// 私有方法实现

Status SSTableReader::ReadMeta() {
  uint64_t file_size = meta_.file_size;

  // SSTable文件格式：
  // [Data Blocks][Index Block][Bloom Filter Block][Meta Block][Footer]
  // Footer包含各个块的位置信息

  // 1. 读取Footer（固定48字节）
  const size_t footer_size = 48;
  if (file_size < footer_size) {
    return Status::Corruption("File too small for footer");
  }

  std::string footer_scratch(footer_size, '\0');
  Slice footer_data;
  Status s = file_->Read(file_size - footer_size, footer_size, &footer_data,
                         &footer_scratch[0]);
  if (!s.ok()) {
    return s;
  }

  // 2. 解析Footer
  Slice input = footer_data;

  // Meta block位置 (8字节offset + 4字节size)
  uint64_t meta_block_offset;
  uint32_t meta_block_size;
  if (!coding::GetVarint64(&input, &meta_block_offset) ||
      !coding::GetVarint32(&input, &meta_block_size)) {
    return Status::Corruption("Invalid footer: meta block info");
  }

  // Index block位置 (8字节offset + 4字节size)
  uint64_t index_block_offset;
  uint32_t index_block_size;
  if (!coding::GetVarint64(&input, &index_block_offset) ||
      !coding::GetVarint32(&input, &index_block_size)) {
    return Status::Corruption("Invalid footer: index block info");
  }

  // Bloom filter block位置 (8字节offset + 4字节size)
  uint64_t bloom_block_offset;
  uint32_t bloom_block_size;
  if (!coding::GetVarint64(&input, &bloom_block_offset) ||
      !coding::GetVarint32(&input, &bloom_block_size)) {
    return Status::Corruption("Invalid footer: bloom block info");
  }

  // 文件格式版本和魔数 (4字节version + 4字节magic)
  uint32_t version, magic;
  if (!coding::GetVarint32(&input, &version) ||
      !coding::GetVarint32(&input, &magic)) {
    return Status::Corruption("Invalid footer: version/magic");
  }

  // 验证魔数
  const uint32_t kSSTableMagic = 0x57A7AB1E; // "SSTable" 的变换
  if (magic != kSSTableMagic) {
    return Status::Corruption("Invalid SSTable magic number");
  }

  if (version != 1) {
    return Status::NotSupported("Unsupported SSTable version: " +
                                std::to_string(version));
  }

  // 3. 读取并解析Meta block
  if (meta_block_offset + meta_block_size > file_size) {
    return Status::Corruption("Meta block extends beyond file");
  }

  std::string meta_scratch(meta_block_size, '\0');
  Slice meta_data;
  s = file_->Read(meta_block_offset, meta_block_size, &meta_data,
                  &meta_scratch[0]);
  if (!s.ok()) {
    return s;
  }

  // 解析Meta block (跳过BlockHeader)
  if (meta_data.size() < BlockHeader::kHeaderSize) {
    return Status::Corruption("Meta block too small");
  }

  // 验证块头
  BlockHeader header;
  if (!header.Decode(Slice(meta_data.data(), BlockHeader::kHeaderSize))) {
    return Status::Corruption("Invalid meta block header");
  }

  if (header.type != BlockType::kMetaBlock) {
    return Status::Corruption("Expected meta block type");
  }

  // 验证校验和
  Slice meta_content(meta_data.data() + BlockHeader::kHeaderSize,
                     meta_data.size() - BlockHeader::kHeaderSize);
  s = VerifyBlockChecksum(meta_content, header.crc32);
  if (!s.ok()) {
    return s;
  }

  // 解析元数据内容
  if (!meta_.Decode(meta_content)) {
    return Status::Corruption("Failed to decode meta data");
  }

  // 设置文件相关信息
  meta_.filename = filename_;
  meta_.file_size = file_size;

  // 存储块位置信息供后续使用
  index_block_offset_ = index_block_offset;
  index_block_size_ = index_block_size;
  bloom_block_offset_ = bloom_block_offset;
  bloom_block_size_ = bloom_block_size;

  return Status::OK();
}

Status SSTableReader::ReadIndex() {
  // 检查是否有有效的索引块位置信息
  if (index_block_size_ == 0) {
    return Status::Corruption("Invalid index block size");
  }

  if (index_block_offset_ + index_block_size_ > meta_.file_size) {
    return Status::Corruption("Index block extends beyond file");
  }

  // 1. 读取索引块数据
  std::string index_scratch(index_block_size_, '\0');
  Slice index_data;
  Status s = file_->Read(index_block_offset_, index_block_size_, &index_data,
                         &index_scratch[0]);
  if (!s.ok()) {
    return s;
  }

  // 2. 验证块头
  if (index_data.size() < BlockHeader::kHeaderSize) {
    return Status::Corruption("Index block too small");
  }

  BlockHeader header;
  if (!header.Decode(Slice(index_data.data(), BlockHeader::kHeaderSize))) {
    return Status::Corruption("Invalid index block header");
  }

  if (header.type != BlockType::kIndexBlock) {
    return Status::Corruption("Expected index block type");
  }

  // 3. 验证校验和
  Slice index_content(index_data.data() + BlockHeader::kHeaderSize,
                      index_data.size() - BlockHeader::kHeaderSize);
  s = VerifyBlockChecksum(index_content, header.crc32);
  if (!s.ok()) {
    return s;
  }

  // 4. 解析索引项
  index_.clear();
  Slice input = index_content;

  // 索引块格式：每个索引项先以长度前缀编码，再是条目本体
  while (!input.empty()) {
    Slice entry_data;
    if (!coding::GetLengthPrefixedSlice(&input, &entry_data)) {
      // 解析失败，可能是块末尾的填充区域
      break;
    }

    IndexEntry entry;
    if (!entry.Decode(entry_data)) {
      break;
    }

    // 验证索引项的有效性
    if (entry.block_offset + entry.block_size > meta_.file_size) {
      return Status::Corruption("Index entry points beyond file");
    }

    // 验证键的排序（除了第一个索引项）
    if (!index_.empty()) {
      int cmp =
          comparator_->Compare(Slice(index_.back().key), Slice(entry.key));
      if (cmp >= 0) {
        return Status::Corruption("Index keys not in sorted order");
      }
    }

    index_.push_back(std::move(entry));
  }

  // 5. 验证至少有一个索引项
  if (index_.empty()) {
    return Status::Corruption("Empty index block");
  }

  // 6. 验证索引覆盖范围与文件元数据一致
  // 注意：索引键是内部键（用户键+8字节后缀），元数据里存的是用户键
  if (!index_.empty()) {
    auto user_key_part = [](const std::string &internal_key) {
      if (internal_key.size() < 8) {
        return Slice(internal_key);
      }
      return Slice(internal_key.data(), internal_key.size() - 8);
    };

    const std::string &first_key = index_.front().key;
    const std::string &last_key = index_.back().key;

    // 检查键范围是否与元数据一致
    // 索引项记录的是各数据块的首键：首项必须等于最小键，
    // 末项（最后一块的首键）必须不大于最大键
    if (!meta_.smallest_key.empty() && !meta_.largest_key.empty()) {
      int cmp_first = comparator_->Compare(user_key_part(first_key),
                                           Slice(meta_.smallest_key));
      int cmp_last = comparator_->Compare(user_key_part(last_key),
                                          Slice(meta_.largest_key));
      if (cmp_first != 0 || cmp_last > 0) {
        return Status::Corruption(
            "Index key range inconsistent with meta data");
      }
    }
  }

  return Status::OK();
}

Status SSTableReader::ReadBloomFilter() {
  // 检查是否有有效的Bloom过滤器块
  if (bloom_block_size_ == 0) {
    // 没有Bloom过滤器是允许的
    bloom_filter_.reset();
    return Status::OK();
  }

  if (bloom_block_offset_ + bloom_block_size_ > meta_.file_size) {
    return Status::Corruption("Bloom filter block extends beyond file");
  }

  // 1. 读取Bloom过滤器块数据
  std::string bloom_scratch(bloom_block_size_, '\0');
  Slice bloom_data;
  Status s = file_->Read(bloom_block_offset_, bloom_block_size_, &bloom_data,
                         &bloom_scratch[0]);
  if (!s.ok()) {
    return s;
  }

  // 2. 验证块头
  if (bloom_data.size() < BlockHeader::kHeaderSize) {
    return Status::Corruption("Bloom filter block too small");
  }

  BlockHeader header;
  if (!header.Decode(Slice(bloom_data.data(), BlockHeader::kHeaderSize))) {
    return Status::Corruption("Invalid bloom filter block header");
  }

  if (header.type != BlockType::kBloomBlock) {
    return Status::Corruption("Expected bloom filter block type");
  }

  // 3. 验证校验和
  Slice bloom_content(bloom_data.data() + BlockHeader::kHeaderSize,
                      bloom_data.size() - BlockHeader::kHeaderSize);
  s = VerifyBlockChecksum(bloom_content, header.crc32);
  if (!s.ok()) {
    return s;
  }

  // 4. 创建Bloom过滤器读取器并加载数据
  bloom_filter_ = std::make_unique<BloomFilterReader>();

  // 使用BloomFilterReader的Initialize方法直接加载数据
  // BloomFilterBuilder::Finish()生成的数据格式与BloomFilterReader兼容
  if (!bloom_filter_->Initialize(bloom_content)) {
    return Status::Corruption("Failed to initialize bloom filter from data");
  }

  return Status::OK();
}

Status SSTableReader::ReadDataBlock(uint64_t offset, uint32_t size,
                                    std::string *result) {
  if (size == 0) {
    result->clear();
    return Status::OK();
  }

  // 索引项记录的是整个数据块（含块头）的范围，读取时跳过块头
  if (size <= BlockHeader::kHeaderSize) {
    return Status::Corruption("Data block too small");
  }

  const uint64_t data_offset = offset + BlockHeader::kHeaderSize;
  const uint32_t data_size = size - BlockHeader::kHeaderSize;

  result->resize(data_size);
  Slice data;
  Status s = file_->Read(data_offset, data_size, &data, &(*result)[0]);
  if (!s.ok()) {
    return s;
  }

  if (data.size() != data_size) {
    return Status::IOError("Short read");
  }

  // 如果数据是直接读入result缓冲区的，就不需要再复制
  if (data.data() != &(*result)[0]) {
    result->assign(data.data(), data.size());
  }

  return Status::OK();
}

Status SSTableReader::VerifyBlockChecksum(const Slice &block,
                                          uint32_t expected_crc) {
  // 调用方传入的是去掉块头后的块内容，直接对整体计算CRC即可
  uint32_t actual_crc = hash_util::CRC32(block.data(), block.size());
  if (actual_crc != expected_crc) {
    return Status::Corruption("Block checksum mismatch");
  }

  return Status::OK();
}

int SSTableReader::FindIndexEntry(const Slice &key) const {
  if (index_.empty()) {
    return -1;
  }

  // 二分查找最后一个首键不大于key的索引项
  // 注意：索引键是内部键（用户键+8字节序列后缀），比较时只取用户键部分
  int left = 0, right = static_cast<int>(index_.size()) - 1;
  int result = -1;

  auto index_user_key = [](const std::string &internal_key) {
    if (internal_key.size() < 8) {
      return Slice(internal_key);
    }
    return Slice(internal_key.data(), internal_key.size() - 8);
  };

  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_->Compare(index_user_key(index_[mid].key), key);

    if (cmp <= 0) {
      result = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return result;
}

// ============================================================================
// SSTableWriter 实现
// ============================================================================

SSTableWriter::SSTableWriter(const SSTableWriterOptions &options)
    : options_(options), opened_(false), finished_(false), num_entries_(0),
      current_offset_(0) {
  write_stats_ = {};
}

SSTableWriter::~SSTableWriter() {
  if (opened_ && !finished_) {
    Abandon(); // 自动清理未完成的文件
  }
}

Status SSTableWriter::Open(const std::string &filename, Env *env,
                           const Comparator *comparator) {
  if (opened_) {
    return Status::InvalidArgument("SSTable writer already opened");
  }

  filename_ = filename;
  comparator_ = comparator;

  // 创建写入文件
  Status s = env->NewWritableFile(filename, &file_);
  if (!s.ok()) {
    return s;
  }

  // 初始化元数据
  meta_.filename = filename;
  meta_.file_number = 0; // 由上层设置
  meta_.file_size = 0;
  meta_.level = SSTableLevel::kLevel0; // 默认L0
  meta_.num_entries = 0;
  meta_.num_deletions = 0;
  meta_.raw_key_size = 0;
  meta_.raw_value_size = 0;
  meta_.creation_time =
      std::chrono::system_clock::now().time_since_epoch().count();
  meta_.oldest_key_time = 0;

  // 初始化Bloom过滤器构建器
  if (options_.enable_bloom_filter) {
    bloom_builder_ = std::make_unique<BloomFilterBuilder>(
        options_.bloom_filter_bits_per_key);
  }

  // 预留缓冲区空间
  data_buffer_.reserve(options_.write_buffer_size);
  index_entries_.reserve(kDefaultIndexEntryReserve); // 使用常量

  opened_ = true;
  current_offset_ = 0;

  return Status::OK();
}

Status SSTableWriter::Add(const Slice &key, const Slice &value) {
  if (!opened_) {
    return Status::InvalidArgument("SSTable writer not opened");
  }

  if (finished_) {
    return Status::InvalidArgument("SSTable writer already finished");
  }

  // 检查键顺序（必须递增）
  if (!last_key_.empty()) {
    int cmp = comparator_->Compare(Slice(last_key_), key);
    if (cmp >= 0) {
      return Status::InvalidArgument("Keys must be added in ascending order");
    }
  }

  // 检查是否需要开始新的数据块
  if (ShouldStartNewBlock()) {
    Status s = WriteDataBlock();
    if (!s.ok()) {
      return s;
    }
  }

  // 如果这是块的第一个键，创建索引条目
  if (data_buffer_.empty()) {
    IndexEntry entry;
    entry.key = key.ToString();
    entry.block_offset = current_offset_;
    entry.block_size = 0; // 稍后更新
    entry.first_key_offset = 0;
    index_entries_.push_back(entry);
  }

  // 编码键值对到数据缓冲区
  coding::PutLengthPrefixedSlice(&data_buffer_, key);
  coding::PutLengthPrefixedSlice(&data_buffer_, value);

  // 添加到Bloom过滤器 - 只添加用户键部分
  if (bloom_builder_) {
    // 解析InternalKey，只将用户键添加到Bloom过滤器
    Slice user_key;
    uint64_t sequence;
    uint8_t type;
    if (coding::ParseInternalKey(key, &user_key, &sequence, &type)) {
      bloom_builder_->AddKey(user_key);
    } else {
      // 如果无法解析，假设它就是用户键
      bloom_builder_->AddKey(key);
    }
  }

  // 更新元数据
  UpdateMeta(key, value);

  // 更新最后键
  last_key_ = key.ToString();
  num_entries_++;

  return Status::OK();
}

Status SSTableWriter::Finish() {
  if (!opened_) {
    return Status::InvalidArgument("SSTable writer not opened");
  }

  if (finished_) {
    return Status::InvalidArgument("SSTable writer already finished");
  }

  Status s;

  // 写入最后的数据块（如果有数据）
  if (!data_buffer_.empty()) {
    s = WriteDataBlock();
    if (!s.ok()) {
      return s;
    }
  }

  // 写入索引块
  s = WriteIndexBlock();
  if (!s.ok()) {
    return s;
  }

  // 写入Bloom过滤器块
  if (bloom_builder_) {
    s = WriteBloomFilterBlock();
    if (!s.ok()) {
      return s;
    }
  }

  // 写入元数据块
  s = WriteMetaBlock();
  if (!s.ok()) {
    return s;
  }

  // 写入Footer（48字节：meta/index/bloom块位置 + 版本 + 魔数）
  {
    std::string footer;
    coding::PutVarint64(&footer, meta_block_offset_);
    coding::PutVarint32(&footer, meta_block_size_);
    coding::PutVarint64(&footer, index_block_offset_);
    coding::PutVarint32(&footer, index_block_size_);
    coding::PutVarint64(&footer, bloom_block_offset_);
    coding::PutVarint32(&footer, bloom_block_size_);
    coding::PutVarint32(&footer, 1);               // 格式版本
    coding::PutVarint32(&footer, 0x57A7AB1E);      // 魔数
    footer.resize(48, '\0');                       // 固定48字节，不足补零
    s = file_->Append(Slice(footer));
    if (!s.ok()) {
      return s;
    }
    current_offset_ += 48;
  }

  // 刷新文件
  s = file_->Flush();
  if (!s.ok()) {
    return s;
  }

  s = file_->Sync();
  if (!s.ok()) {
    return s;
  }

  s = file_->Close();
  if (!s.ok()) {
    return s;
  }

  // 更新最终元数据
  meta_.file_size = current_offset_;
  meta_.num_entries = num_entries_;

  finished_ = true;
  return Status::OK();
}

Status SSTableWriter::Abandon() {
  if (!opened_) {
    return Status::OK();
  }

  if (file_) {
    file_->Close();
  }

  // 删除未完成的文件
  if (!filename_.empty()) {
    Env::Default()->DeleteFile(filename_);
  }

  opened_ = false;
  finished_ = true;

  return Status::OK();
}

uint64_t SSTableWriter::FileSize() const { return current_offset_; }

SSTableWriter::WriteStats SSTableWriter::GetWriteStats() const {
  return write_stats_;
}

// 私有方法实现

bool SSTableWriter::ShouldStartNewBlock() const {
  return data_buffer_.size() >= options_.block_size;
}

Status SSTableWriter::WriteDataBlock() {
  if (data_buffer_.empty()) {
    return Status::OK();
  }

  // 计算CRC32校验和
  uint32_t crc32 = hash_util::CRC32(data_buffer_.data(), data_buffer_.size());

  // 创建块头部
  BlockHeader header;
  header.type = BlockType::kDataBlock;
  header.size = static_cast<uint32_t>(data_buffer_.size());
  header.crc32 = crc32;
  header.compression = 0; // 暂不支持压缩

  // 写入块头部
  Status s = WriteBlockHeader(BlockType::kDataBlock, Slice(data_buffer_));
  if (!s.ok()) {
    return s;
  }

  // 写入数据
  s = file_->Append(Slice(data_buffer_));
  if (!s.ok()) {
    return s;
  }

  // 更新索引条目的块大小
  if (!index_entries_.empty()) {
    index_entries_.back().block_size =
        static_cast<uint32_t>(BlockHeader::kHeaderSize + data_buffer_.size());
  }

  // 更新统计
  write_stats_.data_bytes_written +=
      BlockHeader::kHeaderSize + data_buffer_.size();
  current_offset_ += BlockHeader::kHeaderSize + data_buffer_.size();

  // 清空缓冲区
  data_buffer_.clear();

  return Status::OK();
}

Status SSTableWriter::WriteIndexBlock() {
  if (index_entries_.empty()) {
    return Status::OK();
  }

  std::string index_data;

  // 编码索引条目
  for (const IndexEntry &entry : index_entries_) {
    std::string encoded_entry = entry.Encode();
    coding::PutLengthPrefixedSlice(&index_data, Slice(encoded_entry));
  }

  // 记录索引块位置（供Footer使用）
  index_block_offset_ = current_offset_;
  index_block_size_ =
      static_cast<uint32_t>(BlockHeader::kHeaderSize + index_data.size());

  // 写入索引块
  Status s = WriteBlockHeader(BlockType::kIndexBlock, Slice(index_data));
  if (!s.ok()) {
    return s;
  }

  s = file_->Append(Slice(index_data));
  if (!s.ok()) {
    return s;
  }

  // 更新统计
  write_stats_.index_bytes_written +=
      BlockHeader::kHeaderSize + index_data.size();
  current_offset_ += BlockHeader::kHeaderSize + index_data.size();

  return Status::OK();
}

Status SSTableWriter::WriteBloomFilterBlock() {
  if (!bloom_builder_) {
    return Status::OK();
  }

  // 完成Bloom过滤器构建
  Slice bloom_data = bloom_builder_->Finish();

  if (bloom_data.empty()) {
    return Status::OK();
  }

  // 记录Bloom块位置（供Footer使用）
  bloom_block_offset_ = current_offset_;
  bloom_block_size_ =
      static_cast<uint32_t>(BlockHeader::kHeaderSize + bloom_data.size());

  // 写入Bloom过滤器块
  Status s = WriteBlockHeader(BlockType::kBloomBlock, bloom_data);
  if (!s.ok()) {
    return s;
  }

  s = file_->Append(bloom_data);
  if (!s.ok()) {
    return s;
  }

  // 更新统计
  write_stats_.bloom_bytes_written +=
      BlockHeader::kHeaderSize + bloom_data.size();
  current_offset_ += BlockHeader::kHeaderSize + bloom_data.size();

  return Status::OK();
}

Status SSTableWriter::WriteMetaBlock() {
  // 编码元数据
  std::string meta_data = meta_.Encode();

  // 记录元数据块位置（供Footer使用）
  meta_block_offset_ = current_offset_;
  meta_block_size_ =
      static_cast<uint32_t>(BlockHeader::kHeaderSize + meta_data.size());

  // 写入元数据块
  Status s = WriteBlockHeader(BlockType::kMetaBlock, Slice(meta_data));
  if (!s.ok()) {
    return s;
  }

  s = file_->Append(Slice(meta_data));
  if (!s.ok()) {
    return s;
  }

  // 更新偏移量
  current_offset_ += BlockHeader::kHeaderSize + meta_data.size();

  return Status::OK();
}

Status SSTableWriter::WriteBlockHeader(BlockType type, const Slice &data) {
  BlockHeader header;
  header.type = type;
  header.size = static_cast<uint32_t>(data.size());
  header.crc32 = hash_util::CRC32(data.data(), data.size());
  header.compression = 0;

  std::string header_data = header.Encode();
  return file_->Append(Slice(header_data));
}

Status SSTableWriter::FlushBuffer() { return file_->Flush(); }

void SSTableWriter::UpdateMeta(const Slice &key, const Slice &value) {
  // 解析内部键以获取准确的操作类型
  Slice user_key;
  uint64_t sequence;
  uint8_t type;

  if (coding::ParseInternalKey(key, &user_key, &sequence, &type)) {
    // 使用用户键更新键范围（而不是内部键）
    if (meta_.smallest_key.empty()) {
      meta_.smallest_key = user_key.ToString();
    }
    meta_.largest_key = user_key.ToString();

    // 根据操作类型更新统计信息
    ValueType op_type = static_cast<ValueType>(type);
    switch (op_type) {
    case ValueType::kValue:
      // 普通键值对
      break;
    case ValueType::kDeletion:
      meta_.num_deletions++;
      break;
    case ValueType::kMerge:
      // 合并操作统计（如果需要的话可以在这里添加merger_count字段）
      break;
    default:
      // 未知类型，记录但继续处理
      break;
    }

    // 更新最老键时间：使用序列号来估算时间
    if (meta_.oldest_key_time == 0 || sequence < meta_.oldest_key_time) {
      // 使用序列号作为时间戳的近似值
      // 在真实系统中，可能需要维护序列号到时间戳的映射
      meta_.oldest_key_time = sequence;
    }
  } else {
    // 如果无法解析内部键，使用原始键
    if (meta_.smallest_key.empty()) {
      meta_.smallest_key = key.ToString();
    }
    meta_.largest_key = key.ToString();
  }

  // 更新大小统计（内部键的实际大小）
  meta_.raw_key_size += key.size();
  meta_.raw_value_size += value.size();
}

// ============================================================================
// SSTableCompactor 实现 - 压缩引擎（属于SSTable层）
// ============================================================================

SSTableCompactor::SSTableCompactor(Env *env, const Comparator *comparator)
    : env_(env), comparator_(comparator) {
  // 使用默认的写入选项
  writer_options_.block_size = 64 * 1024; // 64KB
  writer_options_.enable_bloom_filter = true;
  writer_options_.bloom_filter_bits_per_key = 10.0;

  // 初始化压缩配置为默认值
  config_ = CompactionConfig{};
}

SSTableCompactor::~SSTableCompactor() = default;

CompactionResult SSTableCompactor::CompactFiles(const CompactionJob &job) {
  CompactionResult result;

  if (job.input_files.empty()) {
    result.status = Status::InvalidArgument("No input files for compaction");
    return result;
  }

  auto start_time = std::chrono::steady_clock::now();

  // 执行压缩
  result.status = DoCompaction(job, &result);

  auto end_time = std::chrono::steady_clock::now();
  result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  return result;
}

CompactionResult SSTableCompactor::CompactRange(
    const std::vector<std::string> &input_files,
    const std::string &output_file_prefix, SSTableLevel output_level,
    const std::string &start_key, const std::string &end_key) {

  CompactionJob job;
  job.type = CompactionType::kMajor;
  job.input_level = SSTableLevel::kLevel0; // 简化
  job.output_level = output_level;
  job.input_files = input_files;
  job.output_file_prefix = output_file_prefix;
  job.start_key = start_key;
  job.end_key = end_key;
  job.is_manual = true;
  job.delete_obsolete_files = true;

  return CompactFiles(job);
}

Status SSTableCompactor::FlushMemTable(const MemTable *memtable,
                                       const std::string &output_filename,
                                       SSTableLevel output_level,
                                       SSTableMeta *output_meta) {

  if (!memtable) {
    return Status::InvalidArgument("MemTable is null");
  }

  // 创建SSTable写入器
  SSTableWriter writer(writer_options_);
  Status s = writer.Open(output_filename, env_, comparator_);
  if (!s.ok()) {
    return s;
  }

  // 收集MemTable全部条目后按内部键字节序排序写入：
  // MemTable迭代器按InternalKeyComparator序（同用户键新版本在前），
  // 而SSTable内要求字节序（同用户键旧版本在前），直接写入会违反升序约束
  struct Entry {
    std::string key;
    std::string value;
  };
  std::vector<Entry> entries;

  auto iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    InternalKey internal_key(iter->key(), iter->sequence(), iter->type());
    entries.push_back(Entry{internal_key.Encode(), iter->value().ToString()});
    iter->Next();
  }

  s = iter->status();
  if (!s.ok()) {
    writer.Abandon();
    return s;
  }

  // 按内部键字节序排序（用户键升序，同键序列号字节升序）
  std::sort(entries.begin(), entries.end(),
            [](const Entry &a, const Entry &b) { return a.key < b.key; });

  for (const auto &entry : entries) {
    s = writer.Add(Slice(entry.key), Slice(entry.value));
    if (!s.ok()) {
      writer.Abandon();
      return s;
    }
  }

  // 完成写入
  s = writer.Finish();
  if (!s.ok()) {
    return s;
  }

  // 返回元数据
  if (output_meta) {
    *output_meta = writer.GetMeta();
    output_meta->level = output_level;
  }

  return Status::OK();
}

CompactionResult SSTableCompactor::CompactWithSplit(
    const std::string &large_file, const std::string &output_prefix,
    size_t target_file_size, SSTableLevel output_level) {

  CompactionResult result;

  // 打开输入文件
  SSTableReader reader;
  Status s = reader.Open(large_file, env_, comparator_);
  if (!s.ok()) {
    result.status = s;
    return result;
  }

  auto iter = reader.NewIterator();
  if (!iter) {
    result.status = Status::IOError("Failed to create iterator");
    return result;
  }

  iter->SeekToFirst();

  int file_index = 0;
  while (iter->Valid()) {
    // 生成输出文件名
    std::string output_filename =
        output_prefix + "_" + std::to_string(file_index) + ".sst";

    // 创建新的写入器
    SSTableWriter writer(writer_options_);
    s = writer.Open(output_filename, env_, comparator_);
    if (!s.ok()) {
      result.status = s;
      return result;
    }

    // 写入数据直到达到目标大小
    while (iter->Valid() && writer.FileSize() < target_file_size) {
      s = writer.Add(iter->key(), iter->value());
      if (!s.ok()) {
        writer.Abandon();
        result.status = s;
        return result;
      }

      result.num_input_records++;
      iter->Next();
    }

    // 完成当前文件
    s = writer.Finish();
    if (!s.ok()) {
      result.status = s;
      return result;
    }

    // 记录输出文件
    SSTableMeta meta = writer.GetMeta();
    meta.level = output_level;
    result.output_files.push_back(output_filename);
    result.output_metas.push_back(meta);
    result.bytes_written += writer.FileSize();
    result.num_output_records += writer.NumEntries();

    file_index++;
  }

  result.status = iter->status();
  result.bytes_read = reader.GetFileSize();

  return result;
}

Status SSTableCompactor::RebuildBloomFilter(const std::string &input_file,
                                            double new_bits_per_key) {

  // 打开输入文件
  SSTableReader reader;
  Status s = reader.Open(input_file, env_, comparator_);
  if (!s.ok()) {
    return s;
  }

  // 生成临时输出文件名
  std::string temp_file = input_file + ".tmp";

  // 创建新的写入器，使用新的Bloom过滤器参数
  SSTableWriterOptions new_options = writer_options_;
  new_options.bloom_filter_bits_per_key = new_bits_per_key;

  SSTableWriter writer(new_options);
  s = writer.Open(temp_file, env_, comparator_);
  if (!s.ok()) {
    return s;
  }

  // 复制所有数据（Bloom过滤器会自动重建）
  auto iter = reader.NewIterator();
  if (!iter) {
    return Status::IOError("Failed to create iterator");
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    s = writer.Add(iter->key(), iter->value());
    if (!s.ok()) {
      writer.Abandon();
      env_->DeleteFile(temp_file);
      return s;
    }
    iter->Next();
  }

  s = iter->status();
  if (!s.ok()) {
    writer.Abandon();
    env_->DeleteFile(temp_file);
    return s;
  }

  // 完成写入
  s = writer.Finish();
  if (!s.ok()) {
    env_->DeleteFile(temp_file);
    return s;
  }

  // 原子替换文件
  s = env_->RenameFile(temp_file, input_file);
  if (!s.ok()) {
    env_->DeleteFile(temp_file);
    return s;
  }

  return Status::OK();
}

Status
SSTableCompactor::VerifyCompactionResult(const CompactionResult &result) {
  if (!result.IsSuccess()) {
    return result.status;
  }

  // 验证输出文件的完整性
  for (const std::string &filename : result.output_files) {
    SSTableReader reader;
    Status s = reader.Open(filename, env_, comparator_);
    if (!s.ok()) {
      return Status::Corruption("Failed to open output file: " + filename);
    }

    // 验证校验和
    s = reader.VerifyChecksum();
    if (!s.ok()) {
      return Status::Corruption("Checksum verification failed: " + filename);
    }

    // 验证键的顺序
    auto iter = reader.NewIterator();
    if (!iter) {
      return Status::IOError("Failed to create iterator for: " + filename);
    }

    iter->SeekToFirst();
    std::string last_key;
    while (iter->Valid()) {
      std::string current_key = iter->key().ToString();
      if (!last_key.empty() &&
          comparator_->Compare(Slice(last_key), Slice(current_key)) >= 0) {
        return Status::Corruption("Keys not in order in file: " + filename);
      }
      last_key = current_key;
      iter->Next();
    }

    s = iter->status();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// 多路归并迭代器实现（定义在使用前）
class SSTableCompactor::MultiWayMergeIterator {
public:
  struct IteratorWrapper {
    std::unique_ptr<SSTableReader::Iterator> iter;
    std::unique_ptr<SSTableReader> reader;
    bool valid;

    IteratorWrapper() : valid(false) {}

    void Update() { valid = iter && iter->Valid(); }
  };

public:
  MultiWayMergeIterator(const std::vector<std::string> &files, Env *env,
                        const Comparator *comparator)
      : env_(env), comparator_(comparator), current_(-1) {

    // 边界情况检查
    if (files.empty()) {
      return; // 空文件列表，创建空迭代器
    }

    // 预分配空间以提高性能
    iters_.reserve(files.size());

    // 为每个文件创建迭代器
    for (const std::string &filename : files) {
      auto wrapper = std::make_unique<IteratorWrapper>();
      wrapper->reader = std::make_unique<SSTableReader>();

      Status s = wrapper->reader->Open(filename, env, comparator);
      if (s.ok()) {
        wrapper->iter = wrapper->reader->NewIterator();
        if (wrapper->iter) {
          wrapper->Update();
          iters_.push_back(std::move(wrapper));
        }
        // 如果迭代器创建失败，忽略这个文件但继续处理其他文件
      }
      // 如果文件打开失败，忽略这个文件但继续处理其他文件
      // 可能需要记录日志或返回错误
    }

    // 如果没有成功打开任何文件，current_保持-1，Valid()将返回false
  }

  void SeekToFirst() {
    for (auto &wrapper : iters_) {
      if (wrapper->iter) {
        wrapper->iter->SeekToFirst();
        wrapper->Update();
      }
    }
    FindMin();
  }

  void Seek(const Slice &target) {
    for (auto &wrapper : iters_) {
      if (wrapper->iter) {
        wrapper->iter->Seek(target);
        wrapper->Update();
      }
    }
    FindMin();
  }

  void Next() {
    if (current_ >= 0 && current_ < static_cast<int>(iters_.size())) {
      if (iters_[current_]->iter) {
        iters_[current_]->iter->Next();
        iters_[current_]->Update();
      }
    }
    FindMin();
  }

  bool Valid() const {
    return current_ >= 0 && current_ < static_cast<int>(iters_.size()) &&
           iters_[current_]->valid;
  }

  Slice key() const {
    if (!Valid())
      return Slice();
    return iters_[current_]->iter->key();
  }

  Slice value() const {
    if (!Valid())
      return Slice();
    return iters_[current_]->iter->value();
  }

  Status status() const {
    for (const auto &wrapper : iters_) {
      if (wrapper->iter) {
        Status s = wrapper->iter->status();
        if (!s.ok())
          return s;
      }
    }
    return Status::OK();
  }

private:
  void FindMin() {
    current_ = -1;
    Slice min_key;

    for (int i = 0; i < static_cast<int>(iters_.size()); ++i) {
      if (iters_[i]->valid) {
        Slice current_key = iters_[i]->iter->key();
        if (current_ == -1 || comparator_->Compare(current_key, min_key) < 0) {
          current_ = i;
          min_key = current_key;
        }
      }
    }
  }

private:
  Env *env_; // 保留以供将来使用
  const Comparator *comparator_;
  std::vector<std::unique_ptr<IteratorWrapper>> iters_;
  int current_;
};

// 私有方法实现

Status SSTableCompactor::DoCompaction(const CompactionJob &job,
                                      CompactionResult *result) {
  if (job.input_files.size() == 1) {
    // 单文件操作：可能是分割压缩
    return MergeInputFiles(job.input_files, job.output_file_prefix + "_0.sst",
                           job.output_level, result);
  } else {
    // 多文件合并
    return MergeInputFiles(job.input_files,
                           job.output_file_prefix + "_merged.sst",
                           job.output_level, result);
  }
}

Status SSTableCompactor::MergeInputFiles(
    const std::vector<std::string> &input_files, const std::string &output_file,
    SSTableLevel output_level, CompactionResult *result) {

  // 创建多路归并迭代器
  auto merge_iter = CreateMergeIterator(input_files);
  if (!merge_iter) {
    return Status::IOError("Failed to create merge iterator");
  }

  // 创建输出写入器
  SSTableWriter writer(writer_options_);
  Status s = writer.Open(output_file, env_, comparator_);
  if (!s.ok()) {
    return s;
  }

  merge_iter->SeekToFirst();
  std::string last_user_key;

  while (merge_iter->Valid()) {
    Slice current_key = merge_iter->key();
    Slice current_value = merge_iter->value();

    // 解析InternalKey获取用户键、快照序列号、操作类型
    Slice user_key;
    uint64_t snapshot_sequence;
    uint8_t type;
    if (!coding::ParseInternalKey(current_key, &user_key, &snapshot_sequence,
                                  &type)) {
      writer.Abandon();
      return Status::Corruption("Failed to parse internal key");
    }

    // 只保留符合快照要求的键
    if (ShouldKeepKey(current_key)) {
      s = writer.Add(current_key, current_value);
      if (!s.ok()) {
        writer.Abandon();
        return s;
      }
      result->num_output_records++;
    }

    result->num_input_records++;
    merge_iter->Next();
  }

  s = merge_iter->status();
  if (!s.ok()) {
    writer.Abandon();
    return s;
  }

  // 完成写入
  s = writer.Finish();
  if (!s.ok()) {
    return s;
  }

  // 更新结果
  SSTableMeta meta = writer.GetMeta();
  meta.level = output_level;
  result->output_files.push_back(output_file);
  result->output_metas.push_back(meta);
  result->bytes_written += writer.FileSize();

  return Status::OK();
}

std::unique_ptr<SSTableCompactor::MultiWayMergeIterator>
SSTableCompactor::CreateMergeIterator(
    const std::vector<std::string> &input_files) {
  return std::make_unique<MultiWayMergeIterator>(input_files, env_,
                                                 comparator_);
}

bool SSTableCompactor::ShouldKeepKey(const Slice &key) {
  // 解析InternalKey获取快照序列号
  Slice user_key;
  uint64_t snapshot_seq;
  uint8_t type;
  if (!coding::ParseInternalKey(key, &user_key, &snapshot_seq, &type)) {
    return false; // 无效键，过滤掉
  }

  // 1. 如果快照序列号 >= 最老活跃快照序列号，必须保留
  // 2. 如果快照序列号 < 最老活跃快照序列号，可以清理
  if (snapshot_seq >= config_.oldest_snapshot_sequence) {
    return true;
  }

  // 对于旧快照的特殊处理：
  // 删除标记在底层压缩时可以安全清理
  if (type == static_cast<uint8_t>(ValueType::kDeletion) &&
      config_.is_bottommost_level) {
    return false;
  }

  // 其他情况下，序列号过老的键可以清理
  return false;
}

// ============================================================================
// SSTableManager 实现 - 统一管理器（暴露API给上层）
// ============================================================================

SSTableManager::SSTableManager(Env *env, const Comparator *comparator,
                               const SSTableManagerOptions &options)
    : env_(env), comparator_(comparator), options_(options),
      level_files_(static_cast<size_t>(SSTableLevel::kMaxLevel) + 1),
      next_file_number_(1) {

  // 创建压缩器
  compactor_ = std::make_unique<SSTableCompactor>(env, comparator);
}

SSTableManager::~SSTableManager() = default;

Status SSTableManager::RegisterSSTable(const SSTableMeta &meta) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 验证元数据
  Status s = ValidateFileMeta(meta);
  if (!s.ok()) {
    return s;
  }

  // 添加到层级文件列表
  size_t level_index = static_cast<size_t>(meta.level);
  if (level_index >= level_files_.size()) {
    return Status::InvalidArgument("Invalid SSTable level");
  }

  // 检查文件是否已存在
  auto it = file_meta_map_.find(meta.file_number);
  if (it != file_meta_map_.end()) {
    return Status::InvalidArgument("SSTable file already registered");
  }

  // 插入到正确的位置以保持排序
  auto &level_files = level_files_[level_index];
  auto insert_pos =
      std::lower_bound(level_files.begin(), level_files.end(), meta,
                       [this](const SSTableMeta &a, const SSTableMeta &b) {
                         return comparator_->Compare(Slice(a.smallest_key),
                                                     Slice(b.smallest_key)) < 0;
                       });
  level_files.insert(insert_pos, meta);

  // 添加到文件映射
  file_meta_map_[meta.file_number] = meta;

  // 更新下一个文件编号
  if (meta.file_number >= next_file_number_) {
    next_file_number_ = meta.file_number + 1;
  }

  return Status::OK();
}

Status SSTableManager::UnregisterSSTable(uint64_t file_number) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 查找文件元数据
  auto it = file_meta_map_.find(file_number);
  if (it == file_meta_map_.end()) {
    return Status::NotFound("SSTable file not found");
  }

  const SSTableMeta &meta = it->second;

  // 从层级文件列表中移除
  size_t level_index = static_cast<size_t>(meta.level);
  auto &level_files = level_files_[level_index];
  level_files.erase(std::remove_if(level_files.begin(), level_files.end(),
                                   [file_number](const SSTableMeta &m) {
                                     return m.file_number == file_number;
                                   }),
                    level_files.end());

  // 从文件映射中移除
  file_meta_map_.erase(it);

  // 从缓存中移除
  EvictFromCache(file_number);

  return Status::OK();
}

Status SSTableManager::Get(const Slice &key, std::string *value, bool *found,
                           uint64_t snapshot) {
  *found = false;

  // 在锁内收集候选文件编号（GetReader内部需要独占锁，不能持锁调用）
  std::vector<uint64_t> candidates;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // 按层级顺序搜索（L0 -> L1 -> L2 ...）
    for (size_t level = 0; level < level_files_.size(); ++level) {
      const auto &level_files = level_files_[level];

      if (level == 0) {
        // L0层文件可能重叠，需要搜索所有可能包含键的文件（新文件优先）
        for (auto it = level_files.rbegin(); it != level_files.rend(); ++it) {
          if (it->KeyInRange(key, comparator_)) {
            candidates.push_back(it->file_number);
          }
        }
      } else {
        // L1+层文件不重叠，可以使用二分查找
        auto it = std::lower_bound(
            level_files.begin(), level_files.end(), key,
            [this](const SSTableMeta &meta, const Slice &target_key) {
              return comparator_->Compare(Slice(meta.largest_key), target_key) <
                     0;
            });

        if (it != level_files.end() && it->KeyInRange(key, comparator_)) {
          candidates.push_back(it->file_number);
        }
      }
    }
  }

  // 逐个打开读取器查找
  for (uint64_t file_number : candidates) {
    SSTableReader *reader = GetReader(file_number);
    if (!reader) {
      continue;
    }

    if (!reader->MayContainKey(key)) {
      continue;
    }

    bool file_found = false;
    Status s = reader->Get(key, value, &file_found, snapshot);
    if (!s.ok()) {
      return s;
    }

    if (file_found) {
      *found = true;
      return Status::OK();
    }
  }

  return Status::OK();
}

// 层级迭代器实现
SSTableManager::LevelIterator::LevelIterator(SSTableManager *manager,
                                             SSTableLevel level)
    : manager_(manager), level_(level), current_file_(-1) {

  std::shared_lock<std::shared_mutex> lock(manager_->mutex_);
  level_files_ = manager_->level_files_[static_cast<size_t>(level)];
}
void SSTableManager::LevelIterator::SeekToFirst() {
  current_file_ = 0;
  LoadCurrentIterator();
  if (current_iter_) {
    current_iter_->SeekToFirst();
  }
}

void SSTableManager::LevelIterator::SeekToLast() {
  current_file_ = static_cast<int>(level_files_.size()) - 1;
  LoadCurrentIterator();
  if (current_iter_) {
    current_iter_->SeekToLast();
  }
}

void SSTableManager::LevelIterator::Seek(const Slice &target) {
  // 在L0层，需要检查所有文件
  if (level_ == SSTableLevel::kLevel0) {
    current_file_ = 0;
    LoadCurrentIterator();
    if (current_iter_) {
      current_iter_->Seek(target);
    }
  } else {
    // 在L1+层，使用二分查找
    auto it =
        std::lower_bound(level_files_.begin(), level_files_.end(), target,
                         [this](const SSTableMeta &meta, const Slice &key) {
                           return manager_->comparator_->Compare(
                                      Slice(meta.largest_key), key) < 0;
                         });

    if (it != level_files_.end()) {
      current_file_ = static_cast<int>(it - level_files_.begin());
      LoadCurrentIterator();
      if (current_iter_) {
        current_iter_->Seek(target);
      }
    } else {
      current_file_ = -1;
      current_iter_.reset();
    }
  }
}
void SSTableManager::LevelIterator::Next() {
  if (!Valid())
    return;

  current_iter_->Next();
  if (!current_iter_->Valid()) {
    // 移动到下一个文件
    current_file_++;
    LoadCurrentIterator();
    if (current_iter_) {
      current_iter_->SeekToFirst();
    }
  }
}

void SSTableManager::LevelIterator::Prev() {
  if (!Valid())
    return;

  current_iter_->Prev();
  if (!current_iter_->Valid()) {
    // 移动到前一个文件
    current_file_--;
    LoadCurrentIterator();
    if (current_iter_) {
      current_iter_->SeekToLast();
    }
  }
}

bool SSTableManager::LevelIterator::Valid() const {
  return current_iter_ && current_iter_->Valid();
}

Slice SSTableManager::LevelIterator::key() const {
  return Valid() ? current_iter_->key() : Slice();
}

Slice SSTableManager::LevelIterator::value() const {
  return Valid() ? current_iter_->value() : Slice();
}

Status SSTableManager::LevelIterator::status() const {
  return current_iter_ ? current_iter_->status() : Status::OK();
}

void SSTableManager::LevelIterator::LoadCurrentIterator() {
  if (current_file_ < 0 ||
      current_file_ >= static_cast<int>(level_files_.size())) {
    current_iter_.reset();
    return;
  }

  const SSTableMeta &meta = level_files_[current_file_];
  SSTableReader *reader = manager_->GetReader(meta.file_number);
  if (reader) {
    current_iter_ = reader->NewIterator();
  } else {
    current_iter_.reset();
  }
}

std::unique_ptr<SSTableManager::LevelIterator>
SSTableManager::NewLevelIterator(SSTableLevel level) {
  return std::make_unique<LevelIterator>(this, level);
}

std::vector<SSTableMeta>
SSTableManager::GetLevelFiles(SSTableLevel level) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  size_t level_index = static_cast<size_t>(level);
  if (level_index >= level_files_.size()) {
    return {};
  }

  return level_files_[level_index];
}

std::vector<SSTableMeta> SSTableManager::GetAllFiles() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<SSTableMeta> all_files;
  for (const auto &level_files : level_files_) {
    all_files.insert(all_files.end(), level_files.begin(), level_files.end());
  }

  return all_files;
}

std::vector<SSTableMeta>
SSTableManager::GetFilesInRange(const Slice &start_key, const Slice &end_key,
                                SSTableLevel level) const {

  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<SSTableMeta> result;
  size_t level_index = static_cast<size_t>(level);

  if (level_index >= level_files_.size()) {
    return result;
  }

  const auto &level_files = level_files_[level_index];
  for (const SSTableMeta &meta : level_files) {
    // 检查文件是否与范围重叠
    if (comparator_->Compare(Slice(meta.largest_key), start_key) >= 0 &&
        comparator_->Compare(Slice(meta.smallest_key), end_key) <= 0) {
      result.push_back(meta);
    }
  }

  return result;
}

std::vector<SSTableMeta> SSTableManager::GetOverlappingFiles(
    const Slice &start_key, const Slice &end_key, SSTableLevel level) const {

  return GetFilesInRange(start_key, end_key, level);
}

// 压缩相关API（暴露给上层LSMTree）
CompactionResult SSTableManager::CompactFiles(const CompactionJob &job) {
  // 直接委托给压缩器
  return compactor_->CompactFiles(job);
}

Status SSTableManager::FlushMemTableToSSTable(const MemTable *memtable,
                                              SSTableLevel target_level,
                                              SSTableMeta *output_meta) {

  if (!memtable || !output_meta) {
    return Status::InvalidArgument("Invalid arguments");
  }

  // 生成新的文件名
  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string filename = GenerateFileName(file_number, target_level);

  // 使用压缩器执行刷盘
  Status s =
      compactor_->FlushMemTable(memtable, filename, target_level, output_meta);
  if (!s.ok()) {
    return s;
  }

  // 设置文件编号
  output_meta->file_number = file_number;

  // 注册新的SSTable
  s = RegisterSSTable(*output_meta);
  if (!s.ok()) {
    // 清理失败的文件
    env_->DeleteFile(filename);
    return s;
  }

  return Status::OK();
}

Status
SSTableManager::DeleteObsoleteFiles(const std::vector<uint64_t> &file_numbers) {
  for (uint64_t file_number : file_numbers) {
    // 从注册中移除
    Status s = UnregisterSSTable(file_number);
    if (!s.ok()) {
      // 继续处理其他文件
      continue;
    }

    // 删除物理文件
    auto it = file_meta_map_.find(file_number);
    if (it != file_meta_map_.end()) {
      env_->DeleteFile(it->second.filename);
    }
  }

  return Status::OK();
}

Status SSTableManager::RenameFile(const std::string &old_name,
                                  const std::string &new_name) {
  return env_->RenameFile(old_name, new_name);
}

std::vector<SSTableManager::LevelStats> SSTableManager::GetLevelStats() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<LevelStats> stats;

  for (size_t i = 0; i < level_files_.size(); ++i) {
    const auto &level_files = level_files_[i];

    LevelStats level_stat;
    level_stat.level = static_cast<SSTableLevel>(i);
    level_stat.num_files = level_files.size();
    level_stat.total_size = 0;

    uint64_t min_size = UINT64_MAX, max_size = 0;
    for (const SSTableMeta &meta : level_files) {
      level_stat.total_size += meta.file_size;
      min_size = std::min(min_size, meta.file_size);
      max_size = std::max(max_size, meta.file_size);
    }

    level_stat.avg_file_size =
        level_stat.num_files > 0
            ? static_cast<double>(level_stat.total_size) / level_stat.num_files
            : 0.0;

    if (level_stat.num_files > 0) {
      level_stat.size_range =
          std::to_string(min_size) + "-" + std::to_string(max_size);
    } else {
      level_stat.size_range = "0-0";
    }

    stats.push_back(level_stat);
  }

  return stats;
}

size_t SSTableManager::ApproximateMemoryUsage() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  size_t usage = sizeof(*this);

  // 文件元数据
  usage += file_meta_map_.size() * (sizeof(uint64_t) + sizeof(SSTableMeta));
  for (const auto &level_files : level_files_) {
    usage += level_files.size() * sizeof(SSTableMeta);
  }

  // 读取器缓存
  for (const auto &entry : reader_cache_) {
    usage +=
        sizeof(entry) + (entry.second.reader
                             ? entry.second.reader->ApproximateMemoryUsage()
                             : 0);
  }

  return usage;
}

void SSTableManager::EvictFromCache(uint64_t file_number) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  reader_cache_.erase(file_number);
}

void SSTableManager::ClearCache() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  reader_cache_.clear();
}

// 私有方法实现
SSTableReader *SSTableManager::GetReader(uint64_t file_number) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 检查缓存
  auto it = reader_cache_.find(file_number);
  if (it != reader_cache_.end()) {
    // 更新访问时间
    it->second.last_access_time =
        std::chrono::steady_clock::now().time_since_epoch().count();
    it->second.access_count++;
    return it->second.reader.get();
  }

  // 缓存未命中，加载读取器
  SSTableReader *reader = nullptr;
  Status s = LoadReader(file_number, &reader);
  if (!s.ok() || !reader) {
    return nullptr;
  }

  // 检查缓存容量
  if (reader_cache_.size() >= options_.max_open_files) {
    EvictLRUReader();
  }

  // 添加到缓存
  CacheEntry entry;
  entry.reader.reset(reader);
  entry.last_access_time =
      std::chrono::steady_clock::now().time_since_epoch().count();
  entry.access_count = 1;
  reader_cache_[file_number] = std::move(entry);

  return reader_cache_[file_number].reader.get();
}

Status SSTableManager::LoadReader(uint64_t file_number,
                                  SSTableReader **reader) {
  // 查找文件元数据
  auto it = file_meta_map_.find(file_number);
  if (it == file_meta_map_.end()) {
    return Status::NotFound("SSTable file not found");
  }

  const SSTableMeta &meta = it->second;

  // 创建并打开读取器
  auto new_reader = std::make_unique<SSTableReader>();
  Status s = new_reader->Open(meta.filename, env_, comparator_);
  if (!s.ok()) {
    return s;
  }

  *reader = new_reader.release();
  return Status::OK();
}

void SSTableManager::EvictLRUReader() {
  // 优化的LRU实现：结合访问时间和访问次数
  auto oldest_it = reader_cache_.end();
  uint64_t oldest_time = UINT64_MAX;
  uint64_t min_access_count = UINT64_MAX;

  // 首先查找访问次数最少的条目
  for (auto it = reader_cache_.begin(); it != reader_cache_.end(); ++it) {
    if (it->second.access_count < min_access_count ||
        (it->second.access_count == min_access_count &&
         it->second.last_access_time < oldest_time)) {
      oldest_time = it->second.last_access_time;
      min_access_count = it->second.access_count;
      oldest_it = it;
    }
  }

  if (oldest_it != reader_cache_.end()) {
    reader_cache_.erase(oldest_it);
  }
}

Status SSTableManager::ValidateFileMeta(const SSTableMeta &meta) {
  if (meta.filename.empty()) {
    return Status::InvalidArgument("Empty filename");
  }

  if (meta.file_number == 0) {
    return Status::InvalidArgument("Invalid file number");
  }

  if (static_cast<size_t>(meta.level) >= level_files_.size()) {
    return Status::InvalidArgument("Invalid level");
  }

  return Status::OK();
}

std::string SSTableManager::GenerateFileName(uint64_t file_number,
                                             SSTableLevel level) const {
  std::string name = "sst_" + std::to_string(file_number) + "_L" +
                     std::to_string(static_cast<int>(level)) + ".sst";
  if (options_.directory.empty()) {
    return name;
  }
  return options_.directory + "/" + name;
}

// ============================================================================
// SSTableReader 错误恢复方法
// ============================================================================

Status SSTableReader::HandleErrorRecovery(const std::string &component,
                                          const std::string &operation) {
  LOG_INFO << "SSTableReader::HandleErrorRecovery called for component: "
           << component << ", operation: " << operation;

  try {
    // SSTable特定的清理逻辑
    if (operation == "Get" || operation == "Open") {
      // 验证文件完整性
      if (opened_) {
        Status verify_status = VerifyChecksum();
        if (!verify_status.ok()) {
          LOG_WARN << "SSTable checksum verification failed: "
                   << verify_status.ToString();
        }
      }
    }

    // 检查文件可访问性
    if (!filename_.empty() && env_) {
      if (!env_->FileExists(filename_)) {
        LOG_ERROR << "SSTable file no longer exists: " << filename_;
        return Status::NotFound("SSTable file missing: " + filename_);
      }
    }

    // 清理可能的临时状态
    LOG_DEBUG << "SSTableReader error recovery completed successfully";
    return Status::OK();

  } catch (const std::exception &e) {
    LOG_ERROR << "SSTableReader error recovery failed: " << e.what();
    return Status::IOError("SSTableReader recovery failed: " +
                           std::string(e.what()));
  }
}

Status SSTableReader::HandleCorruptionError(const ErrorReport &error) {
  LOG_INFO << "SSTableReader::HandleCorruptionError called for: "
           << error.status.ToString();

  try {
    // 尝试从错误上下文中提取文件路径
    std::string filepath = filename_;
    if (!error.context.additional_info.empty() &&
        error.context.additional_info.find("file:") != std::string::npos) {
      size_t pos = error.context.additional_info.find("file:") + 5;
      filepath = error.context.additional_info.substr(pos);
    }

    LOG_INFO << "Attempting to handle corruption in file: " << filepath;

    // 基本的文件完整性检查
    if (env_ && env_->FileExists(filepath)) {
      uint64_t file_size;
      Status size_status = env_->GetFileSize(filepath, &file_size);
      if (!size_status.ok()) {
        LOG_ERROR << "Cannot get file size for corruption analysis: "
                  << size_status.ToString();
        return size_status;
      }

      LOG_INFO << "Corrupted file size: " << file_size << " bytes";

      // 对于SSTable文件，可以尝试恢复策略：
      // 1. 重建索引（如果只是索引损坏）
      // 2. 跳过损坏的数据块
      // 3. 标记文件为不可用，触发压缩替换

      if (opened_) {
        // 尝试重新验证文件结构
        Status verify_status = VerifyChecksum();
        if (verify_status.ok()) {
          LOG_INFO << "File verification passed after corruption handling";
          return Status::OK();
        } else {
          LOG_WARN << "File still appears corrupted, suggesting replacement: "
                   << verify_status.ToString();
          // 这里可以触发后台压缩来替换损坏的文件
        }
      }
    } else {
      LOG_ERROR << "Cannot access corrupted file: " << filepath;
      return Status::NotFound("Corrupted file not accessible: " + filepath);
    }

    LOG_DEBUG << "SSTableReader corruption error handling completed";
    return Status::OK();

  } catch (const std::exception &e) {
    LOG_ERROR << "SSTableReader corruption error handling failed: " << e.what();
    return Status::IOError("SSTableReader corruption handling failed: " +
                           std::string(e.what()));
  }
}

} // namespace lrdb
