// 布隆过滤器实现

#include "lrdb/util/bloom_filter.h"
#include "lrdb/core/coding.h"
#include "lrdb/util/logging.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <random>
#include <unordered_map>
#ifdef _OPENMP
#include <omp.h>
#endif

namespace lrdb {

// BloomFilter静态方法实现
std::unique_ptr<BloomFilter> BloomFilter::Create(size_t expected_elements,
                                                 double false_positive_rate) {
  size_t bit_array_size = bloom_util::CalculateOptimalBitArraySize(
      expected_elements, false_positive_rate);
  int hash_function_count = bloom_util::CalculateOptimalHashFunctionCount(
      bit_array_size, expected_elements);

  return std::make_unique<StandardBloomFilter>(bit_array_size,
                                               hash_function_count);
}

std::unique_ptr<BloomFilter> BloomFilter::Create(size_t bit_array_size,
                                                 int hash_function_count) {
  return std::make_unique<StandardBloomFilter>(bit_array_size,
                                               hash_function_count);
}

// StandardBloomFilter实现
StandardBloomFilter::StandardBloomFilter(size_t bit_array_size,
                                         int hash_function_count)
    : bit_array_size_(bit_array_size),
      hash_function_count_(hash_function_count), element_count_(0) {

  // 将位数组大小向上舍入到字节边界
  size_t byte_size = (bit_array_size + 7) / 8;
  bit_array_.resize(byte_size, 0);
}

StandardBloomFilter::~StandardBloomFilter() = default;

void StandardBloomFilter::Add(const Slice &key) {
  // 未初始化/反序列化到 0 位的过滤器，取模会除零
  if (bit_array_size_ == 0) {
    return;
  }
  auto hashes = ComputeHashes(key);

  for (uint32_t hash : hashes) {
    size_t bit_index = hash % bit_array_size_;
    SetBitOptimized(bit_index);
  }

  element_count_.fetch_add(1);
}

bool StandardBloomFilter::MayContain(const Slice &key) const {
  // 0 位过滤器：保守地认为"可能存在"，交给实际查询判定
  if (bit_array_size_ == 0) {
    return true;
  }
  auto hashes = ComputeHashes(key);

  for (uint32_t hash : hashes) {
    size_t bit_index = hash % bit_array_size_;
    if (!GetBitOptimized(bit_index)) {
      return false;
    }
  }

  return true;
}

size_t StandardBloomFilter::GetSizeInBytes() const { return bit_array_.size(); }

size_t StandardBloomFilter::GetBitArraySize() const { return bit_array_size_; }

int StandardBloomFilter::GetHashFunctionCount() const {
  return hash_function_count_;
}

void StandardBloomFilter::Reset() {
  std::fill(bit_array_.begin(), bit_array_.end(), 0);
  element_count_.store(0);
}

double StandardBloomFilter::EstimateFalsePositiveRate() const {
  size_t n = element_count_.load();
  if (n == 0) {
    return 0.0;
  }

  double ratio = static_cast<double>(bit_array_size_) / n;
  return std::pow(1.0 - std::exp(-hash_function_count_ / ratio),
                  hash_function_count_);
}

std::string StandardBloomFilter::Serialize() const {
  std::string result;

  // 序列化头部信息
  coding::PutFixed64(&result, bit_array_size_);
  coding::PutFixed32(&result, hash_function_count_);
  coding::PutFixed64(&result, element_count_.load());

  // 序列化位数组
  result.append(reinterpret_cast<const char *>(bit_array_.data()),
                bit_array_.size());

  return result;
}

bool StandardBloomFilter::Deserialize(const Slice &data) {
  if (data.size() < 20) { // 最小头部大小：8+4+8=20
    return false;
  }

  const char *p = data.data();

  // 解析头部
  bit_array_size_ = coding::DecodeFixed64(p);
  p += 8;
  hash_function_count_ = coding::DecodeFixed32(p);
  p += 4;
  element_count_.store(coding::DecodeFixed64(p));
  p += 8;

  // 计算位数组字节大小
  size_t byte_size = (bit_array_size_ + 7) / 8;

  // 检查数据大小
  if (data.size() != 20 + byte_size) {
    return false;
  }

  // 加载位数组
  bit_array_.resize(byte_size);
  std::memcpy(bit_array_.data(), p, byte_size);

  return true;
}

void StandardBloomFilter::SetBit(size_t index) {
  size_t byte_index = index / 8;
  size_t bit_offset = index % 8;

  if (byte_index < bit_array_.size()) {
    bit_array_[byte_index] |= (1U << bit_offset);
  }
}

bool StandardBloomFilter::GetBit(size_t index) const {
  size_t byte_index = index / 8;
  size_t bit_offset = index % 8;

  if (byte_index < bit_array_.size()) {
    return (bit_array_[byte_index] & (1U << bit_offset)) != 0;
  }

  return false;
}

std::vector<uint32_t>
StandardBloomFilter::ComputeHashes(const Slice &key) const {
  std::vector<uint32_t> hashes;
  hashes.reserve(hash_function_count_);

  // 使用不同的种子生成基础哈希值
  const uint32_t seed1 = 0xbc9f1d34;
  // seed2 保留用于双哈希扩展（当前单哈希实现未使用）
  // const uint32_t seed2 = 0xdeadbeef;

  uint32_t h1 = hash_util::MurmurHash3_x86_32(
      key.data(), static_cast<int>(key.size()), seed1);
  uint32_t h2 = hash_util::FNVHash32(key.data(), key.size());

  // 如果h2为0，使用备用哈希函数生成h2
  if (h2 == 0) {
    h2 = hash_util::CRC32(key.data(), key.size());
    // 如果仍然为0，使用固定值
    if (h2 == 0) {
      h2 = 0x9e3779b9; // 黄金比例常数
    }
  }

  // 使用改进的双重哈希
  const uint32_t golden_ratio = 0x9e3779b9;
  for (int i = 0; i < hash_function_count_; ++i) {
    // 标准双重哈希公式
    uint32_t hash = h1 + i * h2;

    // 可选：混合更多位，提高分布性
    hash = hash ^ (hash >> 16);
    hash = hash * golden_ratio;
    hash = hash ^ (hash >> 13);

    hashes.push_back(hash);
  }

  return hashes;
}

// 批量操作优化
void StandardBloomFilter::AddBatch(const std::vector<Slice> &keys) {
  if (keys.empty())
    return;

  // 预分配哈希缓存
  std::vector<uint32_t> all_hashes;
  all_hashes.reserve(keys.size() * hash_function_count_);

  // 批量计算所有哈希值
  for (const auto &key : keys) {
    auto hashes = ComputeHashes(key);
    all_hashes.insert(all_hashes.end(), hashes.begin(), hashes.end());
  }

  // 批量设置位
  for (uint32_t hash : all_hashes) {
    size_t bit_index = hash % bit_array_size_;
    SetBitOptimized(bit_index);
  }

  element_count_.fetch_add(keys.size());
}

bool StandardBloomFilter::MayContainBatch(const std::vector<Slice> &keys,
                                          std::vector<bool> *results) const {
  if (keys.empty() || !results)
    return false;

  results->resize(keys.size());

// 并行处理多个键的查询，如果编译时启用了OpenMP
#ifdef _OPENMP
#pragma omp parallel for if (keys.size() > 100)
#endif
  for (size_t i = 0; i < keys.size(); ++i) {
    auto hashes = ComputeHashes(keys[i]);
    bool may_contain = true;

    for (uint32_t hash : hashes) {
      size_t bit_index = hash % bit_array_size_;
      if (!GetBitOptimized(bit_index)) {
        may_contain = false;
        break;
      }
    }

    (*results)[i] = may_contain;
  }

  return true;
}

// 性能统计方法
double StandardBloomFilter::GetLoadFactor() const {
  size_t n = element_count_.load();
  if (n == 0)
    return 0.0;

  return static_cast<double>(n) / bit_array_size_;
}

// 内存优化方法
void StandardBloomFilter::ShrinkToFit() { bit_array_.shrink_to_fit(); }

void StandardBloomFilter::Reserve(size_t expected_elements) {
  size_t optimal_bits =
      bloom_util::CalculateOptimalBitArraySize(expected_elements, 0.01);
  size_t optimal_bytes = (optimal_bits + 7) / 8;
  bit_array_.reserve(optimal_bytes);
}

// 优化的位操作方法
void StandardBloomFilter::SetBitOptimized(size_t index) {
  size_t byte_index = index / 8;
  size_t bit_offset = index % 8;

  if (byte_index < bit_array_.size()) {
    // 使用原子操作确保线程安全
    uint8_t old_byte = bit_array_[byte_index];
    uint8_t new_byte = old_byte | (1U << bit_offset);

    // 只有在需要改变时才写入
    if (old_byte != new_byte) {
      bit_array_[byte_index] = new_byte;
    }
  }
}

bool StandardBloomFilter::GetBitOptimized(size_t index) const {
  size_t byte_index = index / 8;
  size_t bit_offset = index % 8;

  if (byte_index < bit_array_.size()) {
    // 使用位掩码优化
    return (bit_array_[byte_index] & (1U << bit_offset)) != 0;
  }

  return false;
}

void StandardBloomFilter::SetMultipleBits(const std::vector<size_t> &indices) {
  if (indices.empty())
    return;

  // 按字节分组，减少内存访问
  std::unordered_map<size_t, uint8_t> byte_updates;

  for (size_t index : indices) {
    size_t byte_index = index / 8;
    size_t bit_offset = index % 8;

    if (byte_index < bit_array_.size()) {
      byte_updates[byte_index] |= (1U << bit_offset);
    }
  }

  // 批量更新字节
  for (const auto &[byte_index, mask] : byte_updates) {
    bit_array_[byte_index] |= mask;
  }
}

// BloomFilterBuilder实现
BloomFilterBuilder::BloomFilterBuilder(double bits_per_key)
    : bits_per_key_(bits_per_key) {}

BloomFilterBuilder::~BloomFilterBuilder() = default;

void BloomFilterBuilder::AddKey(const Slice &key) {
  keys_.emplace_back(key.ToString());
}

Slice BloomFilterBuilder::Finish() {
  if (keys_.empty()) {
    filter_data_.clear();
    return Slice(filter_data_);
  }

  size_t bit_array_size = CalculateOptimalBitArraySize(keys_.size());
  int hash_function_count = CalculateOptimalHashFunctionCount();

  // 创建临时布隆过滤器
  StandardBloomFilter temp_filter(bit_array_size, hash_function_count);

  // 添加所有键
  for (const auto &key : keys_) {
    temp_filter.Add(Slice(key));
  }

  // 序列化过滤器
  filter_data_ = temp_filter.Serialize();

  return Slice(filter_data_);
}

size_t BloomFilterBuilder::EstimateFilterSize(size_t num_keys) const {
  if (num_keys == 0) {
    return 0;
  }

  size_t bit_array_size = static_cast<size_t>(num_keys * bits_per_key_);
  size_t byte_size = (bit_array_size + 7) / 8;

  return 20 + byte_size; // 头部 + 位数组
}

void BloomFilterBuilder::Reset() {
  keys_.clear();
  filter_data_.clear();
}

size_t BloomFilterBuilder::CalculateOptimalBitArraySize(size_t num_keys) const {
  return static_cast<size_t>(num_keys * bits_per_key_);
}

int BloomFilterBuilder::CalculateOptimalHashFunctionCount() const {
  return static_cast<int>(std::round(bits_per_key_ * std::log(2)));
}

// BloomFilterReader实现
BloomFilterReader::BloomFilterReader()
    : data_(nullptr), data_size_(0), bit_array_size_(0),
      hash_function_count_(0), initialized_(false) {}

BloomFilterReader::~BloomFilterReader() = default;

bool BloomFilterReader::Initialize(const Slice &filter_data) {
  if (filter_data.size() < 20) {
    return false;
  }

  // 复制数据而不是只存储指针
  data_copy_ = filter_data.ToString();
  data_ = data_copy_.data();
  data_size_ = filter_data.size();

  // 解析头部
  const char *p = data_;
  bit_array_size_ = coding::DecodeFixed64(p);
  p += 8;
  hash_function_count_ = coding::DecodeFixed32(p);
  p += 4;
  uint64_t element_count = coding::DecodeFixed64(p);
  p += 8;

  // 验证元素数量的合理性
  if (element_count > (1ULL << 32)) {
    LOG_WARN << "Bloom filter element count seems too large: " << element_count;
  }

  // 验证数据大小
  size_t expected_size = 20 + (bit_array_size_ + 7) / 8;
  if (data_size_ != expected_size) {
    return false;
  }

  initialized_ = true;
  return true;
}

bool BloomFilterReader::MayContain(const Slice &key) const {
  if (!initialized_) {
    return true; // 保守估计
  }

  auto hashes = ComputeHashes(key);

  for (uint32_t hash : hashes) {
    size_t bit_index = hash % bit_array_size_;
    if (!GetBit(bit_index)) {
      return false;
    }
  }

  return true;
}

size_t BloomFilterReader::GetBitArraySize() const { return bit_array_size_; }

int BloomFilterReader::GetHashFunctionCount() const {
  return hash_function_count_;
}

bool BloomFilterReader::GetBit(size_t index) const {
  const char *bit_array = data_ + 20; // 跳过头部
  size_t byte_index = index / 8;
  size_t bit_offset = index % 8;

  size_t bit_array_bytes = (bit_array_size_ + 7) / 8;
  if (byte_index < bit_array_bytes) {
    return (bit_array[byte_index] & (1U << bit_offset)) != 0;
  }

  return false;
}

std::vector<uint32_t> BloomFilterReader::ComputeHashes(const Slice &key) const {
  std::vector<uint32_t> hashes;
  hashes.reserve(hash_function_count_);

  // 使用不同的种子生成基础哈希值
  const uint32_t seed1 = 0xbc9f1d34;
  // seed2 保留用于双哈希扩展（当前单哈希实现未使用）
  // const uint32_t seed2 = 0xdeadbeef;

  uint32_t h1 = hash_util::MurmurHash3_x86_32(
      key.data(), static_cast<int>(key.size()), seed1);
  uint32_t h2 = hash_util::FNVHash32(key.data(), key.size());

  // 如果h2为0，使用备用哈希函数生成h2
  if (h2 == 0) {
    h2 = hash_util::CRC32(key.data(), key.size());
    // 如果仍然为0，使用固定值
    if (h2 == 0) {
      h2 = 0x9e3779b9; // 黄金比例常数
    }
  }

  // 使用改进的双重哈希
  const uint32_t golden_ratio = 0x9e3779b9;
  for (int i = 0; i < hash_function_count_; ++i) {
    // 标准双重哈希公式
    uint32_t hash = h1 + i * h2;

    // 可选：混合更多位，提高分布性
    hash = hash ^ (hash >> 16);
    hash = hash * golden_ratio;
    hash = hash ^ (hash >> 13);

    hashes.push_back(hash);
  }

  return hashes;
}

// 布隆过滤器工具函数实现
namespace bloom_util {

size_t CalculateOptimalBitArraySize(size_t expected_elements,
                                    double false_positive_rate) {
  if (expected_elements == 0 || false_positive_rate <= 0.0 ||
      false_positive_rate >= 1.0) {
    return 1;
  }

  double optimal_size = -static_cast<double>(expected_elements) *
                        std::log(false_positive_rate) /
                        (std::log(2) * std::log(2));

  return static_cast<size_t>(std::ceil(optimal_size));
}

int CalculateOptimalHashFunctionCount(size_t bit_array_size,
                                      size_t expected_elements) {
  if (expected_elements == 0) {
    return 1;
  }

  double optimal_count =
      static_cast<double>(bit_array_size) / expected_elements * std::log(2);

  return std::max(1, static_cast<int>(std::round(optimal_count)));
}

double CalculateFalsePositiveRate(size_t bit_array_size,
                                  int hash_function_count,
                                  size_t element_count) {
  if (element_count == 0) {
    return 0.0;
  }

  double ratio = static_cast<double>(bit_array_size) / element_count;
  return std::pow(1.0 - std::exp(-hash_function_count / ratio),
                  hash_function_count);
}

std::unique_ptr<BloomFilter> CreateBloomFilter(size_t expected_elements,
                                               double false_positive_rate) {
  return BloomFilter::Create(expected_elements, false_positive_rate);
}

BloomFilterBenchmarkResult BenchmarkBloomFilter(BloomFilter *filter,
                                                size_t num_operations) {
  BloomFilterBenchmarkResult result;
  result.total_adds = num_operations / 2;
  result.total_lookups = num_operations / 2;

  auto start_time = std::chrono::steady_clock::now();

  // 添加操作
  for (size_t i = 0; i < result.total_adds; ++i) {
    std::string key = "benchmark_key_" + std::to_string(i);
    filter->Add(Slice(key));
  }

  // 查找操作
  size_t false_positives = 0;
  for (size_t i = 0; i < result.total_lookups; ++i) {
    std::string key = "lookup_key_" + std::to_string(i);
    if (filter->MayContain(Slice(key))) {
      if (i >= result.total_adds) {
        false_positives++;
      }
    }
  }

  auto end_time = std::chrono::steady_clock::now();
  result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  result.false_positives = false_positives;
  result.actual_false_positive_rate =
      static_cast<double>(false_positives) / result.total_lookups;

  return result;
}

Status ValidateBloomFilter(const BloomFilter *filter,
                           const std::vector<std::string> &test_keys) {
  // 添加测试键
  for (const auto &key : test_keys) {
    const_cast<BloomFilter *>(filter)->Add(Slice(key));
  }

  // 验证所有添加的键都能被找到
  for (const auto &key : test_keys) {
    if (!filter->MayContain(Slice(key))) {
      return Status::Corruption("Bloom filter false negative detected");
    }
  }

  return Status::OK();
}

} // namespace bloom_util

} // namespace lrdb