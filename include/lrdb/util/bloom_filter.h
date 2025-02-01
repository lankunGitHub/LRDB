// 布隆过滤器实现

#pragma once

#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/util/hash.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace lrdb {

// 布隆过滤器接口
class BloomFilter {
public:
  virtual ~BloomFilter() = default;

  // 添加键到过滤器
  virtual void Add(const Slice &key) = 0;

  // 检查键是否可能存在
  virtual bool MayContain(const Slice &key) const = 0;

  // 获取过滤器大小(字节)
  virtual size_t GetSizeInBytes() const = 0;

  // 获取位数组大小
  virtual size_t GetBitArraySize() const = 0;

  // 获取哈希函数数量
  virtual int GetHashFunctionCount() const = 0;

  // 重置过滤器
  virtual void Reset() = 0;

  // 估算假阳性率
  virtual double EstimateFalsePositiveRate() const = 0;

  // 序列化
  virtual std::string Serialize() const = 0;

  // 反序列化
  virtual bool Deserialize(const Slice &data) = 0;

  // 批量操作优化
  virtual void AddBatch(const std::vector<Slice> &keys) = 0;
  virtual bool MayContainBatch(const std::vector<Slice> &keys,
                               std::vector<bool> *results) const = 0;

  // 性能统计
  virtual size_t GetElementCount() const = 0;
  virtual double GetLoadFactor() const = 0;

  // 内存优化
  virtual void ShrinkToFit() = 0;
  virtual void Reserve(size_t expected_elements) = 0;

  // 创建方法
  static std::unique_ptr<BloomFilter> Create(size_t expected_elements,
                                             double false_positive_rate = 0.01);
  static std::unique_ptr<BloomFilter> Create(size_t bit_array_size,
                                             int hash_function_count);
};

// 标准布隆过滤器实现
class StandardBloomFilter : public BloomFilter {
public:
  StandardBloomFilter(size_t bit_array_size, int hash_function_count);
  ~StandardBloomFilter() override;

  void Add(const Slice &key) override;
  bool MayContain(const Slice &key) const override;

  size_t GetSizeInBytes() const override;
  size_t GetBitArraySize() const override;
  int GetHashFunctionCount() const override;

  void Reset() override;
  double EstimateFalsePositiveRate() const override;

  std::string Serialize() const override;
  bool Deserialize(const Slice &data) override;

  // 批量操作优化
  void AddBatch(const std::vector<Slice> &keys) override;
  bool MayContainBatch(const std::vector<Slice> &keys,
                       std::vector<bool> *results) const override;

  // 性能统计
  size_t GetElementCount() const override { return element_count_.load(); }
  double GetLoadFactor() const override;

  // 内存优化
  void ShrinkToFit() override;
  void Reserve(size_t expected_elements) override;

private:
  std::vector<uint8_t> bit_array_;
  size_t bit_array_size_;
  int hash_function_count_;
  mutable std::atomic<size_t> element_count_;

  // 内部方法
  void SetBit(size_t index);
  bool GetBit(size_t index) const;
  std::vector<uint32_t> ComputeHashes(const Slice &key) const;

  // 优化的位操作方法
  void SetBitOptimized(size_t index);
  bool GetBitOptimized(size_t index) const;
  void SetMultipleBits(const std::vector<size_t> &indices);

  // 缓存友好的哈希计算
  mutable std::vector<uint32_t> hash_cache_;
  mutable std::mutex hash_cache_mutex_;

  // 禁止拷贝
  StandardBloomFilter(const StandardBloomFilter &) = delete;
  StandardBloomFilter &operator=(const StandardBloomFilter &) = delete;
};

// 布隆过滤器构建器
class BloomFilterBuilder {
public:
  explicit BloomFilterBuilder(double bits_per_key);
  ~BloomFilterBuilder();

  // 添加键
  void AddKey(const Slice &key);

  // 完成构建并生成过滤器数据
  Slice Finish();

  // 获取预计的过滤器大小
  size_t EstimateFilterSize(size_t num_keys) const;

  // 重置构建器
  void Reset();

private:
  double bits_per_key_;
  std::vector<std::string> keys_;
  std::string filter_data_;

  // 内部方法
  size_t CalculateOptimalBitArraySize(size_t num_keys) const;
  int CalculateOptimalHashFunctionCount() const;

  // 禁止拷贝
  BloomFilterBuilder(const BloomFilterBuilder &) = delete;
  BloomFilterBuilder &operator=(const BloomFilterBuilder &) = delete;
};

// 布隆过滤器读取器
class BloomFilterReader {
public:
  BloomFilterReader();
  ~BloomFilterReader();

  // 从数据初始化
  bool Initialize(const Slice &filter_data);

  // 检查键是否可能存在
  bool MayContain(const Slice &key) const;

  // 获取过滤器信息
  size_t GetBitArraySize() const;
  int GetHashFunctionCount() const;

private:
  std::string data_copy_; // 数据的副本
  const char *data_;
  size_t data_size_;
  size_t bit_array_size_;
  int hash_function_count_;
  bool initialized_;

  // 内部方法
  bool GetBit(size_t index) const;
  std::vector<uint32_t> ComputeHashes(const Slice &key) const;

  // 禁止拷贝
  BloomFilterReader(const BloomFilterReader &) = delete;
  BloomFilterReader &operator=(const BloomFilterReader &) = delete;
};

// 布隆过滤器工具函数
namespace bloom_util {

// 计算最优参数
size_t CalculateOptimalBitArraySize(size_t expected_elements,
                                    double false_positive_rate);
int CalculateOptimalHashFunctionCount(size_t bit_array_size,
                                      size_t expected_elements);
double CalculateFalsePositiveRate(size_t bit_array_size,
                                  int hash_function_count,
                                  size_t element_count);

// 创建过滤器
std::unique_ptr<BloomFilter>
CreateBloomFilter(size_t expected_elements, double false_positive_rate = 0.01);

// 过滤器性能测试
struct BloomFilterBenchmarkResult {
  size_t total_adds;
  size_t total_lookups;
  size_t false_positives;
  double actual_false_positive_rate;
  std::chrono::milliseconds duration;
};

BloomFilterBenchmarkResult BenchmarkBloomFilter(BloomFilter *filter,
                                                size_t num_operations);

// 过滤器验证
Status ValidateBloomFilter(const BloomFilter *filter,
                           const std::vector<std::string> &test_keys);

} // namespace bloom_util

} // namespace lrdb