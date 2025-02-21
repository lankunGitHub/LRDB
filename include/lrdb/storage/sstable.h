// SSTable存储引擎

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/util/bloom_filter.h"
#include "lrdb/util/comparator.h"
#include "lrdb/util/env.h"

namespace lrdb {

// 前向声明
class MemTable;
struct ErrorReport;

// ============================================================================
// SSTable文件格式定义
// ============================================================================

// SSTable文件类型标识
enum class SSTableLevel : int {
  kLevel0 = 0, // L0层：新刷入的SSTable，可能重叠
  kLevel1 = 1, // L1层：第一层排序层
  kLevel2 = 2, // L2层：第二层排序层
  kLevel3 = 3, // L3层：第三层排序层
  kLevel4 = 4, // L4层：第四层排序层
  kLevel5 = 5, // L5层：第五层排序层
  kLevel6 = 6, // L6层：最大层级
  kMaxLevel = 6
};

// 数据块类型
enum class BlockType : uint8_t {
  kDataBlock = 1,      // 数据块
  kIndexBlock = 2,     // 索引块
  kBloomBlock = 3,     // Bloom过滤器块
  kMetaBlock = 4,      // 元数据块
  kCompressionDict = 5 // 压缩字典块（未来扩展）
};

// SSTable元数据
struct SSTableMeta {
  std::string filename; // 文件名
  uint64_t file_number; // 文件编号
  uint64_t file_size;   // 文件大小
  SSTableLevel level;   // 所在层级

  // 键范围
  std::string smallest_key; // 最小键（InternalKey编码）
  std::string largest_key;  // 最大键（InternalKey编码）

  // 统计信息
  uint64_t num_entries;    // 条目数量
  uint64_t num_deletions;  // 删除标记数量
  uint64_t raw_key_size;   // 原始键大小
  uint64_t raw_value_size; // 原始值大小

  // 时间戳
  uint64_t creation_time;   // 创建时间
  uint64_t oldest_key_time; // 最老键时间

  // 构造函数
  SSTableMeta() = default;
  SSTableMeta(const std::string &fname, uint64_t fnum, SSTableLevel lvl)
      : filename(fname), file_number(fnum), file_size(0), level(lvl),
        num_entries(0), num_deletions(0), raw_key_size(0), raw_value_size(0),
        creation_time(0), oldest_key_time(0) {}

  // 序列化/反序列化
  std::string Encode() const;
  bool Decode(const Slice &data);

  // 辅助方法
  bool KeyInRange(const Slice &key, const Comparator *comparator) const;
  bool OverlapsWith(const SSTableMeta &other,
                    const Comparator *comparator) const;
  double CompressionRatio() const;

  // 比较方法
  bool operator<(const SSTableMeta &other) const {
    return file_number < other.file_number;
  }
};

// ============================================================================
// SSTable数据块结构
// ============================================================================

// 数据块头部
struct BlockHeader {
  BlockType type;       // 块类型
  uint32_t size;        // 数据大小（不包括头部）
  uint32_t crc32;       // CRC32校验和
  uint32_t compression; // 压缩类型（0=无压缩）

  static constexpr size_t kHeaderSize = 16;

  std::string Encode() const;
  bool Decode(const Slice &data);
};

// 索引条目
struct IndexEntry {
  std::string key;           // 索引键（InternalKey编码）
  uint64_t block_offset;     // 数据块偏移
  uint32_t block_size;       // 数据块大小
  uint32_t first_key_offset; // 块内第一个键的偏移

  std::string Encode() const;
  bool Decode(const Slice &data);
};

// ============================================================================
// SSTable读取器
// ============================================================================

// 前向声明
class SSTableIterator;

class SSTableReader {
  friend class SSTableIterator;

public:
  // 迭代器接口
  class Iterator {
  public:
    virtual ~Iterator() = default;

    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice &target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;

    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual Status status() const = 0;
  };

public:
  SSTableReader();
  ~SSTableReader();

  // 打开SSTable文件进行读取
  Status Open(const std::string &filename, Env *env,
              const Comparator *comparator);

  // 查找操作
  Status Get(const Slice &key, std::string *value, bool *found,
             uint64_t snapshot = 0);

  // 创建迭代器
  std::unique_ptr<Iterator> NewIterator(uint64_t snapshot = 0);

  // 获取元数据
  const SSTableMeta &GetMeta() const { return meta_; }

  // Bloom过滤器检查
  bool MayContainKey(const Slice &key);

  // 键范围检查
  bool KeyInRange(const Slice &key) const;

  // 验证文件完整性
  Status VerifyChecksum();

  // 获取统计信息
  size_t ApproximateMemoryUsage() const;
  uint64_t GetFileSize() const { return meta_.file_size; }

  // Bloom过滤器统计信息
  struct BloomFilterStats {
    bool has_bloom_filter;
    size_t bloom_memory_usage;
    size_t bit_array_size;
    int hash_function_count;
    double estimated_false_positive_rate;
  };
  BloomFilterStats GetBloomFilterStats() const;

  // 预取数据块
  Status PrefetchRange(const Slice &begin, const Slice &end);

  // 快照可见性检查工具函数
  static bool IsVisibleInSnapshot(uint64_t snapshot_sequence,
                                  uint64_t read_snapshot) {
    return read_snapshot == 0 || snapshot_sequence <= read_snapshot;
  }

  // 错误恢复回调
  Status HandleErrorRecovery(const std::string &component,
                             const std::string &operation);
  Status HandleCorruptionError(const ErrorReport &error);

private:
  // 内部方法
  Status ReadMeta();
  Status ReadIndex();
  Status ReadBloomFilter();
  Status ReadDataBlock(uint64_t offset, uint32_t size, std::string *result);
  Status VerifyBlockChecksum(const Slice &block, uint32_t expected_crc);

  // 二分查找索引
  int FindIndexEntry(const Slice &key) const;

private:
  std::string filename_;
  std::unique_ptr<RandomAccessFile> file_;
  Env *env_;
  const Comparator *comparator_;

  SSTableMeta meta_;
  std::vector<IndexEntry> index_;
  std::unique_ptr<BloomFilterReader> bloom_filter_;

  bool opened_;
  mutable std::shared_mutex mutex_;

  // 块位置信息（从Footer读取）
  uint64_t index_block_offset_;
  uint32_t index_block_size_;
  uint64_t bloom_block_offset_;
  uint32_t bloom_block_size_;

  // 禁止拷贝
  SSTableReader(const SSTableReader &) = delete;
  SSTableReader &operator=(const SSTableReader &) = delete;
};

// ============================================================================
// SSTable写入器
// ============================================================================

struct SSTableWriterOptions {
  size_t block_size = 64 * 1024;           // 64KB数据块大小
  size_t index_block_size = 16 * 1024;     // 16KB索引块大小
  double bloom_filter_bits_per_key = 10.0; // Bloom过滤器精度
  bool enable_bloom_filter = true;         // 启用Bloom过滤器
  bool enable_compression = false;         // 启用压缩（未来扩展）
  size_t write_buffer_size = 1024 * 1024;  // 1MB写缓冲区
};

class SSTableWriter {
public:
  explicit SSTableWriter(
      const SSTableWriterOptions &options = SSTableWriterOptions{});
  ~SSTableWriter();

  // 创建新的SSTable文件
  Status Open(const std::string &filename, Env *env,
              const Comparator *comparator);

  // 添加键值对（必须按键的顺序添加）
  Status Add(const Slice &key, const Slice &value);

  // 完成写入并关闭文件
  Status Finish();

  // 取消写入（删除未完成的文件）
  Status Abandon();

  // 获取当前文件大小
  uint64_t FileSize() const;

  // 获取已写入的条目数
  uint64_t NumEntries() const { return num_entries_; }

  // 获取元数据（仅在Finish后有效）
  const SSTableMeta &GetMeta() const { return meta_; }

  // 获取写入统计
  struct WriteStats {
    uint64_t total_bytes_written;
    uint64_t index_bytes_written;
    uint64_t bloom_bytes_written;
    uint64_t data_bytes_written;
    double compression_ratio;
  };
  WriteStats GetWriteStats() const;

private:
  // 内部方法
  bool ShouldStartNewBlock() const;
  Status WriteDataBlock();
  Status WriteIndexBlock();
  Status WriteBloomFilterBlock();
  Status WriteMetaBlock();
  Status WriteBlockHeader(BlockType type, const Slice &data);
  Status FlushBuffer();

  void UpdateMeta(const Slice &key, const Slice &value);

private:
  SSTableWriterOptions options_;
  std::string filename_;
  std::unique_ptr<WritableFile> file_;
  const Comparator *comparator_;

  // 写入状态
  bool opened_;
  bool finished_;

  // 数据缓冲区
  std::string data_buffer_;
  std::vector<IndexEntry> index_entries_;
  std::unique_ptr<BloomFilterBuilder> bloom_builder_;

  // 元数据
  SSTableMeta meta_;
  uint64_t num_entries_;
  uint64_t current_offset_;
  std::string last_key_;

  // 各块的偏移与大小（写入Footer用）
  uint64_t index_block_offset_{0};
  uint32_t index_block_size_{0};
  uint64_t bloom_block_offset_{0};
  uint32_t bloom_block_size_{0};
  uint64_t meta_block_offset_{0};
  uint32_t meta_block_size_{0};

  // 统计信息
  WriteStats write_stats_;

  // 禁止拷贝
  SSTableWriter(const SSTableWriter &) = delete;
  SSTableWriter &operator=(const SSTableWriter &) = delete;
};

// ============================================================================
// SSTable压缩引擎（属于SSTable层）
// ============================================================================

// 压缩策略类型
enum class CompactionType {
  kMinor,     // 小压缩：L0 -> L1
  kMajor,     // 大压缩：Ln -> Ln+1
  kUniversal, // 通用压缩：多文件合并
  kFIFO,      // FIFO压缩：删除最老文件
  kManual     // 手动压缩
};

// 压缩任务描述
struct CompactionJob {
  CompactionType type;
  SSTableLevel input_level;
  SSTableLevel output_level;
  std::vector<std::string> input_files;
  std::string output_file_prefix;

  // 压缩范围
  std::string start_key;
  std::string end_key;

  // 压缩选项
  bool is_manual;
  bool delete_obsolete_files;

  CompactionJob() = default;
  CompactionJob(CompactionType t, SSTableLevel in_level, SSTableLevel out_level)
      : type(t), input_level(in_level), output_level(out_level),
        is_manual(false), delete_obsolete_files(true) {}
};

// 压缩结果
struct CompactionResult {
  Status status;
  std::vector<std::string> output_files;
  std::vector<SSTableMeta> output_metas;

  // 统计信息
  uint64_t bytes_read;
  uint64_t bytes_written;
  uint64_t num_input_records;
  uint64_t num_output_records;
  uint64_t num_deleted_records;
  std::chrono::milliseconds duration;

  CompactionResult()
      : bytes_read(0), bytes_written(0), num_input_records(0),
        num_output_records(0), num_deleted_records(0), duration(0) {}

  bool IsSuccess() const { return status.ok(); }
  double CompressionRatio() const {
    return bytes_read > 0 ? static_cast<double>(bytes_written) / bytes_read
                          : 1.0;
  }
};

// 压缩配置选项
struct CompactionConfig {
  // TTL相关配置
  bool enable_ttl_cleanup = true;   // 启用TTL清理
  uint64_t default_ttl_seconds = 0; // 默认TTL（0表示永不过期）

  // 删除标记清理配置
  bool enable_deletion_cleanup = true;           // 启用删除标记清理
  uint64_t deletion_cleanup_sequence_gap = 1000; // 删除标记清理的序列号间隔

  // 快照配置
  uint64_t oldest_snapshot_sequence = 0; // 最旧的快照序列号

  // 层级相关配置
  bool is_bottommost_level = false; // 是否是最底层压缩

  // 重复键处理
  bool preserve_deletes = false; // 是否保留删除标记

  CompactionConfig() = default;
};

// SSTable压缩器
class SSTableCompactor {
public:
  explicit SSTableCompactor(Env *env, const Comparator *comparator);
  ~SSTableCompactor();

  // 执行压缩任务
  CompactionResult CompactFiles(const CompactionJob &job);

  // 压缩接口
  CompactionResult CompactRange(const std::vector<std::string> &input_files,
                                const std::string &output_file_prefix,
                                SSTableLevel output_level,
                                const std::string &start_key = "",
                                const std::string &end_key = "");

  // MemTable刷盘到SSTable
  Status FlushMemTable(const MemTable *memtable,
                       const std::string &output_filename,
                       SSTableLevel output_level, SSTableMeta *output_meta);

  // 文件分割压缩（大文件分解为小文件）
  CompactionResult CompactWithSplit(const std::string &large_file,
                                    const std::string &output_prefix,
                                    size_t target_file_size,
                                    SSTableLevel output_level);

  // Bloom过滤器重建
  Status RebuildBloomFilter(const std::string &input_file,
                            double new_bits_per_key = 10.0);

  // 验证压缩结果
  Status VerifyCompactionResult(const CompactionResult &result);

  // 配置管理
  void SetCompactionConfig(const CompactionConfig &config) { config_ = config; }
  const CompactionConfig &GetCompactionConfig() const { return config_; }

private:
  // 内部方法
  Status DoCompaction(const CompactionJob &job, CompactionResult *result);
  Status MergeInputFiles(const std::vector<std::string> &input_files,
                         const std::string &output_file,
                         SSTableLevel output_level, CompactionResult *result);

  // 多路归并迭代器
  class MultiWayMergeIterator;
  std::unique_ptr<MultiWayMergeIterator>
  CreateMergeIterator(const std::vector<std::string> &input_files);

  // 键过滤逻辑
  bool ShouldKeepKey(const Slice &key);

private:
  Env *env_;
  const Comparator *comparator_;
  SSTableWriterOptions writer_options_;
  CompactionConfig config_;

  // 禁止拷贝
  SSTableCompactor(const SSTableCompactor &) = delete;
  SSTableCompactor &operator=(const SSTableCompactor &) = delete;
};

// ============================================================================
// SSTable管理器
// ============================================================================

struct SSTableManagerOptions {
  size_t max_open_files = 1000;                // 最大打开文件数
  size_t table_cache_size = 256 * 1024 * 1024; // 256MB表缓存
  bool enable_file_prefetch = true;            // 启用文件预取
  size_t prefetch_size = 256 * 1024;           // 256KB预取大小
  std::string directory;                       // SSTable文件所在目录（为空则使用当前目录）
};

class SSTableManager {
public:
  explicit SSTableManager(
      Env *env, const Comparator *comparator,
      const SSTableManagerOptions &options = SSTableManagerOptions{});
  ~SSTableManager();

  // 注册SSTable文件
  Status RegisterSSTable(const SSTableMeta &meta);

  // 设置下一个文件编号（打开数据库加载已有文件后调用，避免重名）
  void SetNextFileNumber(uint64_t file_number) {
    next_file_number_.store(file_number);
  }

  // 移除SSTable文件
  Status UnregisterSSTable(uint64_t file_number);

  // 查找操作
  Status Get(const Slice &key, std::string *value, bool *found,
             uint64_t snapshot = 0);

  // 创建层级迭代器
  class LevelIterator {
  public:
    LevelIterator(SSTableManager *manager, SSTableLevel level);

    void SeekToFirst();
    void SeekToLast();
    void Seek(const Slice &target);
    void Next();
    void Prev();
    bool Valid() const;
    Slice key() const;
    Slice value() const;
    Status status() const;

  private:
    void LoadCurrentIterator();

    SSTableManager *manager_;
    SSTableLevel level_;
    std::vector<SSTableMeta> level_files_;
    int current_file_;
    std::unique_ptr<SSTableReader::Iterator> current_iter_;
  };

  std::unique_ptr<LevelIterator> NewLevelIterator(SSTableLevel level);

  // 获取层级文件列表
  std::vector<SSTableMeta> GetLevelFiles(SSTableLevel level) const;
  std::vector<SSTableMeta> GetAllFiles() const;

  // 文件范围查询
  std::vector<SSTableMeta>
  GetFilesInRange(const Slice &start_key, const Slice &end_key,
                  SSTableLevel level = SSTableLevel::kLevel0) const;

  // 获取重叠文件
  std::vector<SSTableMeta> GetOverlappingFiles(const Slice &start_key,
                                               const Slice &end_key,
                                               SSTableLevel level) const;

  // 压缩相关API（暴露给上层LSMTree）
  CompactionResult CompactFiles(const CompactionJob &job);
  Status FlushMemTableToSSTable(const MemTable *memtable,
                                SSTableLevel target_level,
                                SSTableMeta *output_meta);

  // 文件管理
  Status DeleteObsoleteFiles(const std::vector<uint64_t> &file_numbers);
  Status RenameFile(const std::string &old_name, const std::string &new_name);

  // 统计信息
  struct LevelStats {
    SSTableLevel level;
    size_t num_files;
    uint64_t total_size;
    double avg_file_size;
    std::string size_range;
  };
  std::vector<LevelStats> GetLevelStats() const;

  // 内存使用统计
  size_t ApproximateMemoryUsage() const;

  // 缓存管理
  void EvictFromCache(uint64_t file_number);
  void ClearCache();

private:
  // 内部方法
  SSTableReader *GetReader(uint64_t file_number);
  Status LoadReader(uint64_t file_number, SSTableReader **reader);
  void EvictLRUReader();

  Status ValidateFileMeta(const SSTableMeta &meta);
  std::string GenerateFileName(uint64_t file_number, SSTableLevel level) const;

private:
  Env *env_;
  const Comparator *comparator_;
  SSTableManagerOptions options_;

  // 文件元数据（按层级组织）
  std::vector<std::vector<SSTableMeta>> level_files_;
  std::unordered_map<uint64_t, SSTableMeta> file_meta_map_;

  // 读取器缓存（LRU）
  struct CacheEntry {
    std::unique_ptr<SSTableReader> reader;
    uint64_t last_access_time;
    size_t access_count;
  };
  std::unordered_map<uint64_t, CacheEntry> reader_cache_;

  // 压缩器
  std::unique_ptr<SSTableCompactor> compactor_;

  // 线程安全
  mutable std::shared_mutex mutex_;
  std::atomic<uint64_t> next_file_number_;

  // 禁止拷贝
  SSTableManager(const SSTableManager &) = delete;
  SSTableManager &operator=(const SSTableManager &) = delete;
};

} // namespace lrdb
