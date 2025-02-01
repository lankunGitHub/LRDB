#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "lrdb/core/coding.h"
#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/util/comparator.h"
#include "lrdb/util/skiplist.h"

namespace lrdb {

// 前向声明
class SSTableWriter;
struct ErrorReport;

// 内存表条目类型
enum class ValueType : uint8_t {
  kValue = 1,    // 普通值
  kDeletion = 2, // 删除标记
  kMerge = 3     // 合并操作
};

// 内部键格式：user_key + sequence_number + type
class InternalKey {
public:
  InternalKey() = default;
  InternalKey(const Slice &user_key, uint64_t sequence, ValueType type);

  // 解析内部键
  Slice user_key() const;
  uint64_t sequence() const;
  ValueType type() const;

  // 序列化和反序列化
  std::string Encode() const;
  bool Decode(const Slice &internal_key);

  // 比较器
  int Compare(const InternalKey &other, const Comparator *comparator) const;

private:
  std::string data_; // 编码后的内部键
};

// MemTable配置
struct MemTableOptions {
  size_t write_buffer_size = 64 * 1024 * 1024; // 64MB
  size_t max_write_buffer_number = 6;          // 最大MemTable数量
  bool allow_concurrent_memtable_write = true; // 允许并发写入
};

// MemTable实现 - 基于SkipList
class MemTable {
public:
  // 前向声明移到public区域
  struct MemTableEntry;
  class EntryComparator;
  using SkipListType = SkipList<std::string, MemTableEntry *>;

  explicit MemTable(const Comparator *comparator,
                    const MemTableOptions &options = MemTableOptions());

  ~MemTable();

  // 禁止拷贝
  MemTable(const MemTable &) = delete;
  MemTable &operator=(const MemTable &) = delete;

  // 基本操作
  Status Put(const Slice &key, const Slice &value, uint64_t sequence);
  Status Delete(const Slice &key, uint64_t sequence);
  Status Merge(const Slice &key, const Slice &value, uint64_t sequence);

  // 查询操作
  bool Get(const Slice &key, std::string *value, Status *status,
           uint64_t snapshot = 0);

  // 迭代器
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
    virtual ValueType type() const = 0;
    virtual uint64_t sequence() const = 0;

    virtual Status status() const = 0;
  };

  std::unique_ptr<Iterator> NewIterator() const;

  // 状态查询
  size_t ApproximateMemoryUsage() const;
  size_t NumEntries() const;
  bool Empty() const;
  bool ShouldFlush() const;

  // 引用计数管理
  void Ref();
  void Unref();

  // 刷盘操作
  Status FlushToSSTable(const std::string &output_file, SSTableWriter *writer);

  // 获取键值范围
  bool GetRange(std::string *smallest_key, std::string *largest_key) const;

  // 并发控制
  void SetImmutable();
  bool IsImmutable() const;

  // 调试和验证
  bool ValidateStructure() const;
  std::string DebugString() const;

  // 错误恢复回调
  Status HandleErrorRecovery(const std::string &component,
                             const std::string &operation);
  Status HandleMemoryError(const ErrorReport &error);

  // MemTable条目结构
  struct MemTableEntry {
    InternalKey key;
    std::string value;

    MemTableEntry(const InternalKey &k, const Slice &v)
        : key(k), value(v.ToString()) {}

    size_t MemoryUsage() const {
      return sizeof(MemTableEntry) + key.Encode().size() + value.size();
    }
  };

  // 内部键比较器 - 用于SkipList
  class InternalKeyComparator : public Comparator {
  public:
    explicit InternalKeyComparator(const Comparator *user_comparator)
        : user_comparator_(user_comparator) {}

    int Compare(const Slice &a, const Slice &b) const override {
      Slice user_key_a, user_key_b;
      uint64_t seq_a, seq_b;
      uint8_t type_a, type_b;

      if (!coding::ParseInternalKey(a, &user_key_a, &seq_a, &type_a) ||
          !coding::ParseInternalKey(b, &user_key_b, &seq_b, &type_b)) {
        return a.compare(b); // 降级为字节比较
      }

      // 首先比较用户键
      int user_cmp = user_comparator_->Compare(user_key_a, user_key_b);
      if (user_cmp != 0)
        return user_cmp;

      // 用户键相同时，按序列号降序排列（较新的在前）
      if (seq_a > seq_b)
        return -1;
      if (seq_a < seq_b)
        return 1;

      // 序列号相同时，按类型排序（删除 < 值）
      return static_cast<int>(type_a) - static_cast<int>(type_b);
    }

    const char *Name() const override { return "lrdb.InternalKeyComparator"; }

    void FindShortestSeparator(std::string *start,
                               const Slice &limit) const override {
      user_comparator_->FindShortestSeparator(start, limit);
    }

    void FindShortSuccessor(std::string *key) const override {
      user_comparator_->FindShortSuccessor(key);
    }

  private:
    const Comparator *user_comparator_;
  };

private:
  // 成员变量
  std::unique_ptr<InternalKeyComparator> internal_comparator_;
  std::unique_ptr<SkipListType> table_;
  MemTableOptions options_;

  // 引用计数和状态
  std::atomic<int> refs_;
  std::atomic<bool> immutable_;
  std::atomic<size_t> memory_usage_;
  std::atomic<size_t> num_entries_;

  // 并发控制
  mutable std::shared_mutex mutex_;

  // 内存池
  std::vector<std::unique_ptr<MemTableEntry>> entries_;

  // 辅助方法
  MemTableEntry *CreateEntry(const InternalKey &key, const Slice &value);
};

// MemTable管理器 - 管理多个MemTable实例
class MemTableManager {
public:
  explicit MemTableManager(const Comparator *comparator,
                           const MemTableOptions &options = MemTableOptions());

  ~MemTableManager();

  // 禁止拷贝
  MemTableManager(const MemTableManager &) = delete;
  MemTableManager &operator=(const MemTableManager &) = delete;

  // MemTable管理
  MemTable *GetMutableMemTable();
  std::vector<MemTable *> GetImmutableMemTables();

  // 写入操作
  Status Put(const Slice &key, const Slice &value, uint64_t sequence);
  Status Delete(const Slice &key, uint64_t sequence);
  Status Merge(const Slice &key, const Slice &value, uint64_t sequence);

  // 查询操作
  bool Get(const Slice &key, std::string *value, Status *status,
           uint64_t snapshot = 0);

  // 创建新的MemTable（当前的变为不可变）
  Status SwitchMemTable();

  // 状态查询
  size_t TotalMemoryUsage() const;
  size_t NumImmutableMemTables() const;
  bool ShouldSwitchMemTable() const;
  bool HasImmutableMemTables() const;

  // 获取需要刷盘的MemTable（供上层调度使用）
  MemTable *GetMemTableForFlush();

  // 完成刷盘后的清理（供上层调用）
  Status CompleteFlushedMemTable(MemTable *flushed_memtable);

  // 配置管理
  const MemTableOptions &GetOptions() const { return options_; }
  void UpdateOptions(const MemTableOptions &new_options);

private:
  const Comparator *comparator_;
  MemTableOptions options_;

  // MemTable实例
  std::unique_ptr<MemTable> mutable_memtable_;
  std::vector<std::unique_ptr<MemTable>> immutable_memtables_;

  // 并发控制
  mutable std::shared_mutex mutex_;

  // 辅助方法
  void CreateNewMemTable();
};

// 工具函数 - 纯功能性API
namespace memtable_util {

// 创建MemTable
std::unique_ptr<MemTable>
CreateMemTable(const Comparator *comparator,
               const MemTableOptions &options = MemTableOptions());

// 验证MemTable数据完整性
Status ValidateMemTable(MemTable *memtable);

// 估算MemTable刷盘后的SSTable大小
uint64_t EstimateSSTableSize(const MemTable *memtable);

// 获取MemTable的键值统计
struct MemTableStats {
  size_t total_keys;
  size_t unique_keys;
  size_t deletions;
  size_t merges;
  std::string smallest_key;
  std::string largest_key;
};

MemTableStats AnalyzeMemTable(const MemTable *memtable);

} // namespace memtable_util

} // namespace lrdb