// LSM Tree存储引擎 - 统一存储协调器，纯同步API设计

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "lrdb/background/background_manager.h"
#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/storage/memtable.h"
#include "lrdb/storage/sstable.h"
#include "lrdb/util/comparator.h"
#include "lrdb/util/env.h"

namespace lrdb {

// 前向声明
class MemTable;
class MemTableManager;
class SSTableManager;
class BackgroundTaskManager;

// 恢复相关结构体
struct WALRecordForRecovery {
  std::string key;
  std::string value;
  uint64_t sequence;
  bool is_deletion;
  uint64_t timestamp;

  WALRecordForRecovery(const std::string &k, const std::string &v, uint64_t seq,
                       bool del = false)
      : key(k), value(v), sequence(seq), is_deletion(del),
        timestamp(std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count()) {}
};

// ============================================================================
// LSMTree配置
// ============================================================================

struct LSMTreeOptions {
  // MemTable配置
  size_t memtable_size = 64 * 1024 * 1024; // 64MB MemTable大小
  size_t max_memtables = 4;                // 最大MemTable数量（包括immutable）

  // SSTable配置
  size_t l0_compaction_trigger = 4;      // L0层触发压缩的文件数
  size_t l0_slowdown_writes_trigger = 8; // L0层减缓写入的文件数
  size_t l0_stop_writes_trigger = 12;    // L0层停止写入的文件数

  // 层级配置
  size_t level_multiplier = 10;                        // 层级倍增因子
  size_t max_bytes_for_level_base = 256 * 1024 * 1024; // L1基础大小256MB

  // 压缩配置
  bool enable_auto_compaction = true;    // 启用自动压缩
  size_t max_background_compactions = 2; // 最大并发压缩数
  size_t max_background_flushes = 1;     // 最大并发刷盘数

  // 写入配置
  bool sync_writes = false; // 同步写入
  bool disable_wal = false; // 禁用WAL

  // 目录配置
  std::string db_path = "./db"; // 数据库路径

  LSMTreeOptions() = default;
};

// ============================================================================
// LSMTree状态和统计
// ============================================================================

struct LSMTreeStats {
  // MemTable统计
  size_t active_memtable_size;
  size_t num_immutable_memtables;
  size_t total_memtable_memory;

  // SSTable统计
  std::vector<SSTableManager::LevelStats> level_stats;
  size_t total_sst_files;
  uint64_t total_sst_size;

  // 操作统计
  uint64_t num_puts;
  uint64_t num_gets;
  uint64_t num_deletes;
  uint64_t num_flushes;
  uint64_t num_compactions;

  // 性能统计
  double avg_get_latency_ms;
  double avg_put_latency_ms;
  uint64_t bytes_written;
  uint64_t bytes_read;

  LSMTreeStats()
      : active_memtable_size(0), num_immutable_memtables(0),
        total_memtable_memory(0), total_sst_files(0), total_sst_size(0),
        num_puts(0), num_gets(0), num_deletes(0), num_flushes(0),
        num_compactions(0), avg_get_latency_ms(0.0), avg_put_latency_ms(0.0),
        bytes_written(0), bytes_read(0) {}
};

// 写入状态
enum class WriteStatus {
  kOK,           // 正常写入
  kMemTableFull, // MemTable已满，需要刷盘
  kSlowdown,     // 需要减缓写入
  kStop          // 需要停止写入
};

// ============================================================================
// LSMTree迭代器
// ============================================================================

class LSMTreeIterator {
public:
  virtual ~LSMTreeIterator() = default;

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

// ============================================================================
// LSMTree主类
// ============================================================================

class LSMTree {
  friend class LSMTreeMergeIterator;

public:
  explicit LSMTree(const LSMTreeOptions &options = LSMTreeOptions{});
  ~LSMTree();

  // ============================================================================
  // 初始化和关闭
  // ============================================================================

  // 打开LSMTree并注册后台任务
  Status Open(Env *env, const Comparator *comparator,
              BackgroundTaskManager *bg_manager);

  // 关闭LSMTree
  Status Close();

  // 是否已打开
  bool IsOpen() const { return opened_; }

  // ============================================================================
  // 核心读写接口（纯同步API）
  // ============================================================================

  // 写入键值对
  Status Put(const Slice &key, const Slice &value, uint64_t sequence = 0);

  // 删除键
  Status Delete(const Slice &key, uint64_t sequence = 0);

  // 合并操作
  Status Merge(const Slice &key, const Slice &value, uint64_t sequence = 0);

  // 读取键值
  Status Get(const Slice &key, std::string *value, uint64_t snapshot = 0);

  // 创建迭代器
  std::unique_ptr<LSMTreeIterator> NewIterator(uint64_t snapshot = 0);

  // ============================================================================
  // 后台任务API（暴露给BackgroundManager）
  // ============================================================================

  // MemTable刷盘API
  Status TriggerFlush();

  // 把非空 mutable 也变为 immutable 并全部刷盘（干净关闭用）。
  // TriggerFlush 只刷 immutable，对 mutable 是空操作
  Status FlushAll();        // 触发刷盘检查
  Status FlushOldestMemTable(); // 刷盘最老的immutable MemTable
  bool NeedsFlush() const;      // 是否需要刷盘

  // 压缩API
  Status TriggerCompaction();              // 触发压缩检查
  Status CompactLevel(SSTableLevel level); // 压缩指定层级
  bool NeedsCompaction() const;            // 是否需要压缩

  // 手动压缩
  Status CompactRange(const Slice *begin, const Slice *end);

  // ============================================================================
  // 查询和统计接口
  // ============================================================================

  // 获取统计信息
  LSMTreeStats GetStats() const;

  // 获取写入状态
  WriteStatus GetWriteStatus() const;

  // 获取各层级文件
  std::vector<SSTableMeta> GetLevelFiles(SSTableLevel level) const;

  // 估算键的大致位置
  Status GetApproximateSizes(const std::vector<std::pair<Slice, Slice>> &ranges,
                             std::vector<uint64_t> *sizes) const;

  // 内存使用估算
  size_t ApproximateMemoryUsage() const;

  // ============================================================================
  // 配置和控制接口
  // ============================================================================

  // 获取配置
  const LSMTreeOptions &GetOptions() const { return options_; }

  // 更新配置（运行时）
  Status UpdateOptions(const LSMTreeOptions &new_options);

  // 设置写入限制
  void SetWriteLimit(bool slowdown, bool stop);

  // ============================================================================
  // 恢复相关接口
  // ============================================================================

  // 从WAL记录中恢复数据
  Status RecoverFromWALRecord(const std::string &key, const std::string &value,
                              uint64_t sequence, bool is_deletion = false);

  // 批量恢复WAL记录
  Status
  BatchRecoverFromWAL(const std::vector<struct WALRecordForRecovery> &records);

  // 获取/设置恢复模式
  void SetRecoveryMode(bool in_recovery);
  bool IsInRecoveryMode() const;

  // 恢复完成后的同步操作
  Status FinishRecovery(uint64_t final_sequence);

  // 验证恢复后的数据一致性
  Status ValidateRecoveredData() const;

  // ============================================================================
  // 内部状态检查（用于调试和测试）
  // ============================================================================

  // 验证内部状态一致性
  Status ValidateInternalState() const;

  // 获取调试信息
  std::string DebugString() const;

  // 强制进行一次压缩
  Status TEST_CompactMemTable();
  Status TEST_CompactLevel0();

private:
  // ============================================================================
  // 内部组件管理
  // ============================================================================

  // 初始化组件
  Status InitializeComponents();

  // 使当前MemTable变为immutable
  Status MakeMemTableImmutable();

  // 执行MemTable刷盘
  Status DoFlushMemTable(MemTable *memtable);

  // 执行层级压缩
  Status DoLevelCompaction(SSTableLevel level);

  // 选择压缩文件
  Status PickCompactionFiles(SSTableLevel level,
                             std::vector<std::string> *input_files,
                             SSTableLevel *output_level);

  // ============================================================================
  // 写入控制
  // ============================================================================

  // 检查写入条件
  WriteStatus CheckWriteConditions() const;

  // 等待写入条件满足（复用调用方已持有的 write_mutex_，避免自我死锁）
  Status WaitForWriteCondition(std::unique_lock<std::mutex> &write_lock);

  // 更新序列号
  uint64_t GetNextSequenceNumber();

  // 已持 write_mutex_ 的写入内部实现（公共 Put/Delete/Merge 与批量恢复共用）
  Status PutLocked(const Slice &key, const Slice &value, uint64_t sequence);
  Status DeleteLocked(const Slice &key, uint64_t sequence);
  Status MergeLocked(const Slice &key, const Slice &value, uint64_t sequence);

  // ============================================================================
  // 读取优化
  // ============================================================================

  // 在MemTable中查找
  Status GetFromMemTables(const Slice &key, std::string *value,
                          uint64_t snapshot, bool *found);

  // 在SSTable中查找
  Status GetFromSSTables(const Slice &key, std::string *value,
                         uint64_t snapshot, bool *found);

  // ============================================================================
  // 后台任务注册
  // ============================================================================

  // 注册后台任务到BackgroundManager
  Status RegisterBackgroundTasks();

  // 取消注册后台任务
  Status UnregisterBackgroundTasks();

  // 刷盘任务函数
  Status BackgroundFlushTask();

  // 压缩任务函数
  Status BackgroundCompactionTask();

  // ============================================================================
  // 配置验证和路径管理
  // ============================================================================

  // 验证配置
  Status ValidateOptions() const;

  // 创建数据目录
  Status CreateDirectories();

  // 生成文件路径
  std::string GetSSTablePath(uint64_t file_number, SSTableLevel level) const;

  // 加载磁盘上已有的SSTable文件（打开数据库时调用）
  Status LoadExistingSSTables();

private:
  // 配置
  LSMTreeOptions options_;

  // 外部依赖
  Env *env_;
  const Comparator *comparator_;
  BackgroundTaskManager *bg_manager_;

  // 核心组件
  std::unique_ptr<MemTableManager> memtable_manager_;
  std::unique_ptr<SSTableManager> sstable_manager_;

  // 当前活跃的MemTable (用于恢复操作)
  std::shared_ptr<MemTable> current_memtable_;

  // MemTable最大大小配置
  size_t max_memtable_size_;

  // 恢复统计信息结构
  struct RecoveryStats {
    std::atomic<uint64_t> total_recovered_records{0};
    std::atomic<uint64_t> total_recovered_puts{0};
    std::atomic<uint64_t> total_recovered_deletes{0};
    std::atomic<uint64_t> total_recovery_errors{0};
  };
  mutable RecoveryStats recovery_stats_;

  // 状态控制
  bool opened_;
  std::atomic<bool> in_recovery_mode_;
  std::atomic<uint64_t> next_sequence_number_;
  std::atomic<uint64_t> recovery_sequence_number_;

  // 写入控制
  std::atomic<bool> write_slowdown_;
  std::atomic<bool> write_stop_;
  mutable std::condition_variable write_cv_;

  // 后台任务ID
  std::vector<uint64_t> background_task_ids_;

  // 线程安全
  mutable std::shared_mutex mutex_;
  mutable std::mutex write_mutex_;

  // 统计信息
  mutable std::atomic<uint64_t> stats_num_puts_;
  mutable std::atomic<uint64_t> stats_num_gets_;
  mutable std::atomic<uint64_t> stats_num_deletes_;
  mutable std::atomic<uint64_t> stats_num_flushes_;
  mutable std::atomic<uint64_t> stats_num_compactions_;

  // 禁止拷贝
  LSMTree(const LSMTree &) = delete;
  LSMTree &operator=(const LSMTree &) = delete;
};

// ============================================================================
// LSMTree归并迭代器实现
// ============================================================================

class LSMTreeMergeIterator : public LSMTreeIterator {
public:
  LSMTreeMergeIterator(const LSMTree *lsm_tree, uint64_t snapshot);
  ~LSMTreeMergeIterator() override;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice &target) override;
  void Next() override;
  void Prev() override;

  Slice key() const override;
  Slice value() const override;
  Status status() const override;

private:
  void FindSmallest();
  void FindLargest();
  void ClearChildren();

  // 内部键比较：用户键升序（方向由调用方决定），同键新版本（大序列号）在前
  int CompareInternalKeys(const Slice &a, const Slice &b) const;
  // 选择最佳子迭代器（prefer_newest=true取最小用户键，false取最大用户键；
  // 同一用户键永远取序列号最大的版本）
  int PickChild(bool prefer_newest, bool *is_sstable);
  // 获取子迭代器当前键的内部键编码（memtable迭代器只暴露用户键，需重新拼装）
  std::string GetChildInternalKey(bool is_sstable, int index) const;

private:
  const LSMTree *lsm_tree_;
  uint64_t snapshot_;
  const Comparator *comparator_;

  // 遍历方向（反向遍历与正向遍历混用时需要重新锚定子迭代器）
  enum class Direction { kForward, kReverse };
  Direction direction_;

  // 子迭代器：MemTable迭代器 + SSTable层级迭代器
  std::vector<std::unique_ptr<MemTable::Iterator>> memtable_iters_;
  std::vector<std::unique_ptr<SSTableManager::LevelIterator>> sstable_iters_;

  // 当前最小键的迭代器索引
  int current_memtable_index_;
  int current_sstable_index_;

  bool valid_;
  Status status_;

  // 当前键值缓存
  std::string current_key_;
  std::string current_value_;
};

} // namespace lrdb
