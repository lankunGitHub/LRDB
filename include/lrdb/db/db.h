// 数据库核心接口

#pragma once

#include "lrdb/concurrency/snapshot.h"
#include "lrdb/concurrency/transaction.h"
#include "lrdb/concurrency/mvcc.h"
#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/db/iterator.h"
#include "lrdb/db/options.h"

#include <atomic>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace lrdb {

// 前向声明
class ColumnFamily;
class ColumnFamilyImpl;
class BackgroundTaskManager;
class ColumnFamilyManager;
struct SSTableInfo;
struct ColumnFamilyDescriptor;

// 数据库接口
// - 路由对 ColumnFamily 的读写、事务、快照、迭代器与管理操作；
// - DBImpl 在初始化时完成列族集合加载与崩溃恢复，并注册后台任务（MVCC 刷回/GC）。
class DB {
public:
  DB() = default;
  virtual ~DB();

  // ============================================================================
  // 基本读写操作
  // ============================================================================

  // 写入键值对
  virtual Status Put(const WriteOptions &options, const Slice &key,
                     const Slice &value) = 0;
  virtual Status Put(const WriteOptions &options, ColumnFamily *column_family,
                     const Slice &key, const Slice &value) = 0;

  // 删除键
  virtual Status Delete(const WriteOptions &options, const Slice &key) = 0;
  virtual Status Delete(const WriteOptions &options,
                        ColumnFamily *column_family, const Slice &key) = 0;

  // 读取键值
  virtual Status Get(const ReadOptions &options, const Slice &key,
                     std::string *value) = 0;
  virtual Status Get(const ReadOptions &options, ColumnFamily *column_family,
                     const Slice &key, std::string *value) = 0;

  // 批量读取
  virtual std::vector<Status> MultiGet(const ReadOptions &options,
                                       const std::vector<Slice> &keys,
                                       std::vector<std::string> *values) = 0;

  // 合并操作
  virtual Status Merge(const WriteOptions &options, const Slice &key,
                       const Slice &value) = 0;
  virtual Status Merge(const WriteOptions &options, ColumnFamily *column_family,
                       const Slice &key, const Slice &value) = 0;



  // ============================================================================
  // 迭代器操作
  // ============================================================================

  // 创建迭代器
  virtual Iterator *NewIterator(const ReadOptions &options) = 0;
  virtual Iterator *NewIterator(const ReadOptions &options,
                                ColumnFamily *column_family) = 0;

  // 创建多列族迭代器
  virtual Status
  NewIterators(const ReadOptions &options,
               const std::vector<ColumnFamily *> &column_families,
               std::vector<Iterator *> *iterators) = 0;

  // ============================================================================
  // 快照操作 - 委托给列族
  // ============================================================================

  // 创建快照（使用默认列族）
  virtual const Snapshot *GetSnapshot() = 0;
  virtual const Snapshot *GetSnapshot(ColumnFamily* column_family) = 0;

  // 释放快照
  virtual void ReleaseSnapshot(const Snapshot *snapshot) = 0;

  // ============================================================================
  // 事务操作
  // ============================================================================

  // ========== 事务操作 ==========
  
  // 开始事务 - 统一的事务接口
  virtual std::unique_ptr<Transaction> BeginTransaction(
      const TxnOptions& options = TxnOptions{}) = 0;
  
  // 列族特定事务 - 未来扩展使用
  virtual std::unique_ptr<Transaction> BeginTransaction(
      ColumnFamily* column_family,
      const TxnOptions& options = TxnOptions{}) = 0;

  // ============================================================================
  // 列族操作
  // ============================================================================

  // 创建列族
  virtual Status CreateColumnFamily(const ColumnFamilyOptions &options,
                                    const std::string &column_family_name,
                                    ColumnFamily **handle) = 0;

  // 删除列族
  virtual Status DropColumnFamily(ColumnFamily *column_family) = 0;

  // 销毁列族句柄
  virtual Status DestroyColumnFamilyHandle(ColumnFamily *column_family) = 0;

  // 列出所有列族
  virtual std::vector<std::string> ListColumnFamilies() const = 0;

  // 获取默认列族
  virtual ColumnFamily *DefaultColumnFamily() const = 0;

  // ============================================================================
  // 数据库管理操作
  // ============================================================================

  // 刷盘操作
  virtual Status Flush(const FlushOptions &options) = 0;
  virtual Status Flush(const FlushOptions &options,
                       ColumnFamily *column_family) = 0;

  // 压缩操作
  virtual Status CompactRange(const CompactRangeOptions &options,
                              const Slice *begin = nullptr,
                              const Slice *end = nullptr) = 0;
  virtual Status CompactRange(const CompactRangeOptions &options,
                              ColumnFamily *column_family,
                              const Slice *begin = nullptr,
                              const Slice *end = nullptr) = 0;

  // 暂停和恢复后台任务
  virtual Status PauseBackgroundWork() = 0;
  virtual Status ContinueBackgroundWork() = 0;

  // 数据库关闭
  virtual Status Close() = 0;

  // ============================================================================
  // 数据库信息和统计
  // ============================================================================

  // 获取数据库名称
  virtual const std::string &GetName() const = 0;

  // 获取数据库选项
  virtual const DBOptions &GetDBOptions() const = 0;

  // 获取属性
  virtual bool GetProperty(const std::string &property, std::string *value) = 0;
  virtual bool GetProperty(ColumnFamily *column_family,
                           const std::string &property, std::string *value) = 0;

  // 获取近似大小
  virtual uint64_t GetApproximateSize(const Range *ranges, int n) = 0;
  virtual uint64_t GetApproximateSize(ColumnFamily *column_family,
                                      const Range *ranges, int n) = 0;

  // 获取近似条目数量
  virtual uint64_t GetApproximateNumEntries() = 0;
  virtual uint64_t GetApproximateNumEntries(ColumnFamily *column_family) = 0;

  // 后台任务管理
  virtual Status RegisterBackgroundTask(
      const std::string &component_name,
      const std::function<Status()> &task_function,
      const std::string &task_name = "", bool periodic = false,
      std::chrono::milliseconds interval = std::chrono::milliseconds(0));

  virtual Status UnregisterBackgroundTask(const std::string &component_name,
                                          const std::string &task_name);

  // 获取后台任务管理器（供组件使用）
  virtual BackgroundTaskManager *GetBackgroundTaskManager() const = 0;

  // ============================================================================
  // 静态方法
  // ============================================================================

  // 打开数据库
  static Status Open(const DBOptions &db_options, const std::string &name,
                     std::unique_ptr<DB> *dbptr);

  // 打开数据库(带列族)
  static Status Open(const DBOptions &db_options, const std::string &name,
                     const std::vector<ColumnFamilyDescriptor> &column_families,
                     std::vector<ColumnFamily *> *handles,
                     std::unique_ptr<DB> *dbptr);

  // 以只读模式打开
  static Status OpenForReadOnly(const DBOptions &db_options,
                                const std::string &name,
                                std::unique_ptr<DB> *dbptr,
                                bool error_if_wal_file_exists = false);

  // 列出列族
  static Status ListColumnFamilies(const DBOptions &db_options,
                                   const std::string &name,
                                   std::vector<std::string> *column_families);

  // 销毁数据库
  static Status DestroyDB(const std::string &name, const DBOptions &options);

  // 修复数据库
  static Status RepairDB(const std::string &dbname, const DBOptions &options);
};

// 默认DB实现
class DBImpl : public DB {
public:
  DBImpl(const DBOptions &options, const std::string &dbname);
  ~DBImpl() override;

  // 基本读写操作
  Status Put(const WriteOptions &options, const Slice &key,
             const Slice &value) override;
  Status Put(const WriteOptions &options, ColumnFamily *column_family,
             const Slice &key, const Slice &value) override;

  Status Delete(const WriteOptions &options, const Slice &key) override;
  Status Delete(const WriteOptions &options, ColumnFamily *column_family,
                const Slice &key) override;

  Status Get(const ReadOptions &options, const Slice &key,
             std::string *value) override;
  Status Get(const ReadOptions &options, ColumnFamily *column_family,
             const Slice &key, std::string *value) override;

  std::vector<Status> MultiGet(const ReadOptions &options,
                               const std::vector<Slice> &keys,
                               std::vector<std::string> *values) override;

  Status Merge(const WriteOptions &options, const Slice &key,
               const Slice &value) override;
  Status Merge(const WriteOptions &options, ColumnFamily *column_family,
               const Slice &key, const Slice &value) override;



  // 迭代器操作
  Iterator *NewIterator(const ReadOptions &options) override;
  Iterator *NewIterator(const ReadOptions &options,
                        ColumnFamily *column_family) override;
  Status NewIterators(const ReadOptions &options,
                      const std::vector<ColumnFamily *> &column_families,
                      std::vector<Iterator *> *iterators) override;

  // 快照操作 - 委托给列族
  const Snapshot *GetSnapshot() override;
  const Snapshot *GetSnapshot(ColumnFamily* column_family) override;
  void ReleaseSnapshot(const Snapshot *snapshot) override;

  // 事务操作 - 统一的事务管理
  std::unique_ptr<Transaction> BeginTransaction(
      const TxnOptions& options) override;
  std::unique_ptr<Transaction> BeginTransaction(
      ColumnFamily* column_family,
      const TxnOptions& options) override;

  // 列族操作
  Status CreateColumnFamily(const ColumnFamilyOptions &options,
                            const std::string &column_family_name,
                            ColumnFamily **handle) override;
  Status DropColumnFamily(ColumnFamily *column_family) override;
  Status DestroyColumnFamilyHandle(ColumnFamily *column_family) override;
  std::vector<std::string> ListColumnFamilies() const override;
  ColumnFamily *DefaultColumnFamily() const override;

  // 按名称查找列族句柄（内部使用：Open 带描述符重开时复用已存在的列族）
  ColumnFamily *GetColumnFamilyByName(const std::string &name) const;

  // 标记只读模式（OpenForReadOnly 内部使用）
  void SetReadOnly(bool read_only) { read_only_ = read_only; }

  // 数据库管理操作
  Status Flush(const FlushOptions &options) override;
  Status Flush(const FlushOptions &options,
               ColumnFamily *column_family) override;
  Status CompactRange(const CompactRangeOptions &options, const Slice *begin,
                      const Slice *end) override;
  Status CompactRange(const CompactRangeOptions &options,
                      ColumnFamily *column_family, const Slice *begin,
                      const Slice *end) override;
  Status PauseBackgroundWork() override;
  Status ContinueBackgroundWork() override;
  Status Close() override;

  // 数据库信息和统计
  const std::string &GetName() const override;
  const DBOptions &GetDBOptions() const override;
  bool GetProperty(const std::string &property, std::string *value) override;
  bool GetProperty(ColumnFamily *column_family, const std::string &property,
                   std::string *value) override;
  uint64_t GetApproximateSize(const Range *ranges, int n) override;
  uint64_t GetApproximateSize(ColumnFamily *column_family, const Range *ranges,
                              int n) override;
  uint64_t GetApproximateNumEntries() override;
  uint64_t GetApproximateNumEntries(ColumnFamily *column_family) override;

  // 后台任务管理
  BackgroundTaskManager *GetBackgroundTaskManager() const override;

  // 初始化方法
  Status Initialize();

private:
  // 组件管理 - 纯路由器架构：DB层只保留必要的路由组件
  struct Components {
    // 后台任务管理器（可选的全局任务协调）
    std::unique_ptr<BackgroundTaskManager> background_manager;
    
    // 列族管理器（纯路由管理）
    std::unique_ptr<class ColumnFamilyManager> cf_manager;
  };

  DBOptions options_;
  std::string dbname_;
  std::unique_ptr<Components> components_;

  // 列族管理已移至ColumnFamilyManager
  // 序列号和快照管理已移至各个组件中

  // 状态管理
  std::atomic<bool> opened_;
  std::atomic<bool> closed_;
  std::atomic<bool> background_work_paused_;

  // 只读打开（OpenForReadOnly）：拒绝一切写操作
  bool read_only_{false};

  // 后台任务管理
  mutable std::mutex background_tasks_mutex_;
  std::unordered_map<std::string, uint64_t>
      background_tasks_; // 任务名称 -> 任务ID

  mutable std::shared_mutex db_mutex_;

  // 内部方法 - 简化的DB协调层
  Status InitializeComponents();
  Status SetupColumnFamilies();
  Status RecoverFromCrash();
  Status RegisterUnifiedBackgroundTasks();
  
  // 恢复相关辅助方法
  Status ValidateDatabaseStructure();
  Status CheckRecoveryNeeded(bool* needs_recovery);
  Status InitializeRecoveryEnvironment();
  Status RecoverColumnFamilies();
  Status RecoverFromWAL(SequenceNumber* recovered_sequence);
  Status ValidateDataConsistency();
  Status CleanupRecoveryState();
  Status ValidateFileSystemState();
  Status CleanupOldWALFiles();
  Status CleanupOldBackups();
  Status SyncAllData();
  Status CreateCleanShutdownMarker();
  Status RemoveCleanShutdownMarker();
  Status RemoveRecoveryMarker();
  Status FinishLSMTreeRecovery(SequenceNumber recovered_sequence);

  // 后台任务管理
  Status RegisterBackgroundTask(
      const std::string &component_name,
      const std::function<Status()> &task_function,
      const std::string &task_name = "", bool periodic = false,
      std::chrono::milliseconds interval = std::chrono::milliseconds(0)) override;

  Status UnregisterBackgroundTask(const std::string &component_name,
                                  const std::string &task_name) override;

  // 工具方法
  ColumnFamily *ValidateColumnFamily(ColumnFamily *cf) const;

  // 禁止拷贝
  DBImpl(const DBImpl &) = delete;
  DBImpl &operator=(const DBImpl &) = delete;
};

} // namespace lrdb