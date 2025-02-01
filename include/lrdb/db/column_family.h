// 列族(Column Family)支持

#pragma once

#include "lrdb/concurrency/transaction.h"
#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/db/options.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace lrdb {

// 前向声明
class LSMTree;
class Comparator;
class Iterator;
class Snapshot;
class TxManager;
class WALManager;
class SequenceGenerator;
class Transaction;
class ManifestManager;
struct ColumnFamilyDescriptor;

class DB;

// 列族句柄
class ColumnFamily {
public:
  virtual ~ColumnFamily() = default;

  // 获取列族ID
  virtual uint32_t GetID() const = 0;

  // 获取列族名称
  virtual const std::string &GetName() const = 0;

  // 获取列族选项
  virtual const ColumnFamilyOptions &GetOptions() const = 0;

  // 获取比较器
  virtual const Comparator *GetComparator() const = 0;

  // 检查是否为默认列族
  virtual bool IsDefault() const = 0;

  // 获取列族状态
  virtual bool IsActive() const = 0;



  // 更新选项
  virtual Status UpdateOptions(const ColumnFamilyOptions &new_options) = 0;
  
  // ========== 列族级别的数据操作接口 ==========
  
  // 基本操作 - 每个列族独立处理
  virtual Status Put(const Slice& key, const Slice& value) = 0;
  virtual Status Delete(const Slice& key) = 0;
  virtual Status Get(const Slice& key, std::string* value) = 0;
  virtual Status Get(const Slice& key, uint64_t sequence, std::string* value) = 0;
  
  // 批量操作
  virtual Status MultiGet(const std::vector<Slice>& keys, std::vector<std::string>* values) = 0;
  
  // 事务操作 - 列族独立事务管理
  virtual std::unique_ptr<Transaction> BeginTransaction(
      const TxnOptions& options = TxnOptions{}) = 0;
  
  // 迭代器支持
  virtual Iterator* NewIterator() = 0;
  virtual Iterator* NewIterator(uint64_t sequence) = 0;
  
  // 刷盘和压缩 - 列族级别
  virtual Status Flush() = 0;
  virtual Status CompactRange(const Slice* begin, const Slice* end) = 0;
  
  // 内部组件访问
  virtual LSMTree* GetLSMTree() const = 0;
  virtual TxManager* GetTxManager() const = 0;
  
  // 验证列族一致性
  virtual Status ValidateConsistency() const = 0;
  
  // 关闭列族
  virtual Status Shutdown() = 0;
};

// 默认列族实现
class DefaultColumnFamily : public ColumnFamily {
public:
  DefaultColumnFamily(uint32_t id, const std::string &name,
                      const ColumnFamilyOptions &options,
                      const std::string& db_path);
  ~DefaultColumnFamily() override;

  // 基本属性
  uint32_t GetID() const override;
  const std::string &GetName() const override;
  const ColumnFamilyOptions &GetOptions() const override;
  const Comparator *GetComparator() const override;
  bool IsDefault() const override;
  bool IsActive() const override;



  // 选项更新
  Status UpdateOptions(const ColumnFamilyOptions &new_options) override;
  
  // ========== 列族级别的数据操作实现 ==========
  
  // 基本操作实现
  Status Put(const Slice& key, const Slice& value) override;
  Status Delete(const Slice& key) override;
  Status Get(const Slice& key, std::string* value) override;
  Status Get(const Slice& key, uint64_t sequence, std::string* value) override;
  
  // 批量操作实现
  Status MultiGet(const std::vector<Slice>& keys, std::vector<std::string>* values) override;
  
  // 事务操作实现 - 列族独立事务管理
  std::unique_ptr<Transaction> BeginTransaction(
      const TxnOptions& options = TxnOptions{}) override;
  
  // 迭代器支持实现
  Iterator* NewIterator() override;
  Iterator* NewIterator(uint64_t sequence) override;
  
  // 刷盘和压缩实现
  Status Flush() override;
  Status CompactRange(const Slice* begin, const Slice* end) override;
  
  // 内部组件访问实现
  LSMTree* GetLSMTree() const override;
  TxManager* GetTxManager() const override;
  
  // 验证列族一致性
  Status ValidateConsistency() const override;
  Status Shutdown() override;

  // 生命周期管理
  Status Initialize();

  // 引用计数
  void Ref();
  void Unref();
  int RefCount() const;

private:
  uint32_t id_;
  std::string name_;
  ColumnFamilyOptions options_;
  
  // 每个列族完全独立的组件 - 无任何共享
  std::unique_ptr<LSMTree> lsm_tree_;
  std::unique_ptr<TxManager> tx_manager_;
  std::unique_ptr<class WALManager> wal_manager_;           // 独立WAL
  std::unique_ptr<class SequenceGenerator> sequence_generator_; // 独立序列号
  
  // 列族独立的数据目录
  std::string cf_data_path_;
  
  std::atomic<bool> active_;
  std::atomic<int> ref_count_;

  mutable std::mutex mutex_;

  // 禁止拷贝
  DefaultColumnFamily(const DefaultColumnFamily &) = delete;
  DefaultColumnFamily &operator=(const DefaultColumnFamily &) = delete;
};

// 列族管理器
class ColumnFamilyManager {
public:
  ColumnFamilyManager(const std::string &db_path, const DBOptions &db_options);
  ~ColumnFamilyManager();

  // 初始化和关闭
  Status Initialize();
  Status Shutdown();

  // 列族操作
  Status CreateColumnFamily(const ColumnFamilyOptions &options,
                            const std::string &name, ColumnFamily **handle);
  Status DropColumnFamily(ColumnFamily *column_family);
  Status DropColumnFamily(const std::string &name);

  // 列族查询
  ColumnFamily *GetColumnFamily(uint32_t id) const;
  ColumnFamily *GetColumnFamily(const std::string &name) const;
  ColumnFamily *GetDefaultColumnFamily() const;

  // 列举列族
  std::vector<std::string> ListColumnFamilies() const;
  std::vector<ColumnFamily *> GetAllColumnFamilies() const;

  // 列族数量
  size_t GetColumnFamilyCount() const;

  // 列族选项管理
  Status UpdateColumnFamilyOptions(ColumnFamily *column_family,
                                   const ColumnFamilyOptions &new_options);

  // 持久化列族信息
  Status PersistColumnFamilyInfo();
  Status LoadColumnFamilyInfo();
  
  // 恢复相关
  Status RecoverColumnFamilies();
  Status ValidateAllColumnFamilies() const;
  
  // 创建默认列族
  Status CreateDefaultColumnFamily();

  // 列族统计
  struct ManagerStats {
    size_t total_column_families;
    size_t active_column_families;
    uint64_t total_entries;
    uint64_t total_size_bytes;
    uint64_t total_write_count;
    uint64_t total_read_count;
  };
  ManagerStats GetManagerStats() const;

  // 内部方法 - 供DB使用
  Status ValidateColumnFamily(ColumnFamily *column_family) const;
  uint32_t AllocateColumnFamilyID();

private:
  std::string db_path_;
  DBOptions db_options_;

  std::atomic<uint32_t> next_column_family_id_;
  ColumnFamily *default_column_family_; // 改为原始指针，避免循环引用
  std::unordered_map<uint32_t, std::unique_ptr<ColumnFamily>>
      column_families_by_id_;
  std::unordered_map<std::string, ColumnFamily *> column_families_by_name_;
  
  // 加载的列族描述符（用于恢复）
  std::vector<ColumnFamilyDescriptor> loaded_column_families_;
  
  // MANIFEST管理器
  std::unique_ptr<ManifestManager> manifest_manager_;

  mutable std::shared_mutex column_families_mutex_;
  std::atomic<bool> initialized_;

  // 内部方法（移除重复声明）
  Status LoadExistingColumnFamilies();
  Status SaveColumnFamilyManifest();
  Status LoadColumnFamilyManifest();
  std::string GetColumnFamilyManifestPath() const;

  // 禁止拷贝
  ColumnFamilyManager(const ColumnFamilyManager &) = delete;
  ColumnFamilyManager &operator=(const ColumnFamilyManager &) = delete;
};

// 列族描述符集合
class ColumnFamilyDescriptorCollection {
public:
  ColumnFamilyDescriptorCollection();
  ~ColumnFamilyDescriptorCollection();

  // 添加列族描述符
  void Add(const ColumnFamilyDescriptor &descriptor);
  void Add(const std::string &name, const ColumnFamilyOptions &options);

  // 移除列族描述符
  bool Remove(const std::string &name);

  // 查询列族描述符
  const ColumnFamilyDescriptor *Get(const std::string &name) const;
  bool Contains(const std::string &name) const;

  // 获取所有描述符
  std::vector<ColumnFamilyDescriptor> GetAll() const;
  std::vector<std::string> GetNames() const;

  // 清空
  void Clear();

  // 大小
  size_t Size() const;
  bool Empty() const;

  // 验证描述符
  Status Validate() const;

  // 序列化和反序列化
  std::string Serialize() const;
  Status Deserialize(const std::string &data);

private:
  std::unordered_map<std::string, ColumnFamilyDescriptor> descriptors_;
  mutable std::mutex mutex_;

  // 禁止拷贝
  ColumnFamilyDescriptorCollection(const ColumnFamilyDescriptorCollection &) =
      delete;
  ColumnFamilyDescriptorCollection &
  operator=(const ColumnFamilyDescriptorCollection &) = delete;
};

// 列族内存表管理
class ColumnFamilyMemTables {
public:
  explicit ColumnFamilyMemTables(ColumnFamilyManager *cf_manager);
  ~ColumnFamilyMemTables();

  // 获取列族的MemTable
  class MemTable *GetMemTable(uint32_t cf_id) const;

  // 检查列族是否存在
  bool Seek(uint32_t cf_id);

  // 获取当前列族
  ColumnFamily *GetColumnFamily() const;

  // 获取当前MemTable
  class MemTable *GetCurrentMemTable() const;

  // 通知丢弃列族
  void NotifyColumnFamilyDrop();

private:
  ColumnFamilyManager *cf_manager_;
  ColumnFamily *current_cf_;
  mutable std::mutex mutex_;

  // 禁止拷贝
  ColumnFamilyMemTables(const ColumnFamilyMemTables &) = delete;
  ColumnFamilyMemTables &operator=(const ColumnFamilyMemTables &) = delete;
};

// 列族迭代器管理
class ColumnFamilyIteratorManager {
public:
  explicit ColumnFamilyIteratorManager(ColumnFamilyManager *cf_manager);
  ~ColumnFamilyIteratorManager();

  // 创建单列族迭代器
  Iterator *CreateIterator(const struct ReadOptions &options,
                           ColumnFamily *column_family);

  // 创建多列族迭代器
  Status CreateIterators(const struct ReadOptions &options,
                         const std::vector<ColumnFamily *> &column_families,
                         std::vector<Iterator *> *iterators);

  // 创建所有列族迭代器
  Status CreateIteratorsForAllColumnFamilies(
      const struct ReadOptions &options, std::vector<Iterator *> *iterators,
      std::vector<ColumnFamily *> *column_families);

  // 销毁迭代器
  void DestroyIterator(Iterator *iterator);
  void DestroyIterators(const std::vector<Iterator *> &iterators);

private:
  ColumnFamilyManager *cf_manager_;

  // 禁止拷贝
  ColumnFamilyIteratorManager(const ColumnFamilyIteratorManager &) = delete;
  ColumnFamilyIteratorManager &
  operator=(const ColumnFamilyIteratorManager &) = delete;
};

// 列族工具函数
namespace column_family_util {

// 创建列族管理器
std::unique_ptr<ColumnFamilyManager>
CreateColumnFamilyManager(const std::string &db_path,
                          const DBOptions &db_options);

// 创建默认列族选项
ColumnFamilyOptions CreateDefaultColumnFamilyOptions();

// 验证列族名称
Status ValidateColumnFamilyName(const std::string &name);

// 列族名称常量
extern const std::string kDefaultColumnFamilyName;
extern const uint32_t kDefaultColumnFamilyId;

// 列族选项比较
bool CompareColumnFamilyOptions(const ColumnFamilyOptions &lhs,
                                const ColumnFamilyOptions &rhs);

// 列族选项序列化
std::string SerializeColumnFamilyOptions(const ColumnFamilyOptions &options);
Status DeserializeColumnFamilyOptions(const std::string &serialized,
                                      ColumnFamilyOptions *options);

// 列族描述符工具
std::vector<ColumnFamilyDescriptor> CreateDescriptorsFromNames(
    const std::vector<std::string> &names,
    const ColumnFamilyOptions &default_options = ColumnFamilyOptions());

// 列族统计聚合
ColumnFamilyManager::ManagerStats
AggregateColumnFamilyStats(const std::vector<ColumnFamily *> &column_families);

// 列族配置建议
struct ColumnFamilyRecommendation {
  std::string parameter_name;
  std::string current_value;
  std::string recommended_value;
  std::string reason;
};

std::vector<ColumnFamilyRecommendation>
AnalyzeColumnFamilyConfiguration(const ColumnFamily *cf,
                                 uint64_t workload_write_rate = 0);

} // namespace column_family_util

} // namespace lrdb