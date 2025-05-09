// LRDB重构版本主头文件

#pragma once

// 版本信息
#define LRDB_VERSION_MAJOR 1
#define LRDB_VERSION_MINOR 0
#define LRDB_VERSION_PATCH 0

// 核心数据结构
#include "lrdb/core/status.h"
#include "lrdb/core/slice.h"
#include "lrdb/core/coding.h"

// 数据库接口
#include "lrdb/db/db.h"
#include "lrdb/db/options.h"
#include "lrdb/db/iterator.h"

#include "lrdb/concurrency/snapshot.h"
#include "lrdb/db/column_family.h"

// 存储引擎
#include "lrdb/storage/memtable.h"
#include "lrdb/storage/sstable.h"
#include "lrdb/storage/lsm_tree.h"

// 并发控制
#include "lrdb/concurrency/mvcc.h"
#include "lrdb/concurrency/transaction.h"

// WAL和恢复
#include "lrdb/wal/wal.h"

// 过滤器
#include "lrdb/util/bloom_filter.h"

// 后台任务管理
#include "lrdb/background/background_manager.h"

// 工具类
#include "lrdb/util/comparator.h"

namespace lrdb {

// 版本信息
struct Version {
    static constexpr int kMajor = LRDB_VERSION_MAJOR;
    static constexpr int kMinor = LRDB_VERSION_MINOR;
    static constexpr int kPatch = LRDB_VERSION_PATCH;
    
    static std::string GetVersionString() {
        return std::to_string(kMajor) + "." + 
               std::to_string(kMinor) + "." + 
               std::to_string(kPatch);
    }
    
    static std::string GetFullVersionString() {
        return "LRDB " + GetVersionString() + " (C++17, Linux)";
    }
    
    static int GetVersionNumber() {
        return (kMajor << 16) | (kMinor << 8) | kPatch;
    }
};

// 全局初始化和清理
class LRDBEnvironment {
public:
    // 全局初始化
    static Status Initialize();
    
    // 全局清理
    static Status Shutdown();
    
    // 检查是否已初始化
    static bool IsInitialized();
    
    // 获取全局配置
    static const DBOptions& GetGlobalDBOptions();
    static void SetGlobalDBOptions(const DBOptions& options);
    
    // 获取全局内存分配器（待实现）
    // static MemoryAllocator* GetGlobalMemoryAllocator();
    // static Status SetGlobalMemoryAllocator(std::unique_ptr<MemoryAllocator> allocator);
    
    // 获取全局后台任务管理器
    static BackgroundTaskManager* GetGlobalBackgroundTaskManager();

private:
    static bool initialized_;
    static DBOptions global_db_options_;
    // static std::unique_ptr<MemoryAllocator> global_memory_allocator_;  // 待实现
    static std::unique_ptr<BackgroundTaskManager> global_background_manager_;
    static std::mutex initialization_mutex_;
};

// 便捷的数据库操作函数
namespace convenience {

// 打开数据库
std::unique_ptr<DB> OpenDB(const std::string& db_path, 
                          const Options& options = Options());

// 打开只读数据库
std::unique_ptr<DB> OpenReadOnlyDB(const std::string& db_path,
                                  const Options& options = Options());

// 销毁数据库
Status DestroyDB(const std::string& db_path, const Options& options = Options());

// 修复数据库
Status RepairDB(const std::string& db_path, const Options& options = Options());

// 列出数据库中的列族
std::vector<std::string> ListColumnFamilies(const std::string& db_path, 
                                           const DBOptions& options = DBOptions());

// 简单的键值操作
Status Put(DB* db, const std::string& key, const std::string& value);
Status Get(DB* db, const std::string& key, std::string* value);
Status Delete(DB* db, const std::string& key);
bool KeyExists(DB* db, const std::string& key);

// 批量操作
Status BatchPut(DB* db, const std::vector<std::pair<std::string, std::string>>& kv_pairs);
std::vector<std::string> BatchGet(DB* db, const std::vector<std::string>& keys);

// 范围扫描
std::vector<std::pair<std::string, std::string>> RangeScan(
    DB* db, const std::string& start_key, const std::string& end_key, size_t limit = 0);

// 事务便捷函数
Status ExecuteTransaction(DB* db, std::function<Status(Transaction*)> txn_func);

}  // namespace convenience

// 常用的配置预设
namespace presets {

// 默认配置
Options DefaultOptions();

// 高性能写入配置
Options HighWriteThroughputOptions();

// 高性能读取配置  
Options HighReadThroughputOptions();

// 低延迟配置
Options LowLatencyOptions();

// 空间优化配置
Options SpaceOptimizedOptions();

// 大数据集配置
Options LargeDatasetOptions();

// 内存优先配置
Options MemoryFirstOptions();

// 持久化优先配置
Options DurabilityFirstOptions();

}  // namespace presets

// 删除了性能监控和调优命名空间 - 非核心功能

}  // namespace lrdb

// 便捷宏定义
#define LRDB_VERSION_STRING lrdb::Version::GetVersionString()
#define LRDB_FULL_VERSION_STRING lrdb::Version::GetFullVersionString()

#define LRDB_RETURN_IF_ERROR(expr) \
  do { \
    auto status = (expr); \
    if (!status.ok()) { \
      return status; \
    } \
  } while (0)

#define LRDB_CHECK_STATUS(expr) \
  do { \
    auto status = (expr); \
    if (!status.ok()) { \
      throw std::runtime_error("LRDB Error: " + status.ToString()); \
    } \
  } while (0)
