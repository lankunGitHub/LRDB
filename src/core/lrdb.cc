// LRDB主头文件的配套实现：全局环境、便捷函数与配置预设

#include "lrdb/lrdb.h"
#include "lrdb/util/logging.h"

namespace lrdb {

// ============================================================================
// LRDBEnvironment 全局环境
// ============================================================================

bool LRDBEnvironment::initialized_ = false;
DBOptions LRDBEnvironment::global_db_options_;
std::unique_ptr<BackgroundTaskManager> LRDBEnvironment::global_background_manager_;
std::mutex LRDBEnvironment::initialization_mutex_;

Status LRDBEnvironment::Initialize() {
    std::lock_guard<std::mutex> lock(initialization_mutex_);
    if (initialized_) {
        return Status::OK();
    }

    global_background_manager_ = std::make_unique<BackgroundTaskManager>();
    Status s = global_background_manager_->Initialize();
    if (!s.ok()) {
        global_background_manager_.reset();
        return s;
    }

    initialized_ = true;
    return Status::OK();
}

Status LRDBEnvironment::Shutdown() {
    std::lock_guard<std::mutex> lock(initialization_mutex_);
    if (!initialized_) {
        return Status::OK();
    }

    if (global_background_manager_) {
        global_background_manager_->Shutdown();
        global_background_manager_.reset();
    }
    initialized_ = false;
    return Status::OK();
}

bool LRDBEnvironment::IsInitialized() {
    return initialized_;
}

const DBOptions& LRDBEnvironment::GetGlobalDBOptions() {
    return global_db_options_;
}

void LRDBEnvironment::SetGlobalDBOptions(const DBOptions& options) {
    global_db_options_ = options;
}

BackgroundTaskManager* LRDBEnvironment::GetGlobalBackgroundTaskManager() {
    return global_background_manager_.get();
}

// ============================================================================
// convenience 便捷操作函数
// ============================================================================

namespace convenience {

std::unique_ptr<DB> OpenDB(const std::string& db_path, const Options& options) {
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, db_path, &db);
    if (!s.ok()) {
        LOG_ERROR << "Failed to open database " << db_path << ": " << s.ToString();
        return nullptr;
    }
    return db;
}

std::unique_ptr<DB> OpenReadOnlyDB(const std::string& db_path, const Options& options) {
    std::unique_ptr<DB> db;
    Status s = DB::OpenForReadOnly(options, db_path, &db);
    if (!s.ok()) {
        LOG_ERROR << "Failed to open database read-only " << db_path << ": " << s.ToString();
        return nullptr;
    }
    return db;
}

Status DestroyDB(const std::string& db_path, const Options& options) {
    return DB::DestroyDB(db_path, options);
}

Status RepairDB(const std::string& db_path, const Options& options) {
    return DB::RepairDB(db_path, options);
}

std::vector<std::string> ListColumnFamilies(const std::string& db_path,
                                            const DBOptions& options) {
    std::vector<std::string> names;
    Status s = DB::ListColumnFamilies(options, db_path, &names);
    if (!s.ok()) {
        LOG_ERROR << "Failed to list column families of " << db_path << ": " << s.ToString();
        return {};
    }
    return names;
}

Status Put(DB* db, const std::string& key, const std::string& value) {
    if (!db) {
        return Status::InvalidArgument("db is null");
    }
    return db->Put(WriteOptions(), key, value);
}

Status Get(DB* db, const std::string& key, std::string* value) {
    if (!db || !value) {
        return Status::InvalidArgument("db or value is null");
    }
    return db->Get(ReadOptions(), key, value);
}

Status Delete(DB* db, const std::string& key) {
    if (!db) {
        return Status::InvalidArgument("db is null");
    }
    return db->Delete(WriteOptions(), key);
}

bool KeyExists(DB* db, const std::string& key) {
    std::string value;
    return db && db->Get(ReadOptions(), key, &value).ok();
}

Status BatchPut(DB* db, const std::vector<std::pair<std::string, std::string>>& kv_pairs) {
    if (!db) {
        return Status::InvalidArgument("db is null");
    }
    for (const auto& kv : kv_pairs) {
        Status s = db->Put(WriteOptions(), kv.first, kv.second);
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

std::vector<std::string> BatchGet(DB* db, const std::vector<std::string>& keys) {
    std::vector<std::string> values;
    if (!db) {
        return values;
    }
    values.reserve(keys.size());
    for (const auto& key : keys) {
        std::string value;
        Status s = db->Get(ReadOptions(), key, &value);
        values.push_back(s.ok() ? value : "");
    }
    return values;
}

std::vector<std::pair<std::string, std::string>> RangeScan(
    DB* db, const std::string& start_key, const std::string& end_key, size_t limit) {
    std::vector<std::pair<std::string, std::string>> results;
    if (!db) {
        return results;
    }

    std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
    it->Seek(start_key);
    while (it->Valid()) {
        std::string key = it->key().ToString();
        if (!end_key.empty() && key >= end_key) {
            break;
        }
        results.emplace_back(key, it->value().ToString());
        if (limit > 0 && results.size() >= limit) {
            break;
        }
        it->Next();
    }

    if (!it->status().ok()) {
        LOG_ERROR << "RangeScan iterator error: " << it->status().ToString();
        return {};
    }
    return results;
}

Status ExecuteTransaction(DB* db, std::function<Status(Transaction*)> txn_func) {
    if (!db || !txn_func) {
        return Status::InvalidArgument("db or txn_func is null");
    }

    auto txn = db->BeginTransaction();
    if (!txn) {
        return Status::IOError("Failed to begin transaction");
    }

    Status s = txn_func(txn.get());
    if (!s.ok()) {
        txn->Rollback();
        return s;
    }
    return txn->Commit();
}

}  // namespace convenience

// ============================================================================
// presets 常用配置预设
// ============================================================================

namespace presets {

Options DefaultOptions() {
    return Options();
}

Options HighWriteThroughputOptions() {
    Options options;
    options.write_buffer_size = 128 << 20;          // 128MB写缓冲
    options.max_write_buffer_number = 4;
    options.max_background_flushes = 4;
    options.level0_file_num_compaction_trigger = 8;
    options.level0_slowdown_writes_trigger = 24;
    options.level0_stop_writes_trigger = 48;
    return options;
}

Options HighReadThroughputOptions() {
    Options options;
    options.block_size = 16 * 1024;                  // 更大的数据块
    options.filter_policy = nullptr;                 // 使用默认过滤策略
    options.max_open_files = -1;                     // 打开所有文件
    return options;
}

Options LowLatencyOptions() {
    Options options;
    options.write_buffer_size = 4 << 20;             // 4MB小写缓冲，加快刷盘
    options.max_write_buffer_number = 2;
    options.bytes_per_sync = 1 << 20;                // 每1MB同步一次
    options.wal_bytes_per_sync = 512 << 10;
    options.max_background_jobs = 4;
    return options;
}

Options SpaceOptimizedOptions() {
    Options options;
    options.write_buffer_size = 16 << 20;            // 16MB写缓冲
    options.target_file_size_base = 16 << 20;
    options.max_bytes_for_level_base = 128 << 20;
    options.compression = CompressionType::kSnappyCompression;
    options.level_compaction_dynamic_level_bytes = true;
    return options;
}

Options LargeDatasetOptions() {
    Options options;
    options.write_buffer_size = 256 << 20;
    options.max_write_buffer_number = 4;
    options.max_bytes_for_level_base = 1ULL << 30;   // L1层1GB
    options.level0_file_num_compaction_trigger = 8;
    options.filter_policy = nullptr;
    options.max_open_files = -1;
    return options;
}

Options MemoryFirstOptions() {
    Options options;
    options.write_buffer_size = 512 << 20;           // 512MB写缓冲
    options.max_write_buffer_number = 4;
    options.max_background_flushes = 2;
    options.level0_slowdown_writes_trigger = 32;
    options.level0_stop_writes_trigger = 64;
    return options;
}

Options DurabilityFirstOptions() {
    Options options;
    options.write_buffer_size = 8 << 20;
    options.max_write_buffer_number = 2;
    options.bytes_per_sync = 1 << 20;
    options.wal_bytes_per_sync = 1 << 20;
    options.wal_size_limit_mb = 64;
    options.max_total_wal_size = 128 << 20;
    return options;
}

}  // namespace presets

}  // namespace lrdb
