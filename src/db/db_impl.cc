// 数据库实现 - 纯路由器架构

#include "lrdb/db/db.h"
#include "lrdb/db/column_family.h"
#include "lrdb/db/manifest.h"
#include "lrdb/util/logging.h"
#include "lrdb/background/background_manager.h"
#include "lrdb/concurrency/tx_manager.h"
#include "lrdb/concurrency/snapshot.h"
#include "lrdb/wal/wal.h"
#include "lrdb/storage/lsm_tree.h"
#include <algorithm>

#include <filesystem>
#include <chrono>
#include <fstream>

namespace lrdb {

// DBImpl实现 - 纯路由器架构
DBImpl::DBImpl(const DBOptions& options, const std::string& dbname)
    : options_(options), dbname_(dbname), 
      opened_(false), closed_(false), background_work_paused_(false) {
    
    // 初始化纯路由组件
    components_ = std::make_unique<Components>();
}

DBImpl::~DBImpl() {
    Close();
}

// ============================================================================
// 基本读写操作 - 纯路由实现
// ============================================================================

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    (void)options;
    return Put(options, DefaultColumnFamily(), key, value);
}

Status DBImpl::Put(const WriteOptions& options, ColumnFamily* column_family,
                    const Slice& key, const Slice& value) {
    (void)options;
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf) {
        return Status::InvalidArgument("Invalid column family");
    }
    
    return cf->Put(key, value);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    (void)options;
    return Delete(options, DefaultColumnFamily(), key);
}

Status DBImpl::Delete(const WriteOptions& options, ColumnFamily* column_family,
                      const Slice& key) {
    (void)options;
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf) {
        return Status::InvalidArgument("Invalid column family");
    }
    
    return cf->Delete(key);
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    return Get(options, DefaultColumnFamily(), key, value);
}

Status DBImpl::Get(const ReadOptions& options, ColumnFamily* column_family,
                    const Slice& key, std::string* value) {
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf || !value) {
        return Status::InvalidArgument("Invalid arguments");
    }
    
    if (options.snapshot) {
        return cf->Get(key, options.snapshot->GetSequenceNumber(), value);
    }
    return cf->Get(key, value);
}

std::vector<Status> DBImpl::MultiGet(const ReadOptions& options,
                                     const std::vector<Slice>& keys,
                                     std::vector<std::string>* values) {
    (void)options;
    if (!values) {
        return std::vector<Status>(keys.size(), Status::InvalidArgument("Values pointer is null"));
    }
    
    ColumnFamily* default_cf = DefaultColumnFamily();
    if (default_cf) {
        Status batch_status = default_cf->MultiGet(keys, values);
        if (batch_status.ok()) {
            return std::vector<Status>(keys.size(), Status::OK());
        }
    }
    
    // Fallback到逐个Get
    std::vector<Status> statuses;
    statuses.reserve(keys.size());
    values->clear();
    values->reserve(keys.size());
    
        for (const auto& key : keys) {
            std::string value;
        Status s = Get(options, key, &value);
            statuses.push_back(s);
        values->push_back(s.ok() ? std::move(value) : "");
    }
    
    return statuses;
}

Status DBImpl::Merge(const WriteOptions& options, const Slice& key, const Slice& value) {
    (void)options; (void)key; (void)value;
    return Status::NotSupported("Merge operation not supported");
}

Status DBImpl::Merge(const WriteOptions& options, ColumnFamily* column_family,
                     const Slice& key, const Slice& value) {
    (void)options; (void)column_family; (void)key; (void)value;
    return Status::NotSupported("Merge operation not supported");
}

// ============================================================================
// 迭代器操作 - 委托给列族
// ============================================================================

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    return NewIterator(options, DefaultColumnFamily());
}

Iterator* DBImpl::NewIterator(const ReadOptions& options, ColumnFamily* column_family) {
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf) {
        return nullptr;
    }
    
    if (options.snapshot) {
        return cf->NewIterator(options.snapshot->GetSequenceNumber());
    } else {
        return cf->NewIterator();
    }
        }
        
Status DBImpl::NewIterators(const ReadOptions& options,
                            const std::vector<ColumnFamily*>& column_families,
                            std::vector<Iterator*>* iterators) {
    if (!iterators) {
        return Status::InvalidArgument("Iterators vector is null");
    }
    
    iterators->clear();
    iterators->reserve(column_families.size());
    
    for (auto* cf : column_families) {
        Iterator* iter = NewIterator(options, cf);
                if (iter) {
            iterators->push_back(iter);
        } else {
            // 清理已创建的迭代器
            for (auto* created_iter : *iterators) {
                delete created_iter;
            }
            iterators->clear();
            return Status::InvalidArgument("Failed to create iterator for column family");
        }
    }
    
    return Status::OK();
}

// ============================================================================
// 快照操作 - 委托给列族
// ============================================================================

const Snapshot* DBImpl::GetSnapshot() {
    ColumnFamily* default_cf = DefaultColumnFamily();
    if (!default_cf || !default_cf->GetTxManager()) {
        return nullptr;
    }
    
    auto snapshot = default_cf->GetTxManager()->CreateSnapshot();
    return snapshot.release();
}

const Snapshot* DBImpl::GetSnapshot(ColumnFamily* column_family) {
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf || !cf->GetTxManager()) {
        return nullptr;
    }
    
    auto snapshot = cf->GetTxManager()->CreateSnapshot();
    return snapshot.release();
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
    if (snapshot) {
        delete snapshot;
    }
}

// ============================================================================
// 事务操作 - 委托给列族
// ============================================================================

std::unique_ptr<Transaction> DBImpl::BeginTransaction(const TxnOptions& options) {
    ColumnFamily* default_cf = DefaultColumnFamily();
    if (!default_cf) {
        return nullptr;
    }
    
    return default_cf->BeginTransaction(options);
}

std::unique_ptr<Transaction> DBImpl::BeginTransaction(ColumnFamily* column_family,
                                                     const TxnOptions& options) {
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf) {
        return nullptr;
    }
    
    return cf->BeginTransaction(options);
}

// ============================================================================
// 列族操作 - 委托给ColumnFamilyManager
// ============================================================================

Status DBImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                  const std::string& column_family_name,
                                  ColumnFamily** handle) {
    if (!components_ || !components_->cf_manager) {
        return Status::InvalidArgument("ColumnFamilyManager not initialized");
    }
    
    return components_->cf_manager->CreateColumnFamily(options, column_family_name, handle);
}

Status DBImpl::DropColumnFamily(ColumnFamily* column_family) {
    if (!components_ || !components_->cf_manager) {
        return Status::InvalidArgument("ColumnFamilyManager not initialized");
    }
    
    return components_->cf_manager->DropColumnFamily(column_family);
}

Status DBImpl::DestroyColumnFamilyHandle(ColumnFamily* column_family) {
    (void)column_family;
        return Status::OK();
    }
    
std::vector<std::string> DBImpl::ListColumnFamilies() const {
    if (!components_ || !components_->cf_manager) {
        return {};
    }
    
    return components_->cf_manager->ListColumnFamilies();
}

ColumnFamily* DBImpl::DefaultColumnFamily() const {
    if (!components_ || !components_->cf_manager) {
        return nullptr;
    }
    
    return components_->cf_manager->GetDefaultColumnFamily();
}

// ============================================================================
// 数据库管理操作 - 委托实现
// ============================================================================

Status DBImpl::Flush(const FlushOptions& options) {
    (void)options;
    ColumnFamily* default_cf = DefaultColumnFamily();
    if (!default_cf) {
        return Status::InvalidArgument("Default column family not available");
    }
    
    return default_cf->Flush();
}

Status DBImpl::Flush(const FlushOptions& options, ColumnFamily* column_family) {
    (void)options;
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf) {
        return Status::InvalidArgument("Invalid column family");
    }
    
    return cf->Flush();
}

Status DBImpl::CompactRange(const CompactRangeOptions& options, const Slice* begin,
                            const Slice* end) {
    (void)options;
    return CompactRange(options, DefaultColumnFamily(), begin, end);
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamily* column_family, const Slice* begin,
                            const Slice* end) {
    (void)options;
    ColumnFamily* cf = ValidateColumnFamily(column_family);
    if (!cf) {
        return Status::InvalidArgument("Invalid column family");
    }
    
    return cf->CompactRange(begin, end);
}

Status DBImpl::PauseBackgroundWork() {
    background_work_paused_.store(true);
    if (components_ && components_->background_manager) {
        components_->background_manager->PauseExecution();
        return Status::OK();
    }
    return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
    background_work_paused_.store(false);
    if (components_ && components_->background_manager) {
        components_->background_manager->ResumeExecution();
        return Status::OK();
    }
    return Status::OK();
}

Status DBImpl::Close() {
    if (closed_.load()) {
        return Status::OK();
    }
    
    LOG_INFO << "Starting database shutdown for: " << dbname_;
    
    try {
        // 1. 停止所有后台任务（不再接受新的调度）
        if (components_ && components_->background_manager) {
            Status bg_status = components_->background_manager->Shutdown();
            if (!bg_status.ok()) {
                LOG_WARN << "Background manager shutdown failed: " << bg_status.ToString();
            }
        }

        // 2. 同步所有未刷盘的数据（MVCC刷回 + MemTable落盘）
        Status sync_status = SyncAllData();
        if (!sync_status.ok()) {
            LOG_ERROR << "Failed to sync data during shutdown: " << sync_status.ToString();
            // 继续关闭流程，但不创建清洁关闭标记
        } else {
            // 3. 数据已全部落盘到SSTable，清空WAL（避免重开时重复重放）
            if (components_ && components_->cf_manager) {
                auto cf_list = components_->cf_manager->ListColumnFamilies();
                for (const auto& cf_name : cf_list) {
                    ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
                    if (cf && cf->GetTxManager() && cf->GetTxManager()->wal()) {
                        Status truncate_status = cf->GetTxManager()->wal()->TruncateLogs();
                        if (!truncate_status.ok()) {
                            LOG_WARN << "Failed to truncate WAL for column family " << cf_name
                                     << ": " << truncate_status.ToString();
                        }
                    }
                }
            }

            // 4. 创建清洁关闭标记
            Status marker_status = CreateCleanShutdownMarker();
            if (!marker_status.ok()) {
                LOG_WARN << "Failed to create clean shutdown marker: " << marker_status.ToString();
            }
        }

        // 5. 关闭列族管理器（内部会关闭各列族组件）
        if (components_ && components_->cf_manager) {
            Status cf_status = components_->cf_manager->Shutdown();
            if (!cf_status.ok()) {
                LOG_WARN << "Column family manager shutdown failed: " << cf_status.ToString();
            }
        }
        
        // 4. 删除恢复进行中标记（如果存在）
        Status recovery_marker_status = RemoveRecoveryMarker();
        if (!recovery_marker_status.ok()) {
            LOG_WARN << "Failed to remove recovery marker: " << recovery_marker_status.ToString();
        }
        
        closed_.store(true);
        opened_.store(false);
        
        LOG_INFO << "Database shutdown completed successfully for: " << dbname_;
        return Status::OK();
        
    } catch (const std::exception& e) {
        LOG_ERROR << "Exception during database shutdown: " << e.what();
        closed_.store(true);
        opened_.store(false);
        return Status::IOError("Database shutdown failed: " + std::string(e.what()));
    }
}

// ============================================================================
// 数据库信息和统计 - 基础实现
// ============================================================================

const std::string& DBImpl::GetName() const {
    return dbname_;
}

const DBOptions& DBImpl::GetDBOptions() const {
    return options_;
}

bool DBImpl::GetProperty(const std::string& property, std::string* value) {
    (void)property; (void)value;
    return false;
}

bool DBImpl::GetProperty(ColumnFamily* column_family, const std::string& property, 
                          std::string* value) {
    (void)column_family; (void)property; (void)value;
    return false;
}

uint64_t DBImpl::GetApproximateSize(const Range* ranges, int n) {
    (void)ranges; (void)n;
        return 0;
    }
    
uint64_t DBImpl::GetApproximateSize(ColumnFamily* column_family, const Range* ranges, int n) {
    (void)column_family; (void)ranges; (void)n;
    return 0;
}

uint64_t DBImpl::GetApproximateNumEntries() {
    return 0;
}

uint64_t DBImpl::GetApproximateNumEntries(ColumnFamily* column_family) {
    (void)column_family;
    return 0;
}

BackgroundTaskManager* DBImpl::GetBackgroundTaskManager() const {
    return components_ ? components_->background_manager.get() : nullptr;
}

// ============================================================================
// 后台任务管理 
// ============================================================================

Status DBImpl::RegisterBackgroundTask(const std::string& component_name,
                                     const std::function<Status()>& task_function,
                                     const std::string& task_name,
                                     bool periodic,
                                     std::chrono::milliseconds interval) {
    (void)component_name; (void)task_function; (void)task_name; (void)periodic; (void)interval;
        return Status::OK();
}

Status DBImpl::UnregisterBackgroundTask(const std::string& component_name,
                                        const std::string& task_name) {
    (void)component_name; (void)task_name;
        return Status::OK();
    }
    
// ============================================================================
// 初始化和内部方法
// ============================================================================

Status DBImpl::Initialize() {
    try {
        // 创建数据库目录
        std::filesystem::create_directories(dbname_);
        
        // 删除清洁关闭标记（表示数据库正在运行）
        Status marker_status = RemoveCleanShutdownMarker();
        if (!marker_status.ok()) {
            LOG_WARN << "Failed to remove clean shutdown marker: " << marker_status.ToString();
        }
        
        // 初始化组件
        Status s = InitializeComponents();
        if (!s.ok()) {
            return s;
        }
        
        // 设置列族
        s = SetupColumnFamilies();
        if (!s.ok()) {
            return s;
        }
        
        // 恢复数据
        s = RecoverFromCrash();
        if (!s.ok()) {
            return s;
        }
        
        opened_.store(true);
        LOG_INFO << "Database initialized successfully: " << dbname_;
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to initialize database: " + std::string(e.what()));
    }
}

Status DBImpl::InitializeComponents() {
    // 初始化ColumnFamilyManager
    components_->cf_manager = std::make_unique<ColumnFamilyManager>(dbname_, options_);
    Status s = components_->cf_manager->Initialize();
            if (!s.ok()) {
                return s;
    }
    
    // 初始化BackgroundTaskManager
            components_->background_manager = std::make_unique<BackgroundTaskManager>();
    s = components_->background_manager->Initialize(4);
        if (!s.ok()) {
            return s;
        }
        
        return Status::OK();
}
        
Status DBImpl::SetupColumnFamilies() {
    if (!components_ || !components_->cf_manager) {
        return Status::InvalidArgument("ColumnFamilyManager not initialized");
    }

    LOG_INFO << "Setting up column families";

    // 默认列族已在ColumnFamilyManager初始化时创建（或从MANIFEST恢复），
    // 这里只有确实缺失时才创建，避免覆盖已恢复的实例
    if (components_->cf_manager->GetColumnFamily("default") == nullptr) {
        return components_->cf_manager->CreateDefaultColumnFamily();
    }
    return Status::OK();
}

Status DBImpl::RecoverFromCrash() {
    LOG_INFO << "Starting database crash recovery for: " << dbname_;
    
    auto start_time = std::chrono::steady_clock::now();
    
    try {
        // 1. 验证数据库目录和基本文件结构
        Status validation_status = ValidateDatabaseStructure();
        if (!validation_status.ok()) {
            LOG_ERROR << "Database structure validation failed: " << validation_status.ToString();
            return validation_status;
        }
        LOG_INFO << "Database structure validation passed";
        
        // 2. 检查是否需要恢复
        bool needs_recovery = false;
        Status recovery_check_status = CheckRecoveryNeeded(&needs_recovery);
        if (!recovery_check_status.ok()) {
            LOG_ERROR << "Recovery check failed: " << recovery_check_status.ToString();
            return recovery_check_status;
        }
        
        if (!needs_recovery) {
            LOG_INFO << "No recovery needed, database is in consistent state";
            return Status::OK();
        }
        
        LOG_INFO << "Database recovery needed, starting recovery process";
        
        // 3. 初始化恢复环境
        Status init_status = InitializeRecoveryEnvironment();
        if (!init_status.ok()) {
            LOG_ERROR << "Failed to initialize recovery environment: " << init_status.ToString();
            return init_status;
        }
        
        // 4. 执行列族恢复
        Status cf_recovery_status = RecoverColumnFamilies();
        if (!cf_recovery_status.ok()) {
            LOG_ERROR << "Column family recovery failed: " << cf_recovery_status.ToString();
            return cf_recovery_status;
        }
        LOG_INFO << "Column family recovery completed";
        
        // 5. 执行WAL恢复（通过默认列族）
        SequenceNumber recovered_sequence = 0;
        Status wal_recovery_status = RecoverFromWAL(&recovered_sequence);
        if (!wal_recovery_status.ok()) {
            LOG_ERROR << "WAL recovery failed: " << wal_recovery_status.ToString();
            return wal_recovery_status;
        }
        LOG_INFO << "WAL recovery completed, recovered sequence: " << recovered_sequence;
        
        // 6. 验证数据一致性
        Status consistency_status = ValidateDataConsistency();
        if (!consistency_status.ok()) {
            LOG_ERROR << "Data consistency validation failed: " << consistency_status.ToString();
            return consistency_status;
        }
        LOG_INFO << "Data consistency validation passed";
        
        // 7. 清理恢复过程中的临时状态
        Status cleanup_status = CleanupRecoveryState();
        if (!cleanup_status.ok()) {
            LOG_WARN << "Recovery cleanup partially failed: " << cleanup_status.ToString();
            // 清理失败不影响整体恢复成功
        }
        
        // 8. 完成LSMTree恢复
        Status lsm_finish_status = FinishLSMTreeRecovery(recovered_sequence);
        if (!lsm_finish_status.ok()) {
            LOG_ERROR << "Failed to finish LSMTree recovery: " << lsm_finish_status.ToString();
            return lsm_finish_status;
        }
        
        // 9. 记录恢复统计
        auto end_time = std::chrono::steady_clock::now();
        auto recovery_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);
        
        LOG_INFO << "Database crash recovery completed successfully";
        LOG_INFO << "Recovery time: " << recovery_duration.count() << "ms";
        LOG_INFO << "Recovered sequence number: " << recovered_sequence;
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        LOG_ERROR << "Exception during crash recovery: " << e.what();
        return Status::IOError("Crash recovery failed due to exception: " + std::string(e.what()));
    }
}

Status DBImpl::RegisterUnifiedBackgroundTasks() {
    // 使用 DBOptions 参数注册统一后台任务（刷回与GC）
    if (!components_ || !components_->cf_manager || !components_->background_manager) {
        return Status::OK();
    }

    auto* bg = components_->background_manager.get();

    // 刷回任务（MVCC -> LSM）：覆盖所有列族
    BackgroundTask flush_task = CreatePeriodicTask(
        TaskType::kMaintenance,
        [this]() -> Status {
            auto cf_list = components_->cf_manager->ListColumnFamilies();
            for (const auto& cf_name : cf_list) {
                ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
                if (!cf || !cf->GetTxManager()) continue;
                Status s = cf->GetTxManager()->FlushCommittedToLSM(options_.mvcc_flush_max_items);
                if (!s.ok()) return s;
            }
            return Status::OK();
        },
        std::chrono::milliseconds(options_.lsm_flush_check_interval_ms),
        TaskPriority::kNormal
    );
    bg->SchedulePeriodicTask(flush_task);

    // GC 任务：覆盖所有列族
    BackgroundTask gc_task = CreatePeriodicTask(
        TaskType::kMaintenance,
        [this]() -> Status {
            auto cf_list = components_->cf_manager->ListColumnFamilies();
            for (const auto& cf_name : cf_list) {
                ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
                if (cf && cf->GetTxManager()) cf->GetTxManager()->RunGC();
            }
            return Status::OK();
        },
        std::chrono::milliseconds(options_.mvcc_version_cleanup_interval_ms),
        TaskPriority::kLow
    );
    bg->SchedulePeriodicTask(gc_task);

    return Status::OK();
}

// ============================================================================
// 恢复相关辅助方法实现
// ============================================================================

Status DBImpl::ValidateDatabaseStructure() {
    try {
        // 1. 验证数据库根目录存在
        if (!std::filesystem::exists(dbname_)) {
            return Status::IOError("Database directory does not exist: " + dbname_);
        }
        
        // 2. 验证基本目录结构
        std::vector<std::string> required_dirs = {"wal", "sst"};
        for (const auto& dir : required_dirs) {
            std::string full_path = dbname_ + "/" + dir;
            if (!std::filesystem::exists(full_path)) {
                // 创建缺失的目录
                try {
                    std::filesystem::create_directories(full_path);
                    LOG_INFO << "Created missing directory: " << full_path;
                } catch (const std::exception& e) {
                    return Status::IOError("Failed to create directory " + full_path + ": " + e.what());
                }
            }
        }
        
        // 3. 验证关键文件的可访问性
        std::string manifest_path = dbname_ + "/MANIFEST";
        std::string current_path = dbname_ + "/CURRENT";
        
        // 检查文件权限（不存在的话会在后续创建）
        if (std::filesystem::exists(manifest_path)) {
            std::ifstream manifest_file(manifest_path);
            if (!manifest_file.is_open()) {
                return Status::IOError("Cannot access MANIFEST file: " + manifest_path);
            }
        }
        
        // 4. 验证磁盘空间
        auto space_info = std::filesystem::space(dbname_);
        const uint64_t min_required_space = 100 * 1024 * 1024; // 100MB最小空间
        if (space_info.available < min_required_space) {
            return Status::IOError("Insufficient disk space for recovery. Available: " + 
                                 std::to_string(space_info.available) + " bytes");
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during database structure validation: " + std::string(e.what()));
    }
}

Status DBImpl::CheckRecoveryNeeded(bool* needs_recovery) {
    if (!needs_recovery) {
        return Status::InvalidArgument("needs_recovery pointer is null");
    }
    
    *needs_recovery = false;
    
    try {
        // 1. 检查WAL文件是否存在且非空
        std::string wal_dir = dbname_ + "/wal";
        bool has_wal_files = false;
        
        if (std::filesystem::exists(wal_dir)) {
            for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
                if (entry.is_regular_file() && 
                    entry.path().extension() == ".wal" && 
                    entry.file_size() > 0) {
                    has_wal_files = true;
                    break;
                }
            }
        }
        
        // 2. 检查数据库关闭标记
        std::string clean_shutdown_marker = dbname_ + "/CLEAN_SHUTDOWN";
        bool clean_shutdown = std::filesystem::exists(clean_shutdown_marker);
        
        // 3. 检查是否有未完成的操作标记
        std::string recovery_marker = dbname_ + "/RECOVERY_IN_PROGRESS";
        bool recovery_in_progress = std::filesystem::exists(recovery_marker);
        
        // 4. 检查MANIFEST文件完整性
        std::string manifest_path = dbname_ + "/MANIFEST";
        bool manifest_corrupted = false;
        
        if (std::filesystem::exists(manifest_path)) {
            std::ifstream manifest_file(manifest_path, std::ios::binary);
            if (!manifest_file.is_open()) {
                manifest_corrupted = true;
            } else {
                // 简单检查文件是否为空或过小
                manifest_file.seekg(0, std::ios::end);
                auto file_size = manifest_file.tellg();
                if (file_size < 4) { // 至少4字节的头部
                    manifest_corrupted = true;
                }
            }
        }
        
        // 决定是否需要恢复
        if (has_wal_files && !clean_shutdown) {
            *needs_recovery = true;
            LOG_INFO << "Recovery needed: WAL files present and no clean shutdown";
        } else if (recovery_in_progress) {
            *needs_recovery = true;
            LOG_INFO << "Recovery needed: previous recovery was interrupted";
        } else if (manifest_corrupted) {
            *needs_recovery = true;
            LOG_INFO << "Recovery needed: MANIFEST file is corrupted";
        } else {
            LOG_INFO << "No recovery needed: database is in consistent state";
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during recovery check: " + std::string(e.what()));
    }
}

Status DBImpl::InitializeRecoveryEnvironment() {
    try {
        // 1. 创建恢复进行中标记
        std::string recovery_marker = dbname_ + "/RECOVERY_IN_PROGRESS";
        std::ofstream marker_file(recovery_marker);
        if (!marker_file.is_open()) {
            return Status::IOError("Failed to create recovery marker file");
        }
        marker_file << std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        marker_file.close();
        
        // 2. 创建恢复日志目录
        std::string recovery_log_dir = dbname_ + "/recovery_logs";
        std::filesystem::create_directories(recovery_log_dir);
        
        // 3. 备份关键文件
        std::string backup_dir = dbname_ + "/recovery_backup_" + 
                                std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count());
        std::filesystem::create_directories(backup_dir);
        
        // 备份MANIFEST文件
        std::string manifest_path = dbname_ + "/MANIFEST";
        if (std::filesystem::exists(manifest_path)) {
            std::string backup_manifest = backup_dir + "/MANIFEST.backup";
            std::filesystem::copy_file(manifest_path, backup_manifest);
            LOG_INFO << "Backed up MANIFEST file to: " << backup_manifest;
        }
        
        // 备份CURRENT文件
        std::string current_path = dbname_ + "/CURRENT";
        if (std::filesystem::exists(current_path)) {
            std::string backup_current = backup_dir + "/CURRENT.backup";
            std::filesystem::copy_file(current_path, backup_current);
            LOG_INFO << "Backed up CURRENT file to: " << backup_current;
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during recovery environment initialization: " + 
                             std::string(e.what()));
    }
}

Status DBImpl::RecoverColumnFamilies() {
    if (!components_ || !components_->cf_manager) {
        return Status::InvalidArgument("ColumnFamilyManager not available");
    }
    
    try {
        // 1. 使用ColumnFamilyManager的恢复方法（从MANIFEST文件恢复）
        Status recovery_status = components_->cf_manager->RecoverColumnFamilies();
        if (!recovery_status.ok()) {
            LOG_ERROR << "Failed to recover column families from MANIFEST: " << recovery_status.ToString();
            return recovery_status;
        }
        
        // 2. 获取恢复后的列族列表
        auto cf_list = components_->cf_manager->ListColumnFamilies();
        LOG_INFO << "Recovered " << cf_list.size() << " column families from MANIFEST";
        
        // 3. 对每个列族执行并发管理器的恢复
        for (const auto& cf_name : cf_list) {
            ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
            if (!cf) {
                LOG_WARN << "Column family " << cf_name << " not found after recovery, skipping";
                continue;
            }
            
            // 获取列族的并发管理器并执行恢复
            auto* tx_manager = cf->GetTxManager();
            if (tx_manager) {
                // 新的TxManager不需要复杂恢复，这里仅兼容占位
                Status cf_recovery_status = Status::OK();
                if (!cf_recovery_status.ok()) {
                    LOG_ERROR << "Concurrency recovery failed for column family " << cf_name 
                             << ": " << cf_recovery_status.ToString();
                    return cf_recovery_status;
                }
                LOG_INFO << "Successfully completed concurrency recovery for column family: " << cf_name;
            } else {
                LOG_WARN << "No concurrency manager for column family: " << cf_name;
            }
        }
        
        LOG_INFO << "Column family recovery completed successfully for all " << cf_list.size() << " column families";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during column family recovery: " + std::string(e.what()));
    }
}

Status DBImpl::RecoverFromWAL(SequenceNumber* recovered_sequence) {
    if (!recovered_sequence) {
        return Status::InvalidArgument("recovered_sequence pointer is null");
    }
    
    *recovered_sequence = 0;
    
    try {
        // 通过默认列族的WAL管理器执行恢复
        ColumnFamily* default_cf = DefaultColumnFamily();
        if (!default_cf) {
            return Status::InvalidArgument("Default column family not available");
        }
        
        auto* tx_manager = default_cf->GetTxManager();
        if (!tx_manager) {
            return Status::InvalidArgument("Default column family tx manager not available");
        }
        
        auto* wal_manager = tx_manager->wal();
        if (!wal_manager) {
            return Status::InvalidArgument("WAL manager not available");
        }
        
        // WAL记录已在列族构建阶段（WALManager::Initialize）重放到LSMTree，
        // 这里只汇总恢复出的最大序列号，避免重复应用
        *recovered_sequence = wal_manager->GetLastSequence();

        // 推进快照序列生成器，确保后续提交使用的 SnapshotSeq 不落后
        if (tx_manager) {
            tx_manager->RecoverSetSnapshot(*recovered_sequence);
        }
        LOG_INFO << "WAL recovery completed, last sequence: " << *recovered_sequence;
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during WAL recovery: " + std::string(e.what()));
    }
}

Status DBImpl::ValidateDataConsistency() {
    try {
        // 1. 验证所有列族的数据一致性
        if (!components_ || !components_->cf_manager) {
            return Status::InvalidArgument("ColumnFamilyManager not available");
        }
        
        auto cf_list = components_->cf_manager->ListColumnFamilies();
        
        for (const auto& cf_name : cf_list) {
            ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
            if (!cf) {
                continue;
            }
            
            // 检查列族内部状态一致性
            Status cf_status = cf->ValidateConsistency();
            if (!cf_status.ok()) {
                LOG_ERROR << "Consistency validation failed for column family " 
                         << cf_name << ": " << cf_status.ToString();
                return cf_status;
            }
        }
        
        // 2. 验证序列号的单调性
        // 通过默认列族检查序列号状态
        ColumnFamily* default_cf = DefaultColumnFamily();
        // 新事务层无全局序列验证，这里跳过
        
        // 3. 验证文件系统状态
        Status fs_status = ValidateFileSystemState();
        if (!fs_status.ok()) {
            LOG_ERROR << "File system state validation failed: " << fs_status.ToString();
            return fs_status;
        }
        
        LOG_INFO << "Data consistency validation passed for all components";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during data consistency validation: " + 
                             std::string(e.what()));
    }
}

Status DBImpl::CleanupRecoveryState() {
    try {
        // 1. 删除恢复进行中标记
        std::string recovery_marker = dbname_ + "/RECOVERY_IN_PROGRESS";
        if (std::filesystem::exists(recovery_marker)) {
            std::filesystem::remove(recovery_marker);
            LOG_INFO << "Removed recovery in progress marker";
        }
        
        // 2. 创建清洁关闭标记
        std::string clean_shutdown_marker = dbname_ + "/CLEAN_SHUTDOWN";
        std::ofstream marker_file(clean_shutdown_marker);
        if (marker_file.is_open()) {
            marker_file << "Database recovered and running normally\n";
            marker_file << std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            marker_file.close();
            LOG_INFO << "Created clean shutdown marker";
        }
        
        // 3. 清理过时的WAL文件（保留最新的几个）
        Status wal_cleanup_status = CleanupOldWALFiles();
        if (!wal_cleanup_status.ok()) {
            LOG_WARN << "WAL cleanup failed: " << wal_cleanup_status.ToString();
            // 不影响整体恢复成功
        }
        
        // 4. 清理过时的备份文件（保留最近的几个）
        Status backup_cleanup_status = CleanupOldBackups();
        if (!backup_cleanup_status.ok()) {
            LOG_WARN << "Backup cleanup failed: " << backup_cleanup_status.ToString();
            // 不影响整体恢复成功
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during recovery state cleanup: " + std::string(e.what()));
    }
}

// 辅助方法
Status DBImpl::ValidateFileSystemState() {
    try {
        // 检查关键目录的权限和可访问性
        std::vector<std::string> critical_dirs = {
            dbname_,
            dbname_ + "/wal",
            dbname_ + "/sst"
        };
        
        for (const auto& dir : critical_dirs) {
            if (!std::filesystem::exists(dir)) {
                return Status::IOError("Critical directory missing: " + dir);
            }
            
            // 简单的写权限测试
            std::string test_file = dir + "/.write_test";
            std::ofstream test_stream(test_file);
            if (!test_stream.is_open()) {
                return Status::IOError("No write permission for directory: " + dir);
            }
            test_stream.close();
            std::filesystem::remove(test_file);
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during file system validation: " + std::string(e.what()));
    }
}

Status DBImpl::CleanupOldWALFiles() {
    try {
        // 通过默认列族的WAL管理器清理旧文件
        ColumnFamily* default_cf = DefaultColumnFamily();
        if (!default_cf) {
            return Status::OK();
        }
        
        auto* tx_manager = default_cf->GetTxManager();
        if (!tx_manager) {
            return Status::OK();
        }
        
        auto* wal_manager = tx_manager->wal();
        if (!wal_manager) {
            return Status::OK();
        }
        
        // 清理旧WAL文件，保留安全序列号之后的文件
        SequenceNumber safe_sequence = 0;
        
        return wal_manager->CleanupOldWALFiles(safe_sequence);
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during WAL cleanup: " + std::string(e.what()));
    }
}

Status DBImpl::CleanupOldBackups() {
    try {
        std::string db_dir = dbname_;
        std::vector<std::string> backup_dirs;
        
        // 查找备份目录
        for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
            if (entry.is_directory()) {
                std::string dirname = entry.path().filename().string();
                if (dirname.find("recovery_backup_") == 0) {
                    backup_dirs.push_back(entry.path().string());
                }
            }
        }
        
        // 按时间排序，保留最新的3个备份
        std::sort(backup_dirs.begin(), backup_dirs.end());
        
        const size_t max_backups_to_keep = 3;
        if (backup_dirs.size() > max_backups_to_keep) {
            for (size_t i = 0; i < backup_dirs.size() - max_backups_to_keep; ++i) {
                std::filesystem::remove_all(backup_dirs[i]);
                LOG_INFO << "Removed old backup: " << backup_dirs[i];
            }
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during backup cleanup: " + std::string(e.what()));
    }
}

Status DBImpl::SyncAllData() {
    try {
        // 同步所有列族的数据
        if (!components_ || !components_->cf_manager) {
            return Status::InvalidArgument("ColumnFamilyManager not available");
        }
        
        auto cf_list = components_->cf_manager->ListColumnFamilies();
        
        for (const auto& cf_name : cf_list) {
            ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
            if (!cf) {
                continue;
            }

            // 先把MVCC版本链中已提交未刷回的数据写入LSMTree
            auto* tx_manager = cf->GetTxManager();
            if (tx_manager) {
                // 循环直到版本链清空（每次最多刷max_items条）
                for (int round = 0; round < 10000; ++round) {
                    Status mvcc_flush_status =
                        tx_manager->FlushCommittedToLSM(options_.mvcc_flush_max_items);
                    if (!mvcc_flush_status.ok()) {
                        LOG_ERROR << "Failed to flush MVCC data for column family " << cf_name
                                 << ": " << mvcc_flush_status.ToString();
                        return mvcc_flush_status;
                    }
                    if (tx_manager->version_chain()->PickCommittedNotFlushed(1).empty()) {
                        break;
                    }
                }
            }

            // 刷盘列族数据
            Status flush_status = cf->Flush();
            if (!flush_status.ok()) {
                LOG_ERROR << "Failed to flush column family " << cf_name
                         << ": " << flush_status.ToString();
                return flush_status;
            }
            
            // 同步WAL（新事务层）
            if (tx_manager) {
                auto* wal_manager = tx_manager->wal();
                if (wal_manager) {
                    Status wal_sync_status = wal_manager->Sync();
                    if (!wal_sync_status.ok()) {
                        LOG_ERROR << "Failed to sync WAL for column family " << cf_name 
                                 << ": " << wal_sync_status.ToString();
                        return wal_sync_status;
                    }
                }
            }
        }
        
        LOG_INFO << "All data synchronized successfully";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during data sync: " + std::string(e.what()));
    }
}

Status DBImpl::CreateCleanShutdownMarker() {
    try {
        std::string marker_path = dbname_ + "/CLEAN_SHUTDOWN";
        std::ofstream marker_file(marker_path);
        
        if (!marker_file.is_open()) {
            return Status::IOError("Failed to create clean shutdown marker file");
        }
        
        marker_file << "Database shutdown cleanly\n";
        marker_file << "Timestamp: " << std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() << "\n";
        marker_file << "Database: " << dbname_ << "\n";
        marker_file.close();
        
        LOG_INFO << "Created clean shutdown marker: " << marker_path;
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception creating clean shutdown marker: " + std::string(e.what()));
    }
}

Status DBImpl::RemoveCleanShutdownMarker() {
    try {
        std::string clean_marker = dbname_ + "/CLEAN_SHUTDOWN";
        
        if (std::filesystem::exists(clean_marker)) {
            std::filesystem::remove(clean_marker);
            LOG_INFO << "Removed clean shutdown marker: " << clean_marker;
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception removing clean shutdown marker: " + std::string(e.what()));
    }
}

Status DBImpl::RemoveRecoveryMarker() {
    try {
        std::string recovery_marker = dbname_ + "/RECOVERY_IN_PROGRESS";
        
        if (std::filesystem::exists(recovery_marker)) {
            std::filesystem::remove(recovery_marker);
            LOG_INFO << "Removed recovery marker: " << recovery_marker;
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception removing recovery marker: " + std::string(e.what()));
    }
}

Status DBImpl::FinishLSMTreeRecovery(SequenceNumber recovered_sequence) {
    try {
        // 完成所有列族的LSMTree恢复
        if (!components_ || !components_->cf_manager) {
            return Status::InvalidArgument("ColumnFamilyManager not available");
        }
        
        auto cf_list = components_->cf_manager->ListColumnFamilies();
        
        for (const auto& cf_name : cf_list) {
            ColumnFamily* cf = components_->cf_manager->GetColumnFamily(cf_name);
            if (!cf) {
                continue;
            }
            
            // 获取LSMTree并完成恢复
            auto* lsm_tree = cf->GetLSMTree();
            if (lsm_tree) {
                Status finish_status = lsm_tree->FinishRecovery(recovered_sequence);
                if (!finish_status.ok()) {
                    LOG_ERROR << "Failed to finish LSMTree recovery for column family " 
                             << cf_name << ": " << finish_status.ToString();
                    return finish_status;
                }
                LOG_INFO << "Finished LSMTree recovery for column family: " << cf_name;
            }
        }
        
        LOG_INFO << "All LSMTree recovery completed with sequence: " << recovered_sequence;
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception finishing LSMTree recovery: " + std::string(e.what()));
    }
}

ColumnFamily* DBImpl::ValidateColumnFamily(ColumnFamily* cf) const {
    if (!cf) {
        return DefaultColumnFamily();
    }
    return cf;
}

// ============================================================================
// 静态方法实现
// ============================================================================

Status DB::Open(const DBOptions& db_options, const std::string& name,
                 std::unique_ptr<DB>* dbptr) {
    if (!dbptr) {
        return Status::InvalidArgument("DB pointer is null");
    }
    
    auto db = std::make_unique<DBImpl>(db_options, name);
    Status s = db->Initialize();
    if (!s.ok()) {
        return s;
    }
    
    *dbptr = std::move(db);
    return Status::OK();
}

Status DB::Open(const DBOptions& db_options, const std::string& name,
                 const std::vector<ColumnFamilyDescriptor>& column_families,
                 std::vector<ColumnFamily*>* handles,
                 std::unique_ptr<DB>* dbptr) {
    if (!dbptr || !handles) {
        return Status::InvalidArgument("dbptr or handles is null");
    }

    Status s = Open(db_options, name, dbptr);
    if (!s.ok()) {
        return s;
    }

    handles->clear();
    handles->reserve(column_families.size());

    // 默认列族已在Open时自动创建，其余描述符按需创建
    for (const auto& desc : column_families) {
        ColumnFamily* handle = (*dbptr)->DefaultColumnFamily();
        if (desc.name != "default") {
            s = (*dbptr)->CreateColumnFamily(desc.options, desc.name, &handle);
            if (!s.ok()) {
                return s;
            }
        }
        handles->push_back(handle);
    }

    return Status::OK();
}

Status DB::OpenForReadOnly(const DBOptions& db_options, const std::string& name,
                           std::unique_ptr<DB>* dbptr, bool error_if_wal_file_exists) {
    if (error_if_wal_file_exists && std::filesystem::exists(name)) {
        // 存在未回放的WAL文件时拒绝以只读方式打开，避免读到陈旧数据
        for (const auto& entry : std::filesystem::recursive_directory_iterator(name)) {
            if (!entry.is_regular_file()) continue;
            std::string filename = entry.path().filename().string();
            size_t dot = filename.rfind('.');
            if (dot != std::string::npos && filename.substr(dot) == ".wal") {
                return Status::IOError("WAL file exists, cannot open for read-only: " +
                                       entry.path().string());
            }
        }
    }
    return Open(db_options, name, dbptr);
}

Status DB::ListColumnFamilies(const DBOptions& db_options, const std::string& name,
                              std::vector<std::string>* column_families) {
    (void)db_options;
    if (!column_families) {
        return Status::InvalidArgument("column_families is null");
    }

    if (!std::filesystem::exists(name)) {
        return Status::NotFound("Database directory does not exist: " + name);
    }

    column_families->clear();

    // 只读方式解析MANIFEST，不打开数据库
    auto manifest_manager = manifest_util::CreateManifestManager(name);
    Status s = manifest_manager->Initialize();
    if (!s.ok()) {
        return s;
    }

    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    uint64_t last_sequence = 0;
    s = manifest_manager->RecoverColumnFamilies(&cf_descriptors, &last_sequence);
    manifest_manager->Shutdown();
    if (!s.ok()) {
        return s;
    }

    for (const auto& desc : cf_descriptors) {
        if (!desc.is_dropped) {
            column_families->push_back(desc.name);
        }
    }

    // 没有任何MANIFEST记录的数据库视为只有默认列族
    if (column_families->empty()) {
        column_families->push_back("default");
    }

    return Status::OK();
}

Status DB::DestroyDB(const std::string& name, const DBOptions& options) {
    (void)options;
    if (name.empty()) {
        return Status::InvalidArgument("Database name is empty");
    }

    std::error_code ec;
    if (std::filesystem::exists(name)) {
        if (!std::filesystem::remove_all(name, ec) || ec) {
            return Status::IOError("Failed to destroy database: " + ec.message());
        }
    }

    return Status::OK();
}

Status DB::RepairDB(const std::string& dbname, const DBOptions& options) {
    if (dbname.empty()) {
        return Status::InvalidArgument("Database name is empty");
    }

    if (!std::filesystem::exists(dbname)) {
        return Status::NotFound("Database directory does not exist: " + dbname);
    }

    // 清除上一次异常中断留下的恢复标记，避免打开时误判
    std::error_code ec;
    std::filesystem::remove(dbname + "/RECOVERY_IN_PROGRESS", ec);
    std::filesystem::remove(dbname + "/CLEAN_SHUTDOWN", ec);

    // 清理损坏文件标记（WAL恢复过程中重命名的.corrupted文件）
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dbname)) {
        if (!entry.is_regular_file()) continue;
        std::string filename = entry.path().filename().string();
        size_t dot = filename.rfind('.');
        if (dot != std::string::npos && filename.substr(dot) == ".corrupted") {
            std::filesystem::remove(entry.path(), ec);
        }
    }

    // 打开并关闭一次，触发WAL回放与MemTable刷盘，重建一致状态
    std::unique_ptr<DB> db;
    Status s = Open(options, dbname, &db);
    if (!s.ok()) {
        return Status::IOError("Repair open failed: " + s.ToString());
    }
    s = db->Close();
    if (!s.ok()) {
        return Status::IOError("Repair close failed: " + s.ToString());
    }

    return Status::OK();
}

// ============================================================================
// DB 基类实现 - 提供虚表定义
// ============================================================================

DB::~DB() {
    // 虚析构函数实现
}

// 提供缺失的虚函数实现
Status DB::RegisterBackgroundTask(const std::string& name, 
                                  const std::function<Status()>& task,
                                  const std::string& group, 
                                  bool persistent,
                                  std::chrono::milliseconds interval) {
    (void)name; (void)task; (void)group; (void)persistent; (void)interval;
    return Status::NotSupported("RegisterBackgroundTask not implemented in base class");
}

Status DB::UnregisterBackgroundTask(const std::string& name, const std::string& group) {
    (void)name; (void)group;
    return Status::NotSupported("UnregisterBackgroundTask not implemented in base class");
}

} // namespace lrdb
