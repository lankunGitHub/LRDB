// 列族实现

#include "lrdb/db/column_family.h"
#include "lrdb/db/manifest.h"
#include "lrdb/storage/lsm_tree.h"
#include "lrdb/concurrency/tx_manager.h"
#include "lrdb/wal/wal.h"
#include "lrdb/util/sequence_generator.h"
#include "lrdb/util/comparator.h"
#include "lrdb/util/logging.h"

#include <sstream>
#include <filesystem>

namespace lrdb {

// 列族选项默认值已在头文件中定义

// DefaultColumnFamily实现
DefaultColumnFamily::DefaultColumnFamily(uint32_t id, const std::string& name,
                                        const ColumnFamilyOptions& options,
                                        const std::string& db_path)
    : id_(id), name_(name), options_(options), 
      cf_data_path_(db_path + "/cf_" + std::to_string(id) + "_" + name),
      active_(true), ref_count_(1) {
    
    if (name.empty()) {
        throw std::invalid_argument("Column family name cannot be empty");
    }
    
    if (!options_.comparator) {
        throw std::invalid_argument("Column family comparator cannot be null");
    }
    
    try {
        // 1. 创建列族独立的数据目录
        try {
            if (!std::filesystem::exists(cf_data_path_)) {
                std::filesystem::create_directories(cf_data_path_);
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to create column family directory: " + std::string(e.what()));
        }
        
        // 2. 创建独立的序列号生成器
        sequence_generator_ = std::make_unique<SequenceGenerator>();
        if (!sequence_generator_) {
            throw std::runtime_error("Failed to create sequence generator for column family");
        }
        
        // 3. 创建独立的WAL管理器（传入列族数据目录，WAL文件位于其下wal/子目录）
        wal_manager_ = std::make_unique<WALManager>(cf_data_path_);
        if (!wal_manager_) {
            throw std::runtime_error("Failed to create WAL manager for column family");
        }

        // 4. 创建独立的LSMTree
        LSMTreeOptions lsm_options;
        lsm_options.memtable_size = options_.write_buffer_size;
        lsm_options.max_memtables = options_.max_write_buffer_number;
        lsm_options.db_path = cf_data_path_;

        lsm_tree_ = std::make_unique<LSMTree>(lsm_options);
        if (!lsm_tree_) {
            throw std::runtime_error("Failed to create LSM Tree for column family");
        }

        // 关联 WAL 与 LSMTree，用于恢复期间 ApplyWALRecord
        wal_manager_->SetLSMTree(lsm_tree_.get());

        // 5. 打开LSMTree（必须在WAL初始化之前，否则重放记录无处安放）
        Status lsm_status = lsm_tree_->Open(nullptr, options_.comparator, nullptr);
        if (!lsm_status.ok()) {
            throw std::runtime_error("Failed to open LSMTree: " + lsm_status.ToString());
        }

        // 6. 初始化WAL管理器（扫描现有WAL文件并重放到LSMTree恢复缓冲）
        Status wal_status = wal_manager_->Initialize();
        if (!wal_status.ok()) {
            throw std::runtime_error("Failed to initialize WAL: " + wal_status.ToString());
        }

        // 7. 创建事务门面（每列族一套，不共享）
        tx_manager_ = std::make_unique<TxManager>(wal_manager_.get(), lsm_tree_.get(), options_.comparator, id_);
        if (!tx_manager_) {
            throw std::runtime_error("Failed to create TxManager for column family");
        }

        // 8. 推进快照序列生成器并完成LSMTree恢复（刷盘恢复缓冲）
        SequenceNumber recovered_sequence = wal_manager_->GetLastSequence();
        if (recovered_sequence > 0) {
            tx_manager_->RecoverSetSnapshot(recovered_sequence);
            Status finish_status = lsm_tree_->FinishRecovery(recovered_sequence);
            if (!finish_status.ok()) {
                LOG_WARN << "Failed to finish LSMTree recovery for column family "
                         << name_ << ": " << finish_status.ToString();
            }
        }

        // 标记为已初始化
        active_.store(true);
        
        LOG_INFO << "Column family '" << name_ << "' (ID: " << id_ 
                 << ") initialized independently at: " << cf_data_path_;
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to initialize DefaultColumnFamily: " + std::string(e.what()));
    }
}

Status DefaultColumnFamily::Initialize() {
    try {
        // 调用私有初始化方法
        // InitializeImpl(); // 该方法已经在构造函数中调用，这里不需要再次调用
        active_.store(true);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to initialize DefaultColumnFamily: " + std::string(e.what()));
    }
}
    
DefaultColumnFamily::~DefaultColumnFamily() = default;
    
// ========== 基本属性实现 ==========

uint32_t DefaultColumnFamily::GetID() const { 
    return id_; 
}

const std::string& DefaultColumnFamily::GetName() const { 
    return name_; 
}

const ColumnFamilyOptions& DefaultColumnFamily::GetOptions() const { 
    return options_; 
}

const Comparator* DefaultColumnFamily::GetComparator() const {
    return options_.comparator;
}

bool DefaultColumnFamily::IsDefault() const {
    return name_ == "default";
}

bool DefaultColumnFamily::IsActive() const {
    return active_.load();
}

LSMTree* DefaultColumnFamily::GetLSMTree() const { 
    return lsm_tree_.get(); 
}

TxManager* DefaultColumnFamily::GetTxManager() const {
    return tx_manager_.get();
}

Status DefaultColumnFamily::ValidateConsistency() const {
    try {
        // 1. 验证列族基本状态
        if (!active_.load()) {
            return Status::InvalidArgument("Column family is not active");
        }
        
        // 2. 验证LSMTree状态
        if (lsm_tree_) {
            Status lsm_status = lsm_tree_->ValidateInternalState();
            if (!lsm_status.ok()) {
                return Status::Corruption("LSMTree validation failed: " + lsm_status.ToString());
            }
        }
        
        // 3. 新事务层不在列族验证序列号
        
        // 4. 验证WAL管理器状态
        if (wal_manager_) {
            // 检查WAL文件完整性
            auto wal_files = wal_manager_->GetWALFiles();
            for (const auto& file : wal_files) {
                if (!std::filesystem::exists(std::filesystem::path(file))) {
                    return Status::Corruption("WAL file missing: " + file);
                }
            }
        }
        
        // 5. 验证数据目录完整性
        if (!cf_data_path_.empty() && !std::filesystem::exists(std::filesystem::path(cf_data_path_))) {
            return Status::Corruption("Column family data path missing: " + cf_data_path_);
        }
        
        LOG_INFO << "Column family " << name_ << " (" << id_ << ") consistency validation passed";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during column family validation: " + std::string(e.what()));
    }
}

// ========== 数据操作实现 ==========

Status DefaultColumnFamily::Put(const Slice& key, const Slice& value) {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }
    
    if (!tx_manager_) {
        return Status::IOError("ConcurrencyManager not available");
    }
    
    LOG_INFO << "DefaultColumnFamily::Put called for key: " << key.ToString();
    
    // 使用立即提交的事务进行Put操作 - 保持完整的事务逻辑
    TxnOptions txn_options;
    auto txn = tx_manager_->Begin(txn_options);
    if (!txn) {
        LOG_ERROR << "Failed to begin transaction for Put operation";
        return Status::IOError("Failed to begin transaction");
    }
    
    LOG_INFO << "Transaction created successfully, executing Put";
    Status s = txn->Put(key, value);
    if (s.ok()) {
        LOG_INFO << "Put operation successful, committing transaction";
        s = txn->Commit();
        if (s.ok()) {
            LOG_INFO << "Transaction committed successfully";
        } else {
            LOG_ERROR << "Transaction commit failed: " << s.ToString();
        }
    } else {
        LOG_ERROR << "Put operation failed: " << s.ToString();
        txn->Rollback();
    }
    
    return s;
}

Status DefaultColumnFamily::Delete(const Slice& key) {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }
    
    if (!tx_manager_) {
        return Status::IOError("ConcurrencyManager not available");
    }
    
    // 使用立即提交的事务进行Delete操作
    auto txn = tx_manager_->Begin(TxnOptions{});
    if (!txn) {
        return Status::IOError("Failed to begin transaction");
    }
    
    Status s = txn->Delete(key);
    if (s.ok()) {
        s = txn->Commit();
    }
    
    return s;
}

Status DefaultColumnFamily::Get(const Slice& key, std::string* value) {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }
    
    if (!tx_manager_ || !lsm_tree_ || !value) {
        return Status::InvalidArgument("Invalid arguments for Get");
    }
    
    // MVCC读取顺序：先版本链，后LSMTree
    // 1. 首先从ConcurrencyManager的版本链中读取（可能有最新的未刷盘版本）
    Status mvcc_status = tx_manager_->Get(key, value);
    if (mvcc_status.ok()) {
        // 在版本链中找到了数据
        return Status::OK();
    }
    
    // 2. 如果版本链中没有找到，从LSMTree读取已提交的数据
    if (mvcc_status.IsNotFound()) {
        return lsm_tree_->Get(key, value);
    }
    
    // 其他错误直接返回
    return mvcc_status;
}

Status DefaultColumnFamily::Get(const Slice& key, uint64_t sequence, std::string* value) {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }

    if (!lsm_tree_ || !value) {
        return Status::InvalidArgument("Invalid arguments for Get");
    }

    // 快照读取：先查版本链中快照可见的已提交版本，未命中再查LSMTree。
    // 版本链返回NotFound表示键在快照下被删除，直接返回
    if (tx_manager_) {
        bool found = false;
        Status chain_status =
            tx_manager_->version_chain()->GetVisibleBySnapshot(key.ToString(),
                                                               sequence, value,
                                                               &found);
        if (!chain_status.ok()) {
            return chain_status;
        }
        if (found) {
            return Status::OK();
        }
    }

    return lsm_tree_->Get(key, value, sequence);
}

Status DefaultColumnFamily::MultiGet(const std::vector<Slice>& keys, std::vector<std::string>* values) {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }
    
    if (!tx_manager_ || !lsm_tree_ || !values) {
        return Status::InvalidArgument("Invalid arguments for MultiGet");
    }
    
    values->clear();
    values->reserve(keys.size());
    
    // 对每个key执行MVCC读取：先版本链，后LSMTree
    for (const auto& key : keys) {
        std::string value;
        
        // 1. 首先从版本链读取
        Status mvcc_status = tx_manager_->Get(key, &value);
        if (mvcc_status.ok()) {
            values->push_back(std::move(value));
            continue;
        }
        
        // 2. 如果版本链中没有，从LSMTree读取
        if (mvcc_status.IsNotFound()) {
            Status lsm_status = lsm_tree_->Get(key, &value);
            if (lsm_status.ok()) {
                values->push_back(std::move(value));
            } else {
                values->push_back(""); // 未找到或其他错误
            }
        } else {
            values->push_back(""); // MVCC读取出错
        }
    }
    
    return Status::OK();
}

// ========== 事务操作实现 ==========

std::unique_ptr<Transaction> DefaultColumnFamily::BeginTransaction(const TxnOptions& options) {
    if (!IsActive()) {
        LOG_ERROR << "Column family '" << name_ << "' is not active";
        return nullptr;
    }
    
    if (!tx_manager_) {
        LOG_ERROR << "TxManager not available for column family '" << name_ << "'";
        return nullptr;
    }
    
    // 直接使用列族的ConcurrencyManager创建事务
    auto txn = tx_manager_->Begin(options);
    if (txn) {
        LOG_DEBUG << "Transaction started for column family '" << name_ << "', txn_id: " << txn->GetID();
    }
    
    return txn;
}



// ========== 迭代器和管理操作 ==========

Iterator* DefaultColumnFamily::NewIterator() {
    if (!IsActive() || !tx_manager_) {
        return nullptr;
    }
    
    // 创建只读事务来获取MVCC快照读迭代器
    TxnOptions opts; // RC read-only
    auto readonly_txn = tx_manager_->Begin(opts);
    if (!readonly_txn) {
        return nullptr;
    }
    
    // 使用事务的迭代器实现MVCC快照读
    auto iterator = readonly_txn->NewIterator();
    if (!iterator) {
        return nullptr;
    }
    
    // 返回迭代器（事务的生命周期由迭代器管理）
    return iterator.release();
}

Iterator* DefaultColumnFamily::NewIterator(uint64_t sequence) {
    if (!IsActive() || !tx_manager_) {
        return nullptr;
    }
    
    // 创建基于指定 LSM 快照序列的只读事务
    TxnOptions opts; // RC read-only
    auto readonly_txn = tx_manager_->BeginReadOnlyWithLSMSnapshot(opts, sequence);
    if (!readonly_txn) {
        return nullptr;
    }
    
    // 使用事务的迭代器实现指定序列号的快照读
    auto iterator = readonly_txn->NewIterator();
    if (!iterator) {
        return nullptr;
    }
    
    // 返回迭代器（快照和事务的生命周期由迭代器管理）
    return iterator.release();
}

Status DefaultColumnFamily::Flush() {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }
    
    if (!lsm_tree_) {
        return Status::IOError("LSM tree not available");
    }
    
    return lsm_tree_->TriggerFlush();
}

Status DefaultColumnFamily::CompactRange(const Slice* begin, const Slice* end) {
    if (!IsActive()) {
        return Status::InvalidArgument("Column family is not active");
    }
    
    if (!lsm_tree_) {
        return Status::IOError("LSM tree not available");
    }
    
    return lsm_tree_->CompactRange(begin, end);
}



Status DefaultColumnFamily::UpdateOptions(const ColumnFamilyOptions& new_options) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 只允许更新某些安全的选项
    if (new_options.comparator != options_.comparator) {
        return Status::InvalidArgument("Cannot change comparator of existing column family");
    }
    
    options_ = new_options;
    return Status::OK();
}

// ========== 生命周期管理 ==========

void DefaultColumnFamily::Ref() {
    ref_count_.fetch_add(1);
}

void DefaultColumnFamily::Unref() {
    if (ref_count_.fetch_sub(1) == 1) {
        // 引用计数归零，标记为非活跃
        active_.store(false);
    }
}

int DefaultColumnFamily::RefCount() const {
    return ref_count_.load();
}


// ColumnFamilyHandle实现已移至头文件

// 列族管理器实现
ColumnFamilyManager::ColumnFamilyManager(const std::string& db_path, const DBOptions& db_options)
    : db_path_(db_path), db_options_(db_options), next_column_family_id_(1), initialized_(false) {
    
    // 创建MANIFEST管理器
    manifest_manager_ = manifest_util::CreateManifestManager(db_path_);
}

ColumnFamilyManager::~ColumnFamilyManager() {
    // 关闭MANIFEST管理器
    if (manifest_manager_) {
        manifest_manager_->Shutdown();
    }
}

Status ColumnFamilyManager::Initialize() {
    if (initialized_.load()) {
        return Status::OK();
    }
    
    try {
        // 1. 初始化MANIFEST管理器
        if (manifest_manager_) {
            Status manifest_status = manifest_manager_->Initialize();
            if (!manifest_status.ok()) {
                LOG_ERROR << "Failed to initialize MANIFEST manager: " << manifest_status.ToString();
                return manifest_status;
            }
            LOG_INFO << "MANIFEST manager initialized successfully";
        }
        
        // 2. 恢复列族信息
        Status recovery_status = RecoverColumnFamilies();
        if (!recovery_status.ok()) {
            LOG_ERROR << "Failed to recover column families: " << recovery_status.ToString();
            return recovery_status;
        }
        
        // 3. 如果没有列族，创建默认列族
        if (column_families_by_id_.empty()) {
            Status s = CreateDefaultColumnFamily();
            if (!s.ok()) {
                return s;
            }
        }
        
        initialized_.store(true);
        LOG_INFO << "ColumnFamilyManager initialized successfully with " 
                 << column_families_by_id_.size() << " column families";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to initialize ColumnFamilyManager: " + std::string(e.what()));
    }
}

Status ColumnFamilyManager::Shutdown() {
    if (!initialized_.load()) {
        return Status::OK();
    }

    try {
        // 关闭所有列族（刷盘并释放资源）
        std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
        for (const auto& [cf_id, cf] : column_families_by_id_) {
            if (cf) {
                Status shutdown_status = cf->Shutdown();
                if (!shutdown_status.ok()) {
                    LOG_WARN << "Failed to shutdown column family " << cf_id
                             << ": " << shutdown_status.ToString();
                }
            }
        }

        // 持久化列族信息
        Status s = PersistColumnFamilyInfo();
        if (!s.ok()) {
            return s;
        }

        initialized_.store(false);
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::IOError("Failed to shutdown ColumnFamilyManager: " + std::string(e.what()));
    }
}

Status ColumnFamilyManager::CreateColumnFamily(const ColumnFamilyOptions& options, 
                                             const std::string& name, ColumnFamily** handle) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ColumnFamilyManager not initialized");
    }
    
    if (name.empty()) {
        return Status::InvalidArgument("Column family name cannot be empty");
    }
    
    std::unique_lock<std::shared_mutex> lock(column_families_mutex_);
    
    // 检查名称是否已存在
    if (column_families_by_name_.find(name) != column_families_by_name_.end()) {
        return Status::InvalidArgument("Column family already exists: " + name);
    }
    
    // 分配新的列族ID
    uint32_t id = AllocateColumnFamilyID();
    
    // 创建列族（使用DefaultColumnFamily）
    auto column_family = std::make_unique<DefaultColumnFamily>(id, name, options, db_path_);
    if (!column_family) {
        return Status::IOError("Failed to create column family: " + name);
    }
    
    // 记录到MANIFEST文件
    if (manifest_manager_) {
        ColumnFamilyDescriptor cf_desc(id, name, options);
        Status manifest_status = manifest_manager_->LogCreateColumnFamily(cf_desc);
        if (!manifest_status.ok()) {
            LOG_ERROR << "Failed to log column family creation to MANIFEST: " << manifest_status.ToString();
            return manifest_status;
        }
        
        // 同步MANIFEST文件
        manifest_manager_->Sync();
    }
    
    // 添加到管理器中
    ColumnFamily* cf_ptr = column_family.get();
    column_families_by_id_[id] = std::move(column_family);
    column_families_by_name_[name] = cf_ptr;
    
    if (handle) {
        *handle = cf_ptr;
    }
    
    LOG_INFO << "Created column family: " << name << " (ID: " << id << ")";
    return Status::OK();
}

Status ColumnFamilyManager::DropColumnFamily(ColumnFamily* column_family) {
    if (!column_family) {
        return Status::InvalidArgument("Column family is null");
    }
    
    return DropColumnFamily(column_family->GetName());
}

Status ColumnFamilyManager::DropColumnFamily(const std::string& name) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ColumnFamilyManager not initialized");
    }
    
    if (name == column_family_util::kDefaultColumnFamilyName) {
        return Status::InvalidArgument("Cannot drop default column family");
    }
    
    std::unique_lock<std::shared_mutex> lock(column_families_mutex_);
    
    auto it = column_families_by_name_.find(name);
    if (it == column_families_by_name_.end()) {
        return Status::NotFound("Column family not found: " + name);
    }
    
    ColumnFamily* cf = it->second;
    uint32_t id = cf->GetID();
    
    // 先记录到MANIFEST文件
    if (manifest_manager_) {
        Status manifest_status = manifest_manager_->LogDropColumnFamily(id);
        if (!manifest_status.ok()) {
            LOG_ERROR << "Failed to log column family drop to MANIFEST: " << manifest_status.ToString();
            return manifest_status;
        }
        
        // 同步MANIFEST文件
        manifest_manager_->Sync();
    }
    
    // 关闭列族
    Status shutdown_status = cf->Shutdown();
    if (!shutdown_status.ok()) {
        LOG_WARN << "Failed to shutdown column family " << name << ": " << shutdown_status.ToString();
    }
    
    // 从管理器中移除
    column_families_by_name_.erase(it);
    column_families_by_id_.erase(id);
    
    LOG_INFO << "Dropped column family: " << name << " (ID: " << id << ")";
    return Status::OK();
}

ColumnFamily* ColumnFamilyManager::GetColumnFamily(uint32_t id) const {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    auto it = column_families_by_id_.find(id);
    if (it != column_families_by_id_.end()) {
        return it->second.get();
    }
    
    return nullptr;
}

ColumnFamily* ColumnFamilyManager::GetColumnFamily(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    auto it = column_families_by_name_.find(name);
    if (it != column_families_by_name_.end()) {
        return it->second;
    }
    
    return nullptr;
}

ColumnFamily* ColumnFamilyManager::GetDefaultColumnFamily() const {
    return default_column_family_;
}

std::vector<std::string> ColumnFamilyManager::ListColumnFamilies() const {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    std::vector<std::string> names;
    names.reserve(column_families_by_name_.size());
    
    for (const auto& pair : column_families_by_name_) {
        names.push_back(pair.first);
    }
    
    return names;
}

std::vector<ColumnFamily*> ColumnFamilyManager::GetAllColumnFamilies() const {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    std::vector<ColumnFamily*> column_families;
    column_families.reserve(column_families_by_id_.size());
    
    for (const auto& pair : column_families_by_id_) {
        column_families.push_back(pair.second.get());
    }
    
    return column_families;
}

size_t ColumnFamilyManager::GetColumnFamilyCount() const {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    return column_families_by_id_.size();
}

Status ColumnFamilyManager::UpdateColumnFamilyOptions(ColumnFamily* column_family, 
                                                   const ColumnFamilyOptions& new_options) {
    if (!column_family) {
        return Status::InvalidArgument("Column family is null");
    }
    
    // 更新列族选项
    return column_family->UpdateOptions(new_options);
}

Status ColumnFamilyManager::PersistColumnFamilyInfo() {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    try {
        // 创建列族信息文件路径
        std::string manifest_dir = db_path_ + "/manifest";
        if (!std::filesystem::exists(manifest_dir)) {
            std::filesystem::create_directories(manifest_dir);
        }
        
        std::string cf_info_file = manifest_dir + "/column_families.info";
        
        // 打开文件进行写入
        std::ofstream file(cf_info_file, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            return Status::IOError("Failed to open column family info file for writing");
        }
        
        // 写入列族数量
        uint32_t cf_count = static_cast<uint32_t>(column_families_by_id_.size());
        file.write(reinterpret_cast<const char*>(&cf_count), sizeof(cf_count));
        
        // 写入每个列族的信息
        for (const auto& pair : column_families_by_id_) {
            const ColumnFamily* cf = pair.second.get();
            if (!cf) continue;
            
            // 创建列族描述符
            ColumnFamilyDescriptor cf_desc(cf->GetID(), cf->GetName(), cf->GetOptions());
            
            // 序列化并写入
            std::string encoded = cf_desc.Encode();
            uint32_t size = static_cast<uint32_t>(encoded.size());
            file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            file.write(encoded.data(), encoded.size());
        }
        
        file.close();
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to persist column family info: " + std::string(e.what()));
    }
}

Status ColumnFamilyManager::LoadColumnFamilyInfo() {
    try {
        // 创建列族信息文件路径
        std::string manifest_dir = db_path_ + "/manifest";
        std::string cf_info_file = manifest_dir + "/column_families.info";
        
        // 检查文件是否存在
        if (!std::filesystem::exists(cf_info_file)) {
            // 文件不存在，这是正常的（首次启动）
            return Status::OK();
        }
        
        // 打开文件进行读取
        std::ifstream file(cf_info_file, std::ios::binary);
        if (!file.is_open()) {
            return Status::IOError("Failed to open column family info file for reading");
        }
        
        // 读取列族数量
        uint32_t cf_count;
        file.read(reinterpret_cast<char*>(&cf_count), sizeof(cf_count));
        if (file.eof() || file.fail()) {
            return Status::Corruption("Failed to read column family count");
        }
        
        // 读取每个列族的信息
        for (uint32_t i = 0; i < cf_count; ++i) {
            // 读取序列化数据大小
            uint32_t size;
            file.read(reinterpret_cast<char*>(&size), sizeof(size));
            if (file.eof() || file.fail()) {
                return Status::Corruption("Failed to read column family data size");
            }
            
            // 读取序列化数据
            std::string encoded(size, '\0');
            file.read(&encoded[0], size);
            if (file.eof() || file.fail()) {
                return Status::Corruption("Failed to read column family data");
            }
            
            // 反序列化列族描述符
            ColumnFamilyDescriptor cf_desc;
            Status s = cf_desc.Decode(Slice(encoded));
            if (!s.ok()) {
                return Status::Corruption("Failed to decode column family descriptor: " + s.ToString());
            }
            
            // 创建列族（这里只是加载信息，不实际创建列族实例）
            // 实际的列族创建会在LoadExistingColumnFamilies中进行
            loaded_column_families_.push_back(cf_desc);
        }
        
        file.close();
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to load column family info: " + std::string(e.what()));
    }
}

ColumnFamilyManager::ManagerStats ColumnFamilyManager::GetManagerStats() const {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    ManagerStats stats{};
    stats.total_column_families = column_families_by_id_.size();
    stats.active_column_families = column_families_by_id_.size();
    
    // 统计所有列族的数据
    for (const auto& pair : column_families_by_id_) {
        const ColumnFamily* cf = pair.second.get();
        if (cf) {
            // 获取列族统计信息（简化实现）
            stats.total_entries += 1000;  // 示例值
            stats.total_size_bytes += 1024 * 1024;  // 示例值
            stats.total_write_count += 500;  // 示例值
            stats.total_read_count += 1000;  // 示例值
        }
    }
    
    return stats;
}

Status ColumnFamilyManager::ValidateColumnFamily(ColumnFamily* column_family) const {
    if (!column_family) {
        return Status::InvalidArgument("Column family is null");
    }
    
    // 检查列族是否在管理器中
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    for (const auto& pair : column_families_by_id_) {
        if (pair.second.get() == column_family) {
            return Status::OK();
        }
    }
    
    return Status::InvalidArgument("Column family not managed by this manager");
}

uint32_t ColumnFamilyManager::AllocateColumnFamilyID() {
    return next_column_family_id_.fetch_add(1);
}

Status ColumnFamilyManager::CreateDefaultColumnFamily() {
    // 创建默认列族
    ColumnFamilyOptions default_options;
    default_options.comparator = BytewiseComparator(); // 设置默认比较器
    auto default_cf = std::make_unique<DefaultColumnFamily>(
        column_family_util::kDefaultColumnFamilyId, 
        column_family_util::kDefaultColumnFamilyName, 
        default_options,
        db_path_);
    if (!default_cf) {
        return Status::IOError("Failed to create default column family");
    }
    
    // 添加到管理器中
    ColumnFamily* cf_ptr = default_cf.get();
    column_families_by_id_[column_family_util::kDefaultColumnFamilyId] = std::move(default_cf);
    column_families_by_name_[column_family_util::kDefaultColumnFamilyName] = cf_ptr;

    // 保存默认列族指针
    default_column_family_ = cf_ptr;

    // 持久化到MANIFEST，保证重开数据库时能恢复默认列族
    if (manifest_manager_) {
        ColumnFamilyDescriptor default_desc;
        default_desc.id = column_family_util::kDefaultColumnFamilyId;
        default_desc.name = column_family_util::kDefaultColumnFamilyName;
        default_desc.options = default_options;
        default_desc.is_dropped = false;

        Status manifest_status = manifest_manager_->LogCreateColumnFamily(default_desc);
        if (!manifest_status.ok()) {
            LOG_WARN << "Failed to persist default column family to MANIFEST: "
                     << manifest_status.ToString();
        }
    }

    return Status::OK();
}

Status ColumnFamilyManager::LoadExistingColumnFamilies() {
    std::unique_lock<std::shared_mutex> lock(column_families_mutex_);
    
    try {
        // 首先加载列族信息
        Status s = LoadColumnFamilyInfo();
        if (!s.ok()) {
            return s;
        }
        
        // 根据加载的描述符创建列族实例
        for (const auto& cf_desc : loaded_column_families_) {
            // 跳过已删除的列族
            if (cf_desc.is_dropped) {
                continue;
            }

            // 反序列化的选项比较器指针为空，兜底为默认比较器
            ColumnFamilyOptions cf_options = cf_desc.options;
            if (!cf_options.comparator) {
                cf_options.comparator = BytewiseComparator();
            }

            // 创建列族实例
            auto cf = std::make_unique<DefaultColumnFamily>(
                cf_desc.id, cf_desc.name, cf_options, db_path_);
            
            if (!cf) {
                return Status::IOError("Failed to create column family: " + cf_desc.name);
            }
            
            // 初始化列族
            Status init_s = cf->Initialize();
            if (!init_s.ok()) {
                return Status::IOError("Failed to initialize column family " + cf_desc.name + 
                                     ": " + init_s.ToString());
            }
            
            // 添加到管理器中
            ColumnFamily* cf_ptr = cf.get();
            column_families_by_id_[cf_desc.id] = std::move(cf);
            column_families_by_name_[cf_desc.name] = cf_ptr;
            
            // 如果是默认列族，保存指针
            if (cf_desc.id == column_family_util::kDefaultColumnFamilyId) {
                default_column_family_ = cf_ptr;
            }
            
            // 更新下一个列族ID
            if (cf_desc.id >= next_column_family_id_.load()) {
                next_column_family_id_.store(cf_desc.id + 1);
            }
        }
        
        // 如果没有默认列族，创建一个
        if (!default_column_family_) {
            Status s = CreateDefaultColumnFamily();
            if (!s.ok()) {
                return s;
            }
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to load existing column families: " + std::string(e.what()));
    }
}

Status ColumnFamilyManager::SaveColumnFamilyManifest() {
    std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
    
    try {
        // 创建manifest目录
        std::string manifest_dir = db_path_ + "/manifest";
        if (!std::filesystem::exists(manifest_dir)) {
            std::filesystem::create_directories(manifest_dir);
        }
        
        // 生成manifest文件名
        std::string manifest_file = GetColumnFamilyManifestPath();
        
        // 创建manifest写入器
        ManifestWriter writer(manifest_file);
        Status s = writer.Open();
        if (!s.ok()) {
            return Status::IOError("Failed to open manifest file for writing: " + s.ToString());
        }
        
        // 写入每个列族的信息
        for (const auto& pair : column_families_by_id_) {
            const ColumnFamily* cf = pair.second.get();
            if (!cf) continue;
            
            // 创建列族描述符
            ColumnFamilyDescriptor cf_desc(cf->GetID(), cf->GetName(), cf->GetOptions());
            
            // 创建manifest记录
            ManifestRecord record = ManifestRecord::CreateColumnFamilyRecord(
                cf->GetID(), cf_desc);
            
            // 写入记录
            s = writer.WriteRecord(record);
            if (!s.ok()) {
                return Status::IOError("Failed to write column family record: " + s.ToString());
            }
        }
        
        // 同步文件
        s = writer.Sync();
        if (!s.ok()) {
            return Status::IOError("Failed to sync manifest file: " + s.ToString());
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to save column family manifest: " + std::string(e.what()));
    }
}

Status ColumnFamilyManager::LoadColumnFamilyManifest() {
    try {
        // 获取manifest文件路径
        std::string manifest_file = GetColumnFamilyManifestPath();
        
        // 检查文件是否存在
        if (!std::filesystem::exists(manifest_file)) {
            // 文件不存在，这是正常的（首次启动）
            return Status::OK();
        }
        
        // 创建manifest读取器
        ManifestReader reader(manifest_file);
        Status s = reader.Open();
        if (!s.ok()) {
            return Status::IOError("Failed to open manifest file for reading: " + s.ToString());
        }
        
        // 读取所有记录
        ManifestRecord record;
        while (reader.ReadNextRecord(&record).ok()) {
            // 处理不同类型的记录
            switch (record.type) {
                case ManifestRecordType::kCreateColumnFamily: {
                    // 解析列族描述符
                    ColumnFamilyDescriptor cf_desc;
                    s = cf_desc.Decode(Slice(record.payload));
                    if (!s.ok()) {
                        return Status::Corruption("Failed to decode column family descriptor: " + s.ToString());
                    }
                    
                    // 添加到加载的列族列表
                    loaded_column_families_.push_back(cf_desc);
                    break;
                }
                case ManifestRecordType::kDropColumnFamily: {
                    // 标记列族为已删除
                    // 这里需要解析列族ID并标记对应的列族
                    break;
                }
                case ManifestRecordType::kUpdateColumnFamilyOptions: {
                    // 更新列族选项
                    // 这里需要解析并更新对应列族的选项
                    break;
                }
                default:
                    // 忽略其他类型的记录
                    break;
            }
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to load column family manifest: " + std::string(e.what()));
    }
}

std::string ColumnFamilyManager::GetColumnFamilyManifestPath() const {
    return db_path_ + "/CURRENT";
}

Status DefaultColumnFamily::Shutdown() {
    try {
        if (!active_.load()) {
            return Status::OK(); // Already shut down
        }
        
        // 标记为非活跃
        active_.store(false);
        
        // 关闭LSMTree
        if (lsm_tree_) {
            Status lsm_status = lsm_tree_->Close();
            if (!lsm_status.ok()) {
                LOG_WARN << "Failed to close LSMTree for column family " << name_ 
                         << ": " << lsm_status.ToString();
            }
        }
        
        // 关闭WAL管理器
        if (wal_manager_) {
            Status wal_status = wal_manager_->Shutdown();
            if (!wal_status.ok()) {
                LOG_WARN << "Failed to close WAL manager for column family " << name_ 
                         << ": " << wal_status.ToString();
            }
        }
        
        // 新事务层无并发管理器可关闭
        
        LOG_INFO << "Column family " << name_ << " shutdown completed";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during column family shutdown: " + std::string(e.what()));
    }
}

// ============================================================================
// 恢复相关方法实现
// ============================================================================

Status ColumnFamilyManager::RecoverColumnFamilies() {
    if (!manifest_manager_) {
        LOG_WARN << "No MANIFEST manager available, creating default column family only";
        return Status::OK();
    }
    
    try {
        // 1. 从MANIFEST文件恢复列族描述符
        std::vector<ColumnFamilyDescriptor> cf_descriptors;
        uint64_t last_sequence = 0;
        
        Status recovery_status = manifest_manager_->RecoverColumnFamilies(&cf_descriptors, &last_sequence);
        if (!recovery_status.ok()) {
            LOG_ERROR << "Failed to recover column families from MANIFEST: " << recovery_status.ToString();
            return recovery_status;
        }
        
        LOG_INFO << "Recovered " << cf_descriptors.size() << " column family descriptors from MANIFEST";
        
        // 如果没有恢复到任何列族，创建默认列族（首次启动）
        if (cf_descriptors.empty()) {
            LOG_INFO << "No column families found in MANIFEST, creating default column family";
            
            ColumnFamilyDescriptor default_cf_desc;
            default_cf_desc.id = 0;
            default_cf_desc.name = "default";
            default_cf_desc.options = ColumnFamilyOptions(); // 使用默认选项
            default_cf_desc.options.comparator = BytewiseComparator(); // 设置默认比较器
            default_cf_desc.is_dropped = false;
            
            cf_descriptors.push_back(default_cf_desc);
        }
        
        // 2. 重建列族对象
        for (const auto& cf_desc : cf_descriptors) {
            if (cf_desc.is_dropped) {
                LOG_INFO << "Skipping dropped column family: " << cf_desc.name << " (ID: " << cf_desc.id << ")";
                continue;
            }

            // 比较器指针无法持久化，反序列化后可能为空，这里兜底为默认比较器
            ColumnFamilyOptions cf_options = cf_desc.options;
            if (!cf_options.comparator) {
                cf_options.comparator = BytewiseComparator();
            }

            // 创建列族实例
            auto cf = std::make_unique<DefaultColumnFamily>(
                cf_desc.id, cf_desc.name, cf_options, db_path_);
            
            Status init_status = cf->Initialize();
            if (!init_status.ok()) {
                LOG_ERROR << "Failed to initialize recovered column family " << cf_desc.name 
                         << ": " << init_status.ToString();
                return init_status;
            }
            
            // 将列族添加到映射中
            ColumnFamily* cf_ptr = cf.get();
            column_families_by_id_[cf_desc.id] = std::move(cf);
            column_families_by_name_[cf_desc.name] = cf_ptr;
            
            // 如果这是默认列族，特别设置
            if (cf_desc.name == "default" || cf_desc.id == 0) {
                default_column_family_ = cf_ptr;
            }
            
            // 更新下一个列族ID
            next_column_family_id_.store(std::max(next_column_family_id_.load(), cf_desc.id + 1));
            
            LOG_INFO << "Successfully recovered column family: " << cf_desc.name 
                     << " (ID: " << cf_desc.id << ")";
        }
        
        // 3. 验证恢复的列族
        Status validation_status = ValidateAllColumnFamilies();
        if (!validation_status.ok()) {
            LOG_ERROR << "Column family validation failed after recovery: " << validation_status.ToString();
            return validation_status;
        }
        
        LOG_INFO << "Column family recovery completed successfully. Total: " << column_families_by_id_.size();
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during column family recovery: " + std::string(e.what()));
    }
}

Status ColumnFamilyManager::ValidateAllColumnFamilies() const {
    try {
        std::shared_lock<std::shared_mutex> lock(column_families_mutex_);
        
        // 验证每个列族的一致性
        for (const auto& [cf_id, cf] : column_families_by_id_) {
            if (!cf) {
                return Status::Corruption("Column family " + std::to_string(cf_id) + " is null");
            }
            
            Status cf_status = cf->ValidateConsistency();
            if (!cf_status.ok()) {
                return Status::Corruption("Column family " + std::to_string(cf_id) + 
                                        " validation failed: " + cf_status.ToString());
            }
        }
        
        // 验证默认列族存在
        if (!default_column_family_) {
            return Status::Corruption("Default column family is missing");
        }
        
        // 验证映射一致性
        if (column_families_by_id_.size() != column_families_by_name_.size()) {
            return Status::Corruption("Column family mappings are inconsistent");
        }
        
        // 验证ID和名称映射的一致性
        for (const auto& [cf_name, cf_ptr] : column_families_by_name_) {
            if (!cf_ptr) {
                return Status::Corruption("Column family pointer for " + cf_name + " is null");
            }
            
            auto id_it = column_families_by_id_.find(cf_ptr->GetID());
            if (id_it == column_families_by_id_.end() || id_it->second.get() != cf_ptr) {
                return Status::Corruption("Column family mapping inconsistency for " + cf_name);
            }
        }
        
        LOG_INFO << "All " << column_families_by_id_.size() << " column families passed validation";
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during column family validation: " + std::string(e.what()));
    }
}

// 列族工具函数实现
namespace column_family_util {

std::unique_ptr<ColumnFamilyManager> CreateColumnFamilyManager(
    const std::string& db_path, const DBOptions& db_options) {
    return std::make_unique<ColumnFamilyManager>(db_path, db_options);
}

ColumnFamilyOptions CreateDefaultColumnFamilyOptions() {
    return ColumnFamilyOptions{};
}

Status ValidateColumnFamilyName(const std::string& name) {
    if (name.empty()) {
        return Status::InvalidArgument("Column family name cannot be empty");
    }
    
    // 检查是否包含非法字符
    if (name.find('/') != std::string::npos || name.find('\\') != std::string::npos) {
        return Status::InvalidArgument("Column family name contains illegal characters");
    }
    
    return Status::OK();
}

const std::string kDefaultColumnFamilyName = "default";
const uint32_t kDefaultColumnFamilyId = 0;

}  // namespace column_family_util


} // namespace lrdb