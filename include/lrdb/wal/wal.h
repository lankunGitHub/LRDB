// WAL(Write-Ahead Log)预写日志实现

#pragma once

#include <memory>
#include <atomic>
#include <vector>
#include <string>
#include <fstream>
#include <mutex>
#include <functional>
#include <fcntl.h>
#include <unistd.h>
#include "lrdb/core/status.h"
#include "lrdb/core/slice.h"
#include "lrdb/concurrency/mvcc.h"
#include "lrdb/storage/lsm_tree.h"
#include "lrdb/concurrency/mvcc.h"  // 包含 TransactionID 定义
#include "lrdb/background/background_manager.h"  // 包含 BackgroundTaskManager 定义

namespace lrdb {

// 前向声明
// 事务状态枚举
enum class WALTransactionState : uint8_t {
    kBegun = 1,
    kCommitted = 2,
    kAborted = 3
};


// WAL记录类型
enum class WALRecordType : uint8_t {
    kPut = 1,
    kDelete = 2,
    kMerge = 3,
    kBeginTransaction = 4,
    kCommitTransaction = 5,
    kAbortTransaction = 6,
    kCheckpoint = 7,
    kFlushMemTable = 8,
    kCreateColumnFamily = 9,
    kDropColumnFamily = 10,
    kUpdateColumnFamilyOptions = 11,
};

// 版本化条目 - 用于批量写入
struct VersionedEntry {
    WALRecordType type;
    std::string key;
    std::string value;
    SequenceNumber sequence_number;
    uint64_t transaction_id;
    bool is_committed;
    
    VersionedEntry() = default;
    VersionedEntry(WALRecordType t, const std::string& k, const std::string& v, 
                   SequenceNumber seq, uint64_t txn_id = 0, bool committed = true)
        : type(t), key(k), value(v), sequence_number(seq), 
          transaction_id(txn_id), is_committed(committed) {}
};

// WAL记录 - 支持列族隔离
struct WALRecord {
    WALRecordType type;
    SequenceNumber sequence_number;
    uint32_t column_family_id;  // 列族ID - 核心添加
    std::string key;
    std::string value;
    uint64_t transaction_id;
    uint32_t crc;
    
    WALRecord() : type(WALRecordType::kPut), sequence_number(0), column_family_id(0), transaction_id(0), crc(0) {}
    
    // 序列化和反序列化
    std::string Encode() const;
    Status Decode(const Slice& data);
    
    // 计算CRC
    uint32_t CalculateCRC() const;
    bool ValidateCRC() const;
};

// WAL写入器
class WALWriter {
public:
    explicit WALWriter(const std::string& filename);
    ~WALWriter();
    
    // 打开文件
    Status Open();
    
    // 关闭文件
    Status Close();
    
    // 写入记录
    Status WriteRecord(const WALRecord& record);
    
    // 批量写入
    Status WriteRecords(const std::vector<WALRecord>& records);
    
    // 强制刷盘
    Status Sync();
    
    // 获取当前文件大小
    uint64_t GetFileSize() const;
    
    // 检查是否已打开
    bool IsOpen() const;
    
    // 获取写入位置
    uint64_t GetWritePosition() const;

private:
    std::string filename_;
    std::unique_ptr<std::ofstream> file_;
    std::atomic<uint64_t> file_size_;
    std::atomic<uint64_t> write_position_;
    std::atomic<bool> is_open_;
    mutable std::mutex mutex_;
    
    // 内部方法
    Status WriteRecordInternal(const WALRecord& record);
    
    // 禁止拷贝
    WALWriter(const WALWriter&) = delete;
    WALWriter& operator=(const WALWriter&) = delete;
};

// WAL读取器
class WALReader {
public:
    explicit WALReader(const std::string& filename);
    ~WALReader();
    
    // 打开文件
    Status Open();
    
    // 关闭文件
    Status Close();
    
    // 读取下一条记录
    Status ReadNextRecord(WALRecord* record);
    
    // 跳转到指定位置
    Status Seek(uint64_t position);
    
    // 检查是否到达文件末尾
    bool IsEOF() const;
    
    // 获取当前读取位置
    uint64_t GetReadPosition() const;
    
    // 验证文件完整性
    Status ValidateFile();

private:
    std::string filename_;
    std::unique_ptr<std::ifstream> file_;
    std::atomic<uint64_t> read_position_;
    std::atomic<bool> is_open_;
    std::atomic<bool> eof_reached_;
    mutable std::mutex mutex_;
    
    // 内部方法
    Status ReadRecordInternal(WALRecord* record);
    
    // 禁止拷贝
    WALReader(const WALReader&) = delete;
    WALReader& operator=(const WALReader&) = delete;
};

// WAL管理器
// 职责：
// - 写入 WAL：Put/Delete/Merge 以及列族/系统记录；支持批量写与 Sync；
// - 恢复：顺序扫描 wal 与 archive，调用 LSM 恢复接口应用记录，汇总最大序；
// - 清理：基于安全序列与文件序号策略清理老 WAL（当前实现：保留最近3个，预留精确策略）。
class WALManager {
public:
    explicit WALManager(const std::string& db_path);
    ~WALManager();
    
    // 初始化和清理
    Status Initialize();
    Status Shutdown();
    
    // 写入操作 - 支持列族隔离
    Status WritePut(uint32_t column_family_id, const Slice& key, const Slice& value, SequenceNumber sequence);
    Status WriteDelete(uint32_t column_family_id, const Slice& key, SequenceNumber sequence);
    Status WriteMerge(uint32_t column_family_id, const Slice& key, const Slice& value, SequenceNumber sequence);
    
    // 批量写入 - 支持MVCC刷盘和列族
    Status WriteBatch(uint32_t column_family_id, const std::vector<VersionedEntry>& entries);
    
    // 事务操作 - 支持列族隔离
    Status WriteBeginTransaction(uint32_t column_family_id, uint64_t transaction_id, SequenceNumber sequence);
    Status WriteCommitTransaction(uint32_t column_family_id, uint64_t transaction_id, SequenceNumber sequence);
    Status WriteAbortTransaction(uint32_t column_family_id, uint64_t transaction_id, SequenceNumber sequence);
    
    // 列族操作
    Status WriteCreateColumnFamily(uint32_t column_family_id, const std::string& name, 
                                  const std::string& options_data, SequenceNumber sequence);
    Status WriteDropColumnFamily(uint32_t column_family_id, SequenceNumber sequence);
    Status WriteUpdateColumnFamilyOptions(uint32_t column_family_id, 
                                         const std::string& options_data, SequenceNumber sequence);
    
    // 系统操作
    Status WriteCheckpoint(SequenceNumber sequence);
    Status WriteFlushMemTable(SequenceNumber sequence);
    
    // 批量写入

    
    // 强制刷盘
    Status Sync();
    
    // WAL文件管理
    Status CreateNewWALFile();
    Status ArchiveCurrentWAL();
    std::vector<std::string> GetWALFiles() const;
    
    // 恢复操作
    Status RecoverFromWAL(SequenceNumber* last_sequence);
    
    // 清理旧WAL文件
    Status CleanupOldWALFiles(SequenceNumber safe_sequence);
    
    // 获取当前WAL文件大小
    uint64_t GetCurrentWALSize() const;
    
    // 检查是否需要执行后台任务
    bool NeedsSync() const;
    bool NeedsArchive() const;
    bool NeedsCleanup() const;
    
    // 配置管理
    void SetMaxWALSize(uint64_t max_size);
    void SetSyncOnWrite(bool sync_on_write);
    

    
    // 运行时状态查询
    SequenceNumber GetLastSequence() const;
    
    // 暴露给后台任务管理器的接口
    void RegisterSyncTask(std::function<void()> sync_task);
    void RegisterArchiveTask(std::function<void()> archive_task);
    void UnregisterBackgroundTasks();

private:
    std::string db_path_;
    std::string current_wal_filename_;
    std::unique_ptr<WALWriter> current_writer_;
    
    std::atomic<uint64_t> next_file_number_;
    std::atomic<uint64_t> max_wal_size_;
    std::atomic<bool> sync_on_write_;
    std::atomic<bool> initialized_;
    std::atomic<bool> shutdown_;
    
    mutable std::mutex wal_mutex_;
    
    // 统计信息
    mutable std::atomic<uint64_t> total_records_;
    mutable std::atomic<uint64_t> total_bytes_;
    
    // 序列号跟踪
    std::atomic<SequenceNumber> last_sequence_;
    
    // 后台任务回调
    std::function<void()> sync_task_callback_;
    std::function<void()> archive_task_callback_;
    

    
    // LSMTree引用
    LSMTree* lsm_tree_;
    
    // 事务状态跟踪
    std::unordered_map<uint64_t, uint8_t> active_transactions_;  // 使用 uint8_t 表示状态
    mutable std::mutex transaction_mutex_;
    
    // 内部方法
    Status WriteRecordInternal(const WALRecord& record);
    std::string GenerateWALFilename(uint64_t file_number) const;
    Status SwitchToNewWAL();
    bool ShouldSwitchWAL() const;
    

    
    // 恢复辅助方法
    Status RecoverFromWALFile(const std::string& filename, 
                              SequenceNumber* last_sequence,
                              uint64_t* record_count);
    Status RecoverPartialWALFile(const std::string& filename,
                                SequenceNumber* last_sequence,
                                uint64_t* record_count);
    Status ApplyWALRecord(const WALRecord& record);
    
    // 事务一致性和恢复
    Status ValidateTransactionConsistency(const std::unordered_map<uint64_t, TransactionState>& transaction_states);
    Status RollbackIncompleteTransaction(uint64_t transaction_id);
    Status UpdateTransactionState(const WALRecord& record);
    
    // LSMTree集成
    public: void SetLSMTree(LSMTree* lsm_tree);
    public: LSMTree* GetLSMTree() const;
    
    // 禁止拷贝
    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;
};

// 恢复管理器
class RecoveryManager {
public:
    explicit RecoveryManager(const std::string& db_path);
    ~RecoveryManager();
    
    // 执行恢复
    Status RecoverDatabase(SequenceNumber* recovered_sequence);
    
    // 验证数据库一致性
    Status ValidateConsistency();
    
    // 修复损坏的数据
    Status RepairCorruption();
    
    // 创建检查点
    Status CreateCheckpoint(SequenceNumber sequence);
    
    // 恢复到检查点
    Status RestoreFromCheckpoint(SequenceNumber checkpoint_sequence);
    
    // 获取恢复统计
    struct RecoveryStats {
        uint64_t wal_files_processed;
        uint64_t records_recovered;
        uint64_t corrupted_records;
        SequenceNumber recovered_sequence;
        std::chrono::milliseconds recovery_time;
    };
    RecoveryStats GetRecoveryStats() const;

private:
    std::string db_path_;
    RecoveryStats recovery_stats_;
    
    // 内部方法
    Status DiscoverWALFiles(std::vector<std::string>* wal_files);
    Status RecoverFromWALFiles(const std::vector<std::string>& wal_files, 
                              SequenceNumber* last_sequence);
    Status ValidateWALFile(const std::string& filename);
    Status RepairWALFile(const std::string& filename);
    
    // 检查点相关
    Status SaveCheckpoint(SequenceNumber sequence, const std::string& checkpoint_file);
    Status LoadCheckpoint(const std::string& checkpoint_file, SequenceNumber* sequence);
    Status CleanupOldCheckpoints(const std::string& checkpoint_dir, int keep_count);
    
    // 禁止拷贝
    RecoveryManager(const RecoveryManager&) = delete;
    RecoveryManager& operator=(const RecoveryManager&) = delete;
};

// WAL工具函数
namespace wal_util {

// 创建WAL管理器
std::unique_ptr<WALManager> CreateWALManager(const std::string& db_path);

// 创建恢复管理器
std::unique_ptr<RecoveryManager> CreateRecoveryManager(const std::string& db_path);

// WAL文件名解析
Status ParseWALFilename(const std::string& filename, uint64_t* file_number);
std::string FormatWALFilename(uint64_t file_number);

// WAL记录工具
std::string FormatWALRecord(const WALRecord& record);
Status ValidateWALRecord(const WALRecord& record);

// 文件操作工具
Status CopyWALFile(const std::string& src, const std::string& dst);
Status CompressWALFile(const std::string& filename);
Status DecompressWALFile(const std::string& compressed_filename);

}  // namespace wal_util

}  // namespace lrdb