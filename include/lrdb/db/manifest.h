// MANIFEST文件管理 - 数据库元数据持久化

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <fstream>

#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include "lrdb/db/options.h"

namespace lrdb {

// 前向声明
class ColumnFamily;

// ============================================================================
// MANIFEST记录类型
// ============================================================================

enum class ManifestRecordType : uint8_t {
    kCreateColumnFamily = 1,
    kDropColumnFamily = 2,
    kUpdateColumnFamilyOptions = 3,
    kCreateDatabase = 4,
    kSequenceNumber = 5,
    kCheckpoint = 6
};

// ============================================================================
// 列族描述符（用于持久化）
// ============================================================================

struct ColumnFamilyDescriptor {
    uint32_t id;
    std::string name;
    ColumnFamilyOptions options;
    bool is_dropped;
    uint64_t create_sequence;
    uint64_t drop_sequence;
    
    ColumnFamilyDescriptor() : id(0), is_dropped(false), create_sequence(0), drop_sequence(0) {}
    
    ColumnFamilyDescriptor(uint32_t cf_id, const std::string& cf_name, 
                          const ColumnFamilyOptions& cf_options)
        : id(cf_id), name(cf_name), options(cf_options), 
          is_dropped(false), create_sequence(0), drop_sequence(0) {}
    
    // 序列化和反序列化
    std::string Encode() const;
    Status Decode(const Slice& data);
};

// ============================================================================
// MANIFEST记录
// ============================================================================

struct ManifestRecord {
    ManifestRecordType type;
    uint64_t sequence_number;
    std::string payload;  // 序列化后的数据
    uint32_t crc;
    
    ManifestRecord() : type(ManifestRecordType::kCreateDatabase), sequence_number(0), crc(0) {}
    
    // 工厂方法
    static ManifestRecord CreateColumnFamilyRecord(uint64_t sequence, const ColumnFamilyDescriptor& cf_desc);
    static ManifestRecord DropColumnFamilyRecord(uint64_t sequence, uint32_t cf_id);
    static ManifestRecord UpdateColumnFamilyOptionsRecord(uint64_t sequence, uint32_t cf_id, const ColumnFamilyOptions& options);
    static ManifestRecord SequenceNumberRecord(uint64_t sequence);
    static ManifestRecord CheckpointRecord(uint64_t sequence);
    
    // 序列化和反序列化
    std::string Encode() const;
    Status Decode(const Slice& data);
    
    // CRC计算和验证
    uint32_t CalculateCRC() const;
    bool ValidateCRC() const;
};

// ============================================================================
// MANIFEST文件写入器
// ============================================================================

class ManifestWriter {
public:
    explicit ManifestWriter(const std::string& filename);
    ~ManifestWriter();
    
    // 打开文件
    Status Open();
    
    // 关闭文件
    Status Close();
    
    // 写入记录
    Status WriteRecord(const ManifestRecord& record);
    
    // 强制刷盘
    Status Sync();
    
    // 获取文件大小
    uint64_t GetFileSize() const;
    
    // 检查是否已打开
    bool IsOpen() const;

private:
    std::string filename_;
    std::unique_ptr<std::ofstream> file_;
    std::atomic<uint64_t> file_size_;
    std::atomic<bool> is_open_;
    mutable std::mutex mutex_;
    
    // 禁止拷贝
    ManifestWriter(const ManifestWriter&) = delete;
    ManifestWriter& operator=(const ManifestWriter&) = delete;
};

// ============================================================================
// MANIFEST文件读取器
// ============================================================================

class ManifestReader {
public:
    explicit ManifestReader(const std::string& filename);
    ~ManifestReader();
    
    // 打开文件
    Status Open();
    
    // 关闭文件
    Status Close();
    
    // 读取下一条记录
    Status ReadNextRecord(ManifestRecord* record);
    
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
    
    // 禁止拷贝
    ManifestReader(const ManifestReader&) = delete;
    ManifestReader& operator=(const ManifestReader&) = delete;
};

// ============================================================================
// MANIFEST管理器
// ============================================================================

class ManifestManager {
public:
    explicit ManifestManager(const std::string& db_path);
    ~ManifestManager();
    
    // 初始化和关闭
    Status Initialize();
    Status Shutdown();
    
    // 列族操作记录
    Status LogCreateColumnFamily(const ColumnFamilyDescriptor& cf_desc);
    Status LogDropColumnFamily(uint32_t cf_id);
    Status LogUpdateColumnFamilyOptions(uint32_t cf_id, const ColumnFamilyOptions& options);
    
    // 序列号记录
    Status LogSequenceNumber(uint64_t sequence);
    
    // 检查点记录
    Status LogCheckpoint(uint64_t sequence);
    
    // 强制刷盘
    Status Sync();
    
    // 恢复列族信息
    Status RecoverColumnFamilies(std::vector<ColumnFamilyDescriptor>* cf_descriptors, 
                                uint64_t* last_sequence);
    
    // 切换到新的MANIFEST文件
    Status SwitchToNewManifest();
    
    // 清理旧的MANIFEST文件
    Status CleanupOldManifests(int keep_count = 3);
    
    // 获取当前MANIFEST文件大小
    uint64_t GetCurrentManifestSize() const;

private:
    std::string db_path_;
    std::string current_manifest_filename_;
    std::unique_ptr<ManifestWriter> current_writer_;
    
    std::atomic<uint64_t> next_file_number_;
    std::atomic<uint64_t> last_sequence_;
    std::atomic<bool> initialized_;
    std::atomic<bool> shutdown_;
    
    mutable std::mutex manifest_mutex_;
    
    // 内部方法
    Status CreateNewManifestFile();
    std::string GenerateManifestFilename(uint64_t file_number) const;
    Status UpdateCurrentFile(const std::string& manifest_filename);
    Status RecoverFromManifestFile(const std::string& filename,
                                  std::unordered_map<uint32_t, ColumnFamilyDescriptor>* cf_map,
                                  uint64_t* max_sequence);
    
    // 禁止拷贝
    ManifestManager(const ManifestManager&) = delete;
    ManifestManager& operator=(const ManifestManager&) = delete;
};

// ============================================================================
// 工具函数
// ============================================================================

namespace manifest_util {

// 创建MANIFEST管理器
std::unique_ptr<ManifestManager> CreateManifestManager(const std::string& db_path);

// 编码/解码辅助函数
std::string EncodeColumnFamilyOptions(const ColumnFamilyOptions& options);
Status DecodeColumnFamilyOptions(const Slice& data, ColumnFamilyOptions* options);

// MANIFEST文件名解析
Status ParseManifestFilename(const std::string& filename, uint64_t* file_number);
std::string FormatManifestFilename(uint64_t file_number);

// 文件操作工具
Status CopyManifestFile(const std::string& src, const std::string& dst);

}  // namespace manifest_util

}  // namespace lrdb
