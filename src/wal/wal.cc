// WAL预写日志实现

#include "lrdb/wal/wal.h"
#include "lrdb/core/coding.h"
#include "lrdb/util/error_handler.h"

#include "lrdb/util/logging.h"
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <unistd.h>  // 为 getpid() 函数
#include <set>
#include <thread>  // 添加thread头文件
#include <cstring>  // 添加strerror头文件

namespace lrdb {

// WALRecord实现
std::string WALRecord::Encode() const {
    std::string result;
    
    // 记录头: type(1) + sequence(8) + column_family_id(4) + transaction_id(8) + key_len(4) + value_len(4)
    result.push_back(static_cast<char>(type));
    coding::PutFixed64(&result, sequence_number);
    coding::PutFixed32(&result, column_family_id);  // 新增列族ID
    coding::PutFixed64(&result, transaction_id);
    coding::PutFixed32(&result, static_cast<uint32_t>(key.size()));
    coding::PutFixed32(&result, static_cast<uint32_t>(value.size()));
    
    // 数据: key + value
    result.append(key);
    result.append(value);
    
    // 计算并添加CRC
    uint32_t computed_crc = CalculateCRC();
    coding::PutFixed32(&result, computed_crc);
    
    return result;
}

Status WALRecord::Decode(const Slice& data) {
    if (data.size() < 33) { // 最小记录大小: 1+8+4+8+4+4+4=33 (增加了4字节列族ID)
        return Status::Corruption("WAL record too short");
    }
    
    const char* p = data.data();
    
    // 解析头部
    type = static_cast<WALRecordType>(*p++);
    sequence_number = coding::DecodeFixed64(p); p += 8;
    column_family_id = coding::DecodeFixed32(p); p += 4;  // 新增列族ID解析
    transaction_id = coding::DecodeFixed64(p); p += 8;
    uint32_t key_len = coding::DecodeFixed32(p); p += 4;
    uint32_t value_len = coding::DecodeFixed32(p); p += 4;

    // 检查剩余数据长度。
    // 必须用 size_t 计算：uint32 的 key_len + value_len + 4 会回绕，
    // 损坏记录可借此绕过长度校验造成越界读
    const size_t header_len = 1 + 8 + 4 + 8 + 4 + 4;
    if (data.size() != header_len + static_cast<size_t>(key_len) +
                             static_cast<size_t>(value_len) + 4) {
        return Status::Corruption("WAL record length mismatch");
    }
    if (static_cast<size_t>(key_len) > data.size() ||
        static_cast<size_t>(value_len) > data.size() - key_len) {
        return Status::Corruption("WAL record data truncated");
    }

    // 解析键值
    key = std::string(p, key_len); p += key_len;
    value = std::string(p, value_len); p += value_len;

    // 解析CRC
    crc = coding::DecodeFixed32(p);

    // 验证CRC
    if (!ValidateCRC()) {
        return Status::Corruption("WAL record CRC mismatch");
    }

    return Status::OK();
}

uint32_t WALRecord::CalculateCRC() const {
    std::string data;
    data.push_back(static_cast<char>(type));
    coding::PutFixed64(&data, sequence_number);
    coding::PutFixed32(&data, column_family_id);  // 包含列族ID到CRC计算
    coding::PutFixed64(&data, transaction_id);
    data.append(key);
    data.append(value);
    
    return coding::CalculateCRC32(data.data(), data.size());
}

bool WALRecord::ValidateCRC() const {
    return crc == CalculateCRC();
}

// WALWriter实现
WALWriter::WALWriter(const std::string& filename)
    : filename_(filename), file_size_(0), write_position_(0), is_open_(false) {}

WALWriter::~WALWriter() {
    if (is_open_.load()) {
        Close();
    }
}

Status WALWriter::Open() {
    if (is_open_.load()) {
        return Status::InvalidArgument("WAL file already open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        // 尝试打开文件，使用超时机制
        auto start = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(3);
        
        while (std::chrono::steady_clock::now() - start < timeout) {
            try {
                file_ = std::make_unique<std::ofstream>(filename_, 
                                                       std::ios::binary | std::ios::app);
                
                if (file_->is_open()) {
                    // 获取当前文件大小
                    file_->seekp(0, std::ios::end);
                    file_size_.store(file_->tellp());
                    write_position_.store(file_size_.load());
                    
                    is_open_.store(true);
                    return Status::OK();
                }
                
                // 如果打开失败，等待一段时间后重试
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                
            } catch (const std::exception& e) {
                // 等待一段时间后重试
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        
        return Status::IOError("Cannot open WAL file: " + filename_ + " (timeout)");
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception while opening WAL file: " + std::string(e.what()));
    }
}

Status WALWriter::Close() {
    if (!is_open_.load()) {
        return Status::OK();
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (file_) {
        file_->flush();
        file_->close();
        file_.reset();
    }
    
    is_open_.store(false);
    return Status::OK();
}

Status WALWriter::WriteRecord(const WALRecord& record) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("WAL file not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    return WriteRecordInternal(record);
}

Status WALWriter::WriteRecords(const std::vector<WALRecord>& records) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("WAL file not open");
    }
    
    if (records.empty()) {
        return Status::OK();
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        // 批量写入优化：预先计算总大小
        size_t total_size = 0;
        std::vector<std::string> encoded_records;
        encoded_records.reserve(records.size());
        
        for (const auto& record : records) {
            std::string encoded = record.Encode();
            if (encoded.size() > 128 * 1024 * 1024) {
                return Status::InvalidArgument("WAL record too large");
            }
            encoded_records.push_back(std::move(encoded));
            total_size += 4 + encoded_records.back().size(); // 4字节长度头 + 记录
        }
        
        // 构建批量写入缓冲区
        std::string batch_buffer;
        batch_buffer.reserve(total_size);
        
        for (const auto& encoded : encoded_records) {
            uint32_t record_length = static_cast<uint32_t>(encoded.size());
            std::string length_header;
            coding::PutFixed32(&length_header, record_length);
            
            batch_buffer.append(length_header);
            batch_buffer.append(encoded);
        }
        
        // 一次性写入所有记录
        uint64_t current_pos = file_->tellp();
        file_->write(batch_buffer.data(), batch_buffer.size());
        
        if (file_->fail()) {
            return Status::IOError("Failed to write WAL records batch: " + 
                                 std::string(strerror(errno)));
        }
        
        // 验证写入
        uint64_t new_pos = file_->tellp();
        if (new_pos != current_pos + total_size) {
            return Status::IOError("WAL batch write position mismatch");
        }
        
        file_size_.fetch_add(total_size);
        write_position_.fetch_add(total_size);
        unsynced_bytes_.fetch_add(total_size);

        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception in WAL batch write: " + std::string(e.what()));
    }
}

Status WALWriter::Sync() {
    if (!is_open_.load()) {
        return Status::InvalidArgument("WAL file not open");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (file_) {
        file_->flush();
        if (file_->fail()) {
            return Status::IOError("Failed to flush WAL file");
        }
    }

    // flush 只刷了 C++ 流缓冲；事务提交以 Sync 成功为持久化承诺，
    // 必须真正 fsync 到磁盘，否则掉电会丢失"已提交"的记录
    int fd = ::open(filename_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("Failed to open WAL file for fsync: " +
                               std::string(strerror(errno)));
    }
    int ret = ::fsync(fd);
    int close_ret = ::close(fd);
    if (ret != 0) {
        return Status::IOError("Failed to fsync WAL file: " +
                               std::string(strerror(errno)));
    }
    if (close_ret != 0) {
        return Status::IOError("Failed to close WAL file after fsync");
    }

    unsynced_bytes_.store(0);
    return Status::OK();
}

uint64_t WALWriter::GetFileSize() const {
    return file_size_.load();
}

bool WALWriter::IsOpen() const {
    return is_open_.load();
}

bool WALWriter::HasUnsyncedData() const {
    return unsynced_bytes_.load() > 0;
}

uint64_t WALWriter::GetWritePosition() const {
    return write_position_.load();
}

Status WALWriter::WriteRecordInternal(const WALRecord& record) {
    if (!file_) {
        return Status::InvalidArgument("File not open");
    }
    
    try {
        std::string encoded_record = record.Encode();
        
        // 检查记录大小是否合理
        if (encoded_record.size() > 128 * 1024 * 1024) { // 128MB限制
            return Status::InvalidArgument("WAL record too large");
        }
        
        // 写入记录长度
        uint32_t record_length = static_cast<uint32_t>(encoded_record.size());
        std::string length_header;
        coding::PutFixed32(&length_header, record_length);
        
        // 预先检查磁盘空间（简化实现）
        uint64_t bytes_to_write = length_header.size() + encoded_record.size();
        uint64_t current_pos = file_->tellp();
        
        // 批量写入以提高性能
        std::string buffer;
        buffer.reserve(bytes_to_write);
        buffer.append(length_header);
        buffer.append(encoded_record);
        
        file_->write(buffer.data(), buffer.size());
        
        if (file_->fail()) {
            return Status::IOError("Failed to write WAL record: " + 
                                 std::string(strerror(errno)));
        }
        
        // 验证写入位置
        uint64_t new_pos = file_->tellp();
        if (new_pos != current_pos + bytes_to_write) {
            return Status::IOError("WAL write position mismatch");
        }
        
        file_size_.fetch_add(bytes_to_write);
        write_position_.fetch_add(bytes_to_write);
        unsynced_bytes_.fetch_add(bytes_to_write);

        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception in WAL write: " + std::string(e.what()));
    }
}

// WALReader实现
WALReader::WALReader(const std::string& filename)
    : filename_(filename), read_position_(0), is_open_(false), eof_reached_(false) {}

WALReader::~WALReader() {
    if (is_open_.load()) {
        Close();
    }
}

Status WALReader::Open() {
    if (is_open_.load()) {
        return Status::InvalidArgument("WAL file already open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    file_ = std::make_unique<std::ifstream>(filename_, std::ios::binary);
    
    if (!file_->is_open()) {
        return Status::IOError("Cannot open WAL file: " + filename_);
    }
    
    read_position_.store(0);
    eof_reached_.store(false);
    is_open_.store(true);
    
    return Status::OK();
}

Status WALReader::Close() {
    if (!is_open_.load()) {
        return Status::OK();
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (file_) {
        file_->close();
        file_.reset();
    }
    
    is_open_.store(false);
    return Status::OK();
}

Status WALReader::ReadNextRecord(WALRecord* record) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("WAL file not open");
    }
    
    if (eof_reached_.load()) {
        return Status::Incomplete("EOF reached");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    return ReadRecordInternal(record);
}

Status WALReader::Seek(uint64_t position) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("WAL file not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    file_->seekg(position);
    if (file_->fail()) {
        return Status::IOError("Failed to seek in WAL file");
    }
    
    read_position_.store(position);
    eof_reached_.store(false);
    
    return Status::OK();
}

bool WALReader::IsEOF() const {
    return eof_reached_.load();
}

uint64_t WALReader::GetReadPosition() const {
    return read_position_.load();
}

Status WALReader::ValidateFile() {
    if (!is_open_.load()) {
        Status s = Open();
        if (!s.ok()) {
            return s;
        }
    }
    
    // 重置到文件开始
    Status s = Seek(0);
    if (!s.ok()) {
        return s;
    }
    
    // 逐个读取并验证记录
    WALRecord record;
    while (!IsEOF()) {
        s = ReadNextRecord(&record);
        if (s.IsIncomplete()) {
            break; // 到达文件末尾
        } else if (!s.ok()) {
            return s; // 发现错误
        }
    }
    
    return Status::OK();
}

Status WALReader::ReadRecordInternal(WALRecord* record) {
    if (!file_) {
        return Status::InvalidArgument("File not open");
    }
    
    // 读取记录长度
    char length_buf[4];
    file_->read(length_buf, 4);
    
    if (file_->gcount() != 4) {
        if (file_->eof()) {
            eof_reached_.store(true);
            return Status::Incomplete("EOF reached");
        }
        return Status::IOError("Failed to read record length");
    }
    
    uint32_t record_length = coding::DecodeFixed32(length_buf);
    if (record_length == 0 || record_length > 100 * 1024 * 1024) { // 最大100MB
        return Status::Corruption("Invalid record length");
    }
    
    // 读取记录数据
    std::string record_data;
    record_data.resize(record_length);
    file_->read(&record_data[0], record_length);
    
    if (file_->gcount() != record_length) {
        return Status::Corruption("Record data truncated");
    }
    
    // 解码记录
    Status s = record->Decode(Slice(record_data));
    if (!s.ok()) {
        return s;
    }
    
    read_position_.fetch_add(4 + record_length);
    return Status::OK();
}

// WALManager实现
WALManager::WALManager(const std::string& db_path)
    : db_path_(db_path), next_file_number_(1), 
      max_wal_size_(64ULL << 20), sync_on_write_(false),
      initialized_(false), shutdown_(false),
      total_records_(0), total_bytes_(0), last_sequence_(0), lsm_tree_(nullptr) {
      
    // 注册error_handler回调。
    // 不捕获 this：多个 WALManager（每列族一个）共用 "WAL" 键会互相覆盖，
    // 且析构后该单例仍持有带悬垂 this 的 lambda
    ErrorRecoveryManager::Instance().RegisterCleanupCallback(
        "WAL",
        [](const std::string& /*component*/, const std::string& /*operation*/) -> Status {
            return Status::OK(); // WAL清理逻辑
        }
    );
    
    LOG_DEBUG << "WALManager initialized with error recovery callbacks";
}

WALManager::~WALManager() {
    if (initialized_.load() && !shutdown_.load()) {
        Shutdown();
    }
}

Status WALManager::Initialize() {
    if (initialized_.load()) {
        return Status::InvalidArgument("WALManager already initialized");
    }
    
    // 使用超时机制避免阻塞
    auto start = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::seconds(5); // 减少超时时间
    
    while (std::chrono::steady_clock::now() - start < timeout) {
        try {
            // 创建WAL目录（失败直接报错，不能静默降级到 /tmp：
            // 恢复路径扫不到 /tmp，事务提交"成功"但数据不可恢复）
            std::string wal_dir = db_path_ + "/wal";
            if (!std::filesystem::exists(wal_dir)) {
                std::error_code dir_ec;
                std::filesystem::create_directories(wal_dir, dir_ec);
                if (dir_ec) {
                    return Status::IOError("Failed to create WAL directory: " +
                                           dir_ec.message());
                }
            }
            
            // 扫描现有WAL文件，确定下一个文件号
            uint64_t max_file_number = 0;
            
            try {
                for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
                    if (entry.is_regular_file()) {
                        std::string filename = entry.path().filename();
                        if (filename.size() > 4 && filename.substr(filename.size() - 4) == ".wal") {
                            try {
                                uint64_t file_number = std::stoull(filename.substr(0, filename.size() - 4));
                                max_file_number = std::max(max_file_number, file_number);
                            } catch (...) {
                                // 忽略无效的文件名
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                // 如果目录扫描失败，使用默认值
                max_file_number = 0;
            }

            LOG_INFO << "WAL scan: max file number = " << max_file_number;
            next_file_number_.store(max_file_number + 1);

            // 创建当前WAL文件
            Status s = CreateNewWALFile();
            if (s.ok()) {
                // 如果有现有的WAL文件，尝试恢复
                if (max_file_number > 0) {
                    SequenceNumber last_sequence = 0;
                    Status recovery_s = RecoverFromWAL(&last_sequence);
                    if (!recovery_s.ok()) {
                        // 恢复失败必须让初始化失败：
                        // 静默继续会让数据库带着不完整的数据对外服务
                        LOG_ERROR << "WAL recovery failed: "
                                  << recovery_s.ToString();
                        return recovery_s;
                    }
                    LOG_INFO << "WAL recovery succeeded, last sequence: "
                             << last_sequence;
                }

                initialized_.store(true);
                return Status::OK();
            }
            
            // 如果失败，等待一段时间后重试
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
        } catch (const std::exception& e) {
            // 等待一段时间后重试
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    
    return Status::IOError("Failed to initialize WAL manager: timeout after 5 seconds");
}

Status WALManager::Shutdown() {
    if (shutdown_.load()) {
        return Status::OK();
    }
    
    shutdown_.store(true);
    
    // 注销后台任务
    UnregisterBackgroundTasks();
    
    // 同步并关闭当前WAL文件
    std::lock_guard<std::mutex> lock(wal_mutex_);
    if (current_writer_) {
        current_writer_->Sync();
        current_writer_->Close();
        current_writer_.reset();
    }
    
    initialized_.store(false);
    return Status::OK();
}

Status WALManager::WritePut(uint32_t column_family_id, const Slice& key, const Slice& value, SequenceNumber sequence) {
    if (key.empty()) {
        return Status::InvalidArgument("Key cannot be empty");
    }
    
    WALRecord record;
    record.type = WALRecordType::kPut;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;  // 列族标识
    record.key = key.ToString();
    record.value = value.ToString();
    record.transaction_id = 0; // 非事务操作
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteDelete(uint32_t column_family_id, const Slice& key, SequenceNumber sequence) {
    if (key.empty()) {
        return Status::InvalidArgument("Key cannot be empty");
    }
    
    WALRecord record;
    record.type = WALRecordType::kDelete;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;  // 列族标识
    record.key = key.ToString();
    record.transaction_id = 0; // 非事务操作
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteMerge(uint32_t column_family_id, const Slice& key, const Slice& value, SequenceNumber sequence) {
    if (key.empty()) {
        return Status::InvalidArgument("Key cannot be empty");
    }
    
    WALRecord record;
    record.type = WALRecordType::kMerge;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;  // 列族标识
    record.key = key.ToString();
    record.value = value.ToString();
    record.transaction_id = 0; // 非事务操作
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteBeginTransaction(uint32_t column_family_id, uint64_t transaction_id, SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kBeginTransaction;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;  // 列族标识
    record.transaction_id = transaction_id;
    
    // 记录事务开始到 WAL
    Status s = WriteRecordInternal(record);
    if (s.ok()) {
        // 记录事务状态
        std::lock_guard<std::mutex> lock(transaction_mutex_);
        active_transactions_[transaction_id] = 1;  // 1 表示活跃状态
        LOG_DEBUG << "WAL: Transaction " << transaction_id << " began at sequence " << sequence;
    }
    
    return s;
}

Status WALManager::WriteCommitTransaction(uint32_t column_family_id, uint64_t transaction_id, SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kCommitTransaction;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;  // 列族标识
    record.transaction_id = transaction_id;
    
    // 记录事务提交到 WAL
    Status s = WriteRecordInternal(record);
    if (s.ok()) {
        // 更新事务状态
        std::lock_guard<std::mutex> lock(transaction_mutex_);
        active_transactions_[transaction_id] = 2;  // 2 表示已提交状态
        LOG_DEBUG << "WAL: Transaction " << transaction_id << " committed at sequence " << sequence;
    }
    
    return s;
}

Status WALManager::WriteAbortTransaction(uint32_t column_family_id, uint64_t transaction_id, SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kAbortTransaction;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;  // 列族标识
    record.transaction_id = transaction_id;
    
    // 记录事务回滚到 WAL
    Status s = WriteRecordInternal(record);
    if (s.ok()) {
        // 更新事务状态
        std::lock_guard<std::mutex> lock(transaction_mutex_);
        active_transactions_[transaction_id] = 3;  // 3 表示已中止状态
        LOG_DEBUG << "WAL: Transaction " << transaction_id << " aborted at sequence " << sequence;
    }
    
    return s;
}

Status WALManager::WriteCheckpoint(SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kCheckpoint;
    record.sequence_number = sequence;
    record.transaction_id = 0;
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteFlushMemTable(SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kFlushMemTable;
    record.sequence_number = sequence;
    record.transaction_id = 0;
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteCreateColumnFamily(uint32_t column_family_id, const std::string& name, 
                                         const std::string& options_data, SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kCreateColumnFamily;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;
    record.transaction_id = 0;
    
    // 编码列族名称和选项
    coding::PutFixed32(&record.value, static_cast<uint32_t>(name.size()));
    record.value.append(name);
    coding::PutFixed32(&record.value, static_cast<uint32_t>(options_data.size()));
    record.value.append(options_data);
    
    LOG_INFO << "WAL: Creating column family " << column_family_id << " (" << name << ") at sequence " << sequence;
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteDropColumnFamily(uint32_t column_family_id, SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kDropColumnFamily;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;
    record.transaction_id = 0;
    
    LOG_INFO << "WAL: Dropping column family " << column_family_id << " at sequence " << sequence;
    
    return WriteRecordInternal(record);
}

Status WALManager::WriteUpdateColumnFamilyOptions(uint32_t column_family_id, 
                                                 const std::string& options_data, SequenceNumber sequence) {
    WALRecord record;
    record.type = WALRecordType::kUpdateColumnFamilyOptions;
    record.sequence_number = sequence;
    record.column_family_id = column_family_id;
    record.transaction_id = 0;
    record.value = options_data;
    
    LOG_INFO << "WAL: Updating column family " << column_family_id << " options at sequence " << sequence;
    
    return WriteRecordInternal(record);
}


    


Status WALManager::Sync() {
    std::lock_guard<std::mutex> lock(wal_mutex_);
    
    if (current_writer_) {
        return current_writer_->Sync();
    }
    
    return Status::InvalidArgument("No active WAL writer");
}

Status WALManager::CreateNewWALFile() {
    std::lock_guard<std::mutex> lock(wal_mutex_);
    return CreateNewWALFileLocked();
}

Status WALManager::CreateNewWALFileLocked() {
    // 关闭当前文件
    if (current_writer_) {
        current_writer_->Sync();
        current_writer_->Close();
        current_writer_.reset();
    }

    // 创建新文件
    uint64_t file_number = next_file_number_.fetch_add(1);
    current_wal_filename_ = GenerateWALFilename(file_number);

    try {
        current_writer_ = std::make_unique<WALWriter>(current_wal_filename_);
        Status s = current_writer_->Open();
        if (!s.ok()) {
            // 打开失败必须报错，不能静默降级写到 /tmp：
            // 恢复路径扫不到 /tmp 里的文件，事务提交"成功"但数据不可恢复
            std::string failed_name = current_wal_filename_;
            current_writer_.reset();
            current_wal_filename_.clear();
            return Status::IOError("Failed to open WAL file: " + failed_name +
                                   ": " + s.ToString());
        }
        return s;
    } catch (const std::exception& e) {
        return Status::IOError("Failed to create WAL writer: " + std::string(e.what()));
    }
}

Status WALManager::ArchiveCurrentWAL() {
    std::lock_guard<std::mutex> lock(wal_mutex_);
    return ArchiveCurrentWALLocked();
}

Status WALManager::ArchiveCurrentWALLocked() {
    if (!current_writer_) {
        return Status::InvalidArgument("No current WAL file");
    }

    // 同步并关闭当前文件
    Status s = current_writer_->Sync();
    if (!s.ok()) {
        return s;
    }

    s = current_writer_->Close();
    if (!s.ok()) {
        return s;
    }

    // 移动到归档目录
    try {
        std::string archive_dir = db_path_ + "/wal/archive";
        if (!std::filesystem::exists(archive_dir)) {
            std::filesystem::create_directories(archive_dir);
        }

        std::string archive_filename = archive_dir + "/" +
                                     std::filesystem::path(current_wal_filename_).filename().string();
        std::filesystem::rename(current_wal_filename_, archive_filename);
    } catch (const std::exception& e) {
        return Status::IOError("Failed to archive WAL file: " + std::string(e.what()));
    }

    current_writer_.reset();
    current_wal_filename_.clear();

    return Status::OK();
}

std::vector<std::string> WALManager::GetWALFiles() const {
    std::vector<std::string> wal_files;
    
    try {
        std::string wal_dir = db_path_ + "/wal";
        
        // 检查目录是否存在，避免无限等待
        if (!std::filesystem::exists(wal_dir)) {
            return wal_files; // 目录不存在，返回空列表
        }
        
        // 使用超时机制遍历目录
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::milliseconds(1000); // 1秒超时
        
        for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
            // 检查超时
            if (std::chrono::steady_clock::now() - start_time > timeout) {
                break; // 超时退出
            }
            
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename();
                if (filename.size() > 4 && filename.substr(filename.size() - 4) == ".wal") {
                    wal_files.push_back(entry.path());
                }
            }
        }
        
        // 检查归档目录
        std::string archive_dir = wal_dir + "/archive";
        if (std::filesystem::exists(archive_dir)) {
            start_time = std::chrono::steady_clock::now(); // 重置超时
            
            for (const auto& entry : std::filesystem::directory_iterator(archive_dir)) {
                // 检查超时
                if (std::chrono::steady_clock::now() - start_time > timeout) {
                    break; // 超时退出
                }
                
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().filename();
                    if (filename.size() > 4 && filename.substr(filename.size() - 4) == ".wal") {
                        wal_files.push_back(entry.path());
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        // 记录错误但不阻塞
        // 返回已找到的文件
    }

    // 先排序再截断：恢复必须看到所有文件，截断只能在排序后按序进行，
    // 否则可能丢掉最新文件而把未重放的数据当已重放删除
    std::sort(wal_files.begin(), wal_files.end());
    if (wal_files.size() > 1000) {
        LOG_WARN << "Too many WAL files (" << wal_files.size()
                 << "), keeping the first 1000 by name";
        wal_files.resize(1000);
    }

    return wal_files;
}

Status WALManager::RecoverFromWAL(SequenceNumber* last_sequence) {
    if (db_path_.empty()) {
        return Status::InvalidArgument("WALManager not properly configured");
    }

    auto wal_files = GetWALFiles();
    if (wal_files.empty()) {
        *last_sequence = 0;
        return Status::OK();
    }

    // 按文件编号排序以确保恢复顺序
    std::sort(wal_files.begin(), wal_files.end());

    SequenceNumber max_sequence = 0;
    uint64_t recovered_records = 0;
    uint64_t corrupted_records = 0;

    // 事务状态与数据缓冲：跨文件保持，崩溃在半途的事务不会留下部分写入
    std::unordered_map<uint64_t, TransactionState> transaction_states;
    std::unordered_map<uint64_t, std::vector<WALRecord>> txn_buffers;

    for (const auto& filename : wal_files) {
        SequenceNumber file_last_sequence = 0;
        uint64_t file_records = 0;
        uint64_t file_corrupted = 0;

        Status s = RecoverFromWALFile(filename, &file_last_sequence, &file_records,
                                      &transaction_states, &txn_buffers,
                                      &file_corrupted);
        if (!s.ok()) {
            // 严重错误，停止恢复并保留 WAL 文件供下次重试
            *last_sequence = max_sequence;
            return Status::IOError("Fatal error during WAL recovery: " +
                                   s.ToString());
        }
        max_sequence = std::max(max_sequence, file_last_sequence);
        recovered_records += file_records;
        corrupted_records += file_corrupted;
    }

    // 文件结束后仍未提交的事务：丢弃缓冲（记录从未被应用，无需回滚 LSM）
    txn_buffers.clear();

    if (corrupted_records > 0) {
        LOG_WARN << "WAL recovery skipped " << corrupted_records
                 << " corrupted record(s) via resync";
    }

    // 重放完成：把恢复缓冲刷成 SST。只有刷盘成功才能删除 WAL 文件，
    // 否则 WAL 是数据的唯一副本（之前无条件删除，刷盘失败即丢数据）
    if (lsm_tree_) {
        Status fs = lsm_tree_->FinishRecovery(max_sequence);
        if (!fs.ok()) {
            LOG_ERROR << "Failed to finish recovery, WAL files kept: "
                      << fs.ToString();
            *last_sequence = max_sequence;
            return fs;
        }
    }

    *last_sequence = max_sequence;
    if (max_sequence > 0) {
        last_sequence_.store(max_sequence);
    }
    total_records_.store(recovered_records);

    // 删除已重放的WAL文件（当前写入器对应的新文件保留）
    for (const auto &filename : wal_files) {
        if (filename == current_wal_filename_) {
            continue;
        }
        std::error_code ec;
        std::filesystem::remove(filename, ec);
    }

    return Status::OK();
}

Status WALManager::TruncateLogs() {
    std::lock_guard<std::mutex> lock(wal_mutex_);

    // 关闭当前写入器
    if (current_writer_) {
        current_writer_->Close();
        current_writer_.reset();
    }
    current_wal_filename_.clear();

    std::error_code ec;
    std::string wal_dir = db_path_ + "/wal";
    if (std::filesystem::exists(wal_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(wal_dir, ec)) {
            if (ec) break;
            if (!entry.is_regular_file()) continue;
            std::string filename = entry.path().filename().string();
            if (filename.size() > 4 && filename.substr(filename.size() - 4) == ".wal") {
                std::filesystem::remove(entry.path(), ec);
            }
        }
    }

    // 归档目录一并清理（与 ArchiveCurrentWAL 的归档位置一致：wal/archive）
    std::string archive_dir = db_path_ + "/wal/archive";
    if (std::filesystem::exists(archive_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(archive_dir, ec)) {
            if (ec) break;
            if (!entry.is_regular_file()) continue;
            std::string filename = entry.path().filename().string();
            if (filename.size() > 4 && filename.substr(filename.size() - 4) == ".wal") {
                std::filesystem::remove(entry.path(), ec);
            }
        }
    }

    // 注意：这里绝不能重置 last_sequence_（也不能重置文件号）：
    // 干净关闭后 SST 里仍保留着大序列号的数据，重开时调用方依赖
    // GetLastSequence() 恢复快照序列号，清零会导致新会话序列号回退，
    // 遮蔽已持久化的旧数据

    LOG_INFO << "WAL files truncated (last_sequence preserved: "
             << last_sequence_.load() << ")";
    return Status::OK();
}

Status WALManager::CleanupOldWALFiles(SequenceNumber safe_sequence) {
    // 保守策略：只有调用方给出安全序列号（该序列之前的数据已确认落盘）才删除。
    // 旧实现按字典序"保留 3 个"，既与数据是否落盘无关（会删掉唯一数据副本），
    // 又会因为归档路径的字典序把活动文件误删
    if (safe_sequence == 0) {
        return Status::OK(); // 没有安全点，绝不删除
    }

    std::lock_guard<std::mutex> lock(wal_mutex_);
    auto wal_files = GetWALFiles();

    for (const auto &filename : wal_files) {
        // 当前写入文件绝不删除
        if (filename == current_wal_filename_) {
            continue;
        }
        // 只清理归档文件；活动目录中的文件可能仍被需要
        if (filename.find("/archive/") == std::string::npos) {
            continue;
        }
        std::error_code ec;
        std::filesystem::remove(filename, ec);
    }

    return Status::OK();
}

uint64_t WALManager::GetCurrentWALSize() const {
    std::lock_guard<std::mutex> lock(wal_mutex_);
    
    if (current_writer_) {
        return current_writer_->GetFileSize();
    }
    
    return 0;
}

void WALManager::SetMaxWALSize(uint64_t max_size) {
    max_wal_size_.store(max_size);
}

void WALManager::SetSyncOnWrite(bool sync_on_write) {
    sync_on_write_.store(sync_on_write);
}

SequenceNumber WALManager::GetLastSequence() const {
    return last_sequence_.load();
}

void WALManager::RegisterSyncTask(std::function<void()> sync_task) {
    sync_task_callback_ = std::move(sync_task);
}

void WALManager::RegisterArchiveTask(std::function<void()> archive_task) {
    archive_task_callback_ = std::move(archive_task);
}

void WALManager::UnregisterBackgroundTasks() {
    sync_task_callback_ = nullptr;
    archive_task_callback_ = nullptr;
}

Status WALManager::WriteRecordInternal(const WALRecord& record) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("WALManager not initialized");
    }
    
    if (shutdown_.load()) {
        return Status::InvalidArgument("WALManager is shutdown");
    }
    
    std::lock_guard<std::mutex> lock(wal_mutex_);
    
    // 确保当前有可用的WAL文件（本函数已持 wal_mutex_，
    // 必须用不加锁的内部版本，否则二次加锁自死锁）
    if (!current_writer_) {
        Status s = CreateNewWALFileLocked();
        if (!s.ok()) {
            return Status::IOError("Failed to create new WAL file: " + s.ToString());
        }
    }

    // 检查是否需要切换WAL文件
    if (ShouldSwitchWAL()) {
        Status s = SwitchToNewWALLocked();
        if (!s.ok()) {
            return Status::IOError("Failed to switch WAL file: " + s.ToString());
        }
    }
    
    // 验证记录的有效性
    if (record.sequence_number == 0) {
        return Status::InvalidArgument("Invalid sequence number in WAL record");
    }
    
    // 处理事务相关记录
    if (record.type == WALRecordType::kBeginTransaction ||
        record.type == WALRecordType::kCommitTransaction ||
        record.type == WALRecordType::kAbortTransaction) {
        
        Status txn_status = UpdateTransactionState(record);
        if (!txn_status.ok()) {
            return txn_status;
        }
    }
    
    // 写入记录
    Status s = current_writer_->WriteRecord(record);
    if (!s.ok()) {
        return Status::IOError("Failed to write WAL record: " + s.ToString());
    }
    
    // 可选择立即同步
    if (sync_on_write_.load()) {
        s = current_writer_->Sync();
        if (!s.ok()) {
            return Status::IOError("Failed to sync WAL file: " + s.ToString());
        }
    }
    
    // 更新统计（原子操作避免锁竞争）
    total_records_.fetch_add(1);
    size_t record_size = record.Encode().size();
    total_bytes_.fetch_add(record_size);
    
    // 更新最后序列号
    last_sequence_.store(record.sequence_number);
    
    return Status::OK();
}

std::string WALManager::GenerateWALFilename(uint64_t file_number) const {
    std::ostringstream oss;
    oss << db_path_ << "/wal/" << std::setfill('0') << std::setw(6) << file_number << ".wal";
    return oss.str();
}

Status WALManager::SwitchToNewWAL() {
    std::lock_guard<std::mutex> lock(wal_mutex_);
    return SwitchToNewWALLocked();
}

Status WALManager::SwitchToNewWALLocked() {
    // 归档当前WAL（用不加锁版本，本函数已持 wal_mutex_）。
    // 若配置了归档回调（回调可能再进入 WALManager），走回调路径
    if (archive_task_callback_) {
        archive_task_callback_();
    } else {
        Status archive_status = ArchiveCurrentWALLocked();
        if (!archive_status.ok()) {
            LOG_WARN << "Failed to archive WAL during switch: "
                     << archive_status.ToString();
        }
    }

    // 切换到新文件前，确保 wal 目录元数据持久化（提高崩溃安全）
    // 简化实现：尝试打开 wal 目录并 fsync（不同平台可按需实现）
    try {
        int dir_fd = ::open((db_path_ + "/wal").c_str(), O_DIRECTORY | O_RDONLY);
        if (dir_fd >= 0) {
            ::fsync(dir_fd);
            ::close(dir_fd);
        }
    } catch (...) {
        // 忽略目录 fsync 失败
    }

    return CreateNewWALFileLocked();
}

bool WALManager::ShouldSwitchWAL() const {
    if (!current_writer_) {
        return true;
    }
    
    return current_writer_->GetFileSize() >= max_wal_size_.load();
}

// 从单个 WAL 文件恢复：顺序读取记录，应用到 LSM（恢复模式），返回该文件的最大序与记录数
Status WALManager::RecoverFromWALFile(
    const std::string& filename, SequenceNumber* last_sequence,
    uint64_t* record_count,
    std::unordered_map<uint64_t, TransactionState>* txn_states,
    std::unordered_map<uint64_t, std::vector<WALRecord>>* txn_buffers,
    uint64_t* corrupted_count) {
    WALReader reader(filename);
    Status s = reader.Open();
    if (!s.ok()) {
        return s;
    }

    SequenceNumber max_sequence = 0;
    uint64_t count = 0;

    // 进入恢复模式以避免正常写路径副作用。
    // 所有退出路径都必须复位，用 lambda 保证
    if (lsm_tree_) lsm_tree_->SetRecoveryMode(true);
    auto exit_recovery = [&]() {
        if (lsm_tree_) lsm_tree_->SetRecoveryMode(false);
    };

    WALRecord record;
    Status loop_status = Status::OK();
    while (!reader.IsEOF()) {
        s = reader.ReadNextRecord(&record);
        if (s.IsIncomplete()) {
            break; // 写入中断的尾部，正常结束
        } else if (!s.ok()) {
            // 记录损坏（CRC/长度错误）：按长度前缀逐字节重同步，
            // 跳过坏记录继续恢复其后的记录（旧的"部分恢复"实现会从头重放，
            // 既重复应用又放弃损坏点之后的数据）
            uint64_t resync_pos = 0;
            if (!TryResyncWALFile(filename, reader.GetReadPosition(),
                                  &resync_pos)) {
                LOG_WARN << "WAL file " << filename
                         << ": cannot resync after corruption, stopping";
                loop_status = Status::Corruption(
                    "WAL resync failed (file keeps remaining records)");
                break;
            }
            if (corrupted_count) (*corrupted_count)++;
            s = reader.Seek(resync_pos);
            if (!s.ok()) {
                loop_status = s;
                break;
            }
            continue;
        }

        // 应用记录（带事务缓冲）
        SequenceNumber applied_seq = 0;
        s = ApplyRecoveredRecord(record, txn_states, txn_buffers, &applied_seq);
        if (!s.ok()) {
            loop_status = s;
            break;
        }

        max_sequence = std::max(max_sequence, record.sequence_number);
        max_sequence = std::max(max_sequence, applied_seq);
        count++;
    }

    exit_recovery();
    if (!loop_status.ok()) {
        return loop_status;
    }

    *last_sequence = max_sequence;
    *record_count = count;
    return Status::OK();
}

bool WALManager::TryResyncWALFile(const std::string& filename,
                                  uint64_t start_pos, uint64_t* resync_pos) {
    constexpr uint64_t kMaxResyncBytes = 64 * 1024;
    WALRecord probe_record;
    for (uint64_t offset = 1; offset <= kMaxResyncBytes; ++offset) {
        WALReader probe(filename);
        Status s = probe.Open();
        if (!s.ok()) return false;
        s = probe.Seek(start_pos + offset);
        if (!s.ok()) return false;
        s = probe.ReadNextRecord(&probe_record);
        if (s.ok()) {
            // 找到一条长度前缀与 CRC 都合法的完整记录
            *resync_pos = start_pos + offset;
            return true;
        }
    }
    return false;
}

// 恢复路径的记录应用（带事务缓冲）
Status WALManager::ApplyRecoveredRecord(
    const WALRecord& record,
    std::unordered_map<uint64_t, TransactionState>* txn_states,
    std::unordered_map<uint64_t, std::vector<WALRecord>>* txn_buffers,
    SequenceNumber* applied_max_sequence) {
    *applied_max_sequence = 0;

    // 非事务记录直接应用（主提交路径写的就是这类记录）
    if (record.transaction_id == 0) {
        return ApplyWALRecord(record);
    }

    switch (record.type) {
    case WALRecordType::kBeginTransaction:
        (*txn_states)[record.transaction_id] = TransactionState::kActive;
        return Status::OK();
    case WALRecordType::kCommitTransaction: {
        // 事务提交：按序应用缓冲的数据记录
        auto it = txn_buffers->find(record.transaction_id);
        if (it != txn_buffers->end()) {
            for (const auto &r : it->second) {
                Status s = ApplyWALRecord(r);
                if (!s.ok()) return s;
                *applied_max_sequence =
                    std::max(*applied_max_sequence, r.sequence_number);
            }
            txn_buffers->erase(it);
        }
        (*txn_states)[record.transaction_id] = TransactionState::kCommitted;
        return Status::OK();
    }
    case WALRecordType::kAbortTransaction:
        txn_buffers->erase(record.transaction_id);
        (*txn_states)[record.transaction_id] = TransactionState::kAborted;
        return Status::OK();
    default:
        // Put/Delete/Merge 等事务数据记录：先缓冲，确认提交后才应用
        (*txn_buffers)[record.transaction_id].push_back(record);
        return Status::OK();
    }
}

// 将一条 WAL 记录应用到存储引擎（恢复路径）
Status WALManager::ApplyWALRecord(const WALRecord& record) {
    
    try {
        switch (record.type) {
            case WALRecordType::kPut: {
                // 应用Put操作到LSMTree（如果可用）
                if (lsm_tree_) {
                    Status s = lsm_tree_->RecoverFromWALRecord(record.key, record.value, 
                                                              record.sequence_number, false);
                    if (!s.ok()) {
                        return Status::IOError("Failed to apply Put operation during recovery: " + s.ToString());
                    }
                }
                break;
            }
            
            case WALRecordType::kDelete: {
                // 应用Delete操作到LSMTree（如果可用）
                if (lsm_tree_) {
                    Status s = lsm_tree_->RecoverFromWALRecord(record.key, "", 
                                                              record.sequence_number, true);
                    if (!s.ok()) {
                        return Status::IOError("Failed to apply Delete operation during recovery: " + s.ToString());
                    }
                }
                break;
            }
            
            case WALRecordType::kMerge: {
                // Merge 语义未实现：按 Put 重放会把合并语义静默变成覆盖，
                // 宁可显式报错也不伪装成功
                return Status::NotSupported(
                    "WAL replay of kMerge records is not supported");
            }
            
            case WALRecordType::kBeginTransaction: {
                // 开始事务 - 在恢复过程中记录事务状态
                std::lock_guard<std::mutex> lock(transaction_mutex_);
                active_transactions_[record.transaction_id] = 1;  // 1 表示活跃状态
                LOG_DEBUG << "Recovery: Transaction " << record.transaction_id << " begun at sequence " << record.sequence_number;
                break;
            }
            
            case WALRecordType::kCommitTransaction: {
                // 提交事务 - 更新事务状态为已提交
                std::lock_guard<std::mutex> lock(transaction_mutex_);
                auto it = active_transactions_.find(record.transaction_id);
                if (it != active_transactions_.end()) {
                    it->second = 2;  // 2 表示已提交状态
                    LOG_DEBUG << "Recovery: Transaction " << record.transaction_id << " committed at sequence " << record.sequence_number;
                } else {
                    LOG_WARN << "Recovery: Commit for unknown transaction " << record.transaction_id;
                }
                break;
            }
            
            case WALRecordType::kAbortTransaction: {
                // 中止事务 - 更新事务状态为已中止
                std::lock_guard<std::mutex> lock(transaction_mutex_);
                auto it = active_transactions_.find(record.transaction_id);
                if (it != active_transactions_.end()) {
                    it->second = 3;  // 3 表示已中止状态
                    LOG_DEBUG << "Recovery: Transaction " << record.transaction_id << " aborted at sequence " << record.sequence_number;
                } else {
                    LOG_WARN << "Recovery: Abort for unknown transaction " << record.transaction_id;
                }
                break;
            }
            
            case WALRecordType::kCheckpoint: {
                // 检查点记录只用于同步序列号（在收尾时统一处理）。
                // 恢复期间不能触发刷盘：会把恢复用的 memtable 标记为
                // immutable，导致后续恢复写入失败
                LOG_INFO << "Recovery: Checkpoint encountered at sequence "
                         << record.sequence_number;
                break;
            }

            case WALRecordType::kFlushMemTable: {
                // 刷盘标记在重放时无实际作用（数据统一在 FinishRecovery 落盘）
                LOG_INFO << "Recovery: MemTable flush marker at sequence "
                         << record.sequence_number;
                break;
            }
            
            case WALRecordType::kCreateColumnFamily: {
                // 列族创建 - 在恢复过程中记录但不实际创建（由MANIFEST恢复）
                LOG_INFO << "Recovery: Column family " << record.column_family_id 
                         << " creation at sequence " << record.sequence_number;
                
                // 解码列族名称（用于日志）
                if (record.value.size() >= 4) {
                    uint32_t name_len = coding::DecodeFixed32(record.value.data());
                    if (record.value.size() >= 4 + name_len) {
                        std::string cf_name(record.value.data() + 4, name_len);
                        LOG_INFO << "Recovery: Column family name: " << cf_name;
                    }
                }
                break;
            }
            
            case WALRecordType::kDropColumnFamily: {
                // 列族删除 - 在恢复过程中记录但不实际删除（由MANIFEST恢复）
                LOG_INFO << "Recovery: Column family " << record.column_family_id 
                         << " drop at sequence " << record.sequence_number;
                break;
            }
            
            case WALRecordType::kUpdateColumnFamilyOptions: {
                // 列族选项更新 - 在恢复过程中记录（由MANIFEST恢复）
                LOG_INFO << "Recovery: Column family " << record.column_family_id 
                         << " options update at sequence " << record.sequence_number;
                break;
            }
            
            default:
                return Status::InvalidArgument("Unknown WAL record type: " + std::to_string(static_cast<int>(record.type)));
        }
        
        // 更新统计信息
        total_records_.fetch_add(1);
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception applying WAL record: " + std::string(e.what()));
    }
}

// RecoveryManager实现
RecoveryManager::RecoveryManager(const std::string& db_path) : db_path_(db_path) {}

RecoveryManager::~RecoveryManager() = default;

Status RecoveryManager::RecoverDatabase(SequenceNumber* recovered_sequence) {
    if (!recovered_sequence) {
        return Status::InvalidArgument("recovered_sequence cannot be null");
    }
    
    auto start_time = std::chrono::steady_clock::now();
    
    // 重置恢复统计
    recovery_stats_ = RecoveryStats{};
    
    try {
        // 第一步：发现WAL文件
        std::vector<std::string> wal_files;
        Status s = DiscoverWALFiles(&wal_files);
        if (!s.ok()) {
            return Status::IOError("Failed to discover WAL files: " + s.ToString());
        }
        
        recovery_stats_.wal_files_processed = wal_files.size();
        
        // 第二步：验证WAL文件的完整性
        std::vector<std::string> valid_files;
        uint64_t corrupted_count = 0;
        
        for (const auto& file : wal_files) {
            Status validate_status = ValidateWALFile(file);
            if (validate_status.ok()) {
                valid_files.push_back(file);
            } else if (validate_status.IsCorruption()) {
                corrupted_count++;
                
                // 尝试修复损坏的文件
                Status repair_status = RepairWALFile(file);
                if (repair_status.ok()) {
                    valid_files.push_back(file);
                } else {
                    // 记录无法修复的文件
                    std::cerr << "Warning: Cannot repair corrupted WAL file: " << file << std::endl;
                }
            } else {
                return Status::IOError("Failed to validate WAL file: " + file + " - " + validate_status.ToString());
            }
        }
        
        recovery_stats_.corrupted_records = corrupted_count;
        
        // 第三步：从有效的WAL文件恢复数据
        SequenceNumber max_sequence = 0;
        s = RecoverFromWALFiles(valid_files, &max_sequence);
        if (!s.ok()) {
            return Status::IOError("Failed to recover from WAL files: " + s.ToString());
        }
        
        // 第四步：验证恢复后的数据一致性
        Status consistency_status = ValidateConsistency();
        if (!consistency_status.ok()) {
            return Status::Corruption("Data consistency check failed after recovery: " + 
                                    consistency_status.ToString());
        }
        
        // 记录恢复完成时间
        auto end_time = std::chrono::steady_clock::now();
        recovery_stats_.recovery_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        recovery_stats_.recovered_sequence = max_sequence;
        
        *recovered_sequence = max_sequence;
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during database recovery: " + std::string(e.what()));
    }
}

Status RecoveryManager::ValidateConsistency() {
    // 验证数据库一致性
    try {
        if (!std::filesystem::exists(db_path_)) {
            return Status::InvalidArgument("Database path does not exist");
        }
        
        // 第一步：验证WAL文件的完整性和一致性
        std::vector<std::string> wal_files;
        Status s = DiscoverWALFiles(&wal_files);
        if (!s.ok()) {
            return Status::IOError("Failed to discover WAL files: " + s.ToString());
        }
        
        for (const auto& filename : wal_files) {
            s = ValidateWALFile(filename);
            if (!s.ok() && !s.IsCorruption()) {
                return Status::IOError("Fatal error validating WAL file " + filename + ": " + s.ToString());
            }
        }
        
        // 第二步：验证序列号的单调性
        SequenceNumber last_sequence = 0;
        std::set<SequenceNumber> used_sequences;
        
        for (const auto& filename : wal_files) {
            WALReader reader(filename);
            Status open_status = reader.Open();
            if (!open_status.ok()) {
                continue; // 跳过无法打开的文件
            }
            
            WALRecord record;
            while (reader.ReadNextRecord(&record).ok()) {
                if (record.sequence_number > 0) {
                    // 检查序列号是否单调递增
                    if (record.sequence_number <= last_sequence) {
                        return Status::Corruption("Sequence number not monotonic: " + 
                                                 std::to_string(record.sequence_number) + 
                                                 " <= " + std::to_string(last_sequence));
                    }
                    
                    // 检查序列号是否重复
                    if (used_sequences.count(record.sequence_number) > 0) {
                        return Status::Corruption("Duplicate sequence number: " + 
                                                 std::to_string(record.sequence_number));
                    }
                    
                    used_sequences.insert(record.sequence_number);
                    last_sequence = record.sequence_number;
                }
            }
        }
        
        // 第三步：验证事务的完整性
        std::unordered_map<uint64_t, TransactionState> transaction_states;
        
        for (const auto& filename : wal_files) {
            WALReader reader(filename);
            Status open_status = reader.Open();
            if (!open_status.ok()) {
                continue;
            }
            
            WALRecord record;
            while (reader.ReadNextRecord(&record).ok()) {
                if (record.transaction_id > 0) {
                    switch (record.type) {
                        case WALRecordType::kBeginTransaction:
                            if (transaction_states.count(record.transaction_id) > 0) {
                                return Status::Corruption("Transaction already exists: " + 
                                                         std::to_string(record.transaction_id));
                            }
                            transaction_states[record.transaction_id] = TransactionState::kActive;
                            break;
                            
                        case WALRecordType::kCommitTransaction:
                            if (transaction_states.count(record.transaction_id) == 0) {
                                return Status::Corruption("Commit without begin: " + 
                                                         std::to_string(record.transaction_id));
                            }
                            transaction_states[record.transaction_id] = TransactionState::kCommitted;
                            break;
                            
                        case WALRecordType::kAbortTransaction:
                            if (transaction_states.count(record.transaction_id) == 0) {
                                return Status::Corruption("Abort without begin: " + 
                                                         std::to_string(record.transaction_id));
                            }
                            transaction_states[record.transaction_id] = TransactionState::kAborted;
                            break;
                            
                        default:
                            // 其他事务相关操作
                            if (transaction_states.count(record.transaction_id) == 0) {
                                return Status::Corruption("Transaction operation without begin: " + 
                                                         std::to_string(record.transaction_id));
                            }
                            break;
                    }
                }
            }
        }
        
        // 第四步：验证SSTable文件的完整性
        std::string sstable_dir = db_path_;
        
        for (const auto& entry : std::filesystem::directory_iterator(sstable_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.find(".sst") != std::string::npos) {
                    // 验证SSTable文件大小不为零
                    if (entry.file_size() == 0) {
                        return Status::Corruption("Empty SSTable file: " + filename);
                    }
                    
                    // 验证文件可以正常读取
                    std::ifstream file(entry.path(), std::ios::binary);
                    if (!file.is_open()) {
                        return Status::IOError("Cannot open SSTable file: " + filename);
                    }
                    
                    // 简单的文件头验证
                    char header[4];
                    file.read(header, 4);
                    if (file.gcount() < 4) {
                        return Status::Corruption("Truncated SSTable file: " + filename);
                    }
                    
                    file.close();
                }
            }
        }
        
        // 第五步：验证MANIFEST文件（如果存在）
        std::string manifest_file = db_path_ + "/MANIFEST";
        if (std::filesystem::exists(manifest_file)) {
            std::ifstream manifest(manifest_file);
            if (!manifest.is_open()) {
                return Status::IOError("Cannot open MANIFEST file");
            }
            
            // 验证MANIFEST文件不为空
            manifest.seekg(0, std::ios::end);
            if (manifest.tellg() == 0) {
                return Status::Corruption("Empty MANIFEST file");
            }
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during consistency validation: " + 
                               std::string(e.what()));
    }
}

Status RecoveryManager::RepairCorruption() {
    // 尝试修复损坏的数据
    std::vector<std::string> wal_files;
    Status s = DiscoverWALFiles(&wal_files);
    if (!s.ok()) {
        return s;
    }
    
    // 修复损坏的WAL文件
    for (const auto& filename : wal_files) {
        s = ValidateWALFile(filename);
        if (!s.ok()) {
            Status repair_status = RepairWALFile(filename);
            if (!repair_status.ok()) {
                // 如果无法修复，移动到.corrupted扩展名
                try {
                    std::filesystem::rename(filename, filename + ".corrupted");
                } catch (...) {
                    // 忽略重命名错误
                }
            }
        }
    }
    
    return Status::OK();
}

Status RecoveryManager::CreateCheckpoint(SequenceNumber sequence) {
    if (sequence == 0) {
        return Status::InvalidArgument("Invalid sequence number for checkpoint");
    }
    
    try {
        // 确保检查点目录存在
        std::string checkpoint_dir = db_path_ + "/checkpoints";
        std::filesystem::create_directories(checkpoint_dir);
        
        // 生成带时间戳的检查点文件名
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        
        std::ostringstream oss;
        oss << checkpoint_dir << "/checkpoint_" << sequence << "_" << time_t << ".meta";
        std::string checkpoint_file = oss.str();
        
        // 保存检查点
        Status s = SaveCheckpoint(sequence, checkpoint_file);
        if (!s.ok()) {
            return Status::IOError("Failed to save checkpoint: " + s.ToString());
        }
        
        // 清理旧的检查点文件（保留最近的5个）
        Status cleanup_status = CleanupOldCheckpoints(checkpoint_dir, 5);
        if (!cleanup_status.ok()) {
            // 清理失败不影响检查点创建
            std::cerr << "Warning: Failed to cleanup old checkpoints: " << cleanup_status.ToString() << std::endl;
        }
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during checkpoint creation: " + std::string(e.what()));
    }
}

Status RecoveryManager::RestoreFromCheckpoint(SequenceNumber checkpoint_sequence) {
    // CreateCheckpoint 把文件写到 db_path_/checkpoints/checkpoint_<seq>_<ts>.meta，
    // 这里必须去同一目录找（旧实现去 db_path_ 根目录找无后缀文件，永远找不到）
    std::string checkpoint_dir = db_path_ + "/checkpoints";
    std::string checkpoint_file;
    std::error_code ec;
    if (!std::filesystem::exists(checkpoint_dir, ec)) {
        return Status::IOError("Checkpoint directory does not exist");
    }
    for (const auto &entry :
         std::filesystem::directory_iterator(checkpoint_dir, ec)) {
        if (ec) break;
        std::string name = entry.path().filename().string();
        // 匹配该序列号的检查点（时间戳部分任意，取最新即字典序最大）
        std::string prefix =
            "checkpoint_" + std::to_string(checkpoint_sequence) + "_";
        if (name.rfind(prefix, 0) == 0) {
            if (checkpoint_file.empty() || name > std::filesystem::path(checkpoint_file).filename().string()) {
                checkpoint_file = entry.path().string();
            }
        }
    }
    if (checkpoint_file.empty()) {
        return Status::NotFound("Checkpoint " +
                                std::to_string(checkpoint_sequence) +
                                " not found");
    }

    SequenceNumber sequence;
    return LoadCheckpoint(checkpoint_file, &sequence);
}

RecoveryManager::RecoveryStats RecoveryManager::GetRecoveryStats() const {
    return recovery_stats_;
}

Status RecoveryManager::DiscoverWALFiles(std::vector<std::string>* wal_files) {
    try {
        std::string wal_dir = db_path_ + "/wal";
        if (!std::filesystem::exists(wal_dir)) {
            return Status::OK(); // 没有WAL文件
        }
        
        for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename();
                if (filename.size() > 4 && filename.substr(filename.size() - 4) == ".wal") {
                    wal_files->push_back(entry.path());
                }
            }
        }
        
        // 按文件名(文件号)排序
        std::sort(wal_files->begin(), wal_files->end());
        
    } catch (const std::exception& e) {
        return Status::IOError("Failed to discover WAL files: " + std::string(e.what()));
    }
    
    return Status::OK();
}

Status RecoveryManager::RecoverFromWALFiles(const std::vector<std::string>& wal_files,
                                           SequenceNumber* last_sequence) {
    SequenceNumber max_sequence = 0;
    recovery_stats_.records_recovered = 0;
    recovery_stats_.corrupted_records = 0;
    
    for (const auto& filename : wal_files) {
        WALReader reader(filename);
        Status s = reader.Open();
        if (!s.ok()) {
            recovery_stats_.corrupted_records++;
            continue;
        }
        
        WALRecord record;
        while (!reader.IsEOF()) {
            s = reader.ReadNextRecord(&record);
            if (s.IsIncomplete()) {
                break; // 正常结束
            } else if (!s.ok()) {
                recovery_stats_.corrupted_records++;
                break; // 文件损坏
            }

            // 独立 RecoveryManager 没有挂接存储引擎，无法应用记录。
            // 与其"返回成功但什么都没做"（静默丢数据），不如显式报未实现，
            // 让调用方改走 WALManager::RecoverFromWAL（列族初始化已使用该路径）
            return Status::NotSupported(
                "RecoveryManager is not wired to a storage engine; use "
                "WALManager::RecoverFromWAL");
        }
    }

    *last_sequence = max_sequence;
    recovery_stats_.recovered_sequence = max_sequence;

    return Status::OK();
}

Status RecoveryManager::ValidateWALFile(const std::string& filename) {
    WALReader reader(filename);
    return reader.ValidateFile();
}

Status RecoveryManager::RepairWALFile(const std::string& filename) {
    // 尝试修复损坏的WAL文件
    // 简化实现 - 实际需要更复杂的修复逻辑
    return ValidateWALFile(filename);
}

Status RecoveryManager::SaveCheckpoint(SequenceNumber sequence, 
                                      const std::string& checkpoint_file) {
    std::ofstream file(checkpoint_file, std::ios::binary);
    if (!file) {
        return Status::IOError("Cannot create checkpoint file");
    }
    
    // 写入检查点头部
    std::string header;
    coding::PutFixed64(&header, sequence);
    coding::PutFixed64(&header, std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    file.write(header.data(), header.size());
    
    if (file.fail()) {
        return Status::IOError("Failed to write checkpoint");
    }
    
    return Status::OK();
}

Status RecoveryManager::LoadCheckpoint(const std::string& checkpoint_file, 
                                      SequenceNumber* sequence) {
    std::ifstream file(checkpoint_file, std::ios::binary);
    if (!file) {
        return Status::IOError("Cannot open checkpoint file");
    }
    
    // 读取检查点头部
    char header[16];
    file.read(header, 16);
    
    if (file.gcount() != 16) {
        return Status::Corruption("Checkpoint file truncated");
    }
    
    *sequence = coding::DecodeFixed64(header);
    // uint64_t timestamp = coding::DecodeFixed64(header + 8);
    (void)header; // 时间戳字段保留未用
    return Status::OK();
}

// WAL工具函数实现
namespace wal_util {

std::unique_ptr<WALManager> CreateWALManager(const std::string& db_path) {
    auto manager = std::make_unique<WALManager>(db_path);
    
    // 添加超时保护
    auto start = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::seconds(8); // 总超时时间
    
    while (std::chrono::steady_clock::now() - start < timeout) {
        Status s = manager->Initialize();
        if (s.ok()) {
            return manager;
        }
        
        // 如果初始化失败，等待一段时间后重试
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    
    // 超时后返回nullptr
    return nullptr;
}

std::unique_ptr<RecoveryManager> CreateRecoveryManager(const std::string& db_path) {
    return std::make_unique<RecoveryManager>(db_path);
}

Status ParseWALFilename(const std::string& filename, uint64_t* file_number) {
    if (filename.size() <= 4 || filename.substr(filename.size() - 4) != ".wal") {
        return Status::InvalidArgument("Invalid WAL filename");
    }
    
    std::string number_part = filename.substr(0, filename.size() - 4);
    try {
        *file_number = std::stoull(number_part);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Cannot parse file number: " + std::string(e.what()));
    }
}

std::string FormatWALFilename(uint64_t file_number) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(6) << file_number << ".wal";
    return oss.str();
}

std::string FormatWALRecord(const WALRecord& record) {
    std::ostringstream oss;
    oss << "WALRecord{type=" << static_cast<int>(record.type)
        << ", seq=" << record.sequence_number
        << ", txn=" << record.transaction_id
        << ", key=" << record.key
        << ", value_size=" << record.value.size()
        << ", crc=" << std::hex << record.crc << "}";
    return oss.str();
}

Status ValidateWALRecord(const WALRecord& record) {
    if (!record.ValidateCRC()) {
        return Status::Corruption("WAL record CRC mismatch");
    }
    
    // 验证记录类型（枚举值1..11，与WALRecordType一致）
    int type_value = static_cast<int>(record.type);
    if (type_value < static_cast<int>(WALRecordType::kPut) ||
        type_value > static_cast<int>(WALRecordType::kUpdateColumnFamilyOptions)) {
        return Status::Corruption("Invalid WAL record type");
    }
    
    return Status::OK();
}

Status CopyWALFile(const std::string& src, const std::string& dst) {
    try {
        std::filesystem::copy_file(src, dst);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to copy WAL file: " + std::string(e.what()));
    }
}

// 简单RLE压缩（PackBits风格）：
// - 控制字节 0..127：后跟 n+1 个字面字节
// - 控制字节 129..255：后跟 1 个字节，重复 (n-125) 次（即3..130次）
// 该实现零外部依赖，压缩率取决于WAL记录的重复程度
static std::string RLECompress(const std::string& data) {
    std::string out;
    out.reserve(data.size());
    size_t i = 0;
    const size_t n = data.size();

    while (i < n) {
        // 检查从当前位置开始的重复字节
        size_t run_end = i + 1;
        while (run_end < n && data[run_end] == data[i] &&
               run_end - i < 130) {
            ++run_end;
        }

        if (run_end - i >= 3) {
            // 压缩重复串
            out.push_back(static_cast<char>((run_end - i - 3) | 0x80));
            out.push_back(data[i]);
            i = run_end;
        } else {
            // 收集字面字节段
            size_t literal_start = i;
            size_t literal_end = i;
            while (literal_end < n) {
                size_t next_run_end = literal_end + 1;
                while (next_run_end < n && data[next_run_end] == data[literal_end] &&
                       next_run_end - literal_end < 130) {
                    ++next_run_end;
                }
                if (next_run_end - literal_end >= 3) {
                    break; // 后面有可压缩的重复串，字面段到此为止
                }
                ++literal_end;
                if (literal_end - literal_start == 128) {
                    break; // 单段字面最大128字节
                }
            }
            if (literal_end == literal_start) {
                ++literal_end; // 至少前进一个字节
            }
            out.push_back(static_cast<char>(literal_end - literal_start - 1));
            out.append(data, literal_start, literal_end - literal_start);
            i = literal_end;
        }
    }

    return out;
}

static bool RLEDecompress(const std::string& data, std::string* out) {
    out->clear();
    size_t i = 0;
    const size_t n = data.size();

    while (i < n) {
        unsigned char ctrl = static_cast<unsigned char>(data[i++]);
        if (ctrl <= 127) {
            // 字面段
            size_t len = static_cast<size_t>(ctrl) + 1;
            if (i + len > n) {
                return false;
            }
            out->append(data, i, len);
            i += len;
        } else {
            // 重复串
            size_t len = static_cast<size_t>(ctrl) - 125;
            if (i >= n) {
                return false;
            }
            out->append(len, data[i]);
            ++i;
        }
    }

    return true;
}

Status CompressWALFile(const std::string& filename) {
    try {
        std::ifstream in(filename, std::ios::binary);
        if (!in.is_open()) {
            return Status::IOError("Failed to open WAL file for compression");
        }

        std::string data((std::istreambuf_iterator<char>(in)),
                         std::istreambuf_iterator<char>());
        in.close();

        if (data.empty()) {
            return Status::OK();
        }

        std::string compressed = RLECompress(data);
        std::string compressed_filename = filename + ".compressed";
        std::ofstream out(compressed_filename, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            return Status::IOError("Failed to create compressed WAL file");
        }
        out.write(compressed.data(), compressed.size());
        out.close();

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to compress WAL file: " + std::string(e.what()));
    }
}

Status DecompressWALFile(const std::string& compressed_filename) {
    try {
        std::ifstream in(compressed_filename, std::ios::binary);
        if (!in.is_open()) {
            return Status::IOError("Failed to open compressed WAL file");
        }

        std::string data((std::istreambuf_iterator<char>(in)),
                         std::istreambuf_iterator<char>());
        in.close();

        std::string decompressed;
        if (!RLEDecompress(data, &decompressed)) {
            return Status::Corruption("Corrupted compressed WAL data");
        }

        // 输出文件名：去掉.compressed后缀
        std::string output_filename = compressed_filename;
        const std::string suffix = ".compressed";
        if (output_filename.size() > suffix.size() &&
            output_filename.compare(output_filename.size() - suffix.size(),
                                    suffix.size(), suffix) == 0) {
            output_filename.resize(output_filename.size() - suffix.size());
        }

        std::ofstream out(output_filename, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            return Status::IOError("Failed to create decompressed WAL file");
        }
        out.write(decompressed.data(), decompressed.size());
        out.close();

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to decompress WAL file: " + std::string(e.what()));
    }
}

}  // namespace wal_util

// LSMTree集成方法实现
void WALManager::SetLSMTree(LSMTree* lsm_tree) {
    lsm_tree_ = lsm_tree;
}

LSMTree* WALManager::GetLSMTree() const {
    return lsm_tree_;
}

// 检查是否需要执行后台任务
bool WALManager::NeedsSync() const {
    // 加锁访问 current_writer_（Shutdown/Archive/Truncate 会在锁内 reset 它）
    std::lock_guard<std::mutex> lock(wal_mutex_);
    if (!current_writer_ || !current_writer_->IsOpen()) {
        return false;
    }

    // 如果启用了写入时同步，则不需要后台同步
    if (sync_on_write_.load()) {
        return false;
    }

    // 有未 fsync 的数据才需要同步（同步成功后计数归零，任务不会空转）
    return current_writer_->HasUnsyncedData();
}

bool WALManager::NeedsArchive() const {
    if (!current_writer_ || !current_writer_->IsOpen()) {
        return false;
    }
    
    // 检查当前WAL文件是否超过最大大小
    return current_writer_->GetFileSize() >= max_wal_size_.load();
}

bool WALManager::NeedsCleanup() const {
    // 检查是否有多个WAL文件（简化实现）
    auto wal_files = GetWALFiles();
    return wal_files.size() > 3;  // 保留最新的3个文件
}

// ============================================================================
// 批量写入实现 - 支持MVCC刷盘
// ============================================================================

Status WALManager::WriteBatch(uint32_t column_family_id, const std::vector<VersionedEntry>& entries) {
    if (entries.empty()) {
        return Status::OK();
    }

    if (!initialized_.load()) {
        return Status::InvalidArgument("WAL manager not initialized");
    }

    // 不要在外部持 wal_mutex_：WriteRecordInternal 内部会加锁，
    // 这里再持锁会自我死锁
    for (const auto& entry : entries) {
        WALRecord record;
        // 按条目自身的类型与序列号写入（之前的占位实现把 seq 置 0 且
        // 一律当 Put 写，既写不进去也丢失语义）
        record.type = entry.type;
        record.sequence_number = entry.sequence_number;
        record.column_family_id = column_family_id;
        record.key = entry.key;
        record.value = entry.value;
        record.transaction_id = entry.transaction_id;

        Status s = WriteRecordInternal(record);
        if (!s.ok()) {
            return s;
        }
    }

    // 批量写入后立即同步（可选）
    if (sync_on_write_.load()) {
        return Sync();
    }

    return Status::OK();
}

// ============================================================================
// WAL系统优化扩展实现
// ============================================================================

// 部分WAL文件恢复（处理损坏的文件）
// 尝试从损坏的 WAL 文件中恢复尽可能多的记录（容错）
// 事务一致性验证
Status WALManager::ValidateTransactionConsistency(
    const std::unordered_map<uint64_t, TransactionState>& transaction_states) {
    
    for (const auto& [txn_id, state] : transaction_states) {
        // 检查事务状态的有效性
        switch (state) {
            case TransactionState::kActive:
                // 事务开始但未提交，需要回滚
                break;
            case TransactionState::kCommitted:
                // 正常状态
                break;
            case TransactionState::kAborted:
                // 正常状态
                break;
            default:
                return Status::Corruption("Invalid transaction state for transaction " + 
                                         std::to_string(txn_id));
        }
    }
    
    return Status::OK();
}

// 回滚不完整的事务
Status WALManager::RollbackIncompleteTransaction(uint64_t transaction_id) {
    // 简化实现：从活跃事务列表中移除
    std::lock_guard<std::mutex> lock(transaction_mutex_);
    active_transactions_.erase(transaction_id);
    return Status::OK();
}

// 更新事务状态
Status WALManager::UpdateTransactionState(const WALRecord& record) {
    std::lock_guard<std::mutex> lock(transaction_mutex_);
    
    uint64_t txn_id = record.transaction_id;
    
    switch (record.type) {
        case WALRecordType::kBeginTransaction:
            if (active_transactions_.find(txn_id) != active_transactions_.end()) {
                return Status::InvalidArgument("Transaction already active: " + 
                                              std::to_string(txn_id));
            }
            active_transactions_[txn_id] = 1;  // 1 表示活跃状态
            break;
            
        case WALRecordType::kCommitTransaction:
            if (active_transactions_.find(txn_id) == active_transactions_.end()) {
                return Status::InvalidArgument("Transaction not found: " + 
                                              std::to_string(txn_id));
            }
            active_transactions_[txn_id] = 2;  // 2 表示已提交状态
            break;
            
        case WALRecordType::kAbortTransaction:
            if (active_transactions_.find(txn_id) == active_transactions_.end()) {
                return Status::InvalidArgument("Transaction not found: " + 
                                              std::to_string(txn_id));
            }
            active_transactions_[txn_id] = 3;  // 3 表示已中止状态
            break;
            
        default:
            // 非事务相关记录，不需要更新状态
            break;
    }
    
    return Status::OK();
}

// CleanupOldCheckpoints方法实现
Status RecoveryManager::CleanupOldCheckpoints(const std::string& checkpoint_dir, int keep_count) {
    try {
        std::vector<std::string> checkpoint_files;
        
        // 列出检查点目录中的所有文件
        for (const auto& entry : std::filesystem::directory_iterator(checkpoint_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.find("checkpoint_") == 0 && filename.find(".meta") != std::string::npos) {
                    checkpoint_files.push_back(entry.path().string());
                }
            }
        }
        
        // 按修改时间排序（最新的在前）
        std::sort(checkpoint_files.begin(), checkpoint_files.end(), 
                 [](const std::string& a, const std::string& b) {
                     std::filesystem::file_time_type time_a = std::filesystem::last_write_time(a);
                     std::filesystem::file_time_type time_b = std::filesystem::last_write_time(b);
                     return time_a > time_b;
                 });
        
        // 删除多余的检查点文件
        if (checkpoint_files.size() > static_cast<size_t>(keep_count)) {
            for (size_t i = keep_count; i < checkpoint_files.size(); ++i) {
                std::filesystem::remove(checkpoint_files[i]);
            }
        }
        
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to cleanup old checkpoints: " + std::string(e.what()));
    }
}


}  // namespace lrdb
