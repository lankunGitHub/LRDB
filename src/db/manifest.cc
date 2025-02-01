// MANIFEST文件管理实现

#include "lrdb/db/manifest.h"
#include "lrdb/core/coding.h"
#include "lrdb/util/logging.h"

#include <filesystem>
#include <iomanip>
#include <sstream>
#include <algorithm>

namespace lrdb {

// ============================================================================
// ColumnFamilyDescriptor实现
// ============================================================================

std::string ColumnFamilyDescriptor::Encode() const {
    std::string result;
    
    // 编码格式: id(4) + name_len(4) + name + is_dropped(1) + create_seq(8) + drop_seq(8) + options
    coding::PutFixed32(&result, id);
    coding::PutFixed32(&result, static_cast<uint32_t>(name.size()));
    result.append(name);
    result.push_back(is_dropped ? 1 : 0);
    coding::PutFixed64(&result, create_sequence);
    coding::PutFixed64(&result, drop_sequence);
    
    // 编码选项
    std::string options_data = manifest_util::EncodeColumnFamilyOptions(options);
    coding::PutFixed32(&result, static_cast<uint32_t>(options_data.size()));
    result.append(options_data);
    
    return result;
}

Status ColumnFamilyDescriptor::Decode(const Slice& data) {
    if (data.size() < 25) { // 最小大小: 4+4+0+1+8+8=25
        return Status::Corruption("ColumnFamilyDescriptor data too short");
    }
    
    const char* p = data.data();
    
    // 解码基本字段
    id = coding::DecodeFixed32(p); p += 4;
    uint32_t name_len = coding::DecodeFixed32(p); p += 4;
    
    if (data.size() < 25 + name_len) {
        return Status::Corruption("ColumnFamilyDescriptor name length invalid");
    }
    
    name = std::string(p, name_len); p += name_len;
    is_dropped = (*p++ != 0);
    create_sequence = coding::DecodeFixed64(p); p += 8;
    drop_sequence = coding::DecodeFixed64(p); p += 8;
    
    // 解码选项
    if (p + 4 > data.data() + data.size()) {
        return Status::Corruption("ColumnFamilyDescriptor options length missing");
    }
    
    uint32_t options_len = coding::DecodeFixed32(p); p += 4;
    
    if (p + options_len > data.data() + data.size()) {
        return Status::Corruption("ColumnFamilyDescriptor options data truncated");
    }
    
    Slice options_data(p, options_len);
    return manifest_util::DecodeColumnFamilyOptions(options_data, &options);
}

// ============================================================================
// ManifestRecord实现
// ============================================================================

ManifestRecord ManifestRecord::CreateColumnFamilyRecord(uint64_t sequence, const ColumnFamilyDescriptor& cf_desc) {
    ManifestRecord record;
    record.type = ManifestRecordType::kCreateColumnFamily;
    record.sequence_number = sequence;
    record.payload = cf_desc.Encode();
    record.crc = record.CalculateCRC();
    return record;
}

ManifestRecord ManifestRecord::DropColumnFamilyRecord(uint64_t sequence, uint32_t cf_id) {
    ManifestRecord record;
    record.type = ManifestRecordType::kDropColumnFamily;
    record.sequence_number = sequence;
    coding::PutFixed32(&record.payload, cf_id);
    record.crc = record.CalculateCRC();
    return record;
}

ManifestRecord ManifestRecord::UpdateColumnFamilyOptionsRecord(uint64_t sequence, uint32_t cf_id, const ColumnFamilyOptions& options) {
    ManifestRecord record;
    record.type = ManifestRecordType::kUpdateColumnFamilyOptions;
    record.sequence_number = sequence;
    
    coding::PutFixed32(&record.payload, cf_id);
    std::string options_data = manifest_util::EncodeColumnFamilyOptions(options);
    coding::PutFixed32(&record.payload, static_cast<uint32_t>(options_data.size()));
    record.payload.append(options_data);
    
    record.crc = record.CalculateCRC();
    return record;
}

ManifestRecord ManifestRecord::SequenceNumberRecord(uint64_t sequence) {
    ManifestRecord record;
    record.type = ManifestRecordType::kSequenceNumber;
    record.sequence_number = sequence;
    // payload为空，序列号已在sequence_number字段中
    record.crc = record.CalculateCRC();
    return record;
}

ManifestRecord ManifestRecord::CheckpointRecord(uint64_t sequence) {
    ManifestRecord record;
    record.type = ManifestRecordType::kCheckpoint;
    record.sequence_number = sequence;
    // payload为空
    record.crc = record.CalculateCRC();
    return record;
}

std::string ManifestRecord::Encode() const {
    std::string result;
    
    // 记录头: type(1) + sequence(8) + payload_len(4)
    result.push_back(static_cast<char>(type));
    coding::PutFixed64(&result, sequence_number);
    coding::PutFixed32(&result, static_cast<uint32_t>(payload.size()));
    
    // 数据: payload
    result.append(payload);
    
    // CRC
    coding::PutFixed32(&result, crc);
    
    return result;
}

Status ManifestRecord::Decode(const Slice& data) {
    if (data.size() < 17) { // 最小记录大小: 1+8+4+4=17
        return Status::Corruption("Manifest record too short");
    }
    
    const char* p = data.data();
    
    // 解析头部
    type = static_cast<ManifestRecordType>(*p++);
    sequence_number = coding::DecodeFixed64(p); p += 8;
    uint32_t payload_len = coding::DecodeFixed32(p); p += 4;
    
    // 检查剩余数据长度
    size_t remaining = data.size() - (p - data.data());
    if (remaining < payload_len + 4) {
        return Status::Corruption("Manifest record data truncated");
    }
    
    // 解析payload
    payload = std::string(p, payload_len); p += payload_len;
    
    // 解析CRC
    crc = coding::DecodeFixed32(p);
    
    // 验证CRC
    if (!ValidateCRC()) {
        return Status::Corruption("Manifest record CRC mismatch");
    }
    
    return Status::OK();
}

uint32_t ManifestRecord::CalculateCRC() const {
    std::string data;
    data.push_back(static_cast<char>(type));
    coding::PutFixed64(&data, sequence_number);
    data.append(payload);
    
    return coding::CalculateCRC32(data.data(), data.size());
}

bool ManifestRecord::ValidateCRC() const {
    return crc == CalculateCRC();
}

// ============================================================================
// ManifestWriter实现
// ============================================================================

ManifestWriter::ManifestWriter(const std::string& filename)
    : filename_(filename), file_size_(0), is_open_(false) {}

ManifestWriter::~ManifestWriter() {
    if (is_open_.load()) {
        Close();
    }
}

Status ManifestWriter::Open() {
    if (is_open_.load()) {
        return Status::InvalidArgument("Manifest file already open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        file_ = std::make_unique<std::ofstream>(filename_, std::ios::binary | std::ios::app);
        
        if (!file_->is_open()) {
            return Status::IOError("Cannot open manifest file: " + filename_);
        }
        
        // 获取当前文件大小
        file_->seekp(0, std::ios::end);
        file_size_.store(file_->tellp());
        
        is_open_.store(true);
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception while opening manifest file: " + std::string(e.what()));
    }
}

Status ManifestWriter::Close() {
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

Status ManifestWriter::WriteRecord(const ManifestRecord& record) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("Manifest file not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        std::string encoded_record = record.Encode();
        
        // 写入记录长度
        uint32_t record_length = static_cast<uint32_t>(encoded_record.size());
        std::string length_header;
        coding::PutFixed32(&length_header, record_length);
        
        // 写入长度头和记录
        file_->write(length_header.data(), length_header.size());
        file_->write(encoded_record.data(), encoded_record.size());
        
        if (file_->fail()) {
            return Status::IOError("Failed to write manifest record");
        }
        
        file_size_.fetch_add(length_header.size() + encoded_record.size());
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception in manifest write: " + std::string(e.what()));
    }
}

Status ManifestWriter::Sync() {
    if (!is_open_.load()) {
        return Status::InvalidArgument("Manifest file not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (file_) {
        file_->flush();
        if (file_->fail()) {
            return Status::IOError("Failed to sync manifest file");
        }
    }
    
    return Status::OK();
}

uint64_t ManifestWriter::GetFileSize() const {
    return file_size_.load();
}

bool ManifestWriter::IsOpen() const {
    return is_open_.load();
}

// ============================================================================
// ManifestReader实现
// ============================================================================

ManifestReader::ManifestReader(const std::string& filename)
    : filename_(filename), read_position_(0), is_open_(false), eof_reached_(false) {}

ManifestReader::~ManifestReader() {
    if (is_open_.load()) {
        Close();
    }
}

Status ManifestReader::Open() {
    if (is_open_.load()) {
        return Status::InvalidArgument("Manifest file already open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    file_ = std::make_unique<std::ifstream>(filename_, std::ios::binary);
    
    if (!file_->is_open()) {
        return Status::IOError("Cannot open manifest file: " + filename_);
    }
    
    read_position_.store(0);
    eof_reached_.store(false);
    is_open_.store(true);
    
    return Status::OK();
}

Status ManifestReader::Close() {
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

Status ManifestReader::ReadNextRecord(ManifestRecord* record) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("Manifest file not open");
    }
    
    if (eof_reached_.load()) {
        return Status::Incomplete("EOF reached");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
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
    if (record_length == 0 || record_length > 10 * 1024 * 1024) { // 最大10MB
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

Status ManifestReader::Seek(uint64_t position) {
    if (!is_open_.load()) {
        return Status::InvalidArgument("Manifest file not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    file_->seekg(position);
    if (file_->fail()) {
        return Status::IOError("Failed to seek in manifest file");
    }
    
    read_position_.store(position);
    eof_reached_.store(false);
    
    return Status::OK();
}

bool ManifestReader::IsEOF() const {
    return eof_reached_.load();
}

uint64_t ManifestReader::GetReadPosition() const {
    return read_position_.load();
}

Status ManifestReader::ValidateFile() {
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
    ManifestRecord record;
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

// ============================================================================
// ManifestManager实现
// ============================================================================

ManifestManager::ManifestManager(const std::string& db_path)
    : db_path_(db_path), next_file_number_(1), last_sequence_(0),
      initialized_(false), shutdown_(false) {}

ManifestManager::~ManifestManager() {
    if (initialized_.load() && !shutdown_.load()) {
        Shutdown();
    }
}

Status ManifestManager::Initialize() {
    if (initialized_.load()) {
        return Status::InvalidArgument("ManifestManager already initialized");
    }
    
    try {
        // 创建MANIFEST目录
        std::string manifest_dir = db_path_;
        std::filesystem::create_directories(manifest_dir);
        
        // 查找现有的MANIFEST文件
        std::string current_file = db_path_ + "/CURRENT";
        if (std::filesystem::exists(current_file)) {
            // 读取当前使用的MANIFEST文件名
            std::ifstream current_stream(current_file);
            if (current_stream.is_open()) {
                std::getline(current_stream, current_manifest_filename_);
                current_stream.close();
                
                // 验证MANIFEST文件是否存在
                if (!current_manifest_filename_.empty() && 
                    std::filesystem::exists(db_path_ + "/" + current_manifest_filename_)) {
                    
                    // 解析文件号
                    uint64_t file_number;
                    if (manifest_util::ParseManifestFilename(current_manifest_filename_, &file_number).ok()) {
                        next_file_number_.store(file_number + 1);
                    }
                } else {
                    current_manifest_filename_.clear();
                }
            }
        }
        
        // 如果没有现有的MANIFEST文件，创建新的
        if (current_manifest_filename_.empty()) {
            Status s = CreateNewManifestFile();
            if (!s.ok()) {
                return s;
            }
        } else {
            // 打开现有的MANIFEST文件
            current_writer_ = std::make_unique<ManifestWriter>(db_path_ + "/" + current_manifest_filename_);
            Status s = current_writer_->Open();
            if (!s.ok()) {
                return Status::IOError("Failed to open existing manifest file: " + s.ToString());
            }
        }
        
        initialized_.store(true);
        LOG_INFO << "ManifestManager initialized, current file: " << current_manifest_filename_;
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during manifest manager initialization: " + std::string(e.what()));
    }
}

Status ManifestManager::Shutdown() {
    if (shutdown_.load()) {
        return Status::OK();
    }
    
    shutdown_.store(true);
    
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    if (current_writer_) {
        current_writer_->Sync();
        current_writer_->Close();
        current_writer_.reset();
    }
    
    initialized_.store(false);
    return Status::OK();
}

Status ManifestManager::LogCreateColumnFamily(const ColumnFamilyDescriptor& cf_desc) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ManifestManager not initialized");
    }
    
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    uint64_t sequence = last_sequence_.fetch_add(1) + 1;
    ManifestRecord record = ManifestRecord::CreateColumnFamilyRecord(sequence, cf_desc);
    
    if (!current_writer_) {
        return Status::InvalidArgument("No active manifest writer");
    }
    
    return current_writer_->WriteRecord(record);
}

Status ManifestManager::LogDropColumnFamily(uint32_t cf_id) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ManifestManager not initialized");
    }
    
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    uint64_t sequence = last_sequence_.fetch_add(1) + 1;
    ManifestRecord record = ManifestRecord::DropColumnFamilyRecord(sequence, cf_id);
    
    if (!current_writer_) {
        return Status::InvalidArgument("No active manifest writer");
    }
    
    return current_writer_->WriteRecord(record);
}

Status ManifestManager::LogUpdateColumnFamilyOptions(uint32_t cf_id, const ColumnFamilyOptions& options) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ManifestManager not initialized");
    }
    
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    uint64_t sequence = last_sequence_.fetch_add(1) + 1;
    ManifestRecord record = ManifestRecord::UpdateColumnFamilyOptionsRecord(sequence, cf_id, options);
    
    if (!current_writer_) {
        return Status::InvalidArgument("No active manifest writer");
    }
    
    return current_writer_->WriteRecord(record);
}

Status ManifestManager::LogSequenceNumber(uint64_t sequence) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ManifestManager not initialized");
    }
    
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    ManifestRecord record = ManifestRecord::SequenceNumberRecord(sequence);
    last_sequence_.store(sequence);
    
    if (!current_writer_) {
        return Status::InvalidArgument("No active manifest writer");
    }
    
    return current_writer_->WriteRecord(record);
}

Status ManifestManager::LogCheckpoint(uint64_t sequence) {
    if (!initialized_.load()) {
        return Status::InvalidArgument("ManifestManager not initialized");
    }
    
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    ManifestRecord record = ManifestRecord::CheckpointRecord(sequence);
    
    if (!current_writer_) {
        return Status::InvalidArgument("No active manifest writer");
    }
    
    return current_writer_->WriteRecord(record);
}

Status ManifestManager::Sync() {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    if (current_writer_) {
        return current_writer_->Sync();
    }
    
    return Status::InvalidArgument("No active manifest writer");
}

Status ManifestManager::RecoverColumnFamilies(std::vector<ColumnFamilyDescriptor>* cf_descriptors, 
                                             uint64_t* last_sequence) {
    if (!cf_descriptors || !last_sequence) {
        return Status::InvalidArgument("Output parameters cannot be null");
    }
    
    cf_descriptors->clear();
    *last_sequence = 0;
    
    try {
        // 获取所有MANIFEST文件
        std::vector<std::string> manifest_files;
        
        for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename();
                if (filename.find("MANIFEST-") == 0) {
                    manifest_files.push_back(entry.path());
                }
            }
        }
        
        // 按文件号排序
        std::sort(manifest_files.begin(), manifest_files.end());
        
        std::unordered_map<uint32_t, ColumnFamilyDescriptor> cf_map;
        uint64_t max_sequence = 0;
        
        // 恢复每个MANIFEST文件
        for (const auto& filename : manifest_files) {
            uint64_t file_sequence = 0;
            Status s = RecoverFromManifestFile(filename, &cf_map, &file_sequence);
            if (s.ok()) {
                max_sequence = std::max(max_sequence, file_sequence);
            } else if (!s.IsCorruption()) {
                return s; // 严重错误
            }
            // 忽略损坏的文件
        }
        
        // 将映射转换为向量，只包含未删除的列族
        for (const auto& [cf_id, cf_desc] : cf_map) {
            if (!cf_desc.is_dropped) {
                cf_descriptors->push_back(cf_desc);
            }
        }
        
        *last_sequence = max_sequence;
        last_sequence_.store(max_sequence);
        
        LOG_INFO << "Recovered " << cf_descriptors->size() << " column families from MANIFEST files";
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception during column family recovery: " + std::string(e.what()));
    }
}

uint64_t ManifestManager::GetCurrentManifestSize() const {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    
    if (current_writer_) {
        return current_writer_->GetFileSize();
    }
    
    return 0;
}

// 内部方法实现

Status ManifestManager::CreateNewManifestFile() {
    uint64_t file_number = next_file_number_.fetch_add(1);
    current_manifest_filename_ = manifest_util::FormatManifestFilename(file_number);
    
    current_writer_ = std::make_unique<ManifestWriter>(db_path_ + "/" + current_manifest_filename_);
    Status s = current_writer_->Open();
    if (!s.ok()) {
        return s;
    }
    
    // 更新CURRENT文件
    return UpdateCurrentFile(current_manifest_filename_);
}

std::string ManifestManager::GenerateManifestFilename(uint64_t file_number) const {
    return manifest_util::FormatManifestFilename(file_number);
}

Status ManifestManager::UpdateCurrentFile(const std::string& manifest_filename) {
    try {
        std::string current_file = db_path_ + "/CURRENT";
        std::string temp_file = current_file + ".tmp";
        
        // 写入临时文件
        std::ofstream temp_stream(temp_file);
        if (!temp_stream.is_open()) {
            return Status::IOError("Cannot create temp CURRENT file");
        }
        
        temp_stream << manifest_filename << std::endl;
        temp_stream.close();
        
        if (temp_stream.fail()) {
            return Status::IOError("Failed to write temp CURRENT file");
        }
        
        // 原子性替换
        std::filesystem::rename(temp_file, current_file);
        
        return Status::OK();
        
    } catch (const std::exception& e) {
        return Status::IOError("Exception updating CURRENT file: " + std::string(e.what()));
    }
}

Status ManifestManager::RecoverFromManifestFile(const std::string& filename,
                                              std::unordered_map<uint32_t, ColumnFamilyDescriptor>* cf_map,
                                              uint64_t* max_sequence) {
    ManifestReader reader(filename);
    Status s = reader.Open();
    if (!s.ok()) {
        return s;
    }
    
    ManifestRecord record;
    uint64_t local_max_sequence = 0;
    
    while (!reader.IsEOF()) {
        s = reader.ReadNextRecord(&record);
        if (s.IsIncomplete()) {
            break; // 正常结束
        } else if (!s.ok()) {
            return s; // 读取错误
        }
        
        local_max_sequence = std::max(local_max_sequence, record.sequence_number);
        
        // 处理不同类型的记录
        switch (record.type) {
            case ManifestRecordType::kCreateColumnFamily: {
                ColumnFamilyDescriptor cf_desc;
                Status decode_status = cf_desc.Decode(Slice(record.payload));
                if (decode_status.ok()) {
                    (*cf_map)[cf_desc.id] = cf_desc;
                }
                break;
            }
            
            case ManifestRecordType::kDropColumnFamily: {
                if (record.payload.size() >= 4) {
                    uint32_t cf_id = coding::DecodeFixed32(record.payload.data());
                    auto it = cf_map->find(cf_id);
                    if (it != cf_map->end()) {
                        it->second.is_dropped = true;
                        it->second.drop_sequence = record.sequence_number;
                    }
                }
                break;
            }
            
            case ManifestRecordType::kUpdateColumnFamilyOptions: {
                if (record.payload.size() >= 8) {
                    uint32_t cf_id = coding::DecodeFixed32(record.payload.data());
                    uint32_t options_len = coding::DecodeFixed32(record.payload.data() + 4);
                    
                    if (record.payload.size() >= 8 + options_len) {
                        auto it = cf_map->find(cf_id);
                        if (it != cf_map->end()) {
                            Slice options_data(record.payload.data() + 8, options_len);
                            manifest_util::DecodeColumnFamilyOptions(options_data, &it->second.options);
                        }
                    }
                }
                break;
            }
            
            case ManifestRecordType::kSequenceNumber:
            case ManifestRecordType::kCheckpoint:
                // 这些记录的主要目的是记录序列号
                break;
                
            default:
                // 忽略未知记录类型
                break;
        }
    }
    
    *max_sequence = local_max_sequence;
    return Status::OK();
}

// ============================================================================
// 工具函数实现
// ============================================================================

namespace manifest_util {

std::unique_ptr<ManifestManager> CreateManifestManager(const std::string& db_path) {
    return std::make_unique<ManifestManager>(db_path);
}

std::string EncodeColumnFamilyOptions(const ColumnFamilyOptions& options) {
    std::string result;
    
    // 简化的选项编码，只编码关键字段
    coding::PutFixed32(&result, 64 * 1024 * 1024);  // 默认64MB
    coding::PutFixed32(&result, options.max_write_buffer_number);
    coding::PutFixed32(&result, options.level0_file_num_compaction_trigger);
    coding::PutFixed32(&result, options.level0_slowdown_writes_trigger);
    coding::PutFixed32(&result, options.level0_stop_writes_trigger);
    coding::PutFixed64(&result, options.target_file_size_base);
    coding::PutFixed64(&result, options.max_bytes_for_level_base);
    result.push_back(static_cast<uint8_t>(options.compression));
    
    return result;
}

Status DecodeColumnFamilyOptions(const Slice& data, ColumnFamilyOptions* options) {
    if (data.size() < 33) { // 4*5 + 8*2 + 1 = 33
        return Status::Corruption("ColumnFamilyOptions data too short");
    }
    
    const char* p = data.data();
    
    (void)coding::DecodeFixed32(p); p += 4;  // 跳过memtable_size字段
    options->max_write_buffer_number = coding::DecodeFixed32(p); p += 4;
    options->level0_file_num_compaction_trigger = coding::DecodeFixed32(p); p += 4;
    options->level0_slowdown_writes_trigger = coding::DecodeFixed32(p); p += 4;
    options->level0_stop_writes_trigger = coding::DecodeFixed32(p); p += 4;
    options->target_file_size_base = coding::DecodeFixed64(p); p += 8;
    options->max_bytes_for_level_base = coding::DecodeFixed64(p); p += 8;
    options->compression = static_cast<CompressionType>(*p++);
    
    return Status::OK();
}

Status ParseManifestFilename(const std::string& filename, uint64_t* file_number) {
    if (filename.size() <= 9 || filename.substr(0, 9) != "MANIFEST-") {
        return Status::InvalidArgument("Invalid manifest filename");
    }
    
    std::string number_part = filename.substr(9);
    try {
        *file_number = std::stoull(number_part);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Cannot parse file number: " + std::string(e.what()));
    }
}

std::string FormatManifestFilename(uint64_t file_number) {
    std::ostringstream oss;
    oss << "MANIFEST-" << std::setfill('0') << std::setw(6) << file_number;
    return oss.str();
}

Status CopyManifestFile(const std::string& src, const std::string& dst) {
    try {
        std::filesystem::copy_file(src, dst);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to copy manifest file: " + std::string(e.what()));
    }
}

}  // namespace manifest_util

// ManifestManager的实现已经在文件前面定义了

}  // namespace lrdb
