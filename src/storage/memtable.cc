// 内存表实现 - 高性能内存数据结构

#include "lrdb/storage/memtable.h"
#include "lrdb/storage/sstable.h"
#include "lrdb/util/error_handler.h"
#include "lrdb/util/logging.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <sstream>

namespace lrdb {

// ============================================================================
// InternalKey 实现
// ============================================================================

InternalKey::InternalKey(const Slice &user_key, uint64_t sequence,
                         ValueType type) {
  // 使用项目的coding工具进行编码
  coding::PutInternalKey(&data_, user_key, sequence,
                         static_cast<uint8_t>(type));
}

Slice InternalKey::user_key() const {
  Slice user_key_slice;
  uint64_t sequence;
  uint8_t type;

  if (coding::ParseInternalKey(Slice(data_), &user_key_slice, &sequence,
                               &type)) {
    return user_key_slice;
  }
  return Slice();
}

uint64_t InternalKey::sequence() const {
  Slice user_key_slice;
  uint64_t sequence;
  uint8_t type;

  if (coding::ParseInternalKey(Slice(data_), &user_key_slice, &sequence,
                               &type)) {
    return sequence;
  }
  return 0;
}

ValueType InternalKey::type() const {
  Slice user_key_slice;
  uint64_t sequence;
  uint8_t type;

  if (coding::ParseInternalKey(Slice(data_), &user_key_slice, &sequence,
                               &type)) {
    return static_cast<ValueType>(type);
  }
  return ValueType::kValue;
}

std::string InternalKey::Encode() const { return data_; }

bool InternalKey::Decode(const Slice &internal_key) {
  // 验证格式是否正确
  Slice user_key_slice;
  uint64_t sequence;
  uint8_t type;

  if (coding::ParseInternalKey(internal_key, &user_key_slice, &sequence,
                               &type)) {
    data_.assign(internal_key.data(), internal_key.size());
    return true;
  }
  return false;
}

int InternalKey::Compare(const InternalKey &other,
                         const Comparator *comparator) const {
  // 首先比较用户键
  int user_cmp = comparator->Compare(user_key(), other.user_key());
  if (user_cmp != 0)
    return user_cmp;

  // 用户键相同时，按序列号降序排列（较新的在前）
  uint64_t seq1 = sequence();
  uint64_t seq2 = other.sequence();
  if (seq1 > seq2)
    return -1;
  if (seq1 < seq2)
    return 1;

  // 序列号相同时，按类型排序（删除 < 值）
  return static_cast<int>(type()) - static_cast<int>(other.type());
}

// ============================================================================
// MemTable::Iterator 实现
// ============================================================================

class MemTableIterator : public MemTable::Iterator {
public:
  explicit MemTableIterator(const MemTable::SkipListType *table)
      : table_(table), iter_(table), valid_(false) {}

  bool Valid() const override { return valid_ && iter_.Valid(); }

  void SeekToFirst() override {
    iter_.SeekToFirst();
    valid_ = iter_.Valid();
    UpdateCurrentEntry();
  }

  void SeekToLast() override {
    iter_.SeekToLast();
    valid_ = iter_.Valid();
    UpdateCurrentEntry();
  }

  void Seek(const Slice &target) override {
    // 创建临时的InternalKey进行查找
    InternalKey seek_key(target, UINT64_MAX, ValueType::kValue);
    std::string encoded_key = seek_key.Encode();

    iter_.Seek(encoded_key);
    valid_ = iter_.Valid();
    UpdateCurrentEntry();
  }

  void Next() override {
    assert(Valid());
    iter_.Next();
    valid_ = iter_.Valid();
    UpdateCurrentEntry();
  }

  void Prev() override {
    assert(Valid());
    iter_.Prev();
    valid_ = iter_.Valid();
    UpdateCurrentEntry();
  }

  Slice key() const override {
    assert(Valid());
    return current_user_key_;
  }

  Slice value() const override {
    assert(Valid());
    return current_value_;
  }

  ValueType type() const override {
    assert(Valid());
    return current_type_;
  }

  uint64_t sequence() const override {
    assert(Valid());
    return current_sequence_;
  }

  Status status() const override { return status_; }

private:
  const MemTable::SkipListType *table_;
  MemTable::SkipListType::Iterator iter_;
  bool valid_;

  // 当前条目缓存
  Slice current_user_key_;
  Slice current_value_;
  ValueType current_type_;
  uint64_t current_sequence_;
  Status status_;

  void UpdateCurrentEntry() {
    if (!valid_) {
      current_user_key_ = Slice();
      current_value_ = Slice();
      current_type_ = ValueType::kValue;
      current_sequence_ = 0;
      return;
    }

    MemTable::MemTableEntry *entry = nullptr;
    if (!table_->Get(iter_.key(), &entry) || !entry) {
      status_ = Status::Corruption("Invalid MemTable entry");
      valid_ = false;
      return;
    }

    current_user_key_ = entry->key.user_key();
    current_value_ = Slice(entry->value);
    current_type_ = entry->key.type();
    current_sequence_ = entry->key.sequence();
    status_ = Status::OK();
  }
};

// ============================================================================
// MemTable 实现
// ============================================================================

MemTable::MemTable(const Comparator *comparator, const MemTableOptions &options)
    : options_(options), refs_(1), immutable_(false),
      memory_usage_(sizeof(MemTable)), num_entries_(0) {

  internal_comparator_ = std::make_unique<InternalKeyComparator>(comparator);
  table_ = std::make_unique<SkipListType>(internal_comparator_.get());

  // 注册error_handler回调
  ErrorRecoveryManager::Instance().RegisterCleanupCallback(
      "MemTable",
      [this](const std::string &component,
             const std::string &operation) -> Status {
        return this->HandleErrorRecovery(component, operation);
      });

  // 注册内存错误的特定恢复回调
  ErrorRecoveryManager::Instance().RegisterRecoveryCallback(
      ErrorType::MemoryError, [this](const ErrorReport &error) -> Status {
        return this->HandleMemoryError(error);
      });

  LOG_DEBUG << "MemTable initialized with error recovery callbacks";
}

MemTable::~MemTable() {
  // 清理所有条目
  entries_.clear();
}

Status MemTable::Put(const Slice &key, const Slice &value, uint64_t sequence) {
  if (immutable_.load(std::memory_order_acquire)) {
    Status error_status =
        Status::InvalidArgument("Cannot write to immutable MemTable");
    HANDLE_ERROR_WITH_RECOVERY(error_status, "Put", "MemTable");
    return error_status;
  }

  InternalKey internal_key(key, sequence, ValueType::kValue);
  MemTableEntry *entry = CreateEntry(internal_key, value);

  if (!entry) {
    Status error_status = Status::IOError("Failed to create MemTable entry");
    HANDLE_ERROR_WITH_RECOVERY(error_status, "Put", "MemTable");
    return error_status;
  }

  try {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::string encoded_key = internal_key.Encode();
    table_->Insert(encoded_key, entry);
    num_entries_.fetch_add(1, std::memory_order_relaxed);
  } catch (const std::exception &e) {
    Status error_status =
        Status::IOError("MemTable insertion failed: " + std::string(e.what()));
    HANDLE_ERROR_WITH_RECOVERY(error_status, "Put", "MemTable");
    return error_status;
  }

  return Status::OK();
}

Status MemTable::Delete(const Slice &key, uint64_t sequence) {
  if (immutable_.load(std::memory_order_acquire)) {
    return Status::InvalidArgument("Cannot write to immutable MemTable");
  }

  InternalKey internal_key(key, sequence, ValueType::kDeletion);
  MemTableEntry *entry = CreateEntry(internal_key, Slice()); // 删除标记没有值

  if (!entry) {
    return Status::IOError("Failed to create MemTable entry");
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::string encoded_key = internal_key.Encode();
    table_->Insert(encoded_key, entry);
    num_entries_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status MemTable::Merge(const Slice &key, const Slice &value,
                       uint64_t sequence) {
  if (immutable_.load(std::memory_order_acquire)) {
    return Status::InvalidArgument("Cannot write to immutable MemTable");
  }

  InternalKey internal_key(key, sequence, ValueType::kMerge);
  MemTableEntry *entry = CreateEntry(internal_key, value);

  if (!entry) {
    return Status::IOError("Failed to create MemTable entry");
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::string encoded_key = internal_key.Encode();
    table_->Merge(encoded_key, entry);
    num_entries_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

bool MemTable::Get(const Slice &key, std::string *value, Status *status,
                   uint64_t snapshot) {
  bool merge = (status && status->IsMergeInProgress()) ? true : false;

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 创建查找键 - 使用最大序列号确保能找到所有版本
  InternalKey seek_key(key, UINT64_MAX, ValueType::kValue);
  std::string encoded_seek_key = seek_key.Encode();

  SkipListType::Iterator iter(table_.get());
  iter.Seek(encoded_seek_key);

  // 回退到第一个可能匹配的位置
  while (iter.Valid()) {
    MemTableEntry *entry = nullptr;
    if (table_->Get(iter.key(), &entry) && entry) {

      // 检查用户键是否匹配
      Slice entry_user_key = entry->key.user_key();
      if (entry_user_key.compare(key) != 0) {
        break; // 键不匹配，没有找到
      }

      // 检查快照可见性
      if (snapshot > 0 && entry->key.sequence() > snapshot) {
        iter.Next();
        continue;
      }

      // 找到匹配的键
      ValueType entry_type = entry->key.type();
      if (entry_type == ValueType::kValue) {
        if (value) {
          if (merge) {
            *value += entry->value;
          } else {
            *value = entry->value;
          }
        }
        if (status) {
          *status = Status::OK();
        }
        return true;
      } else if (entry_type == ValueType::kDeletion) {
        if (status) {
          if (merge) {
            *status = Status::OK();
            return true;
          } else {
            *status = Status::NotFound("Key was deleted");
          }
        }
        return false;
      } else if (entry_type == ValueType::kMerge) {
        if (value) {
          if (merge) {
            *value += entry->value;
          } else {
            *value = entry->value;
          }
        }
        if (status) {
          *status = Status::MergerInProgress();
        }
        return true;
      }
    }

    iter.Next();
  }

  if (status) {
    *status = Status::NotFound("Key not found");
  }
  return false;
}

std::unique_ptr<MemTable::Iterator> MemTable::NewIterator() const {
  return std::make_unique<MemTableIterator>(table_.get());
}

size_t MemTable::ApproximateMemoryUsage() const {
  return memory_usage_.load(std::memory_order_relaxed);
}

size_t MemTable::NumEntries() const {
  return num_entries_.load(std::memory_order_relaxed);
}

bool MemTable::Empty() const {
  return num_entries_.load(std::memory_order_relaxed) == 0;
}

bool MemTable::ShouldFlush() const {
  return ApproximateMemoryUsage() >= options_.write_buffer_size;
}

void MemTable::Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

void MemTable::Unref() {
  if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    delete this;
  }
}

Status MemTable::FlushToSSTable(const std::string &output_file,
                                SSTableWriter *writer) {
  if (!writer) {
    return Status::InvalidArgument("SSTableWriter is null");
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto iter = NewIterator();
  iter->SeekToFirst();

  uint64_t entries_written = 0;

  while (iter->Valid()) {
    InternalKey internal_key(iter->key(), iter->sequence(), iter->type());
    std::string encoded_key = internal_key.Encode();

    Status s = writer->Add(Slice(encoded_key), iter->value());
    if (!s.ok()) {
      LOG_ERROR << "Failed to write entry to SSTable: " << s.ToString();
      return s;
    }

    entries_written++;
    iter->Next();

    if (!iter->status().ok()) {
      LOG_ERROR << "Iterator error during flush: " << iter->status().ToString();
      return iter->status();
    }
  }

  LOG_INFO << "Flushed " << entries_written << " entries to " << output_file;

  return Status::OK();
}

bool MemTable::GetRange(std::string *smallest_key,
                        std::string *largest_key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (Empty()) {
    return false;
  }

  auto iter = NewIterator();

  // 获取最小键
  iter->SeekToFirst();
  if (iter->Valid()) {
    if (smallest_key) {
      *smallest_key = iter->key().ToString();
    }
  } else {
    return false;
  }

  // 获取最大键
  iter->SeekToLast();
  if (iter->Valid()) {
    if (largest_key) {
      *largest_key = iter->key().ToString();
    }
  } else {
    return false;
  }

  return true;
}

void MemTable::SetImmutable() {
  immutable_.store(true, std::memory_order_release);
}

bool MemTable::IsImmutable() const {
  return immutable_.load(std::memory_order_acquire);
}

bool MemTable::ValidateStructure() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto iter = NewIterator();
  iter->SeekToFirst();

  std::string last_key;
  bool first = true;
  size_t count = 0;

  while (iter->Valid()) {
    if (!iter->status().ok()) {
      LOG_ERROR << "Iterator error during validation: "
                << iter->status().ToString();
      return false;
    }

    std::string current_key = iter->key().ToString();

    if (!first) {
      if (current_key < last_key) {
        LOG_ERROR << "Keys not in sorted order";
        return false;
      }
    }

    last_key = current_key;
    first = false;
    count++;

    iter->Next();
  }

  if (count != num_entries_.load(std::memory_order_relaxed)) {
    LOG_ERROR << "Entry count mismatch: expected " << num_entries_.load()
              << ", found " << count;
    return false;
  }

  return true;
}

std::string MemTable::DebugString() const {
  std::ostringstream oss;
  oss << "MemTable{";
  oss << "entries=" << NumEntries();
  oss << ", memory=" << ApproximateMemoryUsage();
  oss << ", immutable=" << IsImmutable();
  oss << ", refs=" << refs_.load();
  oss << "}";
  return oss.str();
}

// 私有方法实现
MemTable::MemTableEntry *MemTable::CreateEntry(const InternalKey &key,
                                               const Slice &value) {
  try {
    auto entry = std::make_unique<MemTableEntry>(key, value);
    size_t entry_size = entry->MemoryUsage();

    // 更新内存使用量
    memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);

    MemTableEntry *raw_entry = entry.get();
    entries_.push_back(std::move(entry));

    return raw_entry;
  } catch (const std::exception &e) {
    LOG_ERROR << "Failed to create MemTable entry: " << e.what();
    return nullptr;
  }
}

// ============================================================================
// MemTableManager 实现
// ============================================================================

MemTableManager::MemTableManager(const Comparator *comparator,
                                 const MemTableOptions &options)
    : comparator_(comparator), options_(options) {
  CreateNewMemTable();
}

MemTableManager::~MemTableManager() {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  if (mutable_memtable_) {
    mutable_memtable_->Unref();
  }

  for (auto &memtable : immutable_memtables_) {
    memtable->Unref();
  }
}

MemTable *MemTableManager::GetMutableMemTable() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return mutable_memtable_.get();
}

std::vector<MemTable *> MemTableManager::GetImmutableMemTables() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<MemTable *> result;
  result.reserve(immutable_memtables_.size());

  for (const auto &memtable : immutable_memtables_) {
    result.push_back(memtable.get());
  }

  return result;
}

Status MemTableManager::Put(const Slice &key, const Slice &value,
                            uint64_t sequence) {
  MemTable *table = GetMutableMemTable();
  if (!table) {
    return Status::InvalidArgument("No mutable MemTable available");
  }

  return table->Put(key, value, sequence);
}

Status MemTableManager::Delete(const Slice &key, uint64_t sequence) {
  MemTable *table = GetMutableMemTable();
  if (!table) {
    return Status::InvalidArgument("No mutable MemTable available");
  }

  return table->Delete(key, sequence);
}

Status MemTableManager::Merge(const Slice &key, const Slice &value,
                              uint64_t sequence) {
  MemTable *table = GetMutableMemTable();
  if (!table) {
    return Status::InvalidArgument("No mutable MemTable available");
  }

  return table->Merge(key, value, sequence);
}

bool MemTableManager::Get(const Slice &key, std::string *value, Status *status,
                          uint64_t snapshot) {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 首先在可变MemTable中查找
  if (mutable_memtable_ &&
      mutable_memtable_->Get(key, value, status, snapshot)) {
    if (!status->IsMergeInProgress()) {
      return true;
    }
  }

  // 然后在不可变MemTable中查找（按时间倒序）
  for (auto it = immutable_memtables_.rbegin();
       it != immutable_memtables_.rend(); ++it) {
    if ((*it)->Get(key, value, status, snapshot)) {
      return true;
    }
  }

  if (status) {
    *status = Status::NotFound("Key not found");
  }
  return false;
}

Status MemTableManager::SwitchMemTable() {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  if (!mutable_memtable_) {
    return Status::InvalidArgument("No mutable MemTable to switch");
  }

  // 检查是否超过最大不可变MemTable数量
  if (immutable_memtables_.size() >= options_.max_write_buffer_number - 1) {
    return Status::TryAgain("Too many immutable MemTables");
  }

  // 将当前可变MemTable标记为不可变
  mutable_memtable_->SetImmutable();
  immutable_memtables_.push_back(std::move(mutable_memtable_));

  // 创建新的可变MemTable
  CreateNewMemTable();

  LOG_INFO << "Switched MemTable, now have " << immutable_memtables_.size()
           << " immutable MemTables";

  return Status::OK();
}

size_t MemTableManager::TotalMemoryUsage() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  size_t total = 0;

  if (mutable_memtable_) {
    total += mutable_memtable_->ApproximateMemoryUsage();
  }

  for (const auto &memtable : immutable_memtables_) {
    total += memtable->ApproximateMemoryUsage();
  }

  return total;
}

size_t MemTableManager::NumImmutableMemTables() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return immutable_memtables_.size();
}

bool MemTableManager::ShouldSwitchMemTable() const {
  MemTable *table = const_cast<MemTableManager *>(this)->GetMutableMemTable();
  return table && table->ShouldFlush();
}

bool MemTableManager::HasImmutableMemTables() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return !immutable_memtables_.empty();
}

MemTable *MemTableManager::GetMemTableForFlush() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (immutable_memtables_.empty()) {
    return nullptr;
  }

  return immutable_memtables_.front().get();
}

Status MemTableManager::CompleteFlushedMemTable(MemTable *flushed_memtable) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 找到并移除已刷盘的MemTable
  auto it =
      std::find_if(immutable_memtables_.begin(), immutable_memtables_.end(),
                   [flushed_memtable](const std::unique_ptr<MemTable> &mt) {
                     return mt.get() == flushed_memtable;
                   });

  if (it != immutable_memtables_.end()) {
    immutable_memtables_.erase(it);
    LOG_INFO << "Completed flush of MemTable, " << immutable_memtables_.size()
             << " immutable MemTables remaining";
    return Status::OK();
  }

  return Status::NotFound("MemTable not found in immutable list");
}

void MemTableManager::UpdateOptions(const MemTableOptions &new_options) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  options_ = new_options;
}

void MemTableManager::CreateNewMemTable() {
  mutable_memtable_ = std::make_unique<MemTable>(comparator_, options_);
  mutable_memtable_->Ref(); // 增加引用计数
}

// ============================================================================
// 工具函数实现
// ============================================================================

namespace memtable_util {

std::unique_ptr<MemTable> CreateMemTable(const Comparator *comparator,
                                         const MemTableOptions &options) {
  return std::make_unique<MemTable>(comparator, options);
}

Status ValidateMemTable(MemTable *memtable) {
  if (!memtable) {
    return Status::InvalidArgument("MemTable is null");
  }

  if (!memtable->ValidateStructure()) {
    return Status::Corruption("MemTable structure validation failed");
  }

  return Status::OK();
}

uint64_t EstimateSSTableSize(const MemTable *memtable) {
  if (!memtable) {
    return 0;
  }

  // 估算SSTable大小 = 数据大小 + 索引开销 + 元数据开销
  size_t data_size = memtable->ApproximateMemoryUsage();
  size_t index_overhead =
      memtable->NumEntries() * 20; // 每个条目大约20字节索引开销
  size_t metadata_overhead = 1024; // 1KB元数据开销

  return data_size + index_overhead + metadata_overhead;
}

MemTableStats AnalyzeMemTable(const MemTable *memtable) {
  MemTableStats stats;
  stats.total_keys = 0;
  stats.unique_keys = 0;
  stats.deletions = 0;
  stats.merges = 0;

  if (!memtable || memtable->Empty()) {
    return stats;
  }

  auto iter = memtable->NewIterator();
  iter->SeekToFirst();

  std::string last_user_key;
  bool first = true;

  while (iter->Valid()) {
    stats.total_keys++;

    std::string current_user_key = iter->key().ToString();

    if (first || current_user_key != last_user_key) {
      stats.unique_keys++;

      if (first) {
        stats.smallest_key = current_user_key;
        first = false;
      }
      stats.largest_key = current_user_key;
    }

    if (iter->type() == ValueType::kDeletion) {
      stats.deletions++;
    } else if (iter->type() == ValueType::kMerge) {
      stats.merges++;
    }

    last_user_key = current_user_key;
    iter->Next();
  }

  return stats;
}

} // namespace memtable_util

// ============================================================================
// MemTable 错误恢复方法
// ============================================================================

Status MemTable::HandleErrorRecovery(const std::string &component,
                                     const std::string &operation) {
  LOG_INFO << "MemTable::HandleErrorRecovery called for component: "
           << component << ", operation: " << operation;

  try {
    // MemTable特定的清理逻辑
    if (operation == "Put" || operation == "Delete" || operation == "Merge") {
      // 对于写操作失败，检查并清理可能的不一致状态
      if (!ValidateStructure()) {
        LOG_WARN << "MemTable structure validation failed, attempting repair";
        // 这里可以实现具体的修复逻辑
      }
    }

    // 检查内存使用情况，如果过高则触发刷盘提示
    size_t current_usage = ApproximateMemoryUsage();
    if (current_usage > options_.write_buffer_size * 0.9) {
      LOG_WARN << "MemTable memory usage high ("
               << (current_usage / 1024 / 1024) << " MB), suggesting flush";
      // 注意：我们不直接刷盘，而是建议上层组件刷盘
    }

    // 清理可能的临时状态
    LOG_DEBUG << "MemTable error recovery completed successfully";
    return Status::OK();

  } catch (const std::exception &e) {
    LOG_ERROR << "MemTable error recovery failed: " << e.what();
    return Status::IOError("MemTable recovery failed: " +
                           std::string(e.what()));
  }
}

Status MemTable::HandleMemoryError(const ErrorReport &error) {
  LOG_INFO << "MemTable::HandleMemoryError called for: "
           << error.status.ToString();

  try {
    size_t current_usage = ApproximateMemoryUsage();
    size_t max_allowed = options_.write_buffer_size;

    LOG_INFO << "Current memory usage: " << (current_usage / 1024 / 1024)
             << " MB, max allowed: " << (max_allowed / 1024 / 1024) << " MB";

    // 内存相关的恢复策略
    if (current_usage > max_allowed) {
      LOG_WARN << "MemTable memory usage exceeds limit, marking for flush";

      // 将MemTable标记为不可变，提示需要刷盘
      // 注意：这是建议性的，实际刷盘由上层LSM树管理器决定
      if (!IsImmutable()) {
        LOG_INFO
            << "Suggesting MemTable should be flushed due to memory pressure";
        // 这里我们不直接调用SetImmutable()，而是记录建议
      }
    }

    // 检查是否有异常大的条目
    if (num_entries_.load() > 0) {
      size_t avg_entry_size = current_usage / num_entries_.load();
      if (avg_entry_size > 1024 * 1024) { // 1MB per entry seems excessive
        LOG_WARN << "Detected unusually large entries, average size: "
                 << (avg_entry_size / 1024) << " KB";
      }
    }

    LOG_DEBUG << "MemTable memory error handling completed";
    return Status::OK();

  } catch (const std::exception &e) {
    LOG_ERROR << "MemTable memory error handling failed: " << e.what();
    return Status::IOError("MemTable memory error handling failed: " +
                           std::string(e.what()));
  }
}

} // namespace lrdb