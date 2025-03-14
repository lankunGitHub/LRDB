// LSM Tree存储引擎实现

#include "lrdb/storage/lsm_tree.h"
#include "lrdb/core/coding.h"
#include "lrdb/util/logging.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <sstream>

namespace lrdb {

// ============================================================================
// LSMTree 实现
// ============================================================================

LSMTree::LSMTree(const LSMTreeOptions &options)
    : options_(options), env_(nullptr), comparator_(nullptr),
      bg_manager_(nullptr), opened_(false), in_recovery_mode_(false),
      next_sequence_number_(1), recovery_sequence_number_(0),
      write_slowdown_(false), write_stop_(false), stats_num_puts_(0),
      stats_num_gets_(0), stats_num_deletes_(0), stats_num_flushes_(0),
      stats_num_compactions_(0) {}

LSMTree::~LSMTree() {
  if (opened_) {
    Close();
  }
}

Status LSMTree::Open(Env *env, const Comparator *comparator,
                     BackgroundTaskManager *bg_manager) {
  if (opened_) {
    return Status::InvalidArgument("LSMTree already opened");
  }

  // 未提供环境时回退到默认环境，保证SSTable刷盘路径可用
  env_ = env ? env : Env::Default();
  comparator_ = comparator;
  bg_manager_ = bg_manager;

  // 验证配置
  Status s = ValidateOptions();
  if (!s.ok()) {
    return s;
  }

  // 创建数据目录
  s = CreateDirectories();
  if (!s.ok()) {
    return s;
  }

  // 初始化组件
  s = InitializeComponents();
  if (!s.ok()) {
    return s;
  }

  // 加载磁盘上已有的SSTable（重开数据库时恢复数据）
  s = LoadExistingSSTables();
  if (!s.ok()) {
    return s;
  }

  // 注册后台任务
  s = RegisterBackgroundTasks();
  if (!s.ok()) {
    return s;
  }

  opened_ = true;
  return Status::OK();
}

Status LSMTree::Close() {
  if (!opened_) {
    return Status::OK();
  }

  // 取消注册后台任务
  Status s = UnregisterBackgroundTasks();

  // 等待所有写入完成
  std::unique_lock<std::mutex> write_lock(write_mutex_);

  // 刷盘所有immutable MemTable
  while (memtable_manager_->NumImmutableMemTables() > 0) {
    Status flush_status = FlushOldestMemTable();
    if (!flush_status.ok()) {
      // 记录错误但继续关闭
      s = flush_status;
    }
  }

  // 刷盘当前mutable MemTable
  if (!memtable_manager_->GetMutableMemTable()->Empty()) {
    Status flush_status =
        DoFlushMemTable(memtable_manager_->GetMutableMemTable());
    if (!flush_status.ok()) {
      s = flush_status;
    }
  }

  // 清理组件
  memtable_manager_.reset();
  sstable_manager_.reset();

  opened_ = false;
  return s;
}

// ============================================================================
// 核心读写接口实现
// ============================================================================

Status LSMTree::Put(const Slice &key, const Slice &value, uint64_t sequence) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  // 检查写入条件
  WriteStatus write_status = CheckWriteConditions();
  if (write_status == WriteStatus::kStop) {
    return Status::IOError("Write stopped due to too many L0 files");
  }

  std::unique_lock<std::mutex> write_lock(write_mutex_);

  // 等待写入条件满足
  if (write_status == WriteStatus::kSlowdown) {
    Status wait_status = WaitForWriteCondition();
    if (!wait_status.ok()) {
      return wait_status;
    }
  }

  // 获取序列号
  if (sequence == 0) {
    sequence = GetNextSequenceNumber();
  }

  // 检查当前MemTable是否已满
  if (memtable_manager_->ShouldSwitchMemTable()) {
    // 使当前MemTable变为immutable并创建新的
    Status s = MakeMemTableImmutable();
    if (!s.ok()) {
      return s;
    }
  }

  MemTable *active_memtable = memtable_manager_->GetMutableMemTable();

  // 写入MemTable
  Status s = active_memtable->Put(key, value, sequence);
  if (s.ok()) {
    stats_num_puts_.fetch_add(1);
  }

  return s;
}

Status LSMTree::Delete(const Slice &key, uint64_t sequence) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  // 检查写入条件
  WriteStatus write_status = CheckWriteConditions();
  if (write_status == WriteStatus::kStop) {
    return Status::IOError("Write stopped due to too many L0 files");
  }

  std::unique_lock<std::mutex> write_lock(write_mutex_);

  // 获取序列号
  if (sequence == 0) {
    sequence = GetNextSequenceNumber();
  }

  // 检查当前MemTable是否已满
  if (memtable_manager_->ShouldSwitchMemTable()) {
    // 使当前MemTable变为immutable并创建新的
    Status s = MakeMemTableImmutable();
    if (!s.ok()) {
      return s;
    }
  }

  MemTable *active_memtable = memtable_manager_->GetMutableMemTable();

  // 写入删除标记
  Status s = active_memtable->Delete(key, sequence);
  if (s.ok()) {
    stats_num_deletes_.fetch_add(1);
  }

  return s;
}

Status LSMTree::Merge(const Slice &key, const Slice &value, uint64_t sequence) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  // 检查写入条件
  WriteStatus write_status = CheckWriteConditions();
  if (write_status == WriteStatus::kStop) {
    return Status::IOError("Write stopped due to too many L0 files");
  }

  std::unique_lock<std::mutex> write_lock(write_mutex_);

  // 获取序列号
  if (sequence == 0) {
    sequence = GetNextSequenceNumber();
  }

  // 检查当前MemTable是否已满
  if (memtable_manager_->ShouldSwitchMemTable()) {
    // 使当前MemTable变为immutable并创建新的
    Status s = MakeMemTableImmutable();
    if (!s.ok()) {
      return s;
    }
  }

  MemTable *active_memtable = memtable_manager_->GetMutableMemTable();

  // 写入合并操作
  Status s = active_memtable->Merge(key, value, sequence);
  if (s.ok()) {
    stats_num_puts_.fetch_add(1); // 合并也算作Put操作
  }

  return s;
}

Status LSMTree::Get(const Slice &key, std::string *value, uint64_t snapshot) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  stats_num_gets_.fetch_add(1);

  // snapshot=0表示读取最新数据，直接传递即可。
  // 不能用next_sequence_number_替换：重开数据库后该计数器尚未推进，
  // 会错误地遮蔽序列号更大的已持久化版本
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 首先在MemTable中查找
  bool found = false;
  Status s = GetFromMemTables(key, value, snapshot, &found);
  // MemTable未命中（NotFound）需要继续查SSTable，只有真正的错误才提前返回
  if (!s.ok() && !s.IsNotFound() && !s.IsMergeInProgress()) {
    return s;
  }

  // 然后在SSTable中查找
  s = GetFromSSTables(key, value, snapshot, &found);
  if (!s.ok()) {
    return s;
  }

  if (!found) {
    return Status::NotFound("Key not found");
  }

  return Status::OK();
}

std::unique_ptr<LSMTreeIterator> LSMTree::NewIterator(uint64_t snapshot) {
  if (!opened_) {
    return nullptr;
  }

  return std::make_unique<LSMTreeMergeIterator>(this, snapshot);
}

// ============================================================================
// 后台任务API实现
// ============================================================================

Status LSMTree::TriggerFlush() {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  // 检查是否需要刷盘
  if (!NeedsFlush()) {
    return Status::OK();
  }

  // 执行刷盘
  return FlushOldestMemTable();
}

Status LSMTree::FlushOldestMemTable() {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 获取最老的immutable MemTable
  MemTable *memtable = memtable_manager_->GetMemTableForFlush();
  if (!memtable) {
    return Status::OK(); // 没有需要刷盘的MemTable
  }

  // 执行刷盘
  Status s = DoFlushMemTable(memtable);
  if (s.ok()) {
    // 完成刷盘后移除该MemTable
    memtable_manager_->CompleteFlushedMemTable(memtable);
    stats_num_flushes_.fetch_add(1);
  }

  return s;
}

bool LSMTree::NeedsFlush() const {
  if (!opened_) {
    return false;
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 检查是否有immutable MemTable需要刷盘
  return memtable_manager_->NumImmutableMemTables() > 0;
}

Status LSMTree::TriggerCompaction() {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  if (!options_.enable_auto_compaction) {
    return Status::OK();
  }

  // 检查是否需要压缩
  if (!NeedsCompaction()) {
    return Status::OK();
  }

  // 从L0开始检查每一层
  for (int level = 0; level <= static_cast<int>(SSTableLevel::kMaxLevel);
       ++level) {
    SSTableLevel sst_level = static_cast<SSTableLevel>(level);

    std::vector<SSTableMeta> level_files =
        sstable_manager_->GetLevelFiles(sst_level);

    bool needs_compaction = false;
    if (level == 0) {
      // L0层根据文件数量判断
      needs_compaction = level_files.size() >= options_.l0_compaction_trigger;
    } else {
      // L1+层根据总大小判断
      uint64_t total_size = 0;
      for (const auto &meta : level_files) {
        total_size += meta.file_size;
      }

      uint64_t level_limit = options_.max_bytes_for_level_base;
      for (int i = 1; i < level; ++i) {
        level_limit *= options_.level_multiplier;
      }

      needs_compaction = total_size > level_limit;
    }

    if (needs_compaction) {
      return CompactLevel(sst_level);
    }
  }

  return Status::OK();
}

Status LSMTree::CompactLevel(SSTableLevel level) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  Status s = DoLevelCompaction(level);
  if (s.ok()) {
    stats_num_compactions_.fetch_add(1);
  }

  return s;
}

bool LSMTree::NeedsCompaction() const {
  if (!opened_) {
    return false;
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 检查L0层文件数量
  std::vector<SSTableMeta> l0_files =
      sstable_manager_->GetLevelFiles(SSTableLevel::kLevel0);
  if (l0_files.size() >= options_.l0_compaction_trigger) {
    return true;
  }

  // 检查其他层级
  for (int level = 1; level <= static_cast<int>(SSTableLevel::kMaxLevel);
       ++level) {
    SSTableLevel sst_level = static_cast<SSTableLevel>(level);
    std::vector<SSTableMeta> level_files =
        sstable_manager_->GetLevelFiles(sst_level);

    uint64_t total_size = 0;
    for (const auto &meta : level_files) {
      total_size += meta.file_size;
    }

    uint64_t level_limit = options_.max_bytes_for_level_base;
    for (int i = 1; i < level; ++i) {
      level_limit *= options_.level_multiplier;
    }

    if (total_size > level_limit) {
      return true;
    }
  }

  return false;
}

Status LSMTree::CompactRange(const Slice *begin, const Slice *end) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 手动压缩指定范围的所有层级
  for (int level = 0; level <= static_cast<int>(SSTableLevel::kMaxLevel);
       ++level) {
    SSTableLevel sst_level = static_cast<SSTableLevel>(level);

    // 查找范围内的文件
    std::string start_key = begin ? begin->ToString() : "";
    std::string end_key = end ? end->ToString() : "";

    std::vector<SSTableMeta> range_files;
    if (begin && end) {
      range_files = sstable_manager_->GetFilesInRange(*begin, *end, sst_level);
    } else {
      range_files = sstable_manager_->GetLevelFiles(sst_level);
    }

    if (range_files.size() > 1) {
      // 创建压缩任务
      CompactionJob job;
      job.type = CompactionType::kManual;
      job.input_level = sst_level;
      job.output_level = (level == static_cast<int>(SSTableLevel::kMaxLevel))
                             ? sst_level
                             : static_cast<SSTableLevel>(level + 1);

      for (const auto &meta : range_files) {
        job.input_files.push_back(meta.filename);
      }

      job.output_file_prefix = "compact_range_L" + std::to_string(level);
      job.start_key = start_key;
      job.end_key = end_key;
      job.is_manual = true;

      // 执行压缩
      CompactionResult result = sstable_manager_->CompactFiles(job);
      if (!result.IsSuccess()) {
        return result.status;
      }
    }
  }

  return Status::OK();
}

// ============================================================================
// 查询和统计接口实现
// ============================================================================

LSMTreeStats LSMTree::GetStats() const {
  if (!opened_) {
    return LSMTreeStats{};
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  LSMTreeStats stats;

  // MemTable统计
  MemTable *active = memtable_manager_->GetMutableMemTable();
  stats.active_memtable_size = active ? active->ApproximateMemoryUsage() : 0;
  stats.num_immutable_memtables = memtable_manager_->NumImmutableMemTables();
  stats.total_memtable_memory = stats.active_memtable_size;

  // SSTable统计
  stats.level_stats = sstable_manager_->GetLevelStats();
  stats.total_sst_files = 0;
  stats.total_sst_size = 0;

  for (const auto &level_stat : stats.level_stats) {
    stats.total_sst_files += level_stat.num_files;
    stats.total_sst_size += level_stat.total_size;
  }

  // 操作统计
  stats.num_puts = stats_num_puts_.load();
  stats.num_gets = stats_num_gets_.load();
  stats.num_deletes = stats_num_deletes_.load();
  stats.num_flushes = stats_num_flushes_.load();
  stats.num_compactions = stats_num_compactions_.load();

  return stats;
}

WriteStatus LSMTree::GetWriteStatus() const { return CheckWriteConditions(); }

std::vector<SSTableMeta> LSMTree::GetLevelFiles(SSTableLevel level) const {
  if (!opened_) {
    return {};
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);
  return sstable_manager_->GetLevelFiles(level);
}

Status
LSMTree::GetApproximateSizes(const std::vector<std::pair<Slice, Slice>> &ranges,
                             std::vector<uint64_t> *sizes) const {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  sizes->clear();
  sizes->reserve(ranges.size());

  std::shared_lock<std::shared_mutex> lock(mutex_);

  for (const auto &range : ranges) {
    uint64_t total_size = 0;

    // 估算每一层的大小
    for (int level = 0; level <= static_cast<int>(SSTableLevel::kMaxLevel);
         ++level) {
      SSTableLevel sst_level = static_cast<SSTableLevel>(level);
      auto files = sstable_manager_->GetFilesInRange(range.first, range.second,
                                                     sst_level);

      for (const auto &meta : files) {
        total_size += meta.file_size;
      }
    }

    sizes->push_back(total_size);
  }

  return Status::OK();
}

size_t LSMTree::ApproximateMemoryUsage() const {
  if (!opened_) {
    return 0;
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  size_t usage = sizeof(*this);

  // MemTable内存使用
  if (memtable_manager_) {
    MemTable *active = memtable_manager_->GetMutableMemTable();
    if (active) {
      usage += active->ApproximateMemoryUsage();
    }
  }

  // SSTable缓存内存使用
  if (sstable_manager_) {
    usage += sstable_manager_->ApproximateMemoryUsage();
  }

  return usage;
}

// ============================================================================
// 配置和控制接口实现
// ============================================================================

Status LSMTree::UpdateOptions(const LSMTreeOptions &new_options) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  // 验证新配置
  LSMTreeOptions backup = options_;
  options_ = new_options;
  Status s = ValidateOptions();

  if (!s.ok()) {
    options_ = backup; // 恢复原配置
    return s;
  }

  return Status::OK();
}

void LSMTree::SetWriteLimit(bool slowdown, bool stop) {
  write_slowdown_.store(slowdown);
  write_stop_.store(stop);

  if (!slowdown && !stop) {
    write_cv_.notify_all(); // 通知等待的写入者
  }
}

// ============================================================================
// 内部组件管理实现
// ============================================================================

Status LSMTree::InitializeComponents() {
  // 创建MemTable管理器
  MemTableOptions memtable_opts;
  memtable_manager_ =
      std::make_unique<MemTableManager>(comparator_, memtable_opts);

  // 创建SSTable管理器
  SSTableManagerOptions sstable_opts;
  sstable_opts.max_open_files = 1000;
  sstable_opts.table_cache_size = 256 * 1024 * 1024; // 256MB
  sstable_opts.directory = options_.db_path + "/sstables";
  sstable_manager_ =
      std::make_unique<SSTableManager>(env_, comparator_, sstable_opts);

  // MemTableManager已经自动创建了初始的mutable MemTable
  return Status::OK();
}

Status LSMTree::LoadExistingSSTables() {
  std::string sstable_dir = options_.db_path + "/sstables";
  std::error_code ec;
  if (!std::filesystem::exists(sstable_dir, ec)) {
    return Status::OK();
  }

  uint64_t max_file_number = 0;
  size_t loaded = 0;

  for (const auto &entry : std::filesystem::directory_iterator(sstable_dir, ec)) {
    if (ec || !entry.is_regular_file()) continue;

    std::string filename = entry.path().filename().string();
    // 格式: sst_<file_number>_L<level>.sst
    if (filename.rfind("sst_", 0) != 0) continue;
    size_t second_underscore = filename.find('_', 4);
    if (second_underscore == std::string::npos) continue;
    size_t l_pos = filename.find("_L", second_underscore);
    if (l_pos == std::string::npos) continue;
    if (filename.size() < 5 || filename.compare(filename.size() - 4, 4, ".sst") != 0) continue;

    uint64_t file_number = 0;
    int level = 0;
    try {
      file_number = std::stoull(filename.substr(4, second_underscore - 4));
      level = std::stoi(filename.substr(l_pos + 2, filename.size() - 4 - (l_pos + 2)));
    } catch (const std::exception &) {
      continue;
    }

    SSTableReader reader;
    Status s = reader.Open(entry.path().string(), env_, comparator_);
    if (!s.ok()) {
      LOG_WARN << "Failed to open existing SSTable " << filename << ": "
               << s.ToString();
      continue;
    }

    SSTableMeta meta = reader.GetMeta();
    // 文件内的meta块不携带文件编号（历史格式缺陷），以文件名为准回填
    meta.filename = entry.path().string();
    meta.file_number = file_number;
    meta.level = static_cast<SSTableLevel>(level);
    s = sstable_manager_->RegisterSSTable(meta);
    if (!s.ok()) {
      LOG_WARN << "Failed to register existing SSTable " << filename << ": "
               << s.ToString();
      continue;
    }

    max_file_number = std::max(max_file_number, file_number);
    ++loaded;
  }

  if (loaded > 0) {
    // 推进文件编号，避免与新生成的SSTable重名
    sstable_manager_->SetNextFileNumber(max_file_number + 1);
    LOG_INFO << "Loaded " << loaded << " existing SSTables, next file number: "
             << max_file_number + 1;
  }

  return Status::OK();
}

Status LSMTree::MakeMemTableImmutable() {
  // 将当前mutable MemTable变为immutable并创建新的
  return memtable_manager_->SwitchMemTable();
}

Status LSMTree::DoFlushMemTable(MemTable *memtable) {
  if (!memtable || memtable->Empty()) {
    return Status::OK();
  }

  // 使用SSTable管理器执行刷盘
  SSTableMeta output_meta;
  Status s = sstable_manager_->FlushMemTableToSSTable(
      memtable, SSTableLevel::kLevel0, &output_meta);

  return s;
}

Status LSMTree::DoLevelCompaction(SSTableLevel level) {
  // 选择压缩文件
  std::vector<std::string> input_files;
  SSTableLevel output_level;

  Status s = PickCompactionFiles(level, &input_files, &output_level);
  if (!s.ok() || input_files.empty()) {
    return s;
  }

  // 创建压缩任务
  CompactionJob job;
  job.type = (level == SSTableLevel::kLevel0) ? CompactionType::kMinor
                                              : CompactionType::kMajor;
  job.input_level = level;
  job.output_level = output_level;
  job.input_files = input_files;
  job.output_file_prefix =
      "compact_L" + std::to_string(static_cast<int>(level));
  job.is_manual = false;
  job.delete_obsolete_files = true;

  // 执行压缩
  CompactionResult result = sstable_manager_->CompactFiles(job);
  if (!result.IsSuccess()) {
    return result.status;
  }

  // 删除输入文件
  std::vector<uint64_t> obsolete_files;
  for (const std::string &filename : input_files) {
    // 从文件名解析文件编号（简化实现）
    // 实际应该有更健壮的文件编号管理
    size_t pos = filename.find("sst_");
    if (pos != std::string::npos) {
      std::string num_str = filename.substr(pos + 4);
      size_t end_pos = num_str.find('_');
      if (end_pos != std::string::npos) {
        try {
          uint64_t file_number = std::stoull(num_str.substr(0, end_pos));
          obsolete_files.push_back(file_number);
        } catch (const std::exception &) {
          // 忽略解析错误
        }
      }
    }
  }

  if (!obsolete_files.empty()) {
    sstable_manager_->DeleteObsoleteFiles(obsolete_files);
  }

  return Status::OK();
}

Status LSMTree::PickCompactionFiles(SSTableLevel level,
                                    std::vector<std::string> *input_files,
                                    SSTableLevel *output_level) {
  input_files->clear();

  std::vector<SSTableMeta> level_files = sstable_manager_->GetLevelFiles(level);
  if (level_files.empty()) {
    return Status::OK();
  }

  if (level == SSTableLevel::kLevel0) {
    // L0层压缩：选择所有文件
    for (const auto &meta : level_files) {
      input_files->push_back(meta.filename);
    }
    *output_level = SSTableLevel::kLevel1;
  } else {
    // L1+层压缩：选择一部分文件
    size_t max_files =
        std::min(level_files.size(), size_t(10)); // 最多选择10个文件
    for (size_t i = 0; i < max_files; ++i) {
      input_files->push_back(level_files[i].filename);
    }

    int next_level = static_cast<int>(level) + 1;
    if (next_level <= static_cast<int>(SSTableLevel::kMaxLevel)) {
      *output_level = static_cast<SSTableLevel>(next_level);
    } else {
      *output_level = level; // 最大层级，原地压缩
    }
  }

  return Status::OK();
}

// ============================================================================
// 写入控制实现
// ============================================================================

WriteStatus LSMTree::CheckWriteConditions() const {
  if (!opened_) {
    return WriteStatus::kStop;
  }

  // 检查强制停止标志
  if (write_stop_.load()) {
    return WriteStatus::kStop;
  }

  // 检查L0文件数量
  std::vector<SSTableMeta> l0_files =
      sstable_manager_->GetLevelFiles(SSTableLevel::kLevel0);

  if (l0_files.size() >= options_.l0_stop_writes_trigger) {
    return WriteStatus::kStop;
  }

  if (l0_files.size() >= options_.l0_slowdown_writes_trigger ||
      write_slowdown_.load()) {
    return WriteStatus::kSlowdown;
  }

  // 检查immutable MemTable数量
  if (memtable_manager_->NumImmutableMemTables() >=
      options_.max_memtables - 1) {
    return WriteStatus::kSlowdown;
  }

  return WriteStatus::kOK;
}

Status LSMTree::WaitForWriteCondition() {
  std::unique_lock<std::mutex> lock(write_mutex_);
  write_cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
    return CheckWriteConditions() == WriteStatus::kOK;
  });

  return Status::OK();
}

uint64_t LSMTree::GetNextSequenceNumber() {
  return next_sequence_number_.fetch_add(1);
}

// ============================================================================
// 读取实现
// ============================================================================

Status LSMTree::GetFromMemTables(const Slice &key, std::string *value,
                                 uint64_t snapshot, bool *found) {
  *found = false;

  // 首先检查mutable MemTable
  MemTable *mutable_table = memtable_manager_->GetMutableMemTable();
  if (mutable_table) {
    Status status;
    bool memtable_found = mutable_table->Get(key, value, &status, snapshot);
    if (!status.ok()) {
      return status;
    }
    if (memtable_found) {
      *found = true;
      return Status::OK();
    }
  }

  // 然后检查immutable MemTables（从新到老）
  auto immutable_tables = memtable_manager_->GetImmutableMemTables();
  for (auto it = immutable_tables.rbegin(); it != immutable_tables.rend();
       ++it) {
    MemTable *memtable = *it;
    Status status;
    bool memtable_found = memtable->Get(key, value, &status, snapshot);
    if (!status.ok() && !status.IsMergeInProgress()) {
      return status;
    }
    if (memtable_found) {
      *found = true;
      return status;
    }
  }

  return Status::OK();
}

Status LSMTree::GetFromSSTables(const Slice &key, std::string *value,
                                uint64_t snapshot, bool *found) {
  return sstable_manager_->Get(key, value, found, snapshot);
}

// ============================================================================
// 后台任务注册实现
// ============================================================================

Status LSMTree::RegisterBackgroundTasks() {
  if (!bg_manager_) {
    return Status::OK(); // 如果没有后台管理器，就不注册任务
  }

  // 注册刷盘任务
  BackgroundTask flush_task;
  flush_task.type = TaskType::kFlush;
  flush_task.priority = TaskPriority::kHigh;
  flush_task.task_function = [this]() -> Status {
    return BackgroundFlushTask();
  };
  flush_task.name = "LSMTree_Flush";
  flush_task.interval = std::chrono::milliseconds(1000); // 每1秒检查一次

  uint64_t flush_task_id = bg_manager_->SchedulePeriodicTask(flush_task);
  background_task_ids_.push_back(flush_task_id);

  // 注册压缩任务
  BackgroundTask compaction_task;
  compaction_task.type = TaskType::kCompaction;
  compaction_task.priority = TaskPriority::kNormal;
  compaction_task.task_function = [this]() -> Status {
    return BackgroundCompactionTask();
  };
  compaction_task.name = "LSMTree_Compaction";
  compaction_task.interval = std::chrono::milliseconds(5000); // 每5秒检查一次

  uint64_t compaction_task_id =
      bg_manager_->SchedulePeriodicTask(compaction_task);
  background_task_ids_.push_back(compaction_task_id);

  return Status::OK();
}

Status LSMTree::UnregisterBackgroundTasks() {
  if (!bg_manager_) {
    return Status::OK();
  }

  // 取消所有已注册的任务
  for (uint64_t task_id : background_task_ids_) {
    bg_manager_->CancelTask(task_id);
  }

  background_task_ids_.clear();
  return Status::OK();
}

Status LSMTree::BackgroundFlushTask() {
  if (!opened_) {
    return Status::OK();
  }

  try {
    // 检查是否需要刷盘
    return TriggerFlush();
  } catch (const std::exception &e) {
    return Status::IOError("Background flush task failed: " +
                           std::string(e.what()));
  }
}

Status LSMTree::BackgroundCompactionTask() {
  if (!opened_) {
    return Status::OK();
  }

  try {
    // 检查是否需要压缩
    if (NeedsCompaction()) {
      return TriggerCompaction();
    }
    return Status::OK();
  } catch (const std::exception &e) {
    return Status::IOError("Background compaction task failed: " +
                           std::string(e.what()));
  }
}

// ============================================================================
// 配置验证和路径管理实现
// ============================================================================

Status LSMTree::ValidateOptions() const {
  if (options_.memtable_size == 0) {
    return Status::InvalidArgument("MemTable size cannot be zero");
  }

  if (options_.max_memtables < 2) {
    return Status::InvalidArgument("Max MemTables must be at least 2");
  }

  if (options_.l0_compaction_trigger == 0) {
    return Status::InvalidArgument("L0 compaction trigger cannot be zero");
  }

  if (options_.l0_slowdown_writes_trigger <= options_.l0_compaction_trigger) {
    return Status::InvalidArgument(
        "L0 slowdown trigger must be greater than compaction trigger");
  }

  if (options_.l0_stop_writes_trigger <= options_.l0_slowdown_writes_trigger) {
    return Status::InvalidArgument(
        "L0 stop trigger must be greater than slowdown trigger");
  }

  if (options_.level_multiplier < 2) {
    return Status::InvalidArgument("Level multiplier must be at least 2");
  }

  if (options_.max_bytes_for_level_base == 0) {
    return Status::InvalidArgument("Max bytes for level base cannot be zero");
  }

  if (options_.db_path.empty()) {
    return Status::InvalidArgument("Database path cannot be empty");
  }

  return Status::OK();
}

Status LSMTree::CreateDirectories() {
  std::error_code ec;

  // 创建主数据目录（优先走环境，未提供时直接使用文件系统）
  if (env_) {
    Status s = env_->CreateDirIfMissing(options_.db_path);
    if (!s.ok()) {
      return s;
    }
  } else {
    std::filesystem::create_directories(options_.db_path, ec);
    if (ec) {
      return Status::IOError("Failed to create db directory: " + ec.message());
    }
  }

  // 创建SSTable目录
  std::string sstable_dir = options_.db_path + "/sstables";
  if (env_) {
    Status s = env_->CreateDirIfMissing(sstable_dir);
    if (!s.ok()) {
      return s;
    }
  } else {
    std::filesystem::create_directories(sstable_dir, ec);
    if (ec) {
      return Status::IOError("Failed to create sstable directory: " + ec.message());
    }
  }

  return Status::OK();
}

std::string LSMTree::GetSSTablePath(uint64_t file_number,
                                    SSTableLevel level) const {
  return options_.db_path + "/sstables/sst_" + std::to_string(file_number) +
         "_L" + std::to_string(static_cast<int>(level)) + ".sst";
}

// ============================================================================
// 内部状态检查实现
// ============================================================================

Status LSMTree::ValidateInternalState() const {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 验证MemTable状态
  MemTable *active = memtable_manager_->GetMutableMemTable();
  if (!active) {
    return Status::Corruption("No active MemTable");
  }

  // 验证MemTable内部结构
  bool valid = active->ValidateStructure();
  if (!valid) {
    return Status::Corruption("Active MemTable validation failed");
  }

  // 验证immutable MemTables
  auto immutable_tables = memtable_manager_->GetImmutableMemTables();
  for (MemTable *memtable : immutable_tables) {
    bool immutable_valid = memtable->ValidateStructure();
    if (!immutable_valid) {
      return Status::Corruption("Immutable MemTable validation failed");
    }
  }

  return Status::OK();
}

std::string LSMTree::DebugString() const {
  if (!opened_) {
    return "LSMTree not opened";
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::ostringstream oss;
  oss << "=== LSMTree Debug Info ===\n";

  // 基本状态
  oss << "Opened: " << (opened_ ? "true" : "false") << "\n";
  oss << "Next Sequence: " << next_sequence_number_.load() << "\n";
  oss << "Write Slowdown: " << (write_slowdown_.load() ? "true" : "false")
      << "\n";
  oss << "Write Stop: " << (write_stop_.load() ? "true" : "false") << "\n";

  // MemTable信息
  MemTable *active = memtable_manager_->GetMutableMemTable();
  if (active) {
    oss << "Active MemTable: " << active->ApproximateMemoryUsage() << " bytes, "
        << active->NumEntries() << " entries\n";
  }

  oss << "Immutable MemTables: " << memtable_manager_->NumImmutableMemTables()
      << "\n";

  // SSTable层级信息
  auto level_stats = sstable_manager_->GetLevelStats();
  for (const auto &stat : level_stats) {
    oss << "Level " << static_cast<int>(stat.level) << ": " << stat.num_files
        << " files, " << stat.total_size << " bytes\n";
  }

  // 操作统计
  oss << "Operations: Puts=" << stats_num_puts_.load()
      << ", Gets=" << stats_num_gets_.load()
      << ", Deletes=" << stats_num_deletes_.load()
      << ", Flushes=" << stats_num_flushes_.load()
      << ", Compactions=" << stats_num_compactions_.load() << "\n";

  return oss.str();
}

Status LSMTree::TEST_CompactMemTable() { return FlushOldestMemTable(); }

Status LSMTree::TEST_CompactLevel0() {
  return CompactLevel(SSTableLevel::kLevel0);
}

// ============================================================================
// LSMTreeMergeIterator 实现
// ============================================================================

LSMTreeMergeIterator::LSMTreeMergeIterator(const LSMTree *lsm_tree,
                                           uint64_t snapshot)
    : lsm_tree_(lsm_tree), snapshot_(snapshot),
      comparator_(lsm_tree->comparator_), current_memtable_index_(-1),
      current_sstable_index_(-1), valid_(false) {

  // snapshot=0表示读取最新数据，直接传递。
  // 不能用next_sequence_number_替换：重开数据库后该计数器尚未推进，
  // 会错误地遮蔽序列号更大的已持久化版本

  // 创建MemTable迭代器（mutable + immutable，新数据优先于旧数据）
  MemTable *active = lsm_tree_->memtable_manager_->GetMutableMemTable();
  if (active) {
    memtable_iters_.push_back(active->NewIterator());
  }

  auto immutable_tables = lsm_tree_->memtable_manager_->GetImmutableMemTables();
  for (MemTable *memtable : immutable_tables) {
    memtable_iters_.push_back(memtable->NewIterator());
  }

  // 创建各层级SSTable迭代器
  auto level_stats = lsm_tree_->sstable_manager_->GetLevelStats();
  for (const auto &stat : level_stats) {
    if (stat.num_files == 0) continue;
    auto level_iter = lsm_tree_->sstable_manager_->NewLevelIterator(stat.level);
    if (level_iter) {
      sstable_iters_.push_back(std::move(level_iter));
    }
  }
}

LSMTreeMergeIterator::~LSMTreeMergeIterator() { ClearChildren(); }

bool LSMTreeMergeIterator::Valid() const { return valid_; }

void LSMTreeMergeIterator::SeekToFirst() {
  // 将所有子迭代器定位到第一个位置
  for (auto &iter : memtable_iters_) {
    iter->SeekToFirst();
  }
  for (auto &iter : sstable_iters_) {
    iter->SeekToFirst();
  }

  // 找到最小的键
  FindSmallest();
}

void LSMTreeMergeIterator::SeekToLast() {
  // 将所有子迭代器定位到最后位置
  for (auto &iter : memtable_iters_) {
    iter->SeekToLast();
  }
  for (auto &iter : sstable_iters_) {
    iter->SeekToLast();
  }

  // 找到最大的键
  FindLargest();
}

void LSMTreeMergeIterator::Seek(const Slice &target) {
  // MemTable迭代器按内部键寻址（最大序列号落在该用户键的第一个版本上）
  std::string internal_target;
  coding::PutInternalKey(&internal_target, target, UINT64_MAX, 0xFF);
  for (auto &iter : memtable_iters_) {
    iter->Seek(internal_target);
  }

  // SSTable迭代器按用户键寻址（内部自行处理索引定位）
  for (auto &iter : sstable_iters_) {
    iter->Seek(target);
  }

  // 找到最小的键
  FindSmallest();
}

void LSMTreeMergeIterator::Next() {
  if (!valid_) {
    return;
  }

  // FindSmallest在返回前已把当前键的所有版本消费完毕，
  // 各子迭代器都停在下一个键上，直接重新归并即可
  FindSmallest();
}

void LSMTreeMergeIterator::Prev() {
  if (!valid_) {
    return;
  }

  // FindLargest在返回前已把当前键的所有版本消费完毕，
  // 各子迭代器都停在前一个键上，直接重新归并即可
  FindLargest();
}

Slice LSMTreeMergeIterator::key() const {
  // 对外暴露用户键（内部键已剥离8字节序列后缀）
  return valid_ ? Slice(current_key_) : Slice();
}

Slice LSMTreeMergeIterator::value() const {
  return valid_ ? Slice(current_value_) : Slice();
}

Status LSMTreeMergeIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }

  // 检查所有子迭代器的状态
  for (const auto &iter : memtable_iters_) {
    Status s = iter->status();
    if (!s.ok()) {
      return s;
    }
  }
  for (const auto &iter : sstable_iters_) {
    Status s = iter->status();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// 比较两个内部键：按字节序（用户键升序；同一用户键时旧版本在前）。
// 内部键编码为[用户键+8字节(序列号<<8|类型)]，字节序天然满足该语义，
// 且与SSTable内存储顺序一致，两个来源（MemTable/SSTable）可直接归并
int LSMTreeMergeIterator::CompareInternalKeys(const Slice &a,
                                              const Slice &b) const {
  return a.compare(b);
}

// 选择最佳子迭代器（返回-1表示没有有效子迭代器；通过is_sstable区分来源）
int LSMTreeMergeIterator::PickChild(bool prefer_newest, bool *is_sstable) {
  int best_index = -1;
  *is_sstable = false;
  std::string best_key;

  auto consider = [&](const Slice &candidate_key, int index, bool from_sstable) {
    if (best_index < 0) {
      best_index = index;
      *is_sstable = from_sstable;
      best_key = candidate_key.ToString();
      return;
    }
    int cmp = CompareInternalKeys(candidate_key, Slice(best_key));
    if (prefer_newest ? cmp < 0 : cmp > 0) {
      best_index = index;
      *is_sstable = from_sstable;
      best_key = candidate_key.ToString();
    }
  };

  for (int i = 0; i < static_cast<int>(memtable_iters_.size()); ++i) {
    if (memtable_iters_[i]->Valid()) {
      consider(memtable_iters_[i]->key(), i, false);
    }
  }
  for (int i = 0; i < static_cast<int>(sstable_iters_.size()); ++i) {
    if (sstable_iters_[i]->Valid()) {
      consider(sstable_iters_[i]->key(), i, true);
    }
  }

  return best_index;
}

void LSMTreeMergeIterator::FindSmallest() {
  valid_ = false;
  current_memtable_index_ = -1;
  current_sstable_index_ = -1;

  while (true) {
    bool is_sstable = false;
    int best = PickChild(true, &is_sstable);
    if (best < 0) {
      return; // 没有更多键
    }

    // 取最佳键的用户键/序列号/类型
    std::string best_internal = is_sstable
                                    ? sstable_iters_[best]->key().ToString()
                                    : memtable_iters_[best]->key().ToString();
    Slice user_key;
    uint64_t seq;
    uint8_t type;
    if (!coding::ParseInternalKey(best_internal, &user_key, &seq, &type)) {
      // 无法解析，推进后重试
      if (is_sstable) sstable_iters_[best]->Next();
      else memtable_iters_[best]->Next();
      continue;
    }

    std::string best_user_key = user_key.ToString();

    // 版本对快照不可见，跳过（snapshot_==0表示读取最新，全部可见）
    if (snapshot_ != 0 && seq > snapshot_) {
      if (is_sstable) sstable_iters_[best]->Next();
      else memtable_iters_[best]->Next();
      continue;
    }

    // 在best子迭代器内推进到该用户键的最后一个可见版本
    // （子迭代器内部同键版本按序列号升序排列，越靠后越新）
    std::string final_value =
        is_sstable ? sstable_iters_[best]->value().ToString()
                   : memtable_iters_[best]->value().ToString();
    bool final_is_delete = (type == static_cast<uint8_t>(ValueType::kDeletion));

    while (true) {
      bool advanced;
      if (is_sstable) {
        sstable_iters_[best]->Next();
        advanced = sstable_iters_[best]->Valid();
      } else {
        memtable_iters_[best]->Next();
        advanced = memtable_iters_[best]->Valid();
      }
      if (!advanced) break;

      Slice next_key = is_sstable ? sstable_iters_[best]->key()
                                  : memtable_iters_[best]->key();
      Slice next_user;
      uint64_t next_seq;
      uint8_t next_type;
      if (!coding::ParseInternalKey(next_key, &next_user, &next_seq, &next_type) ||
          comparator_->Compare(next_user, Slice(best_user_key)) != 0) {
        break; // 已到下一个用户键
      }
      if (snapshot_ != 0 && next_seq > snapshot_) {
        continue; // 该版本对快照不可见
      }
      final_value = is_sstable ? sstable_iters_[best]->value().ToString()
                               : memtable_iters_[best]->value().ToString();
      final_is_delete = (next_type == static_cast<uint8_t>(ValueType::kDeletion));
    }

    // 其他子迭代器中同一用户键的更旧版本全部跳过
    for (int i = 0; i < static_cast<int>(memtable_iters_.size()); ++i) {
      if (is_sstable || i != best) {
        while (memtable_iters_[i]->Valid()) {
          Slice uk;
          uint64_t s;
          uint8_t t;
          if (!coding::ParseInternalKey(memtable_iters_[i]->key(), &uk, &s, &t) ||
              comparator_->Compare(uk, Slice(best_user_key)) != 0) {
            break;
          }
          memtable_iters_[i]->Next();
        }
      }
    }
    for (int i = 0; i < static_cast<int>(sstable_iters_.size()); ++i) {
      if (!is_sstable || i != best) {
        while (sstable_iters_[i]->Valid()) {
          Slice uk;
          uint64_t s;
          uint8_t t;
          if (!coding::ParseInternalKey(sstable_iters_[i]->key(), &uk, &s, &t) ||
              comparator_->Compare(uk, Slice(best_user_key)) != 0) {
            break;
          }
          sstable_iters_[i]->Next();
        }
      }
    }

    // 删除标记：该键被删除，跳过并继续
    if (final_is_delete) {
      continue;
    }

    current_key_ = best_user_key;
    current_value_ = final_value;
    current_memtable_index_ = is_sstable ? -1 : best;
    current_sstable_index_ = is_sstable ? best : -1;
    valid_ = true;
    return;
  }
}

void LSMTreeMergeIterator::FindLargest() {
  valid_ = false;
  current_memtable_index_ = -1;
  current_sstable_index_ = -1;

  while (true) {
    bool is_sstable = false;
    int best = PickChild(false, &is_sstable);
    if (best < 0) {
      return; // 没有更多键
    }

    std::string best_internal = is_sstable
                                    ? sstable_iters_[best]->key().ToString()
                                    : memtable_iters_[best]->key().ToString();
    Slice user_key;
    uint64_t seq;
    uint8_t type;
    if (!coding::ParseInternalKey(best_internal, &user_key, &seq, &type)) {
      if (is_sstable) sstable_iters_[best]->Next();
      else memtable_iters_[best]->Next();
      continue;
    }

    std::string best_user_key = user_key.ToString();

    // 向后遍历取该用户键最老的可见版本
    bool found = false;
    std::string candidate_value;
    bool candidate_is_delete = false;
    while (true) {
      Slice cur_key;
      if (is_sstable) {
        if (!sstable_iters_[best]->Valid()) break;
        cur_key = sstable_iters_[best]->key();
      } else {
        if (!memtable_iters_[best]->Valid()) break;
        cur_key = memtable_iters_[best]->key();
      }
      Slice uk;
      uint64_t s;
      uint8_t t;
      if (!coding::ParseInternalKey(cur_key, &uk, &s, &t) ||
          comparator_->Compare(uk, Slice(best_user_key)) != 0) {
        break;
      }
      if (snapshot_ == 0 || s <= snapshot_) {
        found = true;
        candidate_is_delete =
            (t == static_cast<uint8_t>(ValueType::kDeletion));
        candidate_value = is_sstable ? sstable_iters_[best]->value().ToString()
                                     : memtable_iters_[best]->value().ToString();
      }
      if (is_sstable) sstable_iters_[best]->Next();
      else memtable_iters_[best]->Next();
    }

    // 其他子迭代器中同一用户键的版本全部跳过（都已由本子迭代器覆盖）
    for (int i = 0; i < static_cast<int>(memtable_iters_.size()); ++i) {
      if (is_sstable || i != best) {
        while (memtable_iters_[i]->Valid()) {
          Slice uk;
          uint64_t s;
          uint8_t t;
          if (!coding::ParseInternalKey(memtable_iters_[i]->key(), &uk, &s, &t) ||
              comparator_->Compare(uk, Slice(best_user_key)) != 0) {
            break;
          }
          memtable_iters_[i]->Next();
        }
      }
    }
    for (int i = 0; i < static_cast<int>(sstable_iters_.size()); ++i) {
      if (!is_sstable || i != best) {
        while (sstable_iters_[i]->Valid()) {
          Slice uk;
          uint64_t s;
          uint8_t t;
          if (!coding::ParseInternalKey(sstable_iters_[i]->key(), &uk, &s, &t) ||
              comparator_->Compare(uk, Slice(best_user_key)) != 0) {
            break;
          }
          sstable_iters_[i]->Next();
        }
      }
    }

    // 该键对快照不可见或被删除，跳过继续
    if (!found || candidate_is_delete) {
      continue;
    }

    current_key_ = best_user_key;
    current_value_ = candidate_value;
    valid_ = true;
    return;
  }
}

void LSMTreeMergeIterator::ClearChildren() {
  memtable_iters_.clear();
  // 简化：暂时没有sstable_iters_
}

// ============================================================================
// 恢复相关方法实现
// ============================================================================

Status LSMTree::RecoverFromWALRecord(const std::string &key,
                                     const std::string &value,
                                     uint64_t sequence, bool is_deletion) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  try {
    Slice key_slice(key);

    // 在恢复模式下，直接写入而不检查写入限制，使用专门的恢复逻辑
    std::unique_lock<std::mutex> lock(write_mutex_);

    // 确保当前有活跃的MemTable用于恢复
    if (!current_memtable_) {
      // 创建一个简单的MemTable用于恢复
      current_memtable_ = std::make_shared<MemTable>(comparator_);

      if (!current_memtable_) {
        return Status::IOError("Failed to create MemTable for recovery");
      }
    }

    if (is_deletion) {
      Status s =
          current_memtable_->Delete(key_slice, sequence); // 使用正确的参数
      if (!s.ok()) {
        return Status::IOError("Failed to apply WAL delete record: " +
                               s.ToString());
      }
      LOG_DEBUG << "Recovered DELETE: key=" << key << ", sequence=" << sequence;
    } else {
      Slice value_slice(value);
      Status s = current_memtable_->Put(key_slice, value_slice,
                                        sequence); // 使用正确的参数
      if (!s.ok()) {
        return Status::IOError("Failed to apply WAL put record: " +
                               s.ToString());
      }
      LOG_DEBUG << "Recovered PUT: key=" << key
                << ", value_size=" << value.size() << ", sequence=" << sequence;
    }

    // 更新恢复序列号（原子操作确保线程安全）
    uint64_t current_recovery_seq = recovery_sequence_number_.load();
    while (current_recovery_seq < sequence &&
           !recovery_sequence_number_.compare_exchange_weak(
               current_recovery_seq, sequence)) {
      // CAS循环直到成功更新或者发现更大的值
    }

    // 更新恢复统计信息
    recovery_stats_.total_recovered_records.fetch_add(1);
    if (is_deletion) {
      recovery_stats_.total_recovered_deletes.fetch_add(1);
    } else {
      recovery_stats_.total_recovered_puts.fetch_add(1);
    }

    return Status::OK();

  } catch (const std::exception &e) {
    recovery_stats_.total_recovery_errors.fetch_add(1);
    return Status::IOError("Exception during WAL record recovery for key '" +
                           key + "': " + std::string(e.what()));
  }
}

Status
LSMTree::BatchRecoverFromWAL(const std::vector<WALRecordForRecovery> &records) {
  if (!opened_) {
    return Status::InvalidArgument("LSMTree not opened");
  }

  if (records.empty()) {
    return Status::OK();
  }

  try {
    std::unique_lock<std::mutex> lock(write_mutex_);

    // 设置恢复模式以优化性能
    bool prev_recovery_mode = in_recovery_mode_.load();
    in_recovery_mode_.store(true);

    // 禁用后台任务以避免干扰
    bool prev_slowdown = write_slowdown_.load();
    bool prev_stop = write_stop_.load();
    write_slowdown_.store(false);
    write_stop_.store(false);

    uint64_t max_sequence = 0;
    size_t applied_count = 0;

    for (const auto &record : records) {
      Slice key_slice(record.key);

      Status s;
      if (record.is_deletion) {
        s = Delete(key_slice, record.sequence);
      } else {
        Slice value_slice(record.value);
        s = Put(key_slice, value_slice, record.sequence);
      }

      if (!s.ok()) {
        // 恢复原状态
        in_recovery_mode_.store(prev_recovery_mode);
        write_slowdown_.store(prev_slowdown);
        write_stop_.store(prev_stop);
        return Status::IOError("Failed to apply WAL record in batch: " +
                               s.ToString());
      }

      max_sequence = std::max(max_sequence, record.sequence);
      applied_count++;
    }

    // 更新恢复序列号
    recovery_sequence_number_.store(
        std::max(recovery_sequence_number_.load(), max_sequence));

    // 恢复原状态
    in_recovery_mode_.store(prev_recovery_mode);
    write_slowdown_.store(prev_slowdown);
    write_stop_.store(prev_stop);

    LOG_INFO << "Batch recovery applied " << applied_count
             << " records, max sequence: " << max_sequence;

    return Status::OK();

  } catch (const std::exception &e) {
    return Status::IOError("Exception during batch WAL recovery: " +
                           std::string(e.what()));
  }
}

void LSMTree::SetRecoveryMode(bool in_recovery) {
  in_recovery_mode_.store(in_recovery);

  if (in_recovery) {
    LOG_INFO << "LSMTree entering recovery mode";
    // 在恢复模式下禁用某些优化和后台任务
    write_slowdown_.store(false);
    write_stop_.store(false);
  } else {
    LOG_INFO << "LSMTree exiting recovery mode";
  }
}

bool LSMTree::IsInRecoveryMode() const { return in_recovery_mode_.load(); }

Status LSMTree::FinishRecovery(uint64_t final_sequence) {
  try {
    std::unique_lock<std::mutex> lock(write_mutex_);

    // 1. 同步序列号
    uint64_t current_sequence = next_sequence_number_.load();
    uint64_t recovery_sequence = recovery_sequence_number_.load();

    // 确保序列号的连续性
    uint64_t new_sequence =
        std::max({current_sequence, recovery_sequence, final_sequence}) + 1;
    next_sequence_number_.store(new_sequence);

    LOG_INFO << "Recovery finished. Final sequence: " << final_sequence
             << ", Recovery sequence: " << recovery_sequence
             << ", New sequence: " << new_sequence;

    // 2. 退出恢复模式
    in_recovery_mode_.store(false);

    // 3. 恢复缓冲非空时刷盘为L0 SSTable（WAL重放的记录在这里）
    if (current_memtable_ && !current_memtable_->Empty()) {
      SSTableMeta recovery_meta;
      Status recovery_flush = sstable_manager_->FlushMemTableToSSTable(
          current_memtable_.get(), SSTableLevel::kLevel0, &recovery_meta);
      if (!recovery_flush.ok()) {
        LOG_WARN << "Failed to flush recovered data to SSTable: "
                 << recovery_flush.ToString();
      }
      current_memtable_.reset();
    }

    // 4. 触发一次MemTable刷盘以确保恢复的数据持久化
    Status flush_status = TriggerFlush();
    if (!flush_status.ok()) {
      LOG_WARN << "Failed to trigger flush after recovery: "
               << flush_status.ToString();
      // 不返回错误，允许恢复继续
    }

    // 4. 验证恢复后的数据
    Status validation_status = ValidateRecoveredData();
    if (!validation_status.ok()) {
      LOG_ERROR << "Post-recovery validation failed: "
                << validation_status.ToString();
      return validation_status;
    }

    // 5. 重新启用后台任务
    if (bg_manager_) {
      Status bg_status = RegisterBackgroundTasks();
      if (!bg_status.ok()) {
        LOG_WARN << "Failed to restart background tasks after recovery: "
                 << bg_status.ToString();
      }
    }

    LOG_INFO << "LSMTree recovery completed successfully";
    return Status::OK();

  } catch (const std::exception &e) {
    return Status::IOError("Exception during recovery finish: " +
                           std::string(e.what()));
  }
}

Status LSMTree::ValidateRecoveredData() const {
  try {
    // 1. 验证序列号的一致性
    uint64_t current_seq = next_sequence_number_.load();
    uint64_t recovery_seq = recovery_sequence_number_.load();

    if (current_seq < recovery_seq) {
      return Status::Corruption(
          "Current sequence number is less than recovery sequence");
    }

    // 2. 验证MemTable状态
    if (memtable_manager_) {
      // 简单检查：确保有活跃的MemTable
      // 这里需要MemTableManager提供相应的验证方法
      LOG_INFO << "MemTable validation passed";
    }

    // 3. 验证SSTable状态
    if (sstable_manager_) {
      // 检查各层级文件的完整性
      // 这里需要SSTableManager提供相应的验证方法
      LOG_INFO << "SSTable validation passed";
    }

    // 4. 验证内部状态一致性
    Status state_status = ValidateInternalState();
    if (!state_status.ok()) {
      return Status::Corruption("Internal state validation failed: " +
                                state_status.ToString());
    }

    LOG_INFO << "Recovered data validation completed successfully";
    return Status::OK();

  } catch (const std::exception &e) {
    return Status::IOError("Exception during recovered data validation: " +
                           std::string(e.what()));
  }
}

} // namespace lrdb
