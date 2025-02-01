// 数据库选项配置

#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <functional>
#include "lrdb/core/status.h"

namespace lrdb {

// 前向声明
class Comparator;
class FilterPolicy;
class MergeOperator;
class CompactionFilter;
class MemoryAllocator;
class Logger;
class Environment;
class Snapshot;

// 压缩类型
enum class CompressionType {
    kNoCompression = 0,
    kSnappyCompression = 1,
    kZlibCompression = 2,
    kBZip2Compression = 3,
    kLZ4Compression = 4,
    kLZ4HCCompression = 5,
    kXpressCompression = 6,
    kZSTDCompression = 7
};

// 压缩策略
enum class CompactionStyle {
    kLevelStyleCompaction = 0,
    kUniversalStyleCompaction = 1,
    kFIFOStyleCompaction = 2
};

// WAL恢复模式
enum class WALRecoveryMode {
    kTolerateCorruptedTailRecords = 0,
    kAbsoluteConsistency = 1,
    kPointInTimeRecovery = 2,
    kSkipAnyCorruptedRecords = 3
};

// 范围结构
struct Range {
    std::string start;
    std::string limit;
    
    Range() = default;
    Range(const std::string& s, const std::string& l) : start(s), limit(l) {}
};

// 写选项
struct WriteOptions {
    // 是否同步写入WAL
    bool sync = false;
    
    // 禁用WAL
    bool disableWAL = false;
    
    // 忽略缺失的列族
    bool ignore_missing_column_families = false;
    
    // 不等待memtable刷盘
    bool no_slowdown = false;
    
    // 写入低优先级
    bool low_pri = false;
    
    // 创建默认选项
    WriteOptions() = default;
};

// 读选项
struct ReadOptions {
    // 读快照
    const Snapshot* snapshot = nullptr;
    
    // 迭代器上界
    const std::string* iterate_upper_bound = nullptr;
    
    // 迭代器下界  
    const std::string* iterate_lower_bound = nullptr;
    
    // 读层级
    int read_tier = 0;
    
    // 验证校验和
    bool verify_checksums = false;
    
    // 填充缓存
    bool fill_cache = true;
    
    // 读取的最大跳过数
    uint64_t max_skippable_internal_keys = 0;
    
    // 读取的总排序顺序
    bool total_order_seek = false;
    
    // 前缀相同键优化
    bool prefix_same_as_start = false;
    
    // 固定前缀长度
    bool pin_data = false;
    
    // 背景清洗率限制
    bool background_purge_on_iterator_cleanup = false;
    
    // 忽略范围删除
    bool ignore_range_deletions = false;
    
    ReadOptions() = default;
};

// 刷盘选项
struct FlushOptions {
    // 等待刷盘完成
    bool wait = true;
    
    // 允许写入停顿
    bool allow_write_stall = false;
    
    FlushOptions() = default;
};

// 压缩范围选项
struct CompactRangeOptions {
    // 压缩输出的级别
    int target_level = -1;
    
    // 排他手动压缩
    bool exclusive_manual_compaction = true;
    
    // 改变级别
    bool change_level = false;
    
    // 目标路径ID
    uint32_t target_path_id = 0;
    
    // 最大子压缩
    uint32_t max_subcompactions = 0;
    
    CompactRangeOptions() = default;
};

// 列族选项
struct ColumnFamilyOptions {
    // 比较器
    const Comparator* comparator = nullptr;
    
    // 合并操作器
    std::shared_ptr<MergeOperator> merge_operator = nullptr;
    
    // 压缩过滤器
    std::shared_ptr<CompactionFilter> compaction_filter = nullptr;
    
    // 写缓冲大小
    size_t write_buffer_size = 64 << 20; // 64MB
    
    // 最大写缓冲数
    int max_write_buffer_number = 2;
    
    // 最小合并写缓冲数
    int min_write_buffer_number_to_merge = 1;
    
    // 最大写缓冲数保持
    int max_write_buffer_number_to_maintain = 0;
    
    // 级别0文件数阈值
    int level0_file_num_compaction_trigger = 4;
    
    // 级别0减慢写入阈值
    int level0_slowdown_writes_trigger = 20;
    
    // 级别0停止写入阈值
    int level0_stop_writes_trigger = 36;
    
    // 目标文件大小基数
    uint64_t target_file_size_base = 64 << 20; // 64MB
    
    // 目标文件大小乘数
    int target_file_size_multiplier = 1;
    
    // 最大级别字节基数
    uint64_t max_bytes_for_level_base = 256 << 20; // 256MB
    
    // 级别大小乘数
    double max_bytes_for_level_multiplier = 10.0;
    
    // 额外级别大小乘数
    std::vector<int> max_bytes_for_level_multiplier_additional;
    
    // 压缩样式
    CompactionStyle compaction_style = CompactionStyle::kLevelStyleCompaction;
    
    // 压缩选项
    struct CompactionOptions {
        uint64_t max_size_amplification_percent = 200;
        int compression_size_percent = -1;
        uint32_t min_merge_width = 2;
        uint32_t max_merge_width = UINT32_MAX;
    } compaction_options_universal;
    
    // FIFO压缩选项
    struct FIFOCompactionOptions {
        uint64_t max_table_files_size = 1ULL << 30; // 1GB
        bool allow_compaction = false;
    } compaction_options_fifo;
    
    // 压缩优先级
    enum CompactionPri {
        kByCompensatedSize = 0,
        kOldestLargestSeqFirst = 1,
        kOldestSmallestSeqFirst = 2,
        kMinOverlappingRatio = 3,
        kRoundRobin = 4
    } compaction_pri = kByCompensatedSize;
    
    // 块大小
    size_t block_size = 4096;
    
    // 块大小偏差
    int block_size_deviation = 10;
    
    // 块重启间隔
    int block_restart_interval = 16;
    
    // 索引块重启间隔
    int index_block_restart_interval = 1;
    
    // 元数据块大小
    uint64_t metadata_block_size = 4096;
    
    // 分区索引过滤器
    bool partition_filters = false;
    
    // 使用增量编码重启
    bool use_delta_encoding = true;
    
    // 过滤策略
    std::shared_ptr<FilterPolicy> filter_policy = nullptr;
    
    // 全过滤器在块中
    bool whole_key_filtering = true;
    
    // 验证校验和
    bool verify_checksums_in_compaction = true;
    
    // 压缩类型
    CompressionType compression = CompressionType::kSnappyCompression;
    
    // 按级别的压缩类型
    std::vector<CompressionType> compression_per_level;
    
    // 压缩选项
    struct CompressionOptions {
        int window_bits = -14;
        int level = 32767;
        int strategy = 0;
        uint32_t max_dict_bytes = 0;
        uint32_t zstd_max_train_bytes = 0;
        bool enabled = false;
        uint32_t max_dict_buffer_bytes = 0;
    } compression_opts;
    
    // 软挂起压缩字节限制
    uint64_t soft_pending_compaction_bytes_limit = 64ULL << 30; // 64GB
    
    // 硬挂起压缩字节限制
    uint64_t hard_pending_compaction_bytes_limit = 256ULL << 30; // 256GB
    
    // 级别动态级别字节
    bool level_compaction_dynamic_level_bytes = false;
    
    // 最大压缩字节
    uint64_t max_compaction_bytes = 0;
    
    // 禁用自动压缩
    bool disable_auto_compactions = false;
    
    // 验证配置
    Status Validate() const;
    
    ColumnFamilyOptions() = default;
};

// DB选项
struct DBOptions {
    // 创建缺失
    bool create_if_missing = false;
    
    // 错误如果存在
    bool error_if_exists = false;
    
    // 偏执检查
    bool paranoid_checks = true;
    
    // 环境
    Environment* env = nullptr;
    
    // 信息日志
    std::shared_ptr<Logger> info_log = nullptr;
    
    // 信息日志级别
    int info_log_level = 1;
    
    // 最大打开文件数
    int max_open_files = -1;
    
    // 最大文件打开数
    int max_file_opening_threads = 16;
    
    // 最大总WAL大小
    uint64_t max_total_wal_size = 0;
    
    // 统计转储周期秒
    unsigned int stats_dump_period_sec = 600;
    
    // 统计持久化周期秒
    unsigned int stats_persist_period_sec = 600;
    
    // 持久化统计到磁盘
    bool persist_stats_to_disk = false;
    
    // 统计历史大小
    size_t stats_history_buffer_size = 1024 * 1024;
    
    // 最大后台作业数
    int max_background_jobs = 2;
    
    // 最大后台压缩数
    int max_background_compactions = -1;
    
    // 最大子压缩数
    uint32_t max_subcompactions = 1;
    
    // 最大后台刷盘数
    int max_background_flushes = -1;
    
    // 最大日志文件大小
    size_t max_log_file_size = 0;
    
    // 日志文件时间到滚动
    size_t log_file_time_to_roll = 0;
    
    // 保持日志文件数
    size_t keep_log_file_num = 1000;
    
    // 回收日志文件数
    size_t recycle_log_file_num = 0;
    
    // 最大清单文件大小
    uint64_t max_manifest_file_size = 1024 * 1024 * 1024; // 1GB
    
    // 表缓存元素移除数量缓存
    int table_cache_numshardbits = 6;
    
    // WAL恢复模式
    WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    
    // 启用WAL
    bool enable_wal = true;
    
    // WAL目录
    std::string wal_dir;
    
    // WAL TTL秒数
    uint64_t wal_ttl_seconds = 0;
    
    // WAL大小限制MB
    uint64_t wal_size_limit_mb = 0;
    
    // 清单预分配大小
    size_t manifest_preallocation_size = 4 * 1024 * 1024;
    
    // 允许mmap读取
    bool allow_mmap_reads = false;
    
    // 允许mmap写入
    bool allow_mmap_writes = false;
    
    // 使用直接读取
    bool use_direct_reads = false;
    
    // 使用直接IO刷盘
    bool use_direct_io_for_flush_and_compaction = false;
    
    // 允许fallocate
    bool allow_fallocate = true;
    
    // 是否为全数据库
    bool is_fd_close_on_exec = true;
    
    // 跳过统计更新删除
    bool skip_stats_update_on_db_open = false;
    
    // 新表阅读器预取索引和过滤器块
    bool new_table_reader_for_compaction_inputs = false;
    
    // 压缩读取头大小
    size_t compaction_readahead_size = 0;
    
    // 随机访问最大缓冲区大小
    size_t random_access_max_buffer_size = 1024 * 1024;
    
    // 可写文件最大缓冲区大小
    size_t writable_file_max_buffer_size = 1024 * 1024;
    
    // 字节每同步
    uint64_t bytes_per_sync = 0;
    
    // WAL字节每同步
    uint64_t wal_bytes_per_sync = 0;
    
    // 监听器
    std::vector<std::shared_ptr<class EventListener>> listeners;
    
    // 启用线程跟踪
    bool enable_thread_tracking = false;
    
    // 延迟写入率
    uint64_t delayed_write_rate = 16 * 1024 * 1024; // 16MB/s
    
    // 启用从所有列族删除范围
    bool enable_write_thread_adaptive_yield = true;
    
    // 写入线程最大yield usec
    uint64_t write_thread_max_yield_usec = 100;
    
    // 写入线程慢速yield usec
    uint64_t write_thread_slow_yield_usec = 3;
    
    // 跳过检查大小兼容性
    bool skip_checking_sst_file_sizes_on_db_open = false;
    
    // 内存分配器类型（简化版本，只支持jemalloc）
    std::string memory_allocator_type = "jemalloc";
    
    // ============================================================================
    // 后台任务配置
    // ============================================================================
    
    // LSM刷盘检查间隔（毫秒）
    uint32_t lsm_flush_check_interval_ms = 1000;
    
    // LSM压缩检查间隔（毫秒）
    uint32_t lsm_compaction_check_interval_ms = 5000;
    
    // WAL同步检查间隔（毫秒）
    uint32_t wal_sync_check_interval_ms = 1000;
    
    // WAL归档检查间隔（毫秒）
    uint32_t wal_archive_check_interval_ms = 30000;
    
    // WAL清理检查间隔（毫秒）
    uint32_t wal_cleanup_check_interval_ms = 60000;
    
    // MVCC版本清理检查间隔（毫秒）
    uint32_t mvcc_version_cleanup_interval_ms = 10000;
    
    // MVCC内存清理检查间隔（毫秒）
    uint32_t mvcc_memory_cleanup_interval_ms = 5000;
    
    // MVCC过期事务清理检查间隔（毫秒）
    uint32_t mvcc_expired_txn_cleanup_interval_ms = 60000;
    
    // 快照管理后台任务配置
    uint32_t snapshot_cleanup_check_interval_ms = 30000;      // 30秒
    uint32_t snapshot_merge_check_interval_ms = 60000;        // 60秒
    uint32_t snapshot_stats_update_interval_ms = 10000;       // 10秒
    
    // 内存管理后台任务配置
    uint32_t memory_reclamation_check_interval_ms = 15000;    // 15秒
    uint32_t memory_stats_update_interval_ms = 20000;         // 20秒
    uint32_t memory_compaction_check_interval_ms = 45000;     // 45秒
    
    // 事务管理后台任务配置
    uint32_t transaction_timeout_check_interval_ms = 5000;     // 5秒
    uint32_t transaction_deadlock_detection_interval_ms = 10000; // 10秒
    uint32_t transaction_log_cleanup_interval_ms = 30000;     // 30秒

    // MVCC刷回最大条目数（每次FlushCommittedToLSM的最大版本条目）
    size_t mvcc_flush_max_items = 1024;
    
    // 验证配置
    Status Validate() const;
    
    DBOptions() = default;
};

// 组合选项
struct Options : public ColumnFamilyOptions, public DBOptions {
    Options() = default;
    Options(const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts)
        : ColumnFamilyOptions(cf_opts), DBOptions(db_opts) {}
};

// 列族描述符在 manifest.h 中定义，避免重复定义

// 选项工具函数
namespace options_util {

// 创建默认选项
Options CreateDefaultOptions();
DBOptions CreateDefaultDBOptions();
ColumnFamilyOptions CreateDefaultColumnFamilyOptions();

// 选项验证
Status ValidateOptions(const Options& options);
Status ValidateDBOptions(const DBOptions& db_options);
Status ValidateColumnFamilyOptions(const ColumnFamilyOptions& cf_options);

// 选项转换
std::string OptionsToString(const Options& options);
Status StringToOptions(const std::string& opts_str, Options* options);

// 从文件加载选项
Status LoadOptionsFromFile(const std::string& filename, Options* options);
Status SaveOptionsToFile(const std::string& filename, const Options& options);

// 优化建议
struct OptimizationSuggestion {
    std::string parameter_name;
    std::string current_value;
    std::string suggested_value;
    std::string reason;
};

std::vector<OptimizationSuggestion> AnalyzeOptions(const Options& options,
                                                   uint64_t db_size = 0,
                                                   uint64_t write_rate = 0);

}  // namespace options_util

}  // namespace lrdb