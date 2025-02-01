// 环境抽象层定义

#pragma once

#include "lrdb/core/slice.h"
#include "lrdb/core/status.h"
#include <chrono>
#include <cstdarg>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace lrdb {

// 前向声明
class RandomAccessFile;
class WritableFile;
class SequentialFile;
class Directory;
class FileLock;
class Logger;
class MemoryMappedFileBuffer;

// 文件属性
struct FileAttributes {
  uint64_t size_bytes = 0;
  std::chrono::system_clock::time_point modification_time;
  bool is_directory = false;
  bool is_symbolic_link = false;
};

// 环境抽象接口
class Env {
public:
  Env() = default;
  virtual ~Env() = default;

  // 获取默认环境实例
  static Env *Default();

  // === 文件系统操作 ===

  // 创建顺序读取文件
  virtual Status NewSequentialFile(const std::string &filename,
                                   std::unique_ptr<SequentialFile> *result) = 0;

  // 创建随机访问文件
  virtual Status
  NewRandomAccessFile(const std::string &filename,
                      std::unique_ptr<RandomAccessFile> *result) = 0;

  // 创建可写文件
  virtual Status NewWritableFile(const std::string &filename,
                                 std::unique_ptr<WritableFile> *result) = 0;

  // 创建可追加写入的文件
  virtual Status NewAppendableFile(const std::string &filename,
                                   std::unique_ptr<WritableFile> *result) = 0;

  // 检查文件是否存在
  virtual bool FileExists(const std::string &filename) = 0;

  // 获取文件属性
  virtual Status GetFileAttributes(const std::string &filename,
                                   FileAttributes *attrs) = 0;

  // 删除文件
  virtual Status DeleteFile(const std::string &filename) = 0;

  // 重命名文件
  virtual Status RenameFile(const std::string &src,
                            const std::string &target) = 0;

  // 链接文件（硬链接）
  virtual Status LinkFile(const std::string &src,
                          const std::string &target) = 0;

  // 获取文件大小
  virtual Status GetFileSize(const std::string &filename, uint64_t *size) = 0;

  // 读取整个文件到字符串
  virtual Status ReadFileToString(const std::string &filename,
                                  std::string *data) = 0;

  // 将字符串写入文件
  virtual Status WriteStringToFile(const std::string &data,
                                   const std::string &filename) = 0;

  // === 目录操作 ===

  // 创建目录
  virtual Status CreateDir(const std::string &dirname) = 0;

  // 创建目录（如果不存在）
  virtual Status CreateDirIfMissing(const std::string &dirname) = 0;

  // 删除目录
  virtual Status DeleteDir(const std::string &dirname) = 0;

  // 列出目录内容
  virtual Status GetChildren(const std::string &dirname,
                             std::vector<std::string> *result) = 0;

  // 获取目录对象
  virtual Status NewDirectory(const std::string &dirname,
                              std::unique_ptr<Directory> *result) = 0;

  // === 文件锁定 ===

  // 锁定文件
  virtual Status LockFile(const std::string &filename,
                          std::unique_ptr<FileLock> *lock) = 0;

  // 释放文件锁
  virtual Status UnlockFile(std::unique_ptr<FileLock> lock) = 0;

  // === 时间和调度 ===

  // 获取当前时间（微秒）
  virtual uint64_t NowMicros() = 0;

  // 获取当前时间（纳秒）
  virtual uint64_t NowNanos() = 0;

  // 睡眠指定微秒
  virtual void SleepForMicroseconds(int micros) = 0;

  // === 线程管理 ===

  // 启动线程
  virtual void StartThread(void (*function)(void *arg), void *arg) = 0;

  // === 系统信息 ===

  // 获取CPU核心数
  virtual unsigned int GetNumberOfCpus() = 0;

  // 获取系统内存大小
  virtual uint64_t GetSystemMemory() = 0;

  // 获取进程内存使用量
  virtual uint64_t GetProcessMemoryUsage() = 0;

  // === 日志记录 ===

  // 创建日志记录器
  virtual Status NewLogger(const std::string &filename,
                           std::unique_ptr<Logger> *result) = 0;

  // === 随机数生成 ===

  // 生成随机数
  virtual uint64_t GenerateRandom() = 0;

  // === 原子操作 ===

  // 原子地写入文件（先写临时文件，再重命名）
  virtual Status WriteFileAtomically(const std::string &filename,
                                     const std::string &data) = 0;

  // 同步目录（确保目录操作持久化）
  virtual Status SyncDir(const std::string &dirname) = 0;

  // === 内存映射 ===

  // 内存映射文件
  virtual Status NewMemoryMappedFileBuffer(
      const std::string &filename,
      std::unique_ptr<MemoryMappedFileBuffer> *result) = 0;

  // === 环境特定功能 ===

  // 获取临时目录
  virtual std::string GetTempDirectory() = 0;

  // 获取测试目录
  virtual std::string GetTestDirectory() = 0;
};

// 顺序文件接口
class SequentialFile {
public:
  SequentialFile() = default;
  virtual ~SequentialFile() = default;

  // 读取数据
  virtual Status Read(size_t n, Slice *result, char *scratch) = 0;

  // 跳过数据
  virtual Status Skip(uint64_t n) = 0;

  // 获取文件名
  virtual std::string GetFileName() const { return ""; }
};

// 随机访问文件接口
class RandomAccessFile {
public:
  enum AccessPattern { kNormal, kRandom, kSequential, kWillNeed, kWontNeed };

  RandomAccessFile() = default;
  virtual ~RandomAccessFile() = default;

  // 在指定偏移处读取数据
  virtual Status Read(uint64_t offset, size_t n, Slice *result,
                      char *scratch) const = 0;

  // 获取文件大小
  virtual size_t GetUniqueId(char *id, size_t max_size) const {
    (void)id;
    (void)max_size;
    return 0;
  }

  // 提示访问模式
  virtual void Hint(AccessPattern pattern) { (void)pattern; }
};

// 可写文件接口
class WritableFile {
public:
  WritableFile() = default;
  virtual ~WritableFile() = default;

  // 写入数据
  virtual Status Append(const Slice &data) = 0;

  // 关闭文件
  virtual Status Close() = 0;

  // 刷新缓冲区
  virtual Status Flush() = 0;

  // 同步到磁盘
  virtual Status Sync() = 0;

  // 获取文件大小 - 如果需要实现，子类必须提供真正的实现
  virtual uint64_t GetFileSize() = 0;
};

// 目录接口
class Directory {
public:
  virtual ~Directory() = default;

  // 同步目录
  virtual Status Fsync() = 0;
};

// 文件锁接口
class FileLock {
public:
  FileLock() = default;
  virtual ~FileLock() = default;

  FileLock(const FileLock &) = delete;
  FileLock &operator=(const FileLock &) = delete;
};

// 内存映射文件缓冲区
class MemoryMappedFileBuffer {
public:
  MemoryMappedFileBuffer() = default;
  virtual ~MemoryMappedFileBuffer() = default;

  // 获取数据指针
  virtual void *GetBase() const = 0;

  // 获取大小
  virtual size_t GetLength() const = 0;
};

// 日志记录器接口
class Logger {
public:
  Logger() = default;
  virtual ~Logger() = default;

  // 记录日志
  virtual void Logv(const char *format, va_list args) = 0;

  // 便利方法
  void Log(const char *format, ...) {
    va_list args;
    va_start(args, format);
    Logv(format, args);
    va_end(args);
  }

  // 日志级别
  enum Level {
    DEBUG_LEVEL = 0,
    INFO_LEVEL = 1,
    WARN_LEVEL = 2,
    ERROR_LEVEL = 3,
    FATAL_LEVEL = 4
  };

  // 设置日志级别 - 子类必须提供真实实现
  virtual void SetLevel(Level level) = 0;
  // 获取当前日志级别 - 子类必须提供真实实现
  virtual Level GetLevel() const = 0;

  virtual void Debug(const char *format, ...) {
    va_list args;
    va_start(args, format);
    if (GetLevel() <= DEBUG_LEVEL)
      Logv(format, args);
    va_end(args);
  }

  virtual void Info(const char *format, ...) {
    va_list args;
    va_start(args, format);
    if (GetLevel() <= INFO_LEVEL)
      Logv(format, args);
    va_end(args);
  }

  virtual void Warn(const char *format, ...) {
    va_list args;
    va_start(args, format);
    if (GetLevel() <= WARN_LEVEL)
      Logv(format, args);
    va_end(args);
  }

  virtual void Error(const char *format, ...) {
    va_list args;
    va_start(args, format);
    if (GetLevel() <= ERROR_LEVEL)
      Logv(format, args);
    va_end(args);
  }
};

// 环境工厂函数
std::unique_ptr<Env> NewPosixEnv();
std::unique_ptr<Env> NewMemEnv(Env *base_env = nullptr);

// 环境选项
struct EnvOptions {
  bool use_mmap_reads = true;
  bool use_mmap_writes = false;
  bool use_direct_reads = false;
  bool use_direct_writes = false;
  size_t compaction_readahead_size = 0;
  size_t random_access_max_buffer_size = 1024 * 1024;
  size_t writable_file_max_buffer_size = 1024 * 1024;
  bool bytes_per_sync = 0;
  bool fallocate_with_keep_size = true;
};

} // namespace lrdb