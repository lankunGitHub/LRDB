// 日志记录系统

#pragma once

#include "lrdb/util/env.h"
#include <memory>
#include <string>
#include <sstream>
#include <atomic>
#include <thread>
#include <mutex>

namespace lrdb {

// 日志级别
enum class LogLevel {
    kDebug = 0,
    kInfo = 1,
    kWarn = 2,
    kError = 3,
    kFatal = 4
};

// 日志接口
class Log {
public:
    virtual ~Log() = default;
    
    // 写入日志
    virtual void Write(LogLevel level, const char* file, int line, 
                      const std::string& message) = 0;
    
    // 设置日志级别
    virtual void SetLevel(LogLevel level) = 0;
    
    // 获取日志级别
    virtual LogLevel GetLevel() const = 0;
    
    // 刷新日志缓冲区
    virtual void Flush() = 0;
};

// 日志管理器
class LogManager {
public:
    static LogManager& Instance();
    
    // 设置日志记录器
    void SetLogger(std::unique_ptr<Log> logger);
    
    // 获取日志记录器
    Log* GetLogger() const;
    
    // 写入日志
    void Write(LogLevel level, const char* file, int line, const std::string& message);
    
    // 设置全局日志级别
    void SetLevel(LogLevel level);
    
    // 获取全局日志级别
    LogLevel GetLevel() const;
    
private:
    LogManager() = default;
    ~LogManager() = default;
    
    std::unique_ptr<Log> logger_;
    mutable std::mutex mutex_;
};

// 日志流类
class LogStream {
public:
    LogStream(LogLevel level, const char* file, int line);
    ~LogStream();
    
    template<typename T>
    LogStream& operator<<(const T& value) {
        stream_ << value;
        return *this;
    }
    
private:
    LogLevel level_;
    const char* file_;
    int line_;
    std::ostringstream stream_;
};

// 文件日志实现
class FileLog : public Log {
public:
    explicit FileLog(const std::string& filename);
    explicit FileLog(std::unique_ptr<Logger> logger);
    ~FileLog() override;
    
    void Write(LogLevel level, const char* file, int line, 
              const std::string& message) override;
    void SetLevel(LogLevel level) override;
    LogLevel GetLevel() const override;
    void Flush() override;
    
private:
    std::string LevelToString(LogLevel level) const;
    
    std::unique_ptr<Logger> logger_;
    std::atomic<LogLevel> level_{LogLevel::kInfo};
    mutable std::mutex mutex_;
};

// 控制台日志实现
class ConsoleLog : public Log {
public:
    ConsoleLog();
    ~ConsoleLog() override = default;
    
    void Write(LogLevel level, const char* file, int line, 
              const std::string& message) override;
    void SetLevel(LogLevel level) override;
    LogLevel GetLevel() const override;
    void Flush() override;
    
private:
    std::string LevelToString(LogLevel level) const;
    const char* LevelToColor(LogLevel level) const;
    
    std::atomic<LogLevel> level_{LogLevel::kInfo};
    mutable std::mutex mutex_;
};

// 多目标日志实现
class MultiLog : public Log {
public:
    MultiLog() = default;
    ~MultiLog() override = default;
    
    void AddLogger(std::unique_ptr<Log> logger);
    
    void Write(LogLevel level, const char* file, int line, 
              const std::string& message) override;
    void SetLevel(LogLevel level) override;
    LogLevel GetLevel() const override;
    void Flush() override;
    
private:
    std::vector<std::unique_ptr<Log>> loggers_;
    std::atomic<LogLevel> level_{LogLevel::kInfo};
    mutable std::mutex mutex_;
};

// 工厂函数
std::unique_ptr<Log> CreateFileLog(const std::string& filename);
std::unique_ptr<Log> CreateConsoleLog();
std::unique_ptr<Log> CreateMultiLog();

// 日志宏定义
#define LOG_DEBUG lrdb::LogStream(lrdb::LogLevel::kDebug, __FILE__, __LINE__)
#define LOG_INFO lrdb::LogStream(lrdb::LogLevel::kInfo, __FILE__, __LINE__)
#define LOG_WARN lrdb::LogStream(lrdb::LogLevel::kWarn, __FILE__, __LINE__)
#define LOG_ERROR lrdb::LogStream(lrdb::LogLevel::kError, __FILE__, __LINE__)
#define LOG_FATAL lrdb::LogStream(lrdb::LogLevel::kFatal, __FILE__, __LINE__)

// 条件日志（condition 必须被真正求值；旧实现把条件直接丢掉，恒输出）
#define LOG_DEBUG_IF(condition) if (condition) lrdb::LogStream(lrdb::LogLevel::kDebug, __FILE__, __LINE__)
#define LOG_INFO_IF(condition) if (condition) lrdb::LogStream(lrdb::LogLevel::kInfo, __FILE__, __LINE__)
#define LOG_WARN_IF(condition) if (condition) lrdb::LogStream(lrdb::LogLevel::kWarn, __FILE__, __LINE__)
#define LOG_ERROR_IF(condition) if (condition) lrdb::LogStream(lrdb::LogLevel::kError, __FILE__, __LINE__)

// 断言宏
#define LRDB_ASSERT(condition) \
    do { \
        if (!(condition)) { \
            LOG_FATAL << "Assertion failed: " #condition; \
            std::abort(); \
        } \
    } while (0)

#define LRDB_ASSERT_MSG(condition, message) \
    do { \
        if (!(condition)) { \
            LOG_FATAL << "Assertion failed: " #condition << ". " << message; \
            std::abort(); \
        } \
    } while (0)

// 删除了性能监控功能 - 非核心功能

// 日志辅助工具
namespace log_util {

// 格式化字符串
std::string Format(const char* format, ...);

// 获取线程ID字符串
std::string GetThreadId();

// 获取进程ID字符串
std::string GetProcessId();

// 获取时间戳字符串
std::string GetTimestamp();

// 获取源文件名（去掉路径）
std::string GetBasename(const std::string& path);

} // namespace log_util

} // namespace lrdb