// 日志记录系统实现

#include "lrdb/util/logging.h"
#include "lrdb/util/env.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <cstdarg>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>

namespace lrdb {

// LogManager实现
LogManager& LogManager::Instance() {
    static LogManager instance;
    return instance;
}

void LogManager::SetLogger(std::unique_ptr<Log> logger) {
    std::lock_guard<std::mutex> lock(mutex_);
    logger_ = std::move(logger);
}

Log* LogManager::GetLogger() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return logger_.get();
}

void LogManager::Write(LogLevel level, const char* file, int line, const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (logger_) {
        logger_->Write(level, file, line, message);
    } else {
        // 默认输出到stderr
        std::cerr << "[" << log_util::GetTimestamp() << "] "
                  << "[" << log_util::GetBasename(file) << ":" << line << "] "
                  << message << std::endl;
    }
}

void LogManager::SetLevel(LogLevel level) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (logger_) {
        logger_->SetLevel(level);
    }
}

LogLevel LogManager::GetLevel() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (logger_) {
        return logger_->GetLevel();
    }
    return LogLevel::kInfo;
}

// LogStream实现
LogStream::LogStream(LogLevel level, const char* file, int line)
    : level_(level), file_(file), line_(line) {
}

LogStream::~LogStream() {
    LogManager::Instance().Write(level_, file_, line_, stream_.str());
    
    // 如果是FATAL级别，终止程序
    if (level_ == LogLevel::kFatal) {
        std::abort();
    }
}

// FileLog实现
FileLog::FileLog(const std::string& filename) {
    Status s = Env::Default()->NewLogger(filename, &logger_);
    if (!s.ok()) {
        throw std::runtime_error("Failed to create logger: " + s.ToString());
    }
}

FileLog::FileLog(std::unique_ptr<Logger> logger) 
    : logger_(std::move(logger)) {
}

FileLog::~FileLog() = default;

void FileLog::Write(LogLevel level, const char* file, int line, const std::string& message) {
    if (level < level_.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    if (logger_) {
        logger_->Log("[%s] [%s:%d] [%s] %s",
                    log_util::GetTimestamp().c_str(),
                    log_util::GetBasename(file).c_str(),
                    line,
                    LevelToString(level).c_str(),
                    message.c_str());
    }
}

void FileLog::SetLevel(LogLevel level) {
    level_.store(level);
}

LogLevel FileLog::GetLevel() const {
    return level_.load();
}

void FileLog::Flush() {
    // Logger接口没有提供Flush方法，这里可以扩展
}

std::string FileLog::LevelToString(LogLevel level) const {
    switch (level) {
        case LogLevel::kDebug: return "DEBUG";
        case LogLevel::kInfo:  return "INFO";
        case LogLevel::kWarn:  return "WARN";
        case LogLevel::kError: return "ERROR";
        case LogLevel::kFatal: return "FATAL";
        default:              return "UNKNOWN";
    }
}

// ConsoleLog实现
ConsoleLog::ConsoleLog() = default;

void ConsoleLog::Write(LogLevel level, const char* file, int line, const std::string& message) {
    if (level < level_.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 使用颜色输出
    std::ostream& out = (level >= LogLevel::kError) ? std::cerr : std::cout;
    
    out << LevelToColor(level)
        << "[" << log_util::GetTimestamp() << "] "
        << "[" << log_util::GetBasename(file) << ":" << line << "] "
        << "[" << LevelToString(level) << "] "
        << message
        << "\033[0m"  // 重置颜色
        << std::endl;
}

void ConsoleLog::SetLevel(LogLevel level) {
    level_.store(level);
}

LogLevel ConsoleLog::GetLevel() const {
    return level_.load();
}

void ConsoleLog::Flush() {
    std::cout.flush();
    std::cerr.flush();
}

std::string ConsoleLog::LevelToString(LogLevel level) const {
    switch (level) {
        case LogLevel::kDebug: return "DEBUG";
        case LogLevel::kInfo:  return "INFO";
        case LogLevel::kWarn:  return "WARN";
        case LogLevel::kError: return "ERROR";
        case LogLevel::kFatal: return "FATAL";
        default:              return "UNKNOWN";
    }
}

const char* ConsoleLog::LevelToColor(LogLevel level) const {
    switch (level) {
        case LogLevel::kDebug: return "\033[36m";  // 青色
        case LogLevel::kInfo:  return "\033[32m";  // 绿色
        case LogLevel::kWarn:  return "\033[33m";  // 黄色
        case LogLevel::kError: return "\033[31m";  // 红色
        case LogLevel::kFatal: return "\033[35m";  // 紫色
        default:              return "\033[0m";   // 默认
    }
}

// MultiLog实现
void MultiLog::AddLogger(std::unique_ptr<Log> logger) {
    std::lock_guard<std::mutex> lock(mutex_);
    loggers_.push_back(std::move(logger));
}

void MultiLog::Write(LogLevel level, const char* file, int line, const std::string& message) {
    if (level < level_.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& logger : loggers_) {
        logger->Write(level, file, line, message);
    }
}

void MultiLog::SetLevel(LogLevel level) {
    level_.store(level);
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& logger : loggers_) {
        logger->SetLevel(level);
    }
}

LogLevel MultiLog::GetLevel() const {
    return level_.load();
}

void MultiLog::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& logger : loggers_) {
        logger->Flush();
    }
}

// 工厂函数实现
std::unique_ptr<Log> CreateFileLog(const std::string& filename) {
    return std::make_unique<FileLog>(filename);
}

std::unique_ptr<Log> CreateConsoleLog() {
    return std::make_unique<ConsoleLog>();
}

std::unique_ptr<Log> CreateMultiLog() {
    return std::make_unique<MultiLog>();
}

// 删除了PerfTimer实现 - 非核心功能

// log_util函数实现
namespace log_util {

std::string Format(const char* format, ...) {
    va_list args;
    va_start(args, format);
    
    // 获取需要的缓冲区大小
    int size = vsnprintf(nullptr, 0, format, args);
    va_end(args);
    
    if (size <= 0) {
        return {};
    }
    
    std::string result(size + 1, '\0');
    
    va_start(args, format);
    vsnprintf(&result[0], size + 1, format, args);
    va_end(args);
    
    result.resize(size);  // 移除null终止符
    return result;
}

std::string GetThreadId() {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    return oss.str();
}

std::string GetProcessId() {
    return std::to_string(getpid());
}

std::string GetTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::ostringstream oss;
    // localtime 不是线程安全的（返回进程级静态指针），改用 localtime_r
    struct tm tm_buf;
    if (localtime_r(&time_t, &tm_buf) != nullptr) {
        oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    } else {
        oss << "unknown-time";
    }
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    
    return oss.str();
}

std::string GetBasename(const std::string& path) {
    size_t pos = path.find_last_of("/\\");
    if (pos == std::string::npos) {
        return path;
    }
    return path.substr(pos + 1);
}

} // namespace log_util

} // namespace lrdb