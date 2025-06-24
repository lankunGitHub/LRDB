// 错误处理系统

#pragma once

#include "lrdb/core/status.h"
#include "lrdb/util/logging.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

namespace lrdb {

// 错误级别
enum class ErrorSeverity {
    Low = 0,        // 低级别错误，可以继续运行
    Medium = 1,     // 中等错误，需要关注但不致命
    High = 2,       // 高级别错误，可能影响性能或功能
    Critical = 3    // 严重错误，需要立即处理
};

// 错误类型
enum class ErrorType {
    IOError,
    CorruptionError,
    NotSupportedError,
    InvalidArgumentError,
    NotFoundError,
    CompactionError,
    MemoryError,
    ConcurrencyError,
    RecoveryError,
    SystemError
};

// 错误上下文
struct ErrorContext {
    std::string operation;
    std::string component;
    std::string additional_info;
    uint64_t timestamp;
    std::thread::id thread_id;
    
    ErrorContext() : timestamp(0) {}
    
    ErrorContext(const std::string& op, const std::string& comp, 
                const std::string& info = "")
        : operation(op), component(comp), additional_info(info),
          timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count()),
          thread_id(std::this_thread::get_id()) {}
};

// 错误报告
struct ErrorReport {
    Status status;
    ErrorType type;
    ErrorSeverity severity;
    ErrorContext context;
    size_t occurrence_count;
    uint64_t first_occurrence;
    uint64_t last_occurrence;
    
    ErrorReport()
        : type(ErrorType::MemoryError), severity(ErrorSeverity::Low),
          occurrence_count(0), first_occurrence(), last_occurrence() {}
    
    ErrorReport(const Status& s, ErrorType t, ErrorSeverity sev, 
               const ErrorContext& ctx)
        : status(s), type(t), severity(sev), context(ctx),
          occurrence_count(1), 
          first_occurrence(ctx.timestamp),
          last_occurrence(ctx.timestamp) {}
};

// 错误处理器接口
class ErrorHandler {
public:
    virtual ~ErrorHandler() = default;
    
    // 处理错误
    virtual void HandleError(const Status& status, ErrorType type, 
                           ErrorSeverity severity, const ErrorContext& context) = 0;
    
    // 获取错误统计
    virtual std::vector<ErrorReport> GetErrorReports() const = 0;
    
    // 清空错误历史
    virtual void ClearErrorHistory() = 0;
    
    // 设置错误回调
    virtual void SetErrorCallback(std::function<void(const ErrorReport&)> callback) = 0;
};

// 默认错误处理器
class DefaultErrorHandler : public ErrorHandler {
public:
    explicit DefaultErrorHandler(size_t max_reports = 1000);
    ~DefaultErrorHandler() override = default;
    
    void HandleError(const Status& status, ErrorType type, 
                    ErrorSeverity severity, const ErrorContext& context) override;
    
    std::vector<ErrorReport> GetErrorReports() const override;
    void ClearErrorHistory() override;
    void SetErrorCallback(std::function<void(const ErrorReport&)> callback) override;
    
    // 设置严重性阈值（只有达到此级别的错误才会被记录）
    void SetSeverityThreshold(ErrorSeverity threshold);
    
    // 获取错误统计信息
    size_t GetTotalErrorCount() const;
    size_t GetErrorCountByType(ErrorType type) const;
    size_t GetErrorCountBySeverity(ErrorSeverity severity) const;
    
private:
    mutable std::mutex mutex_;
    std::vector<ErrorReport> error_reports_;
    std::unordered_map<std::string, size_t> error_signature_map_;
    size_t max_reports_;
    std::atomic<ErrorSeverity> severity_threshold_;
    std::function<void(const ErrorReport&)> error_callback_;
    
    // 生成错误签名
    std::string GenerateErrorSignature(const Status& status, ErrorType type,
                                     const ErrorContext& context) const;
    
    // 清理旧的错误报告
    void CleanupOldReports();
};

// 全局错误处理器管理
class ErrorHandlerManager {
public:
    static ErrorHandlerManager& Instance();
    
    // 设置错误处理器
    void SetHandler(std::unique_ptr<ErrorHandler> handler);
    
    // 获取错误处理器
    ErrorHandler* GetHandler() const;
    
    // 报告错误
    void ReportError(const Status& status, ErrorType type, 
                    ErrorSeverity severity, const ErrorContext& context);
    
private:
    ErrorHandlerManager();
    ~ErrorHandlerManager() = default;
    
    std::unique_ptr<ErrorHandler> handler_;
    mutable std::mutex mutex_;
};

// 错误处理工具类
class ErrorUtils {
public:
    // 根据Status代码推断错误类型
    static ErrorType InferErrorType(const Status& status);
    
    // 根据错误类型推断严重性
    static ErrorSeverity InferSeverity(ErrorType type, const Status& status);
    
    // 创建错误上下文
    static ErrorContext CreateContext(const std::string& operation,
                                    const std::string& component,
                                    const std::string& additional_info = "");
    
    // 格式化错误报告
    static std::string FormatErrorReport(const ErrorReport& report);
    
    // 错误类型转字符串
    static std::string ErrorTypeToString(ErrorType type);
    
    // 错误严重性转字符串
    static std::string ErrorSeverityToString(ErrorSeverity severity);
};

// 错误处理宏
#define HANDLE_ERROR(status, operation, component) \
    do { \
        if (!status.ok()) { \
            auto type = lrdb::ErrorUtils::InferErrorType(status); \
            auto severity = lrdb::ErrorUtils::InferSeverity(type, status); \
            auto context = lrdb::ErrorUtils::CreateContext(operation, component); \
            lrdb::ErrorHandlerManager::Instance().ReportError(status, type, severity, context); \
        } \
    } while (0)

#define HANDLE_ERROR_WITH_INFO(status, operation, component, info) \
    do { \
        if (!status.ok()) { \
            auto type = lrdb::ErrorUtils::InferErrorType(status); \
            auto severity = lrdb::ErrorUtils::InferSeverity(type, status); \
            auto context = lrdb::ErrorUtils::CreateContext(operation, component, info); \
            lrdb::ErrorHandlerManager::Instance().ReportError(status, type, severity, context); \
        } \
    } while (0)

#define HANDLE_CUSTOM_ERROR(status, type, severity, operation, component) \
    do { \
        if (!status.ok()) { \
            auto context = lrdb::ErrorUtils::CreateContext(operation, component); \
            lrdb::ErrorHandlerManager::Instance().ReportError(status, type, severity, context); \
        } \
    } while (0)

// 错误恢复回调类型
using ErrorRecoveryCallback = std::function<Status(const ErrorReport& error)>;

// 组件清理回调类型  
using ComponentCleanupCallback = std::function<Status(const std::string& component, const std::string& operation)>;

// 重试操作回调类型
using RetryOperationCallback = std::function<Status()>;

// 错误恢复管理器 - 基于回调的设计
class ErrorRecoveryManager {
public:
    static ErrorRecoveryManager& Instance();
    
    // 注册组件清理回调（由各模块自己注册）
    void RegisterCleanupCallback(const std::string& component_name, ComponentCleanupCallback callback);

    // 注销组件清理回调（组件析构时必须调用，
    // 否则注册表持有捕获 this 的 lambda 形成悬垂指针）
    void UnregisterCleanupCallback(const std::string& component_name);

    // 注册错误类型恢复回调
    void RegisterRecoveryCallback(ErrorType error_type, ErrorRecoveryCallback callback);

    // 注销错误类型恢复回调
    void UnregisterRecoveryCallback(ErrorType error_type);
    
    // 注册通用恢复回调（处理所有错误）
    void RegisterGlobalRecoveryCallback(ErrorRecoveryCallback callback);
    
    // 尝试恢复错误（会调用相应的注册回调）
    Status TryRecover(const ErrorReport& error);
    
    // 执行组件清理
    Status CleanupComponent(const std::string& component_name, const std::string& operation);
    
    // 获取已注册的组件
    std::vector<std::string> GetRegisteredComponents() const;
    
    // 获取支持的错误类型
    std::vector<ErrorType> GetSupportedErrorTypes() const;
    
    // 清除所有回调（主要用于测试）
    void ClearAllCallbacks();
    
private:
    // 组件清理回调映射
    std::unordered_map<std::string, ComponentCleanupCallback> cleanup_callbacks_;
    
    // 错误类型恢复回调映射
    std::unordered_map<ErrorType, ErrorRecoveryCallback> recovery_callbacks_;
    
    // 全局恢复回调列表
    std::vector<ErrorRecoveryCallback> global_callbacks_;
    
    mutable std::mutex mutex_;
};

// 通用重试工具类 - 提供给各模块使用
class RetryHelper {
public:
    explicit RetryHelper(int max_retries = 3, int initial_delay_ms = 1000);
    
    // 设置退避策略
    void SetBackoffStrategy(double backoff_multiplier = 2.0, int max_delay_ms = 30000);
    
    // 执行重试操作
    Status ExecuteWithRetry(RetryOperationCallback operation, const std::string& operation_name = "");
    
    // 检查错误是否适合重试
    static bool IsRetriable(const Status& status);
    
private:
    int max_retries_;
    int initial_delay_ms_;
    double backoff_multiplier_;
    int max_delay_ms_;
    
    // 计算延迟时间（指数退避）
    int CalculateDelay(int attempt) const;
};

// 错误处理宏扩展 - 支持自动恢复
#define HANDLE_ERROR_WITH_RECOVERY(status, operation, component) \
    do { \
        if (!status.ok()) { \
            auto type = lrdb::ErrorUtils::InferErrorType(status); \
            auto severity = lrdb::ErrorUtils::InferSeverity(type, status); \
            auto context = lrdb::ErrorUtils::CreateContext(operation, component); \
            lrdb::ErrorHandlerManager::Instance().ReportError(status, type, severity, context); \
            \
            lrdb::ErrorReport error_report(status, type, severity, context); \
            lrdb::Status recovery_status = lrdb::ErrorRecoveryManager::Instance().TryRecover(error_report); \
            if (recovery_status.ok()) { \
                LOG_INFO << "Error recovery successful for " << operation << " in " << component; \
            } else { \
                LOG_WARN << "Error recovery failed: " << recovery_status.ToString(); \
            } \
        } \
    } while (0)

} // namespace lrdb