// 错误处理系统实现

#include "lrdb/util/error_handler.h"
#include "lrdb/util/logging.h"
#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

namespace lrdb {

// DefaultErrorHandler实现
DefaultErrorHandler::DefaultErrorHandler(size_t max_reports)
    : max_reports_(max_reports), severity_threshold_(ErrorSeverity::Low) {}

void DefaultErrorHandler::HandleError(const Status &status, ErrorType type,
                                      ErrorSeverity severity,
                                      const ErrorContext &context) {
  // 原子读取，避免与 SetSeverityThreshold 的并发写构成数据竞争
  if (severity < severity_threshold_.load(std::memory_order_acquire)) {
    return;
  }

  // 错误回调必须等 mutex_ 释放后再调用：回调里可能再调
  // ReportError/GetErrorReports/ClearErrorHistory 等接口，
  // 持锁调用会对非递归 mutex 二次加锁自死锁。
  // 因此这里在锁内只更新状态并拷贝出回调与报告副本
  std::function<void(const ErrorReport &)> callback;
  ErrorReport report_copy;

  {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string signature = GenerateErrorSignature(status, type, context);

    auto it = error_signature_map_.find(signature);
    if (it != error_signature_map_.end()) {
      // 更新现有错误报告
      size_t index = it->second;
      if (index < error_reports_.size()) {
        ErrorReport &report = error_reports_[index];
        report.occurrence_count++;
        report.last_occurrence = context.timestamp;

        // 记录日志
        LOG_WARN << "Recurring error [" << signature << "] occurred "
                 << report.occurrence_count << " times: " << status.ToString();
      }
    } else {
      // 创建新的错误报告
      ErrorReport report(status, type, severity, context);

      if (error_reports_.size() >= max_reports_) {
        CleanupOldReports();
      }

      size_t index = error_reports_.size();
      error_reports_.emplace_back(std::move(report));
      error_signature_map_[signature] = index;

      // 记录日志
      LogLevel log_level;
      switch (severity) {
      case ErrorSeverity::Critical:
        log_level = LogLevel::kFatal;
        break;
      case ErrorSeverity::High:
        log_level = LogLevel::kError;
        break;
      case ErrorSeverity::Medium:
        log_level = LogLevel::kWarn;
        break;
      default:
        log_level = LogLevel::kDebug;
        break;
      }

      LogManager::Instance().Write(log_level, __FILE__, __LINE__,
                                   "Error in " + context.component +
                                       "::" + context.operation + " - " +
                                       status.ToString() +
                                       (!context.additional_info.empty()
                                            ? " (" + context.additional_info + ")"
                                            : ""));

      // 拷贝回调与报告，锁外调用
      callback = error_callback_;
      report_copy = error_reports_.back();
    }
  }

  if (callback) {
    callback(report_copy);
  }
}

std::vector<ErrorReport> DefaultErrorHandler::GetErrorReports() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return error_reports_;
}

void DefaultErrorHandler::ClearErrorHistory() {
  std::lock_guard<std::mutex> lock(mutex_);
  error_reports_.clear();
  error_signature_map_.clear();
}

void DefaultErrorHandler::SetErrorCallback(
    std::function<void(const ErrorReport &)> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  error_callback_ = callback;
}

void DefaultErrorHandler::SetSeverityThreshold(ErrorSeverity threshold) {
  severity_threshold_.store(threshold, std::memory_order_release);
}

size_t DefaultErrorHandler::GetTotalErrorCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t total = 0;
  for (const auto &report : error_reports_) {
    total += report.occurrence_count;
  }
  return total;
}

size_t DefaultErrorHandler::GetErrorCountByType(ErrorType type) const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t count = 0;
  for (const auto &report : error_reports_) {
    if (report.type == type) {
      count += report.occurrence_count;
    }
  }
  return count;
}

size_t
DefaultErrorHandler::GetErrorCountBySeverity(ErrorSeverity severity) const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t count = 0;
  for (const auto &report : error_reports_) {
    if (report.severity == severity) {
      count += report.occurrence_count;
    }
  }
  return count;
}

std::string DefaultErrorHandler::GenerateErrorSignature(
    const Status &status, ErrorType type, const ErrorContext &context) const {
  std::ostringstream oss;
  oss << static_cast<int>(type) << "|" << status.code() << "|"
      << context.operation << "|" << context.component;
  return oss.str();
}

void DefaultErrorHandler::CleanupOldReports() {
  if (error_reports_.size() < max_reports_ / 2) {
    return;
  }

  // 按最后发生时间排序，保留最近的错误
  std::sort(error_reports_.begin(), error_reports_.end(),
            [](const ErrorReport &a, const ErrorReport &b) {
              return a.last_occurrence > b.last_occurrence;
            });

  size_t keep_count = max_reports_ / 2;
  error_reports_.resize(keep_count);

  // 重建签名映射
  error_signature_map_.clear();
  for (size_t i = 0; i < error_reports_.size(); i++) {
    std::string signature =
        GenerateErrorSignature(error_reports_[i].status, error_reports_[i].type,
                               error_reports_[i].context);
    error_signature_map_[signature] = i;
  }
}

// ErrorHandlerManager实现
ErrorHandlerManager &ErrorHandlerManager::Instance() {
  static ErrorHandlerManager instance;
  return instance;
}

ErrorHandlerManager::ErrorHandlerManager() {
  handler_ = std::make_unique<DefaultErrorHandler>();
}

void ErrorHandlerManager::SetHandler(std::unique_ptr<ErrorHandler> handler) {
  std::lock_guard<std::mutex> lock(mutex_);
  handler_ = std::move(handler);
}

ErrorHandler *ErrorHandlerManager::GetHandler() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return handler_.get();
}

void ErrorHandlerManager::ReportError(const Status &status, ErrorType type,
                                      ErrorSeverity severity,
                                      const ErrorContext &context) {
  // 锁内只取指针，锁外调用：HandleError 可能触发用户回调，
  // 跨用户代码持锁容易自死锁
  ErrorHandler* handler = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    handler = handler_.get();
  }
  if (handler) {
    handler->HandleError(status, type, severity, context);
  }
}

// ErrorUtils实现
ErrorType ErrorUtils::InferErrorType(const Status &status) {
  switch (status.code()) {
  case StatusCode::kOk:
    return ErrorType::SystemError; // 不应该发生
  case StatusCode::kNotFound:
  case StatusCode::kDeleted:
    return ErrorType::NotFoundError;
  case StatusCode::kCorruption:
    return ErrorType::CorruptionError;
  case StatusCode::kNotSupported:
    return ErrorType::NotSupportedError;
  case StatusCode::kInvalidArgument:
    return ErrorType::InvalidArgumentError;
  case StatusCode::kIOError:
    return ErrorType::IOError;
  default:
    return ErrorType::SystemError;
  }
}

ErrorSeverity ErrorUtils::InferSeverity(ErrorType type, const Status &status) {
  // 基于错误类型推断严重程度，status可用于更精细的判断
  switch (type) {
  case ErrorType::CorruptionError:
    return ErrorSeverity::Critical;
  case ErrorType::IOError:
  case ErrorType::MemoryError:
  case ErrorType::RecoveryError:
    // 可以基于status的具体内容进一步判断严重程度
    return status.IsAborted() ? ErrorSeverity::Critical : ErrorSeverity::High;
  case ErrorType::CompactionError:
  case ErrorType::ConcurrencyError:
    return ErrorSeverity::Medium;
  case ErrorType::NotFoundError:
  case ErrorType::InvalidArgumentError:
    return ErrorSeverity::Low;
  default:
    return ErrorSeverity::Medium;
  }
}

ErrorContext ErrorUtils::CreateContext(const std::string &operation,
                                       const std::string &component,
                                       const std::string &additional_info) {
  return ErrorContext(operation, component, additional_info);
}

std::string ErrorUtils::FormatErrorReport(const ErrorReport &report) {
  std::ostringstream oss;

  oss << "Error Report:\n";
  oss << "  Type: " << ErrorTypeToString(report.type) << "\n";
  oss << "  Severity: " << ErrorSeverityToString(report.severity) << "\n";
  oss << "  Status: " << report.status.ToString() << "\n";
  oss << "  Operation: " << report.context.operation << "\n";
  oss << "  Component: " << report.context.component << "\n";
  oss << "  Additional Info: " << report.context.additional_info << "\n";
  oss << "  Occurrence Count: " << report.occurrence_count << "\n";
  oss << "  First Occurrence: " << report.first_occurrence << "\n";
  oss << "  Last Occurrence: " << report.last_occurrence << "\n";

  return oss.str();
}

std::string ErrorUtils::ErrorTypeToString(ErrorType type) {
  switch (type) {
  case ErrorType::IOError:
    return "IOError";
  case ErrorType::CorruptionError:
    return "CorruptionError";
  case ErrorType::NotSupportedError:
    return "NotSupportedError";
  case ErrorType::InvalidArgumentError:
    return "InvalidArgumentError";
  case ErrorType::NotFoundError:
    return "NotFoundError";
  case ErrorType::CompactionError:
    return "CompactionError";
  case ErrorType::MemoryError:
    return "MemoryError";
  case ErrorType::ConcurrencyError:
    return "ConcurrencyError";
  case ErrorType::RecoveryError:
    return "RecoveryError";
  case ErrorType::SystemError:
    return "SystemError";
  default:
    return "Unknown";
  }
}

std::string ErrorUtils::ErrorSeverityToString(ErrorSeverity severity) {
  switch (severity) {
  case ErrorSeverity::Low:
    return "Low";
  case ErrorSeverity::Medium:
    return "Medium";
  case ErrorSeverity::High:
    return "High";
  case ErrorSeverity::Critical:
    return "Critical";
  default:
    return "Unknown";
  }
}

// ErrorRecoveryManager实现
ErrorRecoveryManager &ErrorRecoveryManager::Instance() {
  static ErrorRecoveryManager instance;
  return instance;
}

void ErrorRecoveryManager::RegisterCleanupCallback(
    const std::string &component_name, ComponentCleanupCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  cleanup_callbacks_[component_name] = callback;
  LOG_DEBUG << "Registered cleanup callback for component: " << component_name;
}

void ErrorRecoveryManager::UnregisterCleanupCallback(
    const std::string &component_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  cleanup_callbacks_.erase(component_name);
}

void ErrorRecoveryManager::RegisterRecoveryCallback(
    ErrorType error_type, ErrorRecoveryCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  recovery_callbacks_[error_type] = callback;
  LOG_DEBUG << "Registered recovery callback for error type: "
            << ErrorUtils::ErrorTypeToString(error_type);
}

void ErrorRecoveryManager::UnregisterRecoveryCallback(ErrorType error_type) {
  std::lock_guard<std::mutex> lock(mutex_);
  recovery_callbacks_.erase(error_type);
}

void ErrorRecoveryManager::RegisterGlobalRecoveryCallback(
    ErrorRecoveryCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  global_callbacks_.push_back(callback);
  LOG_DEBUG << "Registered global recovery callback";
}

Status ErrorRecoveryManager::TryRecover(const ErrorReport &error) {
  std::lock_guard<std::mutex> lock(mutex_);

  LOG_INFO << "Attempting error recovery for " << error.context.operation
           << " in " << error.context.component
           << " (error type: " << ErrorUtils::ErrorTypeToString(error.type)
           << ")";

  Status recovery_status =
      Status::NotSupported("No recovery callback available");
  bool recovery_attempted = false;

  // 1. 尝试组件特定的清理回调
  auto cleanup_it = cleanup_callbacks_.find(error.context.component);
  if (cleanup_it != cleanup_callbacks_.end()) {
    LOG_DEBUG << "Calling component cleanup for: " << error.context.component;
    recovery_status =
        cleanup_it->second(error.context.component, error.context.operation);
    recovery_attempted = true;

    if (recovery_status.ok()) {
      LOG_INFO << "Component cleanup successful for: "
               << error.context.component;
      return recovery_status;
    } else {
      LOG_WARN << "Component cleanup failed: " << recovery_status.ToString();
    }
  }

  // 2. 尝试错误类型特定的恢复回调
  auto recovery_it = recovery_callbacks_.find(error.type);
  if (recovery_it != recovery_callbacks_.end()) {
    LOG_DEBUG << "Calling error type recovery for: "
              << ErrorUtils::ErrorTypeToString(error.type);
    recovery_status = recovery_it->second(error);
    recovery_attempted = true;

    if (recovery_status.ok()) {
      LOG_INFO << "Error type recovery successful for: "
               << ErrorUtils::ErrorTypeToString(error.type);
      return recovery_status;
    } else {
      LOG_WARN << "Error type recovery failed: " << recovery_status.ToString();
    }
  }

  // 3. 尝试全局恢复回调
  for (const auto &global_callback : global_callbacks_) {
    LOG_DEBUG << "Calling global recovery callback";
    Status global_status = global_callback(error);
    recovery_attempted = true;

    if (global_status.ok()) {
      LOG_INFO << "Global recovery successful";
      return global_status;
    } else {
      LOG_WARN << "Global recovery failed: " << global_status.ToString();
      recovery_status = global_status; // 保存最后一个错误
    }
  }

  if (!recovery_attempted) {
    LOG_INFO << "No recovery callbacks registered for error in component: "
             << error.context.component
             << " (type: " << ErrorUtils::ErrorTypeToString(error.type) << ")";
    return Status::NotSupported("No recovery callbacks available");
  }

  return recovery_status;
}

Status ErrorRecoveryManager::CleanupComponent(const std::string &component_name,
                                              const std::string &operation) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = cleanup_callbacks_.find(component_name);
  if (it != cleanup_callbacks_.end()) {
    LOG_DEBUG << "Executing cleanup for component: " << component_name;
    return it->second(component_name, operation);
  }

  return Status::NotFound("No cleanup callback registered for component: " +
                          component_name);
}

std::vector<std::string> ErrorRecoveryManager::GetRegisteredComponents() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::string> components;
  for (const auto &pair : cleanup_callbacks_) {
    components.push_back(pair.first);
  }
  return components;
}

std::vector<ErrorType> ErrorRecoveryManager::GetSupportedErrorTypes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<ErrorType> types;
  for (const auto &pair : recovery_callbacks_) {
    types.push_back(pair.first);
  }
  return types;
}

void ErrorRecoveryManager::ClearAllCallbacks() {
  std::lock_guard<std::mutex> lock(mutex_);
  cleanup_callbacks_.clear();
  recovery_callbacks_.clear();
  global_callbacks_.clear();
  LOG_DEBUG << "Cleared all recovery callbacks";
}

// RetryHelper实现
RetryHelper::RetryHelper(int max_retries, int initial_delay_ms)
    : max_retries_(max_retries), initial_delay_ms_(initial_delay_ms),
      backoff_multiplier_(2.0), max_delay_ms_(30000) {}

void RetryHelper::SetBackoffStrategy(double backoff_multiplier,
                                     int max_delay_ms) {
  backoff_multiplier_ = backoff_multiplier;
  max_delay_ms_ = max_delay_ms;
}

Status RetryHelper::ExecuteWithRetry(RetryOperationCallback operation,
                                     const std::string &operation_name) {
  if (!operation) {
    return Status::InvalidArgument("Operation callback is null");
  }

  std::string op_name =
      operation_name.empty() ? "unknown operation" : operation_name;
  LOG_DEBUG << "Starting retry execution for: " << op_name
            << " (max attempts: " << max_retries_ << ")";

  Status last_status;

  for (int attempt = 1; attempt <= max_retries_; attempt++) {
    if (attempt > 1) {
      int delay = CalculateDelay(attempt - 1);
      LOG_DEBUG << "Retry attempt " << attempt << " of " << max_retries_
                << " after " << delay << "ms delay for: " << op_name;
      std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    } else {
      LOG_DEBUG << "Initial attempt " << attempt << " of " << max_retries_
                << " for: " << op_name;
    }

    // 执行操作
    Status status = operation();

    if (status.ok()) {
      if (attempt > 1) {
        LOG_INFO << "Retry successful on attempt " << attempt
                 << " for: " << op_name;
      }
      return Status::OK();
    }

    last_status = status;
    LOG_WARN << "Attempt " << attempt << " failed for " << op_name << ": "
             << status.ToString();

    // 检查是否应该停止重试
    if (!IsRetriable(status)) {
      LOG_ERROR
          << "Non-retriable error encountered, stopping retry attempts for "
          << op_name << ": " << status.ToString();
      break;
    }
  }

  LOG_ERROR << "All retry attempts failed for: " << op_name
            << " - Last error: " << last_status.ToString();
  return Status::Aborted("All retry attempts failed: " +
                         last_status.ToString());
}

bool RetryHelper::IsRetriable(const Status &status) {
  // 判断错误是否适合重试
  if (status.ok()) {
    return false; // 成功不需要重试
  }

  // 以下错误类型不适合重试
  if (status.IsCorruption() || status.IsInvalidArgument() ||
      status.IsNotSupported()) {
    return false;
  }

  // 以下错误类型适合重试
  if (status.IsIOError() || status.IsAborted() || // 某些abort是临时性的
      status.code() == StatusCode::kBusy) {
    return true;
  }

  // 默认适合重试
  return true;
}

int RetryHelper::CalculateDelay(int attempt) const {
  // 指数退避算法
  int delay = initial_delay_ms_;
  for (int i = 0; i < attempt; i++) {
    delay = static_cast<int>(delay * backoff_multiplier_);
    if (delay > max_delay_ms_) {
      delay = max_delay_ms_;
      break;
    }
  }
  return delay;
}

} // namespace lrdb