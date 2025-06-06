// Status状态码 - 完整头文件实现

#pragma once

#include <iostream>
#include <memory>
#include <string>

namespace lrdb {

// 状态码枚举
enum class StatusCode : int {
  kOk = 0,
  kNotFound = 1,
  kCorruption = 2,
  kNotSupported = 3,
  kInvalidArgument = 4,
  kIOError = 5,
  kMergeInProgress = 6,
  kIncomplete = 7,
  kShutdownInProgress = 8,
  kTimedOut = 9,
  kAborted = 10,
  kBusy = 11,
  kExpired = 12,
  kTryAgain = 13,
  kCompactionTooLarge = 14,
  kColumnFamilyDropped = 15,
  // 内部信号：读到可见的删除标记（墓碑），用于阻断向更老层的回退查找
  kDeleted = 16,
  kMaxCode = 17
};

// 状态类 - 完整头文件实现
class Status {
public:
  // 构造函数
  Status() noexcept : code_(StatusCode::kOk), message_(nullptr) {}

  Status(StatusCode code) noexcept : code_(code), message_(nullptr) {}

  Status(StatusCode code, const std::string &msg) : code_(code) {
    if (!msg.empty()) {
      message_ = std::make_unique<std::string>(msg);
    }
  }

  // 拷贝构造函数
  Status(const Status &other) : code_(other.code_) {
    if (other.message_) {
      message_ = std::make_unique<std::string>(*other.message_);
    }
  }

  // 移动构造函数
  Status(Status &&other) noexcept
      : code_(other.code_), message_(std::move(other.message_)) {
    other.code_ = StatusCode::kOk;
  }

  // 拷贝赋值操作符
  Status &operator=(const Status &other) {
    if (this != &other) {
      code_ = other.code_;
      if (other.message_) {
        message_ = std::make_unique<std::string>(*other.message_);
      } else {
        message_.reset();
      }
    }
    return *this;
  }

  // 移动赋值操作符
  Status &operator=(Status &&other) noexcept {
    if (this != &other) {
      code_ = other.code_;
      message_ = std::move(other.message_);
      other.code_ = StatusCode::kOk;
    }
    return *this;
  }

  // 析构函数
  ~Status() = default;

  // 状态检查
  bool ok() const noexcept { return code_ == StatusCode::kOk; }
  bool IsOk() const noexcept { return ok(); }
  bool IsNotFound() const noexcept { return code_ == StatusCode::kNotFound; }
  bool IsCorruption() const noexcept {
    return code_ == StatusCode::kCorruption;
  }
  bool IsNotSupported() const noexcept {
    return code_ == StatusCode::kNotSupported;
  }
  bool IsInvalidArgument() const noexcept {
    return code_ == StatusCode::kInvalidArgument;
  }
  bool IsIOError() const noexcept { return code_ == StatusCode::kIOError; }
  bool IsMergeInProgress() const noexcept {
    return code_ == StatusCode::kMergeInProgress;
  }
  bool IsIncomplete() const noexcept {
    return code_ == StatusCode::kIncomplete;
  }
  bool IsShutdownInProgress() const noexcept {
    return code_ == StatusCode::kShutdownInProgress;
  }
  bool IsTimedOut() const noexcept { return code_ == StatusCode::kTimedOut; }
  bool IsAborted() const noexcept { return code_ == StatusCode::kAborted; }
  bool IsBusy() const noexcept { return code_ == StatusCode::kBusy; }
  bool IsExpired() const noexcept { return code_ == StatusCode::kExpired; }
  bool IsTryAgain() const noexcept { return code_ == StatusCode::kTryAgain; }
  bool IsDeleted() const noexcept { return code_ == StatusCode::kDeleted; }

  // 获取信息
  StatusCode code() const noexcept { return code_; }

  const std::string &ToString() const {
    if (message_) {
      return *message_;
    }
    return GetDefaultMessage();
  }

  const char *getState() const {
    if (message_) {
      return message_->c_str();
    }
    return GetDefaultMessage().c_str();
  }

  // 比较操作符
  bool operator==(const Status &other) const noexcept {
    return code_ == other.code_;
  }

  bool operator!=(const Status &other) const noexcept {
    return !(*this == other);
  }

  // 静态创建方法
  static Status OK() { return Status(); }

  static Status NotFound(const std::string &msg = "") {
    return Status(StatusCode::kNotFound, msg);
  }

  static Status Corruption(const std::string &msg = "") {
    return Status(StatusCode::kCorruption, msg);
  }

  static Status NotSupported(const std::string &msg = "") {
    return Status(StatusCode::kNotSupported, msg);
  }

  static Status InvalidArgument(const std::string &msg = "") {
    return Status(StatusCode::kInvalidArgument, msg);
  }

  static Status IOError(const std::string &msg = "") {
    return Status(StatusCode::kIOError, msg);
  }

  static Status IOError(const std::string &msg, const std::string &details) {
    return Status(StatusCode::kIOError, msg + ": " + details);
  }

  static Status MergerInProgress(const std::string &msg = "") {
    return Status(StatusCode::kMergeInProgress, msg);
  }

  static Status Incomplete(const std::string &msg = "") {
    return Status(StatusCode::kIncomplete, msg);
  }

  static Status DeadLock(const std::string &msg = "") {
    return Status(StatusCode::kAborted, msg); // 使用Aborted代表死锁
  }

  static Status ShutdownInProgress(const std::string &msg = "") {
    return Status(StatusCode::kShutdownInProgress, msg);
  }

  static Status TimedOut(const std::string &msg = "") {
    return Status(StatusCode::kTimedOut, msg);
  }

  static Status Aborted(const std::string &msg = "") {
    return Status(StatusCode::kAborted, msg);
  }

  static Status Busy(const std::string &msg = "") {
    return Status(StatusCode::kBusy, msg);
  }

  static Status Expired(const std::string &msg = "") {
    return Status(StatusCode::kExpired, msg);
  }

  static Status TryAgain(const std::string &msg = "") {
    return Status(StatusCode::kTryAgain, msg);
  }

  static Status Deleted(const std::string &msg = "") {
    return Status(StatusCode::kDeleted, msg);
  }

private:
  StatusCode code_;
  std::unique_ptr<std::string> message_;

  const std::string &GetDefaultMessage() const {
    static const std::string kOkMessage = "OK";
    static const std::string kNotFoundMessage = "NotFound";
    static const std::string kCorruptionMessage = "Corruption";
    static const std::string kNotSupportedMessage = "NotSupported";
    static const std::string kInvalidArgumentMessage = "InvalidArgument";
    static const std::string kIOErrorMessage = "IOError";
    static const std::string kIncompleteMessage = "Incomplete";
    static const std::string kShutdownInProgressMessage = "ShutdownInProgress";
    static const std::string kTimedOutMessage = "TimedOut";
    static const std::string kAbortedMessage = "Aborted";
    static const std::string kBusyMessage = "Busy";
    static const std::string kExpiredMessage = "Expired";
    static const std::string kTryAgainMessage = "TryAgain";
    static const std::string kDeletedMessage = "Deleted";
    static const std::string kUnknownMessage = "Unknown";

    switch (code_) {
    case StatusCode::kOk:
      return kOkMessage;
    case StatusCode::kNotFound:
      return kNotFoundMessage;
    case StatusCode::kCorruption:
      return kCorruptionMessage;
    case StatusCode::kNotSupported:
      return kNotSupportedMessage;
    case StatusCode::kInvalidArgument:
      return kInvalidArgumentMessage;
    case StatusCode::kIOError:
      return kIOErrorMessage;
    case StatusCode::kIncomplete:
      return kIncompleteMessage;
    case StatusCode::kShutdownInProgress:
      return kShutdownInProgressMessage;
    case StatusCode::kTimedOut:
      return kTimedOutMessage;
    case StatusCode::kAborted:
      return kAbortedMessage;
    case StatusCode::kBusy:
      return kBusyMessage;
    case StatusCode::kExpired:
      return kExpiredMessage;
    case StatusCode::kTryAgain:
      return kTryAgainMessage;
    case StatusCode::kDeleted:
      return kDeletedMessage;
    default:
      return kUnknownMessage;
    }
  }
};

// StatusCode输出运算符
inline std::ostream &operator<<(std::ostream &os, StatusCode code) {
  switch (code) {
  case StatusCode::kOk:
    return os << "OK";
  case StatusCode::kNotFound:
    return os << "NotFound";
  case StatusCode::kCorruption:
    return os << "Corruption";
  case StatusCode::kNotSupported:
    return os << "NotSupported";
  case StatusCode::kInvalidArgument:
    return os << "InvalidArgument";
  case StatusCode::kIOError:
    return os << "IOError";
  case StatusCode::kMergeInProgress:
    return os << "MergeInProgress";
  case StatusCode::kIncomplete:
    return os << "Incomplete";
  case StatusCode::kShutdownInProgress:
    return os << "ShutdownInProgress";
  case StatusCode::kTimedOut:
    return os << "TimedOut";
  case StatusCode::kAborted:
    return os << "Aborted";
  case StatusCode::kBusy:
    return os << "Busy";
  case StatusCode::kExpired:
    return os << "Expired";
  case StatusCode::kTryAgain:
    return os << "TryAgain";
  case StatusCode::kCompactionTooLarge:
    return os << "CompactionTooLarge";
  case StatusCode::kColumnFamilyDropped:
    return os << "ColumnFamilyDropped";
  case StatusCode::kDeleted:
    return os << "Deleted";
  default:
    return os << "Unknown(" << static_cast<int>(code) << ")";
  }
}

} // namespace lrdb