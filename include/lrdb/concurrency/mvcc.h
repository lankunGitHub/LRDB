#pragma once

#include <cstdint>

namespace lrdb {

// 基础序号类型
// TransactionID: 标识未提交版本归属（MVCC 可见性依据）；
// SnapshotSequence: 提交后写入 WAL/LSM 的顺序（读快照可见性依据），与 TransactionID 无因果关系。
using SequenceNumber = uint64_t;      // 通用序列号（WAL/LSM）
using SnapshotSequence = uint64_t;    // 提交后的可见性/写入 LSM 的序号
using TransactionID = uint64_t;       // 事务ID（标识未提交版本归属）

// 隔离级别
enum class IsolationLevel {
    kReadUncommitted,
    kReadCommitted,
    kRepeatableRead,
    kSerializable
};

// 锁类型（当前使用 S/X）
enum class LockType {
    kSharedLock = 0,
    kExclusiveLock = 1,
};

// 事务状态（用于版本链标注）
enum class TransactionState {
    kActive,
    kCommitted,
    kAborted,
    kPrepared
};

} // namespace lrdb


