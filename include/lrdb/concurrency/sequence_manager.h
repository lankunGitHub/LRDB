#pragma once

#include <atomic>

#include "lrdb/concurrency/mvcc.h"

namespace lrdb {

// 独立的两类序号生成器，互不关联
// - TxnID: 标识事务与其未提交版本归属，仅出现在 MVCC 可见性计算与版本链中；
// - SnapshotSequence: 提交后用于 WAL/LSM 的排序与可见性，不与 TxnID 建立映射或因果关系；
//   在 WAL 重放后由 TxManager 推进到 recovered+1，保证后续提交的单调性。

// 事务ID生成器：仅用于标识事务与其未提交版本归属
class TxnIDGenerator {
public:
    TxnIDGenerator() : next_id_(1) {}

    TransactionID Next() {
        return next_id_.fetch_add(1, std::memory_order_acq_rel);
    }

    TransactionID Current() const {
        TransactionID v = next_id_.load(std::memory_order_acquire);
        return v > 0 ? v - 1 : 0;
    }

    void Reset(TransactionID start_from = 1) {
        next_id_.store(start_from, std::memory_order_release);
    }

private:
    std::atomic<TransactionID> next_id_;
};

// 快照序列号生成器：提交时为记录分配的可见性序列，用于 LSM 写入顺序
class SnapshotSequenceGenerator {
public:
    SnapshotSequenceGenerator() : next_seq_(1) {}

    SnapshotSequence Next() {
        return next_seq_.fetch_add(1, std::memory_order_acq_rel);
    }

    SnapshotSequence Current() const {
        SnapshotSequence v = next_seq_.load(std::memory_order_acquire);
        return v > 0 ? v - 1 : 0;
    }

    void Reset(SnapshotSequence start_from = 1) {
        next_seq_.store(start_from, std::memory_order_release);
    }

private:
    std::atomic<SnapshotSequence> next_seq_;
};

}  // namespace lrdb


