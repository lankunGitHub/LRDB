#pragma once

#include <chrono>
#include <vector>
#include <algorithm>
#include <string>

#include "lrdb/concurrency/mvcc.h"

namespace lrdb {

// 轻量快照接口（用于对接外部 ReadOptions::snapshot 等）
// SnapshotSequence 语义：表示 LSM 的可见上界（包含该序号），用于只读一致性
// 视图；事务场景下 ReadView 以 TransactionID 为主，提交进入 LSM 后才使用 SnapshotSequence。
class Snapshot {
public:
    virtual ~Snapshot() = default;
    virtual SequenceNumber GetSequenceNumber() const = 0;
    virtual uint64_t GetID() const = 0;
    virtual bool IsValid() const = 0;
};

// 默认实现（可选）
class DefaultSnapshot final : public Snapshot {
public:
    explicit DefaultSnapshot(SequenceNumber seq, uint64_t id = 0)
        : seq_(seq), id_(id), valid_(true) {}
    SequenceNumber GetSequenceNumber() const override { return seq_; }
    uint64_t GetID() const override { return id_; }
    bool IsValid() const override { return valid_; }
    void Invalidate() { valid_ = false; }
private:
    SequenceNumber seq_;
    uint64_t id_;
    bool valid_;
};

// ReadView 策略：RC 使用“当前提交可见”；RR 固定一个事务ID视图（活跃事务集合）
enum class ReadViewPolicy {
    kReadCommitted,
    kRepeatableRead,
};

// 版本可见性以事务ID为准：
// - 在提交到 LSM 之前，版本仅带有创建者 txn_id 与其状态（Active/Committed/Aborted）。
// - RC：读时只要版本是 Committed（或创建者是自己）即可见。
// - RR：在创建 ReadView 时冻结一个活跃事务集合 active_txn_ids_ 以及上界 up_limit_txn_id_（当时的 next_txn_id）。
//        版本可见当且仅当：
//        1) 创建者是自己，或
//        2) 版本 Committed 且 creator_txn_id < up_limit_txn_id_ 且 creator_txn_id 不在 active_txn_ids_ 中。
class ReadView {
public:
    ReadView(ReadViewPolicy policy,
             TransactionID my_txn_id,
             TransactionID up_limit_txn_id,
             std::vector<TransactionID> active_txn_ids)
        : policy_(policy), my_txn_id_(my_txn_id), up_limit_txn_id_(up_limit_txn_id),
          active_txn_ids_(std::move(active_txn_ids)), created_time_(std::chrono::system_clock::now()) {
        std::sort(active_txn_ids_.begin(), active_txn_ids_.end());
        active_txn_ids_.erase(std::unique(active_txn_ids_.begin(), active_txn_ids_.end()), active_txn_ids_.end());
    }

    ReadViewPolicy Policy() const { return policy_; }
    TransactionID MyTxn() const { return my_txn_id_; }
    TransactionID UpLimit() const { return up_limit_txn_id_; }
    const std::vector<TransactionID>& ActiveTxnIDs() const { return active_txn_ids_; }
    std::chrono::time_point<std::chrono::system_clock> CreatedAt() const { return created_time_; }

    // 可见性判断（基于事务ID与状态）。
    // 参数：creator_txn_id 版本创建者；is_committed 是否已提交；is_aborted 是否已回滚。
    bool IsVisible(TransactionID creator_txn_id, bool is_committed, bool is_aborted) const {
        if (creator_txn_id == my_txn_id_) {
            return !is_aborted; // 自己的未提交版本在未回滚前可见
        }
        if (is_aborted) {
            return false;
        }

        if (policy_ == ReadViewPolicy::kReadCommitted) {
            return is_committed;
        }

        // Repeatable Read 基于冻结的活跃事务集合
        if (!is_committed) {
            return false; // 其他事务未提交的版本不可见
        }
        if (creator_txn_id >= up_limit_txn_id_) {
            return false;
        }
        return !IsTxnActiveAtSnapshot(creator_txn_id);
    }

private:
    bool IsTxnActiveAtSnapshot(TransactionID txn_id) const {
        return std::binary_search(active_txn_ids_.begin(), active_txn_ids_.end(), txn_id);
    }

private:
    ReadViewPolicy policy_;
    TransactionID my_txn_id_;
    TransactionID up_limit_txn_id_;
    std::vector<TransactionID> active_txn_ids_;
    std::chrono::time_point<std::chrono::system_clock> created_time_;
};

}  // namespace lrdb


