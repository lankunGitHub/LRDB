#include "lrdb/concurrency/tx_manager.h"
#include "lrdb/wal/wal.h"
#include "lrdb/storage/lsm_tree.h"

namespace lrdb {

ReadView TxManager::BuildReadView(TransactionID my_txn, const TxnOptions& opts) {
    if (opts.isolation == IsolationLevel::kReadCommitted) {
        // RC：活跃集为空，上界为当前 next_txn（不限制）
        return ReadView(ReadViewPolicy::kReadCommitted, my_txn, txn_gen_.Current() + 1, {});
    }
    // RR：冻结活跃集合与上界
    std::vector<TransactionID> active;
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        active.assign(active_txns_.begin(), active_txns_.end());
    }
    return ReadView(ReadViewPolicy::kRepeatableRead, my_txn, txn_gen_.Current() + 1, std::move(active));
}

std::unique_ptr<Transaction> TxManager::Begin(const TxnOptions& opts) {
    TransactionID id = txn_gen_.Next();
    {
        std::unique_lock<std::shared_mutex> lk(mu_);
        active_txns_.insert(id);
    }
    ReadView view = BuildReadView(id, opts);
    SnapshotSequence lsm_read_seq = snap_gen_.Current();
    return std::make_unique<TransactionImpl>(this, id, opts, view, lsm_read_seq,
                                             &lock_mgr_, &vcm_, wal_, lsm_, cmp_);
}

std::unique_ptr<Transaction> TxManager::BeginReadOnlyWithLSMSnapshot(const TxnOptions& opts,
                                                                     SnapshotSequence lsm_snapshot_seq) {
    // 为只读事务分配一个临时 txn_id=0 的 ReadView（RC/RR 均允许）
    ReadView view = BuildReadView(0, opts);
    return std::make_unique<TransactionImpl>(this, 0, opts, view, lsm_snapshot_seq,
                                             &lock_mgr_, &vcm_, wal_, lsm_, cmp_);
}

SnapshotSequence TxManager::PrepareCommit(TransactionID txn_id, bool allocate_new_snapshot) {
    (void)txn_id;
    if (allocate_new_snapshot) {
        return snap_gen_.Next();
    }
    return snap_gen_.Current();
}

void TxManager::OnCommitted(TransactionID txn_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    active_txns_.erase(txn_id);
}

void TxManager::OnAborted(TransactionID txn_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    active_txns_.erase(txn_id);
}

Status TxManager::Put(const Slice& key, const Slice& value) {
    TxnOptions opts; // 默认RC
    auto tx = Begin(opts);
    Status s = tx->Put(key, value);
    if (!s.ok()) { tx->Rollback(); return s; }
    return tx->Commit(false);
}

Status TxManager::Delete(const Slice& key) {
    TxnOptions opts; // 默认RC
    auto tx = Begin(opts);
    Status s = tx->Delete(key);
    if (!s.ok()) { tx->Rollback(); return s; }
    return tx->Commit(false);
}

Status TxManager::Get(const Slice& key, std::string* value) {
    // 非事务读：构造 RC 读视图
    TxnOptions opts; opts.isolation = IsolationLevel::kReadCommitted;
    ReadView view = BuildReadView(0, opts);
    std::string v;
    bool found = false;
    Status s = vcm_.GetVisible(key.ToString(), view, &v, &found);
    if (!s.ok()) return s;
    if (found) { *value = std::move(v); return Status::OK(); }
    if (!lsm_) return Status::NotFound("not found");
    return lsm_->Get(key, value, snap_gen_.Current());
}

Status TxManager::FlushCommittedToLSM(size_t max_items) {
    if (!lsm_ || !wal_) return Status::InvalidArgument("components missing");

    auto items = vcm_.PickCommittedNotFlushed(max_items);
    if (items.empty()) return Status::OK();

    std::vector<std::pair<std::string, SnapshotSequence>> mark;
    mark.reserve(items.size());

    for (const auto& kv : items) {
        const std::string& k = kv.first;
        const VersionRecord& rec = kv.second;
        Status s;
        if (rec.is_delete) s = lsm_->Delete(Slice(k), rec.commit_snapshot);
        else s = lsm_->Put(Slice(k), Slice(rec.value), rec.commit_snapshot);
        if (!s.ok()) return s;
        mark.emplace_back(k, rec.commit_snapshot);
    }

    vcm_.MarkFlushed(mark);
    return Status::OK();
}

void TxManager::RunGC() {
    // 基于最老活跃 ReadView 的安全边界进行 GC
    std::vector<TransactionID> active;
    TransactionID up_limit = 0;
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        active.assign(active_txns_.begin(), active_txns_.end());
        up_limit = txn_gen_.Current() + 1; // 下一可分配 txn_id 作为上界
    }
    vcm_.GarbageCollect(active, up_limit);
}


} // namespace lrdb


