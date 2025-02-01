#include "lrdb/concurrency/transaction.h"
#include "lrdb/concurrency/lock_manager.h"
#include "lrdb/concurrency/version_chain_manager.h"
#include "lrdb/concurrency/sequence_manager.h"
#include "lrdb/storage/lsm_tree.h"
#include "lrdb/concurrency/tx_manager.h"
#include "lrdb/wal/wal.h"
#include "lrdb/util/logging.h"
#include <map>
#include <set>

namespace lrdb {

TransactionImpl::TransactionImpl(TxManager* mgr,
                                 TransactionID txn_id,
                                 const TxnOptions& opts,
                                 const ReadView& view,
                                 SnapshotSequence lsm_read_seq,
                                 LockManager* lock_mgr,
                                 VersionChainManager* vcm,
                                 WALManager* wal,
                                 LSMTree* lsm,
                                 const Comparator* cmp)
    : mgr_(mgr), txn_id_(txn_id), opts_(opts), view_(view), lsm_read_seq_(lsm_read_seq),
      lock_mgr_(lock_mgr), vcm_(vcm), wal_(wal), lsm_(lsm), cmp_(cmp) {}

TransactionImpl::~TransactionImpl() {
    if (lock_mgr_) lock_mgr_->ReleaseAll(txn_id_);
}

Status TransactionImpl::Put(const Slice& key, const Slice& value) {
    if (!lock_mgr_ || !vcm_) return Status::InvalidArgument("TX components missing");
    std::string k = key.ToString();
    Status s = lock_mgr_->Acquire(k, txn_id_, LockType::kExclusiveLock, opts_.lock_timeout);
    if (!s.ok()) return s;
    touched_keys_.insert(k);
    return vcm_->PutActive(k, value.ToString(), txn_id_);
}

Status TransactionImpl::Delete(const Slice& key) {
    if (!lock_mgr_ || !vcm_) return Status::InvalidArgument("TX components missing");
    std::string k = key.ToString();
    Status s = lock_mgr_->Acquire(k, txn_id_, LockType::kExclusiveLock, opts_.lock_timeout);
    if (!s.ok()) return s;
    touched_keys_.insert(k);
    return vcm_->DeleteActive(k, txn_id_);
}

Status TransactionImpl::Get(const Slice& key, std::string* value) {
    if (!value) return Status::InvalidArgument("null value");
    if (!vcm_) return Status::InvalidArgument("TX components missing");

    std::string v;
    bool found = false;
    // 先看本事务或可见提交版本
    Status s = vcm_->GetVisible(key.ToString(), view_, &v, &found);
    if (!s.ok()) return s;
    if (found) {
        *value = std::move(v);
        if (opts_.enable_rr_validation) {
            read_set_[key.ToString()] = ReadEntry{true, *value};
        }
        return Status::OK();
    }

    // 再查 LSM（使用 lsm_read_seq_）
    if (!lsm_) return Status::NotFound("not found");
    Status ls = lsm_->Get(key, value, lsm_read_seq_);
    if (opts_.enable_rr_validation && ls.ok()) {
        read_set_[key.ToString()] = ReadEntry{true, *value};
    }
    return ls;
}

Status TransactionImpl::GetForUpdate(const Slice& key, std::string* value) {
    if (!lock_mgr_) return Status::InvalidArgument("TX components missing");
    Status s = lock_mgr_->Acquire(key.ToString(), txn_id_, LockType::kExclusiveLock, opts_.lock_timeout);
    if (!s.ok()) return s;
    return Get(key, value);
}

Status TransactionImpl::Commit(bool allocate_new_snapshot) {
    // RR 读集验证（可选）：确保可见性未发生违背（基于当前 ReadView）
    if (opts_.enable_rr_validation) {
        for (const auto& kv : read_set_) {
            const std::string& k = kv.first;
            const ReadEntry& re = kv.second;
            std::string cur;
            bool cur_found = false;
            Status s = vcm_->GetVisible(k, view_, &cur, &cur_found);
            if (!s.ok()) return s;
            if (re.found != cur_found) return Status::Aborted("RR validation failed");
            if (re.found && cur != re.value) return Status::Aborted("RR validation failed");
        }
    }
    if (!wal_ || !vcm_) return Status::InvalidArgument("TX components missing");

    // 提交：由 TxManager 分配提交序号（可共享或递增）
    // 若调用方未显式指定，使用选项开关决定是否分配新快照
    bool use_new = allocate_new_snapshot || opts_.new_snapshot_on_commit;
    SnapshotSequence commit_seq = mgr_ ? mgr_->PrepareCommit(txn_id_, use_new) : 0;

    // 将 touched_keys_ 的最新本事务 Active 记录写入 WAL（普通 Put/Delete）
    for (const auto& k : touched_keys_) {
        VersionRecord rec;
        bool has = vcm_->GetLatestActiveForTxn(k, txn_id_, &rec);
        if (!has) continue; // 可能被覆盖或未产生有效记录
        Status s;
        if (rec.is_delete) {
            s = wal_->WriteDelete(mgr_->cf_id(), Slice(k), commit_seq);
        } else {
            s = wal_->WritePut(mgr_->cf_id(), Slice(k), Slice(rec.value), commit_seq);
        }
        if (!s.ok()) return s;
    }

    // 同步 WAL
    Status sync_st = wal_->Sync();
    if (!sync_st.ok()) return sync_st;

    // 标注提交（设置 commit_snapshot）并从活动事务集中移除
    vcm_->MarkTransactionCommitted(txn_id_, commit_seq);
    if (mgr_) mgr_->OnCommitted(txn_id_);
    if (lock_mgr_) lock_mgr_->ReleaseAll(txn_id_);
    touched_keys_.clear();
    return Status::OK();
}

Status TransactionImpl::Rollback() {
    if (!vcm_) return Status::InvalidArgument("TX components missing");
    vcm_->MarkTransactionAborted(txn_id_);
    if (mgr_) mgr_->OnAborted(txn_id_);
    if (lock_mgr_) lock_mgr_->ReleaseAll(txn_id_);
    touched_keys_.clear();
    return Status::OK();
}

// 事务可见迭代器：简化占位，后续实现为合并迭代器（MVCC链 + LSM）
class TxnIterator : public Iterator {
public:
    TxnIterator(const VersionChainManager* vcm, const ReadView& view, const Comparator* cmp,
                LSMTree* lsm, SnapshotSequence lsm_read_seq)
        : vcm_(vcm), view_(view), cmp_(cmp), lsm_(lsm), lsm_seq_(lsm_read_seq),
          mvcc_idx_(0), valid_(false) {
        // 收集 MVCC 可见视图（仅构建可见集映射与有序键列表）
        if (vcm_) {
            std::vector<std::pair<std::string, VersionRecord>> list;
            vcm_->CollectVisible(view_, &list);
            for (const auto& kv : list) {
                mvcc_map_[kv.first] = kv.second;
                mvcc_keys_.push_back(kv.first);
            }
            std::sort(mvcc_keys_.begin(), mvcc_keys_.end(), [&](const std::string& a, const std::string& b){
                if (!cmp_) return a < b;
                return cmp_->Compare(Slice(a), Slice(b)) < 0;
            });
            mvcc_keys_.erase(std::unique(mvcc_keys_.begin(), mvcc_keys_.end()), mvcc_keys_.end());
        }
        if (lsm_) {
            lsm_iter_ = lsm_->NewIterator(lsm_seq_);
        }
    }

    bool Valid() const override { return valid_; }
    void SeekToFirst() override {
        if (lsm_iter_) lsm_iter_->SeekToFirst();
        mvcc_idx_ = 0;
        Advance();
    }
    void SeekToLast() override {
        // 不支持反向流式，退化为清空
        valid_ = false;
    }
    void Seek(const Slice& target) override {
        if (lsm_iter_) lsm_iter_->Seek(target);
        mvcc_idx_ = LowerBoundMVCC(target);
        Advance();
    }
    void SeekForPrev(const Slice& target) override {
        // 不支持，置为无效
        valid_ = false;
    }
    void Next() override {
        if (!valid_) return;
        Advance();
    }
    void Prev() override {
        // 不支持反向
        valid_ = false;
    }
    Slice key() const override { return valid_ ? Slice(cur_key_) : Slice(); }
    Slice value() const override { return valid_ ? Slice(cur_value_) : Slice(); }
    Status status() const override { return Status::OK(); }

private:
    int LowerBoundMVCC(const Slice& target) const {
        int l = 0, r = static_cast<int>(mvcc_keys_.size());
        auto less = [&](const std::string& k){ return cmp_ ? cmp_->Compare(Slice(k), target) < 0 : k < target.ToString(); };
        while (l < r) {
            int m = (l + r) >> 1;
            if (less(mvcc_keys_[m])) l = m + 1; else r = m;
        }
        return l;
    }

    int CompareKeys(const std::string& a, const std::string& b) const {
        if (!cmp_) {
            if (a < b) return -1; if (a > b) return 1; return 0;
        }
        return cmp_->Compare(Slice(a), Slice(b));
    }

    void Advance() {
        // 迭代到下一个有效条目
        while (true) {
            std::string mv_key;
            VersionRecord* mv_rec = nullptr;
            bool has_mv = (mvcc_idx_ < static_cast<int>(mvcc_keys_.size()));
            if (has_mv) {
                mv_key = mvcc_keys_[mvcc_idx_];
                mv_rec = &mvcc_map_[mv_key];
            }

            bool has_lsm = lsm_iter_ && lsm_iter_->Valid();
            if (!has_mv && !has_lsm) { valid_ = false; return; }

            if (has_mv && (!has_lsm || CompareKeys(mv_key, lsm_iter_->key().ToString()) <= 0)) {
                // 优先 MVCC
                // 若为删除，跳过该键（同时若 LSM 在同键则也前进）
                if (mv_rec->is_delete) {
                    if (has_lsm && CompareKeys(mv_key, lsm_iter_->key().ToString()) == 0) lsm_iter_->Next();
                    ++mvcc_idx_;
                    continue;
                }
                cur_key_ = mv_key;
                cur_value_ = mv_rec->value;
                // 若 LSM 在同键，前进 LSM
                if (has_lsm && CompareKeys(mv_key, lsm_iter_->key().ToString()) == 0) lsm_iter_->Next();
                ++mvcc_idx_;
                valid_ = true;
                return;
            } else {
                // 选择 LSM
                std::string lk = lsm_iter_->key().ToString();
                auto it = mvcc_map_.find(lk);
                if (it != mvcc_map_.end()) {
                    // 被 MVCC 覆盖或删除
                    if (it->second.is_delete) { lsm_iter_->Next(); continue; }
                    cur_key_ = lk;
                    cur_value_ = it->second.value;
                    lsm_iter_->Next();
                    valid_ = true;
                    return;
                } else {
                    cur_key_ = lk;
                    cur_value_ = lsm_iter_->value().ToString();
                    lsm_iter_->Next();
                    valid_ = true;
                    return;
                }
            }
        }
    }

private:
    const VersionChainManager* vcm_;
    ReadView view_;
    const Comparator* cmp_;
    LSMTree* lsm_;
    SnapshotSequence lsm_seq_;
    std::unique_ptr<LSMTreeIterator> lsm_iter_;
    std::unordered_map<std::string, VersionRecord> mvcc_map_;
    std::vector<std::string> mvcc_keys_;
    int mvcc_idx_;
    bool valid_;
    std::string cur_key_;
    std::string cur_value_;
};

std::unique_ptr<Iterator> TransactionImpl::NewIterator() {
    // 在 Serializable 下获取全范围读锁，避免幻读（可后续扩展为用户指定范围）
    if (opts_.isolation == IsolationLevel::kSerializable && lock_mgr_) {
        (void)lock_mgr_->AcquireRange("", "", txn_id_, /*exclusive=*/false, opts_.lock_timeout);
    }
    return std::make_unique<TxnIterator>(vcm_, view_, cmp_, lsm_, lsm_read_seq_);
}

} // namespace lrdb


