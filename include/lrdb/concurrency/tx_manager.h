#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "lrdb/core/status.h"
#include "lrdb/core/slice.h"
#include "lrdb/concurrency/mvcc.h"
#include "lrdb/concurrency/sequence_manager.h"
#include "lrdb/concurrency/snapshot.h"
#include "lrdb/concurrency/lock_manager.h"
#include "lrdb/concurrency/version_chain_manager.h"
#include "lrdb/concurrency/transaction.h"

namespace lrdb {

class WALManager;
class LSMTree;
class Comparator;

// 事务门面：单列族实例一套，不支持跨列族事务
// TxManager
// 作用：
// - 负责单列族事务门面：生成 TransactionID/ReadView，路由读写到 MVCC/LSM/WAL；
// - 控制提交的 SnapshotSequence 分配（新/复用）；
// - 提供 MVCC->LSM 的批量刷回与基于活跃事务边界的 GC；
// - 在 WAL 重放后推进快照序列，保证后续提交单调不回退。
// 重要时序：
//   Begin -> (Get/Put/Delete...) -> Commit(RC/RR 可选读集校验) -> WAL 写入/Sync -> 标记提交
// 读路径：优先查 MVCC 可见版本，不命中再用 lsm_read_seq_ 读 LSM；
// 写路径：在 MVCC 追加 Active 版本，提交时转为 WAL Put/Delete 记录并 Sync 成功即算成功。
class TxManager {
public:
    TxManager(WALManager* wal, LSMTree* lsm, const Comparator* cmp, uint32_t cf_id)
        : wal_(wal), lsm_(lsm), cmp_(cmp), cf_id_(cf_id) {}

    // 开启事务（RC/RR），返回事务对象
    std::unique_ptr<Transaction> Begin(const TxnOptions& opts);

    // 开启只读事务，指定 LSM 读取快照序列（用于外部快照迭代等）
    std::unique_ptr<Transaction> BeginReadOnlyWithLSMSnapshot(const TxnOptions& opts,
                                                              SnapshotSequence lsm_snapshot_seq);

    // 特殊非事务接口（立即提交）
    Status Put(const Slice& key, const Slice& value);
    Status Delete(const Slice& key);
    Status Get(const Slice& key, std::string* value);

    // 刷回：将已提交未刷盘的最新版本批量写入LSM
    Status FlushCommittedToLSM(size_t max_items);

    // 供事务实现调用：分配提交快照号、标记提交/中止
    SnapshotSequence PrepareCommit(TransactionID txn_id, bool allocate_new_snapshot);
    void OnCommitted(TransactionID txn_id);
    void OnAborted(TransactionID txn_id);

    // 恢复：设置当前快照序列（用于 WAL 重放完成后与 LSM 保持一致）
    void RecoverSetSnapshot(SnapshotSequence recovered_max_seq) {
        if (recovered_max_seq == 0) return;
        snap_gen_.Reset(recovered_max_seq + 1);
    }

    // 组件访问
    LockManager* lock_manager() { return &lock_mgr_; }
    VersionChainManager* version_chain() { return &vcm_; }
    WALManager* wal() { return wal_; }
    LSMTree* lsm() { return lsm_; }
    const Comparator* comparator() const { return cmp_; }
    uint32_t cf_id() const { return cf_id_; }

    // 触发一次基于活跃视图边界的 GC
    void RunGC();

    // 创建只读快照（用于 ReadOptions::snapshot），基于当前 SnapshotSequence
    std::unique_ptr<Snapshot> CreateSnapshot() const {
        return std::unique_ptr<Snapshot>(new DefaultSnapshot(snap_gen_.Current()));
    }

    // 当前快照序列号（RC 事务读 LSM 时取读时刻的最新序列）
    SnapshotSequence CurrentSnapshotSequence() const { return snap_gen_.Current(); }

private:
    // 构建 ReadView
    ReadView BuildReadView(TransactionID my_txn, const TxnOptions& opts);

private:
    TxnIDGenerator txn_gen_;
    SnapshotSequenceGenerator snap_gen_;
    LockManager lock_mgr_;
    VersionChainManager vcm_;
    WALManager* wal_;
    LSMTree* lsm_;
    const Comparator* cmp_;
    uint32_t cf_id_;

    mutable std::shared_mutex mu_;
    std::unordered_set<TransactionID> active_txns_;
};

} // namespace lrdb


