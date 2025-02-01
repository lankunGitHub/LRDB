#pragma once

#include <memory>
#include <string>
#include <unordered_set>

#include "lrdb/core/status.h"
#include "lrdb/core/slice.h"
#include "lrdb/db/iterator.h"
#include "lrdb/concurrency/mvcc.h"
#include "lrdb/concurrency/snapshot.h"

namespace lrdb {

class LockManager;
class VersionChainManager;
class WALManager;
class LSMTree;
class Comparator;
class TxManager;

// 事务选项
// isolation:
//   - kReadCommitted: 每次读取观察到当时最新的已提交版本；
//   - kRepeatableRead: 在 Begin 时冻结活跃事务集合与上界，确保同一键重复读取一致；
// lock_timeout: 点锁/范围锁获取的最大等待时间；
// new_snapshot_on_commit: 提交是否分配新的 SnapshotSequence（否则复用当前序列以批次提交，优化写放大）；
// enable_rr_validation: RR 下可选读集校验（提交前再次验证可见结果与首次读取一致）。
struct TxnOptions {
    IsolationLevel isolation{IsolationLevel::kReadCommitted};
    std::chrono::milliseconds lock_timeout{std::chrono::milliseconds(10000)};
    // 提交时是否分配新的快照序列号（否则共享当前序列号）
    bool new_snapshot_on_commit{false};
    // 在 RR 下启用读集验证（提交前再次验证可见结果是否与首次读取一致）
    bool enable_rr_validation{false};
};

// Transaction 抽象
// - 对外统一事务接口；具体逻辑由 TransactionImpl 实现
// - Put/Delete 将在 MVCC 中以 Active 版本形式暂存，提交后转为 WAL 记录
// - Get 优先查询 MVCC（含本事务未提交版本），未命中再依据 lsm_read_seq_ 读取 LSM
class Transaction {
public:
    virtual ~Transaction() = default;

    virtual Status Put(const Slice& key, const Slice& value) = 0;
    virtual Status Delete(const Slice& key) = 0;
    virtual Status Get(const Slice& key, std::string* value) = 0;
    virtual Status GetForUpdate(const Slice& key, std::string* value) = 0; // 获取X锁后读取

    virtual Status Commit(bool allocate_new_snapshot = false) = 0;
    virtual Status Rollback() = 0;

    virtual std::unique_ptr<Iterator> NewIterator() = 0; // MVCC 覆盖的只读迭代器

    virtual TransactionID GetID() const = 0;
};

// 前置实现类句柄
// TransactionImpl
// - 构造时绑定 TxManager/LockManager/VersionChainManager/WAL/LSMTree 等组件；
// - Commit: RR 可选读集校验 -> 由 TxManager 分配/复用 SnapshotSequence ->
//   将 touched_keys_ 最近一次 Active 版本写 WAL Put/Delete -> Sync -> 标记提交及释放锁。
// - Iterator: 以 ReadView 收集 MVCC 可见集 + 打开 LSM 快照迭代器，流式合并，
//   删除标记会遮蔽 LSM 同键；实现 Seek/Next 的正向流式，不支持反向遍历。
class TransactionImpl : public Transaction {
public:
    TransactionImpl(TxManager* mgr,
                    TransactionID txn_id,
                    const TxnOptions& opts,
                    const ReadView& view,
                    SnapshotSequence lsm_read_seq,
                    LockManager* lock_mgr,
                    VersionChainManager* vcm,
                    WALManager* wal,
                    LSMTree* lsm,
                    const Comparator* cmp);

    ~TransactionImpl() override;

    Status Put(const Slice& key, const Slice& value) override;
    Status Delete(const Slice& key) override;
    Status Get(const Slice& key, std::string* value) override;
    Status GetForUpdate(const Slice& key, std::string* value) override;

    Status Commit(bool allocate_new_snapshot = false) override;
    Status Rollback() override;

    std::unique_ptr<Iterator> NewIterator() override;

    TransactionID GetID() const override { return txn_id_; }

private:
    TxManager* mgr_;
    TransactionID txn_id_;
    TxnOptions opts_;
    ReadView view_;
    SnapshotSequence lsm_read_seq_;

    LockManager* lock_mgr_;
    VersionChainManager* vcm_;
    WALManager* wal_;
    LSMTree* lsm_;
    const Comparator* cmp_;

    std::unordered_set<std::string> touched_keys_;

    struct ReadEntry {
        bool found{false};
        std::string value; // 仅在 found=true 时有效
    };
    std::unordered_map<std::string, ReadEntry> read_set_;
};

} // namespace lrdb


