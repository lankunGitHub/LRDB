#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "lrdb/core/status.h"
#include "lrdb/concurrency/mvcc.h"
#include "lrdb/concurrency/snapshot.h"

namespace lrdb {

// 版本记录状态
enum class VersionState {
    kActive,
    kCommitted,
    kAborted,
};

struct VersionRecord {
    std::string value;      // 空串 + is_delete=true 表示删除
    bool is_delete{false};
    TransactionID txn_id{0};
    VersionState state{VersionState::kActive};
    SnapshotSequence commit_snapshot{0}; // 提交后设置
    bool flushed{false};                 // 写入LSM后标记

    VersionRecord() = default;
    VersionRecord(std::string v, bool del, TransactionID t)
        : value(std::move(v)), is_delete(del), txn_id(t) {}
};

// 针对单列族的版本链管理器
// 职责：
// - 维护每个 key 的版本序列（尾部追加 Active 版本/删除标记）；
// - 依据 ReadView 返回最新可见版本；
// - 事务提交时将属于该 txn 的 Active 版本标记为 Committed，并记录 commit_snapshot；
// - 为刷回挑选“每个 key 最新的已提交未刷盘”版本；
// - 垃圾回收：清理 Aborted；保留最新 Committed + 所有 Active（可基于活跃视图边界进一步精细）。
class VersionChainManager {
public:
    VersionChainManager() = default;

    // 追加未提交版本（Active）
    Status PutActive(const std::string& key, const std::string& value, TransactionID txn_id);
    Status DeleteActive(const std::string& key, TransactionID txn_id);

    // 事务状态更新
    Status MarkTransactionCommitted(TransactionID txn_id, SnapshotSequence commit_snapshot);
    Status MarkTransactionAborted(TransactionID txn_id);

    // 读取：先返回本事务未提交最新版本，否则按 ReadView 可见性返回可见的最新提交版本
    Status GetVisible(const std::string& key,
                      const ReadView& view,
                      std::string* value,
                      bool* found) const;

    // 按快照序列号读取：返回commit_snapshot <= snapshot_seq的最新已提交版本
    // （快照读路径使用；删除标记表示为found=false且OK）
    Status GetVisibleBySnapshot(const std::string& key,
                                SnapshotSequence snapshot_seq,
                                std::string* value,
                                bool* found) const;

    // 列出所有 key 的“对该 ReadView 可见”的最新版本（包含删除标记）
    void CollectVisible(const ReadView& view,
                        std::vector<std::pair<std::string, VersionRecord>>* out) const;

    // 读取该 key 上“属于指定事务”的最新 Active 版本（用于提交）
    bool GetLatestActiveForTxn(const std::string& key, TransactionID txn_id,
                               VersionRecord* rec_out) const;

    // 选择需要刷回的“最新已提交且未刷盘”版本（每个key最多一条）
    std::vector<std::pair<std::string, VersionRecord>> PickCommittedNotFlushed(size_t max_items) const;

    // 标记指定键（到指定 commit_snapshot）的已提交版本为已刷盘
    void MarkFlushed(const std::vector<std::pair<std::string, SnapshotSequence>>& keys);

    // 垃圾回收：清理对所有活跃读视图不可见的老版本与 Aborted 版本
    void GarbageCollect(const std::vector<TransactionID>& active_txn_ids,
                        TransactionID up_limit_txn_id);

private:
    // 获取或创建某 key 的版本链（新版本追加在链表尾端即可，读取按逆序扫描）
    std::vector<VersionRecord>& Chain(const std::string& key);
    const std::vector<VersionRecord>* ChainIfExists(const std::string& key) const;

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::vector<VersionRecord>> chains_;
};

} // namespace lrdb


