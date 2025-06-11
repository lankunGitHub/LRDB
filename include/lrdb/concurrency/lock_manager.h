#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lrdb/core/status.h"
#include "lrdb/concurrency/mvcc.h"

namespace lrdb {

struct LockRequest {
    TransactionID txn_id{0};
    LockType type{LockType::kExclusiveLock};
};

struct LockEntry {
    std::string key;
    std::vector<LockRequest> holders;
    std::deque<LockRequest> waiters; // deque：允许移除非队首的等待者（超时/死锁中止时清理）
    std::condition_variable cv;
    std::mutex mu;

    explicit LockEntry(std::string k) : key(std::move(k)) {}
};

// 等待图 + DFS 检测
// 维护 waiter->holders 边集，用于快速判断新增等待是否引入环；
// 提供 DetectCycle 选择牺牲者（策略：txn_id 最大者），供后台死锁检测使用。
class WaitForGraph {
public:
    void AddEdge(TransactionID waiter, TransactionID holder);
    void RemoveEdgesFor(TransactionID txn);
    bool WouldCreateCycle(TransactionID waiter, TransactionID holder) const;

    // 在当前等待图中检测是否存在环；若存在，选择一个牺牲者（策略：txn_id 最大者）
    bool DetectCycle(TransactionID* victim) const;

private:
    bool Dfs(TransactionID start, TransactionID cur,
             std::unordered_set<TransactionID>& visited,
             std::unordered_set<TransactionID>& stack,
             std::unordered_set<TransactionID>* cycle_out) const;

private:
    // 邻接表：waiter -> {holders}
    std::unordered_map<TransactionID, std::unordered_set<TransactionID>> g_;
};

// LockManager
// - 提供点锁(S/X)和范围锁（Serializable 防幻读）
// - 支持超时、TryAcquire、ReleaseAll；
// - 死锁：Acquire 前预检 WouldCreateCycle；后台可周期调用 DetectAndResolveDeadlocks 标记牺牲者，
//   牺牲者后续 Acquire/TryAcquire 立刻返回 Aborted，促进收敛。
class LockManager {
public:
    LockManager() = default;

    // 获取锁（可阻塞等待），支持超时
    Status Acquire(const std::string& key, TransactionID txn_id, LockType type,
                   std::chrono::milliseconds timeout);

    // 非阻塞尝试
    Status TryAcquire(const std::string& key, TransactionID txn_id, LockType type);

    // 释放事务持有的某键锁
    void Release(const std::string& key, TransactionID txn_id);

    // 释放事务持有的全部锁
    void ReleaseAll(TransactionID txn_id);

    // 范围锁（用于Serializable避免幻读）。
    // 约定区间为 [start, end)，end 为空表示正无穷，start 为空表示负无穷。
    Status AcquireRange(const std::string& start, const std::string& end,
                        TransactionID txn_id, bool exclusive,
                        std::chrono::milliseconds timeout);

    // 后台死锁检测入口（由上层以固定频率调用）
    void DetectAndResolveDeadlocks();

private:
    static bool Compatible(LockType held, LockType requested);
    static bool CanGrant(const LockEntry& entry, TransactionID requester, LockType req);

    std::shared_ptr<LockEntry> GetOrCreate(const std::string& key);

    // 从等待队列中移除指定事务（无论其是否在队首）
    static void RemoveWaiter(LockEntry* entry, TransactionID txn_id);
    // 查询事务是否被标记为死锁牺牲者
    bool IsAborted(TransactionID txn_id) const;

    struct RangeLock { TransactionID txn_id; std::string start; std::string end; bool exclusive; };
    static bool Overlap(const std::string& a_start, const std::string& a_end,
                        const std::string& b_start, const std::string& b_end);
    bool RangeConflicts(const std::string& key, LockType req, TransactionID requester) const;
    bool RangeConflictsWithRange(const std::string& start, const std::string& end, bool exclusive, TransactionID requester) const;

private:
    mutable std::shared_mutex table_mu_;
    std::unordered_map<std::string, std::shared_ptr<LockEntry>> table_;

    mutable std::mutex graph_mu_;
    WaitForGraph graph_;

    // 范围锁集合
    mutable std::mutex range_mu_;
    std::vector<RangeLock> range_locks_;
    std::condition_variable range_cv_;

    // 牺牲者集合：被选中的事务将被拒绝后续锁请求
    std::unordered_set<TransactionID> aborted_;
};

} // namespace lrdb


