#include "lrdb/concurrency/lock_manager.h"

#include <algorithm>

namespace lrdb {

void WaitForGraph::AddEdge(TransactionID waiter, TransactionID holder) {
    g_[waiter].insert(holder);
}

void WaitForGraph::RemoveEdgesFor(TransactionID txn) {
    // 移除以 txn 为起点的边
    g_.erase(txn);
    // 移除所有指向 txn 的边
    for (auto& kv : g_) {
        kv.second.erase(txn);
    }
}

bool WaitForGraph::WouldCreateCycle(TransactionID waiter, TransactionID holder) const {
    // 临时添加边，检测环
    auto g = g_;
    g[waiter].insert(holder);

    std::unordered_set<TransactionID> visited;
    std::unordered_set<TransactionID> stack;
    return Dfs(waiter, waiter, visited, stack);
}

bool WaitForGraph::Dfs(TransactionID start, TransactionID cur,
                       std::unordered_set<TransactionID>& visited,
                       std::unordered_set<TransactionID>& stack) const {
    if (stack.count(cur)) return true;
    if (visited.count(cur)) return false;
    visited.insert(cur);
    stack.insert(cur);

    auto it = g_.find(cur);
    if (it != g_.end()) {
        for (TransactionID nxt : it->second) {
            if (Dfs(start, nxt, visited, stack)) return true;
        }
    }

    stack.erase(cur);
    return false;
}

bool WaitForGraph::DetectCycle(TransactionID* victim) const {
    // 若检测到环，选择涉及到的最大 txn_id 作为牺牲者
    std::unordered_set<TransactionID> nodes;
    for (const auto& kv : g_) {
        nodes.insert(kv.first);
        for (auto h : kv.second) nodes.insert(h);
    }
    for (auto t : nodes) {
        std::unordered_set<TransactionID> visited, st;
        if (Dfs(t, t, visited, st)) {
            TransactionID mx = 0;
            for (auto n : nodes) mx = std::max(mx, n);
            if (victim) *victim = mx;
            return true;
        }
    }
    return false;
}

static bool IsX(LockType t) { return t == LockType::kExclusiveLock; }
static bool IsS(LockType t) { return t == LockType::kSharedLock; }

bool LockManager::Compatible(LockType held, LockType requested) {
    if (IsX(held) || IsX(requested)) return held == requested && held == LockType::kExclusiveLock ? true : false;
    return true; // S 与 S 兼容
}

bool LockManager::CanGrant(const LockEntry& entry, TransactionID requester, LockType req) {
    for (const auto& h : entry.holders) {
        if (h.txn_id == requester) continue; // 允许同一事务重复持有
        if (!Compatible(h.type, req)) return false;
    }
    return entry.waiters.empty() || (entry.waiters.front().txn_id == requester && entry.waiters.front().type == req);
}

std::shared_ptr<LockEntry> LockManager::GetOrCreate(const std::string& key) {
    std::shared_lock<std::shared_mutex> rlock(table_mu_);
    auto it = table_.find(key);
    if (it != table_.end()) return it->second;
    rlock.unlock();
    std::unique_lock<std::shared_mutex> wlock(table_mu_);
    auto& ptr = table_[key];
    if (!ptr) ptr = std::make_shared<LockEntry>(key);
    return ptr;
}

Status LockManager::Acquire(const std::string& key, TransactionID txn_id, LockType type,
                            std::chrono::milliseconds timeout) {
    auto entry = GetOrCreate(key);
    std::unique_lock<std::mutex> lk(entry->mu);

    // 若被标记为牺牲者则快速失败
    {
        std::lock_guard<std::mutex> gl(graph_mu_);
        if (aborted_.count(txn_id)) {
            aborted_.erase(txn_id);
            return Status::Aborted("Deadlock victim");
        }
    }

    // 与范围锁冲突时按范围锁条件等待
    if (RangeConflicts(key, type, txn_id)) {
        auto until = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> rlk(range_mu_);
        while (RangeConflicts(key, type, txn_id)) {
            if (timeout.count() == 0) return Status::TimedOut("Range lock busy");
            if (range_cv_.wait_until(rlk, until) == std::cv_status::timeout) {
                return Status::TimedOut("Range lock timeout");
            }
        }
    }

    if (CanGrant(*entry, txn_id, type)) {
        entry->holders.push_back({txn_id, type});
        return Status::OK();
    }

    // 加入等待队列
    entry->waiters.push({txn_id, type});

    // 更新等待图
    {
        std::lock_guard<std::mutex> gl(graph_mu_);
        for (const auto& h : entry->holders) {
            graph_.AddEdge(txn_id, h.txn_id);
        }
        // 死锁预检：若本次等待会成环，直接返回 Aborted
        for (const auto& h : entry->holders) {
            if (graph_.WouldCreateCycle(txn_id, h.txn_id)) {
                // 移除排队
                // 在锁内清理队首
                if (!entry->waiters.empty() && entry->waiters.front().txn_id == txn_id) {
                    entry->waiters.pop();
                }
                return Status::Aborted("Deadlock detected");
            }
        }
    }

    // 等待
    bool ok = entry->cv.wait_for(lk, timeout, [&](){ return CanGrant(*entry, txn_id, type); });
    if (!ok) {
        // 超时，移除队列与等待图边
        if (!entry->waiters.empty() && entry->waiters.front().txn_id == txn_id) {
            entry->waiters.pop();
        }
        std::lock_guard<std::mutex> gl(graph_mu_);
        graph_.RemoveEdgesFor(txn_id);
        return Status::TimedOut("Lock wait timeout");
    }

    // 授予
    if (!entry->waiters.empty() && entry->waiters.front().txn_id == txn_id) {
        entry->waiters.pop();
    }
    entry->holders.push_back({txn_id, type});
    std::lock_guard<std::mutex> gl(graph_mu_);
    graph_.RemoveEdgesFor(txn_id);
    return Status::OK();
}

Status LockManager::TryAcquire(const std::string& key, TransactionID txn_id, LockType type) {
    auto entry = GetOrCreate(key);
    std::unique_lock<std::mutex> lk(entry->mu);
    if (RangeConflicts(key, type, txn_id)) {
        return Status::TimedOut("Range lock busy");
    }
    if (!CanGrant(*entry, txn_id, type)) {
        return Status::TimedOut("Lock busy");
    }
    entry->holders.push_back({txn_id, type});
    return Status::OK();
}

void LockManager::Release(const std::string& key, TransactionID txn_id) {
    auto entry = GetOrCreate(key);
    std::unique_lock<std::mutex> lk(entry->mu);
    auto& h = entry->holders;
    h.erase(std::remove_if(h.begin(), h.end(), [&](const LockRequest& r){ return r.txn_id == txn_id; }), h.end());
    entry->cv.notify_all();
}

void LockManager::ReleaseAll(TransactionID txn_id) {
    std::vector<std::shared_ptr<LockEntry>> entries;
    {
        std::shared_lock<std::shared_mutex> rlock(table_mu_);
        for (const auto& kv : table_) entries.push_back(kv.second);
    }
    for (auto& e : entries) {
        std::unique_lock<std::mutex> lk(e->mu);
        auto& h = e->holders;
        size_t before = h.size();
        h.erase(std::remove_if(h.begin(), h.end(), [&](const LockRequest& r){ return r.txn_id == txn_id; }), h.end());
        if (h.size() != before) e->cv.notify_all();
    }
    std::lock_guard<std::mutex> gl(graph_mu_);
    graph_.RemoveEdgesFor(txn_id);
    // 释放范围锁
    {
        std::lock_guard<std::mutex> rl(range_mu_);
        size_t before = range_locks_.size();
        range_locks_.erase(std::remove_if(range_locks_.begin(), range_locks_.end(),
            [&](const RangeLock& r){ return r.txn_id == txn_id; }), range_locks_.end());
        if (range_locks_.size() != before) range_cv_.notify_all();
    }
}
bool LockManager::Overlap(const std::string& a_start, const std::string& a_end,
                          const std::string& b_start, const std::string& b_end) {
    // 空字符串表示无界
    const std::string& s1 = a_start;
    const std::string& e1 = a_end;
    const std::string& s2 = b_start;
    const std::string& e2 = b_end;
    bool left_ok = (s1.empty() || e2.empty() || s1 < e2);
    bool right_ok = (s2.empty() || e1.empty() || s2 < e1);
    return left_ok && right_ok;
}

bool LockManager::RangeConflicts(const std::string& key, LockType req, TransactionID requester) const {
    std::lock_guard<std::mutex> rl(range_mu_);
    for (const auto& r : range_locks_) {
        if (r.txn_id == requester) continue;
        if (!Overlap(key, key + "\0", r.start, r.end)) continue;
        if (r.exclusive || IsX(req)) return true;
    }
    return false;
}

bool LockManager::RangeConflictsWithRange(const std::string& start, const std::string& end, bool exclusive, TransactionID requester) const {
    std::lock_guard<std::mutex> rl(range_mu_);
    for (const auto& r : range_locks_) {
        if (r.txn_id == requester) continue;
        if (!Overlap(start, end, r.start, r.end)) continue;
        if (r.exclusive || exclusive) return true;
    }
    return false;
}

Status LockManager::AcquireRange(const std::string& start, const std::string& end,
                                TransactionID txn_id, bool exclusive,
                                std::chrono::milliseconds timeout) {
    auto until = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lk(range_mu_);
    while (RangeConflictsWithRange(start, end, exclusive, txn_id)) {
        if (timeout.count() == 0) return Status::TimedOut("Range lock busy");
        if (range_cv_.wait_until(lk, until) == std::cv_status::timeout) {
            return Status::TimedOut("Range lock timeout");
        }
    }
    range_locks_.push_back(RangeLock{txn_id, start, end, exclusive});
    return Status::OK();
}

// 死锁检测与牺牲者选择：发现环后选择 txn_id 最大者为牺牲者
void LockManager::DetectAndResolveDeadlocks() {
    TransactionID victim = 0;
    bool has_cycle = false;
    {
        std::lock_guard<std::mutex> gl(graph_mu_);
        has_cycle = graph_.DetectCycle(&victim);
        if (has_cycle && victim) {
            aborted_.insert(victim);
        }
    }
    if (has_cycle) {
        // 广播唤醒等待者，让被标记事务快速失败
        std::shared_lock<std::shared_mutex> rlock(table_mu_);
        for (const auto& kv : table_) {
            kv.second->cv.notify_all();
        }
        std::lock_guard<std::mutex> rl(range_mu_);
        range_cv_.notify_all();
    }
}

} // namespace lrdb


