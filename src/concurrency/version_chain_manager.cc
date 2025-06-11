#include "lrdb/concurrency/version_chain_manager.h"

#include <algorithm>
#include <mutex>

namespace lrdb {

std::vector<VersionRecord>& VersionChainManager::Chain(const std::string& key) {
    return chains_[key];
}

const std::vector<VersionRecord>* VersionChainManager::ChainIfExists(const std::string& key) const {
    auto it = chains_.find(key);
    if (it == chains_.end()) return nullptr;
    return &it->second;
}

Status VersionChainManager::PutActive(const std::string& key, const std::string& value, TransactionID txn_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    auto& chain = Chain(key);
    chain.emplace_back(value, false, txn_id);
    return Status::OK();
}

Status VersionChainManager::DeleteActive(const std::string& key, TransactionID txn_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    auto& chain = Chain(key);
    chain.emplace_back(std::string(), true, txn_id);
    return Status::OK();
}

Status VersionChainManager::MarkTransactionCommitted(TransactionID txn_id, SnapshotSequence commit_snapshot) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    for (auto& kv : chains_) {
        auto& chain = kv.second;
        for (auto& rec : chain) {
            if (rec.txn_id == txn_id && rec.state == VersionState::kActive) {
                rec.state = VersionState::kCommitted;
                rec.commit_snapshot = commit_snapshot;
            }
        }
    }
    return Status::OK();
}

Status VersionChainManager::MarkTransactionAborted(TransactionID txn_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    for (auto& kv : chains_) {
        auto& chain = kv.second;
        for (auto& rec : chain) {
            if (rec.txn_id == txn_id && rec.state == VersionState::kActive) {
                rec.state = VersionState::kAborted;
            }
        }
    }
    return Status::OK();
}

Status VersionChainManager::GetVisible(const std::string& key,
                                       const ReadView& view,
                                       std::string* value,
                                       bool* found) const {
    if (!value || !found) {
        return Status::InvalidArgument("Null output parameter");
    }
    *found = false;

    std::shared_lock<std::shared_mutex> lk(mu_);
    const auto* chain = ChainIfExists(key);
    if (!chain) return Status::OK();

    // 从尾到头扫描，找到最新可见版本
    for (auto it = chain->rbegin(); it != chain->rend(); ++it) {
        const VersionRecord& rec = *it;
        bool is_committed = (rec.state == VersionState::kCommitted);
        bool is_aborted = (rec.state == VersionState::kAborted);

        if (!view.IsVisible(rec.txn_id, is_committed, is_aborted)) {
            continue;
        }

        if (rec.is_delete) {
            // 可见的删除标记：用 Deleted 状态区分"已删除"与"无版本"，
            // 调用方据此阻断回退到 LSM 的旧值
            *found = false;
            return Status::Deleted("Key deleted at visible version");
        }

        *value = rec.value;
        *found = true;
        return Status::OK();
    }

    return Status::OK();
}

Status VersionChainManager::GetVisibleBySnapshot(const std::string& key,
                                                 SnapshotSequence snapshot_seq,
                                                 std::string* value,
                                                 bool* found) const {
    if (!value || !found) {
        return Status::InvalidArgument("Null output parameter");
    }
    *found = false;

    std::shared_lock<std::shared_mutex> lk(mu_);
    const auto* chain = ChainIfExists(key);
    if (!chain) return Status::OK();

    // 从尾到头扫描，找commit_snapshot <= snapshot_seq的最新提交版本
    for (auto it = chain->rbegin(); it != chain->rend(); ++it) {
        const VersionRecord& rec = *it;
        if (rec.state != VersionState::kCommitted) {
            continue;
        }
        if (rec.commit_snapshot > snapshot_seq) {
            continue; // 该版本在快照之后提交，继续找更老的
        }

        if (rec.is_delete) {
            // 可见的删除标记 => 键在快照下不存在
            *found = false;
            return Status::NotFound("Key deleted at snapshot");
        }

        *value = rec.value;
        *found = true;
        return Status::OK();
    }

    return Status::OK();
}

std::vector<std::pair<std::string, VersionRecord>> VersionChainManager::PickCommittedNotFlushed(size_t max_items) const {
    std::vector<std::pair<std::string, VersionRecord>> out;
    out.reserve(max_items);

    std::shared_lock<std::shared_mutex> lk(mu_);
    for (const auto& kv : chains_) {
        const std::string& key = kv.first;
        const auto& chain = kv.second;

        // 收集该 key 所有“已提交且未刷盘”的版本（不只最新一条）：
        // 老版本同样可能被长读事务/快照读依赖，必须先落盘才能被 GC 清理
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            const VersionRecord& rec = *it;
            if (rec.state == VersionState::kCommitted && !rec.flushed) {
                out.emplace_back(key, rec);
            }
        }

        if (out.size() >= max_items) break;
    }
    return out;
}

void VersionChainManager::MarkFlushed(const std::vector<std::pair<std::string, SnapshotSequence>>& keys) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    for (const auto& kv : keys) {
        const std::string& key = kv.first;
        SnapshotSequence seq = kv.second;
        auto it = chains_.find(key);
        if (it == chains_.end()) continue;
        for (auto& rec : it->second) {
            if (rec.state == VersionState::kCommitted && rec.commit_snapshot == seq) {
                rec.flushed = true;
            }
        }
    }
}

void VersionChainManager::GarbageCollect() {
    std::unique_lock<std::shared_mutex> lk(mu_);

    for (auto& kv : chains_) {
        auto& chain = kv.second;

        // 先丢弃 Aborted 版本
        chain.erase(std::remove_if(chain.begin(), chain.end(), [](const VersionRecord& r){
            return r.state == VersionState::kAborted;
        }), chain.end());

        // 从尾向头保留：全部 Active、最新一条 Committed、所有未落盘的 Committed。
        // 已落盘的老提交版本可安全清理：长读事务/快照读会回退到 LSM 读取对应序号的数据
        //（刷回任务保证所有提交版本最终都会落盘，见 PickCommittedNotFlushed）。
        bool kept_latest_committed = false;
        std::vector<VersionRecord> kept;
        kept.reserve(chain.size());
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            const VersionRecord& r = *it;
            if (r.state == VersionState::kActive) {
                kept.push_back(r);
            } else if (r.state == VersionState::kCommitted) {
                if (!kept_latest_committed || !r.flushed) {
                    kept.push_back(r);
                    kept_latest_committed = true;
                }
                // 其余（已落盘的老提交版本）丢弃
            }
        }
        std::reverse(kept.begin(), kept.end());
        chain.swap(kept);
    }
}

void VersionChainManager::CollectVisible(const ReadView& view,
                                         std::vector<std::pair<std::string, VersionRecord>>* out) const {
    if (!out) return;
    out->clear();
    std::shared_lock<std::shared_mutex> lk(mu_);
    for (const auto& kv : chains_) {
        const auto& key = kv.first;
        const auto& chain = kv.second;
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            const VersionRecord& rec = *it;
            bool is_committed = (rec.state == VersionState::kCommitted);
            bool is_aborted = (rec.state == VersionState::kAborted);
            if (view.IsVisible(rec.txn_id, is_committed, is_aborted)) {
                out->emplace_back(key, rec);
                break;
            }
        }
    }
}

bool VersionChainManager::GetLatestActiveForTxn(const std::string& key, TransactionID txn_id,
                                                VersionRecord* rec_out) const {
    std::shared_lock<std::shared_mutex> lk(mu_);
    const auto* chain = ChainIfExists(key);
    if (!chain) return false;
    for (auto it = chain->rbegin(); it != chain->rend(); ++it) {
        if (it->txn_id == txn_id && it->state == VersionState::kActive) {
            if (rec_out) *rec_out = *it;
            return true;
        }
    }
    return false;
}

} // namespace lrdb


