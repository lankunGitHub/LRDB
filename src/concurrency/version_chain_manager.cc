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
            // 可见的删除标记 => 不存在
            *found = false;
            return Status::OK();
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

        // 找到该 key 最新的已提交且未刷盘版本
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            const VersionRecord& rec = *it;
            if (rec.state == VersionState::kCommitted && !rec.flushed) {
                out.emplace_back(key, rec);
                break;
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

void VersionChainManager::GarbageCollect(const std::vector<TransactionID>& active_txn_ids,
                                         TransactionID up_limit_txn_id) {
    std::unique_lock<std::shared_mutex> lk(mu_);

    std::vector<TransactionID> active = active_txn_ids;
    std::sort(active.begin(), active.end());
    active.erase(std::unique(active.begin(), active.end()), active.end());

    auto is_active = [&](TransactionID id) {
        return std::binary_search(active.begin(), active.end(), id);
    };

    for (auto& kv : chains_) {
        auto& chain = kv.second;

        // 先丢弃 Aborted 版本
        chain.erase(std::remove_if(chain.begin(), chain.end(), [](const VersionRecord& r){
            return r.state == VersionState::kAborted;
        }), chain.end());

        // 保留最新的一个对任何可能读视图可见的提交版本，其余更老的提交版本若对所有未来读视图都不可见则清理。
        // 简化策略：保留最新的提交版本 + 所有 Active。
        bool found_latest_committed = false;
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            if (it->state == VersionState::kCommitted) { found_latest_committed = true; break; }
        }
        if (!found_latest_committed) continue;

        bool kept_latest = false;
        std::vector<VersionRecord> kept;
        kept.reserve(chain.size());
        for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
            const VersionRecord& r = *it;
            if (r.state == VersionState::kActive) {
                kept.push_back(r);
                continue;
            }
            if (r.state == VersionState::kCommitted) {
                if (!kept_latest) {
                    kept.push_back(r); // 保留最新提交
                    kept_latest = true;
                } else {
                    // 老的提交版本：若其创建者在 up_limit 前且不会再次被任何 RR 视图可见（保守略过复杂判断，直接清理）
                }
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


