// 序列号生成器 - 每个列族独立使用

#pragma once

#include "lrdb/concurrency/mvcc.h"
#include <atomic>

namespace lrdb {

// 简单的序列号生成器
class SequenceGenerator {
public:
    SequenceGenerator() : current_sequence_(1) {}
    ~SequenceGenerator() = default;

    // 获取下一个序列号
    SequenceNumber Next() {
        return current_sequence_.fetch_add(1, std::memory_order_acq_rel);
    }
    
    // 获取当前序列号
    SequenceNumber Current() const {
        return current_sequence_.load(std::memory_order_acquire);
    }
    
    // 设置序列号（用于恢复）
    void SetCurrent(SequenceNumber seq) {
        current_sequence_.store(seq, std::memory_order_release);
    }

private:
    std::atomic<SequenceNumber> current_sequence_;
    
    // 禁止拷贝
    SequenceGenerator(const SequenceGenerator&) = delete;
    SequenceGenerator& operator=(const SequenceGenerator&) = delete;
};

} // namespace lrdb
