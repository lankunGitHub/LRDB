// 数据库迭代器接口

#pragma once

#include "lrdb/core/status.h"
#include "lrdb/core/slice.h"
#include <functional>
#include <vector>
#include <cstdint>

namespace lrdb {

// 迭代器接口
class Iterator {
public:
    virtual ~Iterator() = default;
    
    // 迭代器控制
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& target) = 0;
    virtual void SeekForPrev(const Slice& target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    
    // 数据访问
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual Status status() const = 0;
    
    // 迭代器属性
    virtual bool IsKeyPinned() const { return false; }
    virtual bool IsValuePinned() const { return false; }
    
    // 刷新迭代器状态
    virtual Status Refresh() { return Status::NotSupported("Refresh"); }
    
    // 获取属性
    virtual Status GetProperty(const std::string& prop_name, std::string* prop) {
        (void)prop_name; // 标记参数为有意未使用
        (void)prop; 
        return Status::NotSupported("GetProperty");
    }
};

// 合并迭代器 - 合并多个有序迭代器
class MergeIterator : public Iterator {
public:
    MergeIterator(const class Comparator* comparator,
                  Iterator** children, int n);
    ~MergeIterator() override;
    
    bool Valid() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const Slice& target) override;
    void SeekForPrev(const Slice& target) override;
    void Next() override;
    void Prev() override;
    
    Slice key() const override;
    Slice value() const override;
    Status status() const override;

private:
    struct IteratorState {
        Iterator* iter;
        bool valid;
    };
    
    const class Comparator* comparator_;
    std::vector<IteratorState> children_;
    int current_;
    mutable Status status_;
    
    void FindSmallest();
    void FindLargest();
    void ClearError();
    
    // 禁止拷贝
    MergeIterator(const MergeIterator&) = delete;
    MergeIterator& operator=(const MergeIterator&) = delete;
};

// 连接迭代器 - 顺序连接多个迭代器
class ConcatenatingIterator : public Iterator {
public:
    ConcatenatingIterator(const class Comparator* comparator,
                         const std::vector<Iterator*>& iterators);
    ~ConcatenatingIterator() override;
    
    bool Valid() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const Slice& target) override;
    void SeekForPrev(const Slice& target) override;
    void Next() override;
    void Prev() override;
    
    Slice key() const override;
    Slice value() const override;
    Status status() const override;

private:
    const class Comparator* comparator_;
    std::vector<Iterator*> children_;
    int current_;
    mutable Status status_;
    
    void SkipEmptyChildren();
    void SkipEmptyChildrenReverse();
    int FindChild(const Slice& target) const;
    
    // 禁止拷贝
    ConcatenatingIterator(const ConcatenatingIterator&) = delete;
    ConcatenatingIterator& operator=(const ConcatenatingIterator&) = delete;
};

// 两级迭代器 - 用于SSTable文件
class TwoLevelIterator : public Iterator {
public:
    TwoLevelIterator(Iterator* index_iter,
                     std::function<Iterator*(const Slice&)> block_function);
    ~TwoLevelIterator() override;
    
    bool Valid() const override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Seek(const Slice& target) override;
    void SeekForPrev(const Slice& target) override;
    void Next() override;
    void Prev() override;
    
    Slice key() const override;
    Slice value() const override;
    Status status() const override;
    
    bool IsKeyPinned() const override;
    bool IsValuePinned() const override;

private:
    std::unique_ptr<Iterator> index_iter_;
    std::unique_ptr<Iterator> data_iter_;
    std::function<Iterator*(const Slice&)> block_function_;
    mutable Status status_;
    
    void SetDataIterator(Iterator* data_iter);
    void SkipEmptyDataBlocksForward();
    void SkipEmptyDataBlocksBackward();
    void InitDataBlock();
    
    // 禁止拷贝
    TwoLevelIterator(const TwoLevelIterator&) = delete;
    TwoLevelIterator& operator=(const TwoLevelIterator&) = delete;
};

// 空迭代器
class EmptyIterator : public Iterator {
public:
    explicit EmptyIterator(const Status& s) : status_(s) {}
    
    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void Seek(const Slice& target) override { (void)target; }
    void SeekForPrev(const Slice& target) override { (void)target; }
    void Next() override { assert(false); }
    void Prev() override { assert(false); }
    
    Slice key() const override {
        assert(false);
        return Slice();
    }
    
    Slice value() const override {
        assert(false);
        return Slice();
    }
    
    Status status() const override { return status_; }

private:
    Status status_;
};

// 错误迭代器 - 表示错误状态的迭代器
class ErrorIterator : public Iterator {
public:
    explicit ErrorIterator(const Status& status) : status_(status) {}
    ~ErrorIterator() override = default;
    
    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void Seek(const Slice& target) override { (void)target; }
    void SeekForPrev(const Slice& target) override { (void)target; }
    void Next() override {}
    void Prev() override {}
    
    Slice key() const override { return Slice(); }
    Slice value() const override { return Slice(); }
    Status status() const override { return status_; }

private:
    Status status_;
    
    // 禁止拷贝
    ErrorIterator(const ErrorIterator&) = delete;
    ErrorIterator& operator=(const ErrorIterator&) = delete;
};

// 迭代器包装器 - 添加额外功能
class IteratorWrapper {
public:
    IteratorWrapper() : iter_(nullptr), valid_(false) {}
    explicit IteratorWrapper(Iterator* iter) : iter_(iter) {
        Update();
    }
    
    ~IteratorWrapper() {
        delete iter_;
    }
    
    Iterator* iter() const { return iter_; }
    
    void Set(Iterator* iter) {
        delete iter_;
        iter_ = iter;
        if (iter_ == nullptr) {
            valid_ = false;
        } else {
            Update();
        }
    }
    
    bool Valid() const { return valid_; }
    Slice key() const { assert(Valid()); return key_; }
    Slice value() const { assert(Valid()); return iter_->value(); }
    Status status() const { assert(iter_); return iter_->status(); }
    
    void Next() {
        assert(iter_);
        iter_->Next();
        Update();
    }
    
    void Prev() {
        assert(iter_);
        iter_->Prev();
        Update();
    }
    
    void Seek(const Slice& k) {
        assert(iter_);
        iter_->Seek(k);
        Update();
    }
    
    void SeekForPrev(const Slice& k) {
        assert(iter_);
        iter_->SeekForPrev(k);
        Update();
    }
    
    void SeekToFirst() {
        assert(iter_);
        iter_->SeekToFirst();
        Update();
    }
    
    void SeekToLast() {
        assert(iter_);
        iter_->SeekToLast();
        Update();
    }

private:
    void Update() {
        valid_ = iter_ != nullptr && iter_->Valid();
        if (valid_) {
            key_ = iter_->key();
        }
    }
    
    Iterator* iter_;
    bool valid_;
    Slice key_;
    
    // 禁止拷贝
    IteratorWrapper(const IteratorWrapper&) = delete;
    IteratorWrapper& operator=(const IteratorWrapper&) = delete;
};

// 迭代器工具函数
namespace iterator_util {

// 创建空迭代器
Iterator* NewEmptyIterator();
Iterator* NewErrorIterator(const Status& status);

// 创建合并迭代器
Iterator* NewMergeIterator(const class Comparator* comparator,
                          Iterator** children, int n);

// 创建连接迭代器
Iterator* NewConcatenatingIterator(const class Comparator* comparator,
                                  const std::vector<Iterator*>& iterators);

// 创建两级迭代器
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                             std::function<Iterator*(const Slice&)> block_function);

// 创建连接迭代器
Iterator* NewConcatenatingIterator(const class Comparator* comparator,
                                  const std::vector<Iterator*>& iterators);

// 迭代器测试工具
void SeekToKey(Iterator* iter, const Slice& key);
bool IteratorEqual(Iterator* iter1, Iterator* iter2);
std::vector<std::pair<std::string, std::string>> IteratorToVector(Iterator* iter);

// 迭代器统计
struct IteratorStats {
    uint64_t seek_count;
    uint64_t next_count;
    uint64_t prev_count;
    uint64_t key_size_sum;
    uint64_t value_size_sum;
};

IteratorStats CollectIteratorStats(Iterator* iter);

// 迭代器验证
Status ValidateIterator(Iterator* iter);
Status ValidateIteratorKeys(Iterator* iter, const class Comparator* comparator);

}  // namespace iterator_util

}  // namespace lrdb