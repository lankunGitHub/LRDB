// 数据库迭代器实现

#include "lrdb/db/iterator.h"
#include "lrdb/util/comparator.h"
#include <vector>
#include <queue>
#include <memory>

namespace lrdb {

// 实现MergeIterator类
MergeIterator::MergeIterator(const Comparator* comparator, Iterator** children, int n)
    : comparator_(comparator), current_(-1) {
    
    // 过滤无效的迭代器
    children_.clear();
    for (int i = 0; i < n; i++) {
        if (children[i]) {
            IteratorState state;
            state.iter = children[i];
            state.valid = children[i]->Valid();
            children_.push_back(state);
        }
    }
    
    if (!children_.empty()) {
        current_ = 0;
        children_[current_].iter->SeekToFirst();
        children_[current_].valid = children_[current_].iter->Valid();
    }
}

MergeIterator::~MergeIterator() = default;

bool MergeIterator::Valid() const {
    return current_ >= 0 && current_ < static_cast<int>(children_.size()) && 
           children_[current_].valid;
}

void MergeIterator::SeekToFirst() {
    for (auto& state : children_) {
        state.iter->SeekToFirst();
        state.valid = state.iter->Valid();
    }
    current_ = 0;
    FindSmallest();
}

void MergeIterator::SeekToLast() {
    for (auto& state : children_) {
        state.iter->SeekToLast();
        state.valid = state.iter->Valid();
    }
    current_ = 0;
    FindLargest();
}

void MergeIterator::Seek(const Slice& target) {
    for (auto& state : children_) {
        state.iter->Seek(target);
        state.valid = state.iter->Valid();
    }
    current_ = 0;
    FindSmallest();
}

void MergeIterator::SeekForPrev(const Slice& target) {
    for (auto& state : children_) {
        state.iter->SeekForPrev(target);
        state.valid = state.iter->Valid();
    }
    current_ = 0;
    FindLargest();
}

void MergeIterator::Next() {
    if (!Valid()) return;
    
    children_[current_].iter->Next();
    children_[current_].valid = children_[current_].iter->Valid();
    FindSmallest();
}

void MergeIterator::Prev() {
    if (!Valid()) return;
    
    children_[current_].iter->Prev();
    children_[current_].valid = children_[current_].iter->Valid();
    FindLargest();
}

Slice MergeIterator::key() const {
    return Valid() ? children_[current_].iter->key() : Slice();
}

Slice MergeIterator::value() const {
    return Valid() ? children_[current_].iter->value() : Slice();
}

Status MergeIterator::status() const {
    Status s = Status::OK();
    for (const IteratorState& state : children_) {
        s = state.iter->status();
        if (!s.ok()) break;
    }
    return s;
}

void MergeIterator::FindSmallest() {
    if (children_.empty()) {
        current_ = -1;
        return;
    }
    
    current_ = 0;
    for (size_t i = 1; i < children_.size(); i++) {
        if (children_[i].valid && 
            (!children_[current_].valid || 
             comparator_->Compare(children_[i].iter->key(), children_[current_].iter->key()) < 0)) {
            current_ = static_cast<int>(i);
        }
    }
}

void MergeIterator::FindLargest() {
    if (children_.empty()) {
        current_ = -1;
        return;
    }
    
    current_ = 0;
    for (size_t i = 1; i < children_.size(); i++) {
        if (children_[i].valid && 
            (!children_[current_].valid || 
             comparator_->Compare(children_[i].iter->key(), children_[current_].iter->key()) > 0)) {
            current_ = static_cast<int>(i);
        }
    }
}

void MergeIterator::ClearError() {
    for (auto& state : children_) {
        if (!state.iter->status().ok()) {
            state.iter->SeekToFirst();
            state.valid = state.iter->Valid();
        }
    }
}

// 实现ConcatenatingIterator类
ConcatenatingIterator::ConcatenatingIterator(const Comparator* comparator, 
                                           const std::vector<Iterator*>& iterators)
    : comparator_(comparator), children_(iterators), current_(0) {
    
    // 过滤掉空的迭代器
    std::vector<Iterator*> valid_children;
    for (Iterator* iter : children_) {
        if (iter) {
            valid_children.push_back(iter);
        }
    }
    children_ = valid_children;
    
    if (!children_.empty()) {
        current_ = 0;
    } else {
        current_ = -1;
    }
}

ConcatenatingIterator::~ConcatenatingIterator() = default;

bool ConcatenatingIterator::Valid() const {
    return current_ >= 0 && current_ < static_cast<int>(children_.size()) && 
           children_[current_]->Valid();
}

void ConcatenatingIterator::SeekToFirst() {
    current_ = 0;
    if (!children_.empty()) {
        children_[current_]->SeekToFirst();
        SkipEmptyChildren();
    }
}

void ConcatenatingIterator::SeekToLast() {
    current_ = static_cast<int>(children_.size()) - 1;
    if (current_ >= 0) {
        children_[current_]->SeekToLast();
        SkipEmptyChildrenReverse();
    }
}

void ConcatenatingIterator::Seek(const Slice& target) {
    // 二分查找确定应该从哪个子迭代器开始
    current_ = FindChild(target);
    if (current_ >= 0 && current_ < static_cast<int>(children_.size())) {
        children_[current_]->Seek(target);
        if (!children_[current_]->Valid()) {
            Next();
        }
    }
}

void ConcatenatingIterator::SeekForPrev(const Slice& target) {
    // 类似Seek，但是向前查找
    Seek(target);
    if (Valid()) {
        // 如果找到的键大于目标，向前移动
        if (comparator_->Compare(key(), target) > 0) {
            Prev();
        }
    }
}

void ConcatenatingIterator::Next() {
    if (!Valid()) return;
    
    children_[current_]->Next();
    if (!children_[current_]->Valid()) {
        // 当前子迭代器结束，移动到下一个
        current_++;
        if (current_ < static_cast<int>(children_.size())) {
            children_[current_]->SeekToFirst();
            SkipEmptyChildren();
        }
    }
}

void ConcatenatingIterator::Prev() {
    if (!Valid()) return;
    
    children_[current_]->Prev();
    if (!children_[current_]->Valid()) {
        // 当前子迭代器结束，移动到上一个
        current_--;
        if (current_ >= 0) {
            children_[current_]->SeekToLast();
            SkipEmptyChildrenReverse();
        }
    }
}

Slice ConcatenatingIterator::key() const {
    return Valid() ? children_[current_]->key() : Slice();
}

Slice ConcatenatingIterator::value() const {
    return Valid() ? children_[current_]->value() : Slice();
}

Status ConcatenatingIterator::status() const {
    Status s = Status::OK();
    for (const Iterator* iter : children_) {
        s = iter->status();
        if (!s.ok()) break;
    }
    return s;
}

void ConcatenatingIterator::SkipEmptyChildren() {
    while (current_ < static_cast<int>(children_.size()) && !children_[current_]->Valid()) {
        current_++;
        if (current_ < static_cast<int>(children_.size())) {
            children_[current_]->SeekToFirst();
        }
    }
    if (current_ >= static_cast<int>(children_.size())) {
        current_ = -1;
    }
}

void ConcatenatingIterator::SkipEmptyChildrenReverse() {
    while (current_ >= 0 && !children_[current_]->Valid()) {
        current_--;
        if (current_ >= 0) {
            children_[current_]->SeekToLast();
        }
    }
}

int ConcatenatingIterator::FindChild(const Slice& target) const {
    // 简化实现：线性查找
    // 实际应该根据每个子迭代器的键范围进行二分查找
    for (size_t i = 0; i < children_.size(); i++) {
        children_[i]->Seek(target);
        if (children_[i]->Valid()) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

// 工厂函数实现
Iterator* NewEmptyIterator() {
    return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
    return new ErrorIterator(status);
}

Iterator* NewMergeIterator(const Comparator* comparator, Iterator** children, int n) {
    std::vector<Iterator*> iterators;
    for (int i = 0; i < n; i++) {
        if (children[i]) {
            iterators.push_back(children[i]);
        }
    }
    return new MergeIterator(comparator, children, n);
}

Iterator* NewConcatenatingIterator(const Comparator* comparator, const std::vector<Iterator*>& iterators) {
    return new ConcatenatingIterator(comparator, iterators);
}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                             std::function<Iterator*(const Slice&)> block_function) {
    return new TwoLevelIterator(index_iter, block_function);
}

// 实现iterator_util命名空间中的函数
namespace iterator_util {

Iterator* NewMergeIterator(const Comparator* comparator, Iterator** children, int n) {
    return ::lrdb::NewMergeIterator(comparator, children, n);
}

Iterator* NewEmptyIterator() {
    return ::lrdb::NewEmptyIterator();
}

Iterator* NewErrorIterator(const Status& status) {
    return ::lrdb::NewErrorIterator(status);
}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                             std::function<Iterator*(const Slice&)> block_function) {
    return ::lrdb::NewTwoLevelIterator(index_iter, block_function);
}

} // namespace iterator_util

// TwoLevelIterator实现
TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   std::function<Iterator*(const Slice&)> block_function)
    : index_iter_(index_iter), block_function_(block_function), status_(Status::OK()) {
    InitDataBlock();
}

TwoLevelIterator::~TwoLevelIterator() = default;

bool TwoLevelIterator::Valid() const {
    return data_iter_ && data_iter_->Valid();
}

void TwoLevelIterator::SeekToFirst() {
    index_iter_->SeekToFirst();
    InitDataBlock();
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
    index_iter_->SeekToLast();
    InitDataBlock();
    SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Seek(const Slice& target) {
    index_iter_->Seek(target);
    InitDataBlock();
    if (data_iter_) {
        data_iter_->Seek(target);
    }
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekForPrev(const Slice& target) {
    index_iter_->SeekForPrev(target);
    InitDataBlock();
    if (data_iter_) {
        data_iter_->SeekForPrev(target);
    }
    SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
    assert(Valid());
    data_iter_->Next();
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
    assert(Valid());
    data_iter_->Prev();
    SkipEmptyDataBlocksBackward();
}

Slice TwoLevelIterator::key() const {
    assert(Valid());
    return data_iter_->key();
}

Slice TwoLevelIterator::value() const {
    assert(Valid());
    return data_iter_->value();
}

Status TwoLevelIterator::status() const {
    if (!status_.ok()) {
        return status_;
    }
    if (index_iter_ && !index_iter_->status().ok()) {
        return index_iter_->status();
    }
    if (data_iter_ && !data_iter_->status().ok()) {
        return data_iter_->status();
    }
    return status_;
}

bool TwoLevelIterator::IsKeyPinned() const {
    return Valid() && data_iter_->IsKeyPinned();
}

bool TwoLevelIterator::IsValuePinned() const {
    return Valid() && data_iter_->IsValuePinned();
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
    data_iter_.reset(data_iter);
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
    // Valid() 定义为 data_iter_->Valid()，旧条件 "Valid() && !data_iter_->Valid()"
    // 恒为假，跨块推进从未执行（若真满足又会自递归栈溢出）。
    // 正确语义：当前数据块耗尽时前进索引并打开下一块
    while (data_iter_ == nullptr || !data_iter_->Valid()) {
        if (!index_iter_->Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_->Next();
        InitDataBlock();
        if (data_iter_ != nullptr) {
            data_iter_->SeekToFirst();
        }
    }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
    while (data_iter_ == nullptr || !data_iter_->Valid()) {
        if (!index_iter_->Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_->Prev();
        InitDataBlock();
        if (data_iter_ != nullptr) {
            data_iter_->SeekToLast();
        }
    }
}

void TwoLevelIterator::InitDataBlock() {
    if (!index_iter_->Valid()) {
        SetDataIterator(nullptr);
        return;
    }
    
    Slice handle_value = index_iter_->value();
    Iterator* iter = block_function_(handle_value);
    SetDataIterator(iter);
}

} // namespace lrdb