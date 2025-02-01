// Skiplist数据结构定义

#pragma once

#include "lrdb/core/slice.h"
#include "lrdb/util/comparator.h"
#include <atomic>
#include <cassert>
#include <memory>
#include <random>
#include <stdexcept>

namespace lrdb {

// 前向声明
class Comparator;

// Skiplist模板类
template <typename Key, typename Value> class SkipList {
public:
  struct Node;

  // 构造函数
  explicit SkipList(const Comparator *comparator);
  ~SkipList();

  // 禁止拷贝和移动
  SkipList(const SkipList &) = delete;
  SkipList &operator=(const SkipList &) = delete;

  // 插入键值对
  void Insert(const Key &key, const Value &value);

  // 合并键
  void Merge(const Key &key, const Value &value);

  // 删除键
  bool Delete(const Key &key);

  // 查找键
  bool Contains(const Key &key) const;

  // 获取值
  bool Get(const Key &key, Value *value) const;

  // 获取第一个节点
  Node *GetFirst() const {
    Node *first = head_->Next(0);
    return (first != nullptr) ? first : nullptr;
  }

  // 获取最后一个节点
  Node *GetLast() const;

  // 估算内存使用量
  size_t EstimateCount() const {
    return count_.load(std::memory_order_relaxed);
  }

  // 清空所有元素
  void Clear();

  // 检查是否为空
  bool Empty() const { return count_.load(std::memory_order_relaxed) == 0; }

  // 获取最大高度
  int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  // 范围查询
  template <typename Callback>
  void RangeQuery(const Key &start, const Key &end, Callback callback) const;

  // 迭代器类
  class Iterator {
  public:
    explicit Iterator(const SkipList *list);

    bool Valid() const { return node_ != nullptr && node_ != list_->head_; }

    const Key &key() const {
      assert(Valid());
      return node_->key;
    }

    const Value &value() const {
      assert(Valid());
      return node_->value;
    }

    void Next() {
      assert(Valid());
      node_ = node_->Next(0);
    }

    void Prev();

    void Seek(const Key &target);
    void SeekToFirst();
    void SeekToLast();

  private:
    const SkipList *list_;
    Node *node_;
  };

  // 节点结构
  struct Node {
    Key key;
    Value value;

    explicit Node(const Key &k, const Value &v, int height) : key(k), value(v) {
      assert(height > 0);
      for (int i = 0; i < height; i++) {
        next_[i].store(nullptr, std::memory_order_relaxed);
      }
    }

    Node *Next(int level) const {
      assert(level >= 0);
      return next_[level].load(std::memory_order_acquire);
    }

    void SetNext(int level, Node *node) {
      assert(level >= 0);
      next_[level].store(node, std::memory_order_release);
    }

  private:
    // 灵活数组成员，存储指向下一层节点的指针
    std::atomic<Node *> next_[1];

    friend class SkipList;
  };

private:
  enum { kMaxHeight = 12 };

  // 获取随机高度
  int GetRandomHeight();

  // 分配节点
  Node *AllocateNode(const Key &key, const Value &value, int height);

  // 释放节点
  void DeallocateNode(Node *node);

  // 查找前驱节点
  Node *FindGreaterOrEqual(const Key &key, Node **prev) const;
  Node *FindLessThan(const Key &key) const;
  Node *FindLast() const;

  // 验证参数
  void ValidateKey(const Key &key) const;

  const Comparator *const comparator_;
  Node *const head_;

  std::atomic<int> max_height_;
  std::atomic<size_t> count_;

  // 随机数生成器
  mutable std::mt19937 rnd_;
};

// 模板实现

template <typename Key, typename Value>
SkipList<Key, Value>::SkipList(const Comparator *comparator)
    : comparator_(comparator), head_(AllocateNode(Key{}, Value{}, kMaxHeight)),
      max_height_(1), count_(0), rnd_(0xdeadbeef) {
  assert(comparator_ != nullptr);
}

template <typename Key, typename Value> SkipList<Key, Value>::~SkipList() {
  Clear();
  DeallocateNode(head_);
}

template <typename Key, typename Value>
int SkipList<Key, Value>::GetRandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;

  while (height < kMaxHeight && ((rnd_() % kBranching) == 0)) {
    height++;
  }

  return height;
}

template <typename Key, typename Value>
typename SkipList<Key, Value>::Node *
SkipList<Key, Value>::AllocateNode(const Key &key, const Value &value,
                                   int height) {
  assert(height > 0 && height <= kMaxHeight);

  size_t node_size = sizeof(Node) + (height - 1) * sizeof(std::atomic<Node *>);

  char *mem = new char[node_size];
  return new (mem) Node(key, value, height);
}

template <typename Key, typename Value>
void SkipList<Key, Value>::DeallocateNode(Node *node) {
  if (node == nullptr)
    return;

  delete[] reinterpret_cast<char *>(node);
}

template <typename Key, typename Value>
void SkipList<Key, Value>::ValidateKey(const Key &key) const {
  // 可以在这里添加键的验证逻辑
  (void)key; // 避免未使用参数警告
}

template <typename Key, typename Value>
typename SkipList<Key, Value>::Node *
SkipList<Key, Value>::FindGreaterOrEqual(const Key &key, Node **prev) const {
  ValidateKey(key);

  Node *x = head_;
  int level = max_height_.load(std::memory_order_acquire) - 1;

  while (true) {
    Node *next = x->Next(level);

    if (next != nullptr) {
      // 对于std::string类型，直接比较字符串内容
      if constexpr (std::is_same_v<Key, std::string>) {
        if (next->key < key) {
          x = next;
          continue;
        }
      } else {
        // 对于其他类型，使用Slice比较
        if (comparator_->Compare(
                Slice(reinterpret_cast<const char *>(&next->key), sizeof(Key)),
                Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) < 0) {
          x = next;
          continue;
        }
      }
    }

    if (prev != nullptr) {
      prev[level] = x;
    }
    if (level == 0) {
      return next;
    } else {
      level--;
    }
  }
}

template <typename Key, typename Value>
typename SkipList<Key, Value>::Node *
SkipList<Key, Value>::FindLessThan(const Key &key) const {
  ValidateKey(key);

  Node *x = head_;
  int level = max_height_.load(std::memory_order_acquire) - 1;

  while (true) {
    Node *next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      bool should_continue = false;

      // 对于std::string类型，直接比较字符串内容
      if constexpr (std::is_same_v<Key, std::string>) {
        if (next->key < key) {
          x = next;
          should_continue = true;
        }
      } else {
        // 对于其他类型，使用Slice比较
        if (comparator_->Compare(
                Slice(reinterpret_cast<const char *>(&next->key), sizeof(Key)),
                Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) < 0) {
          x = next;
          should_continue = true;
        }
      }

      if (should_continue) {
        continue;
      }

      if (level == 0) {
        return x;
      } else {
        level--;
      }
    }
  }
}

template <typename Key, typename Value>
typename SkipList<Key, Value>::Node *SkipList<Key, Value>::FindLast() const {
  Node *x = head_;
  int level = max_height_.load(std::memory_order_acquire) - 1;

  while (true) {
    Node *next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, typename Value>
void SkipList<Key, Value>::Insert(const Key &key, const Value &value) {
  ValidateKey(key);

  Node *prev[kMaxHeight];
  Node *x = FindGreaterOrEqual(key, prev);

  // 如果键已存在，更新值
  if (x != nullptr) {
    bool key_exists = false;

    // 对于std::string类型，直接比较字符串内容
    if constexpr (std::is_same_v<Key, std::string>) {
      key_exists = (x->key == key);
    } else {
      // 对于其他类型，使用Slice比较
      key_exists =
          (comparator_->Compare(
               Slice(reinterpret_cast<const char *>(&x->key), sizeof(Key)),
               Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) == 0);
    }

    if (key_exists) {
      x->value = value;
      return;
    }
  }

  int height = GetRandomHeight();
  if (height > max_height_.load(std::memory_order_relaxed)) {
    for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
      prev[i] = head_;
    }
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = AllocateNode(key, value, height);
  for (int i = 0; i < height; i++) {
    x->SetNext(i, prev[i]->Next(i));
    prev[i]->SetNext(i, x);
  }

  count_.fetch_add(1, std::memory_order_relaxed);
}

// Merge 操作: 如果 key 不存在则插入; 如果存在则原地合并 value
template <typename Key, typename Value>
void SkipList<Key, Value>::Merge(const Key &key, const Value &value) {
  ValidateKey(key);

  Node *prev[kMaxHeight];
  Node *x = FindGreaterOrEqual(key, prev);

  bool key_exists = false;

  if (x != nullptr) {
    // 判断 key 是否相等
    if constexpr (std::is_same_v<Key, std::string>) {
      key_exists = (x->key == key);
    } else {
      key_exists =
          (comparator_->Compare(
               Slice(reinterpret_cast<const char *>(&x->key), sizeof(Key)),
               Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) == 0);
    }
  }

  if (key_exists) {
    // 对于指针类型 Value，默认覆盖为新值；非指针类型尝试使用 operator+=，否则覆盖
    if constexpr (std::is_pointer_v<Value>) {
      x->value = value;
    } else {
      x->value = value; // C++17: 直接覆盖，避免 requires 检查
    }
    return;
  }

  // 否则，走插入逻辑
  int height = GetRandomHeight();
  if (height > max_height_.load(std::memory_order_relaxed)) {
    for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
      prev[i] = head_;
    }
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = AllocateNode(key, value, height);
  for (int i = 0; i < height; i++) {
    x->SetNext(i, prev[i]->Next(i));
    prev[i]->SetNext(i, x);
  }

  count_.fetch_add(1, std::memory_order_relaxed);
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::Delete(const Key &key) {
  ValidateKey(key);

  Node *prev[kMaxHeight];
  // 初始化prev数组
  for (int i = 0; i < kMaxHeight; i++) {
    prev[i] = head_;
  }

  Node *x = FindGreaterOrEqual(key, prev);

  // 如果键不存在，返回false
  if (x == nullptr) {
    return false;
  }

  bool key_exists = false;

  // 对于std::string类型，直接比较字符串内容
  if constexpr (std::is_same_v<Key, std::string>) {
    key_exists = (x->key == key);
  } else {
    // 对于其他类型，使用Slice比较
    key_exists =
        (comparator_->Compare(
             Slice(reinterpret_cast<const char *>(&x->key), sizeof(Key)),
             Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) == 0);
  }

  if (!key_exists) {
    return false;
  }

  // 从所有层中删除节点
  for (int i = 0; i < kMaxHeight; i++) {
    if (prev[i]->Next(i) == x) {
      prev[i]->SetNext(i, x->Next(i));
    }
  }

  // 释放节点
  DeallocateNode(x);

  // 更新计数
  count_.fetch_sub(1, std::memory_order_relaxed);

  // 更新最大高度
  while (max_height_.load(std::memory_order_relaxed) > 1 &&
         head_->Next(max_height_.load(std::memory_order_relaxed) - 1) ==
             nullptr) {
    max_height_.fetch_sub(1, std::memory_order_relaxed);
  }

  return true;
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::Contains(const Key &key) const {
  ValidateKey(key);

  Node *x = FindGreaterOrEqual(key, nullptr);
  if (x == nullptr) {
    return false;
  }

  // 对于std::string类型，直接比较字符串内容
  if constexpr (std::is_same_v<Key, std::string>) {
    return (x->key == key);
  } else {
    // 对于其他类型，使用Slice比较
    return (comparator_->Compare(
                Slice(reinterpret_cast<const char *>(&x->key), sizeof(Key)),
                Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) == 0);
  }
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::Get(const Key &key, Value *value) const {
  ValidateKey(key);

  Node *x = FindGreaterOrEqual(key, nullptr);
  if (x == nullptr) {
    return false;
  }

  bool key_exists = false;

  // 对于std::string类型，直接比较字符串内容
  if constexpr (std::is_same_v<Key, std::string>) {
    key_exists = (x->key == key);
  } else {
    // 对于其他类型，使用Slice比较
    key_exists =
        (comparator_->Compare(
             Slice(reinterpret_cast<const char *>(&x->key), sizeof(Key)),
             Slice(reinterpret_cast<const char *>(&key), sizeof(Key))) == 0);
  }

  if (key_exists) {
    if (value) {
      *value = x->value;
    }
    return true;
  }
  return false;
}

template <typename Key, typename Value>
typename SkipList<Key, Value>::Node *SkipList<Key, Value>::GetLast() const {
  Node *x = FindLast();
  return (x == head_) ? nullptr : x;
}

template <typename Key, typename Value> void SkipList<Key, Value>::Clear() {
  Node *node = head_->Next(0);
  while (node != nullptr) {
    Node *next = node->Next(0);
    DeallocateNode(node);
    node = next;
  }

  // 重置头节点的所有指针
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }

  // 重置状态
  max_height_.store(1, std::memory_order_relaxed);
  count_.store(0, std::memory_order_relaxed);
}

template <typename Key, typename Value>
template <typename Callback>
void SkipList<Key, Value>::RangeQuery(const Key &start, const Key &end,
                                      Callback callback) const {
  ValidateKey(start);
  ValidateKey(end);

  Node *current = FindGreaterOrEqual(start, nullptr);
  while (current != nullptr) {
    bool should_continue = false;

    // 对于std::string类型，直接比较字符串内容
    if constexpr (std::is_same_v<Key, std::string>) {
      if (current->key <= end) {
        callback(current->key, current->value);
        should_continue = true;
      }
    } else {
      // 对于其他类型，使用Slice比较
      if (comparator_->Compare(
              Slice(reinterpret_cast<const char *>(&current->key), sizeof(Key)),
              Slice(reinterpret_cast<const char *>(&end), sizeof(Key))) <= 0) {
        callback(current->key, current->value);
        should_continue = true;
      }
    }

    if (!should_continue) {
      break;
    }

    current = current->Next(0);
  }
}

// 迭代器实现

template <typename Key, typename Value>
SkipList<Key, Value>::Iterator::Iterator(const SkipList *list)
    : list_(list), node_(nullptr) {
  assert(list_ != nullptr);
}

template <typename Key, typename Value>
void SkipList<Key, Value>::Iterator::Prev() {
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, typename Value>
void SkipList<Key, Value>::Iterator::Seek(const Key &target) {
  assert(list_ != nullptr);
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, typename Value>
void SkipList<Key, Value>::Iterator::SeekToFirst() {
  assert(list_ != nullptr);
  node_ = list_->head_->Next(0);
}

template <typename Key, typename Value>
void SkipList<Key, Value>::Iterator::SeekToLast() {
  assert(list_ != nullptr);
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

} // namespace lrdb