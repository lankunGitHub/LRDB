// Slice字节片段

#pragma once

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace lrdb {

// 字节片段类
// 提供对连续内存区域的轻量级引用，不负责内存管理
class Slice {
public:
  // 构造函数
  Slice() : data_(""), size_(0) {}

  Slice(const char *d, size_t n) : data_(d), size_(n) {}

  Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

  Slice(const char *s) : data_(s), size_(strlen(s)) {}

  Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

  // 拷贝构造函数和赋值操作符使用默认实现
  Slice(const Slice &) = default;
  Slice &operator=(const Slice &) = default;

  // 获取数据
  const char *data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // 清空
  void clear() {
    data_ = "";
    size_ = 0;
  }

  // 移除前缀
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // 移除后缀
  void remove_suffix(size_t n) {
    assert(n <= size());
    size_ -= n;
  }

  // 转换为string
  std::string ToString() const { return std::string(data_, size_); }

  // 转换为string_view
  std::string_view ToStringView() const {
    return std::string_view(data_, size_);
  }

  // 比较函数
  int compare(const Slice &b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
      if (size_ < b.size_)
        r = -1;
      else if (size_ > b.size_)
        r = +1;
    }
    return r;
  }

  // 判断是否以prefix开头
  bool starts_with(const Slice &prefix) const {
    return ((size_ >= prefix.size_) &&
            (memcmp(data_, prefix.data_, prefix.size_) == 0));
  }

  // 判断是否以suffix结尾
  bool ends_with(const Slice &suffix) const {
    return ((size_ >= suffix.size_) &&
            (memcmp(data_ + size_ - suffix.size_, suffix.data_, suffix.size_) ==
             0));
  }

  // 获取子片段
  Slice substr(size_t start, size_t len = std::string::npos) const {
    assert(start <= size_);
    if (len == std::string::npos) {
      len = size_ - start;
    } else {
      len = std::min(len, size_ - start);
    }
    return Slice(data_ + start, len);
  }

  // 查找字符
  size_t find(char c, size_t start = 0) const {
    if (start >= size_)
      return std::string::npos;
    const char *result =
        static_cast<const char *>(memchr(data_ + start, c, size_ - start));
    return (result != nullptr) ? static_cast<size_t>(result - data_)
                               : std::string::npos;
  }

  // 查找子串
  size_t find(const Slice &needle, size_t start = 0) const {
    if (start > size_)
      return std::string::npos;
    if (needle.size_ == 0)
      return start;
    // needle 比剩余部分还长时 size_ - needle.size_ 会无符号下溢
    // （回绕成极大值 → 循环恒真 → memcmp 越界读），必须提前返回
    if (needle.size_ > size_ - start)
      return std::string::npos;

    for (size_t i = start; i <= size_ - needle.size_; ++i) {
      if (memcmp(data_ + i, needle.data_, needle.size_) == 0) {
        return i;
      }
    }
    return std::string::npos;
  }

  // 比较操作符
  bool operator==(const Slice &x) const {
    return ((size_ == x.size_) && (memcmp(data_, x.data_, size_) == 0));
  }

  bool operator!=(const Slice &x) const { return !(*this == x); }

  bool operator<(const Slice &x) const { return compare(x) < 0; }

  bool operator<=(const Slice &x) const { return compare(x) <= 0; }

  bool operator>(const Slice &x) const { return compare(x) > 0; }

  bool operator>=(const Slice &x) const { return compare(x) >= 0; }

  // 迭代器支持
  const char *begin() const { return data_; }
  const char *end() const { return data_ + size_; }
  const char *cbegin() const { return data_; }
  const char *cend() const { return data_ + size_; }

private:
  const char *data_;
  size_t size_;
};

// 全局操作符
inline std::ostream &operator<<(std::ostream &os, const Slice &slice) {
  os.write(slice.data(), static_cast<std::streamsize>(slice.size()));
  return os;
}

// 工具函数
namespace slice_util {

// 创建Slice的便捷函数
inline Slice SliceFromString(const std::string &s) { return Slice(s); }

inline Slice SliceFromCString(const char *s) { return Slice(s); }

inline Slice SliceFromBuffer(const char *data, size_t size) {
  return Slice(data, size);
}

// 比较函数对象
struct SliceComparator {
  bool operator()(const Slice &a, const Slice &b) const {
    return a.compare(b) < 0;
  }
};

// Hash函数对象
struct SliceHasher {
  std::size_t operator()(const Slice &slice) const {
    // 简单的hash实现，可以后续优化
    std::size_t h = 0;
    const char *data = slice.data();
    for (size_t i = 0; i < slice.size(); ++i) {
      h = h * 31 + static_cast<std::size_t>(data[i]);
    }
    return h;
  }
};

// 连接多个Slice
inline std::string ConcatenateSlices(const std::vector<Slice> &slices) {
  std::string result;
  size_t total_size = 0;
  for (const auto &slice : slices) {
    total_size += slice.size();
  }
  result.reserve(total_size);
  for (const auto &slice : slices) {
    result.append(slice.data(), slice.size());
  }
  return result;
}

} // namespace slice_util

} // namespace lrdb

// 为std::unordered_map提供hash特化
namespace std {
template <> struct hash<lrdb::Slice> {
  std::size_t operator()(const lrdb::Slice &slice) const {
    return lrdb::slice_util::SliceHasher{}(slice);
  }
};
} // namespace std