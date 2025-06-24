// 比较器实现

#include "lrdb/util/comparator.h"
#include <cstring>
#include <cstdint>
#include <limits>
#include <algorithm>

namespace lrdb {

// 字节比较器实现
class BytewiseComparatorImpl : public Comparator {
public:
    BytewiseComparatorImpl() = default;
    ~BytewiseComparatorImpl() override = default;
    
    const char* Name() const override {
        return "lrdb.BytewiseComparator";
    }
    
    int Compare(const Slice& a, const Slice& b) const override {
        size_t min_len = std::min(a.size(), b.size());
        int result = std::memcmp(a.data(), b.data(), min_len);
        
        if (result == 0) {
            if (a.size() < b.size()) {
                return -1;
            } else if (a.size() > b.size()) {
                return 1;
            }
        }
        return result;
    }
    
    void FindShortestSeparator(std::string* start, const Slice& limit) const override {
        // 寻找最短的字符串，使其大于start但小于limit
        size_t min_len = std::min(start->size(), limit.size());
        size_t diff_index = 0;
        
        while (diff_index < min_len && (*start)[diff_index] == limit[diff_index]) {
            diff_index++;
        }
        
        if (diff_index >= min_len) {
            // start是limit的前缀，无法优化
        } else {
            uint8_t byte = static_cast<uint8_t>((*start)[diff_index]);
            if (byte < 0xff && byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
                (*start)[diff_index]++;
                start->resize(diff_index + 1);
            }
        }
    }
    
    void FindShortSuccessor(std::string* key) const override {
        // 寻找大于key的最小字符串
        size_t n = key->size();
        for (size_t i = 0; i < n; i++) {
            const uint8_t byte = static_cast<uint8_t>((*key)[i]);
            if (byte != 0xff) {
                (*key)[i] = static_cast<char>(byte + 1);
                key->resize(i + 1);
                return;
            }
        }
        // 所有字节都是0xff，无法找到后继
    }
};

// 反向比较器实现
class ReverseBytewiseComparatorImpl : public Comparator {
public:
    ReverseBytewiseComparatorImpl() = default;
    ~ReverseBytewiseComparatorImpl() override = default;
    
    const char* Name() const override {
        return "lrdb.ReverseBytewiseComparator";
    }
    
    int Compare(const Slice& a, const Slice& b) const override {
        // 反向比较
        return -BytewiseComparator()->Compare(a, b);
    }
    
    void FindShortestSeparator(std::string* start, const Slice& limit) const override {
        // 反向操作
        std::string tmp = limit.ToString();
        BytewiseComparator()->FindShortestSeparator(&tmp, Slice(*start));
        *start = tmp;
    }
    
    void FindShortSuccessor(std::string* key) const override {
        // 反向查找前驱
        std::string tmp = *key;
        BytewiseComparator()->FindShortSuccessor(&tmp);
        
        // 寻找前驱
        if (!tmp.empty()) {
            size_t n = tmp.size();
            for (size_t i = 0; i < n; i++) {
                uint8_t byte = static_cast<uint8_t>(tmp[i]);
                if (byte > 0) {
                    tmp[i] = static_cast<char>(byte - 1);
                    // 后续字节设为0xff
                    for (size_t j = i + 1; j < n; j++) {
                        tmp[j] = static_cast<char>(0xff);
                    }
                    break;
                }
            }
        }
        *key = tmp;
    }
};

// 数值比较器实现（将键解释为固定长度的数值）
template<typename T>
class NumericComparatorImpl : public Comparator {
public:
    NumericComparatorImpl() = default;
    ~NumericComparatorImpl() override = default;
    
    const char* Name() const override {
        if (sizeof(T) == 4) {
            return "lrdb.Numeric32Comparator";
        } else if (sizeof(T) == 8) {
            return "lrdb.Numeric64Comparator";
        } else {
            return "lrdb.NumericComparator";
        }
    }
    
    int Compare(const Slice& a, const Slice& b) const override {
        if (a.size() < sizeof(T) || b.size() < sizeof(T)) {
            // 如果长度不足，按字节比较
            return BytewiseComparator()->Compare(a, b);
        }
        
        T val_a = *reinterpret_cast<const T*>(a.data());
        T val_b = *reinterpret_cast<const T*>(b.data());
        
        if (val_a < val_b) return -1;
        if (val_a > val_b) return 1;
        
        // 值相等，比较剩余部分
        if (a.size() == sizeof(T) && b.size() == sizeof(T)) {
            return 0;
        }
        
        Slice remaining_a = Slice(a.data() + sizeof(T), a.size() - sizeof(T));
        Slice remaining_b = Slice(b.data() + sizeof(T), b.size() - sizeof(T));
        return BytewiseComparator()->Compare(remaining_a, remaining_b);
    }
    
    void FindShortestSeparator(std::string* start, const Slice& limit) const override {
        if (start->size() >= sizeof(T) && limit.size() >= sizeof(T)) {
            T val_start = *reinterpret_cast<const T*>(start->data());
            T val_limit = *reinterpret_cast<const T*>(limit.data());
            
            if (val_start < val_limit && val_start + 1 < val_limit) {
                // 可以在数值层面优化
                T separator = val_start + 1;
                *start = std::string(reinterpret_cast<const char*>(&separator), sizeof(T));
                return;
            }
        }
        
        // 回退到字节比较
        BytewiseComparator()->FindShortestSeparator(start, limit);
    }
    
    void FindShortSuccessor(std::string* key) const override {
        if (key->size() >= sizeof(T)) {
            T val = *reinterpret_cast<const T*>(key->data());
            if (val < std::numeric_limits<T>::max()) {
                T successor = val + 1;
                *key = std::string(reinterpret_cast<const char*>(&successor), sizeof(T));
                return;
            }
        }
        
        // 回退到字节比较
        BytewiseComparator()->FindShortSuccessor(key);
    }
};

// 单例实例
static BytewiseComparatorImpl bytewise_comparator;
static ReverseBytewiseComparatorImpl reverse_bytewise_comparator;
static NumericComparatorImpl<uint32_t> numeric32_comparator;
static NumericComparatorImpl<uint64_t> numeric64_comparator;

// 公共接口
const Comparator* BytewiseComparator() {
    return &bytewise_comparator;
}

const Comparator* ReverseBytewiseComparator() {
    return &reverse_bytewise_comparator;
}

const Comparator* NumericComparator32() {
    return &numeric32_comparator;
}

const Comparator* NumericComparator64() {
    return &numeric64_comparator;
}

// 自定义比较器基类实现
CustomComparator::CustomComparator(const std::string& name, CompareFunction compare_func)
    : name_(name), compare_func_(compare_func) {
}

const char* CustomComparator::Name() const {
    return name_.c_str();
}

int CustomComparator::Compare(const Slice& a, const Slice& b) const {
    return compare_func_(a, b);
}

void CustomComparator::FindShortestSeparator(std::string* start, const Slice& limit) const {
    // 默认实现，回退到字节比较
    BytewiseComparator()->FindShortestSeparator(start, limit);
}

void CustomComparator::FindShortSuccessor(std::string* key) const {
    // 默认实现，回退到字节比较
    BytewiseComparator()->FindShortSuccessor(key);
}

// 工厂函数
std::unique_ptr<Comparator> CreateCustomComparator(const std::string& name, 
                                                  CompareFunction compare_func) {
    return std::make_unique<CustomComparator>(name, compare_func);
}

// 前缀提取比较器
class PrefixComparatorImpl : public Comparator {
public:
    explicit PrefixComparatorImpl(size_t prefix_len)
        : name_("lrdb.PrefixComparator." + std::to_string(prefix_len)),
          prefix_len_(prefix_len) {}
    ~PrefixComparatorImpl() override = default;

    const char* Name() const override {
        // 名字必须是实例成员：函数级 static 会被所有实例共享，
        // prefix_len 不同的比较器返回同一个名字，按名比较时被误判为相同
        return name_.c_str();
    }
    
    int Compare(const Slice& a, const Slice& b) const override {
        Slice prefix_a = Slice(a.data(), std::min(a.size(), prefix_len_));
        Slice prefix_b = Slice(b.data(), std::min(b.size(), prefix_len_));
        return BytewiseComparator()->Compare(prefix_a, prefix_b);
    }

private:
    std::string name_;
    
    void FindShortestSeparator(std::string* start, const Slice& limit) const override {
        if (start->size() > prefix_len_) {
            start->resize(prefix_len_);
        }
        
        std::string limit_prefix = limit.substr(0, std::min(limit.size(), prefix_len_)).ToString();
        BytewiseComparator()->FindShortestSeparator(start, Slice(limit_prefix));
    }
    
    void FindShortSuccessor(std::string* key) const override {
        if (key->size() > prefix_len_) {
            key->resize(prefix_len_);
        }
        BytewiseComparator()->FindShortSuccessor(key);
    }
    
private:
    size_t prefix_len_;
};

std::unique_ptr<Comparator> CreatePrefixComparator(size_t prefix_length) {
    return std::make_unique<PrefixComparatorImpl>(prefix_length);
}

} // namespace lrdb