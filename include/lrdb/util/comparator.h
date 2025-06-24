// 比较器接口定义

#pragma once

#include "lrdb/core/slice.h"
#include <memory>
#include <string>
#include <functional>

namespace lrdb {

// 比较器接口
class Comparator {
public:
    virtual ~Comparator() = default;
    
    // 返回比较器的名称
    virtual const char* Name() const = 0;
    
    // 比较两个slice，返回值：
    // < 0  如果 a < b
    // == 0 如果 a == b  
    // > 0  如果 a > b
    virtual int Compare(const Slice& a, const Slice& b) const = 0;
    
    // 寻找位于start和limit之间的最短分隔符
    // 修改*start为一个短字符串，使得:
    //   start <= *start < limit (如果原start < limit)
    // 简化实现可以不做任何修改
    virtual void FindShortestSeparator(std::string* start, 
                                      const Slice& limit) const = 0;
    
    // 寻找大于key的最短后继字符串
    // 修改*key为一个短字符串，使得:
    //   *key >= 原key
    // 简化实现可以不做任何修改  
    virtual void FindShortSuccessor(std::string* key) const = 0;
    
    // 便利方法
    bool Equal(const Slice& a, const Slice& b) const {
        return Compare(a, b) == 0;
    }
    
    bool LessThan(const Slice& a, const Slice& b) const {
        return Compare(a, b) < 0;
    }
    
    bool GreaterThan(const Slice& a, const Slice& b) const {
        return Compare(a, b) > 0;
    }
};

// 内置比较器

// 字节比较器 - 按字典序比较
const Comparator* BytewiseComparator();

// 反向字节比较器 - 反向字典序
const Comparator* ReverseBytewiseComparator();

// 数值比较器 - 将前4/8字节解释为uint32_t/uint64_t进行比较
const Comparator* NumericComparator32();
const Comparator* NumericComparator64();

// 自定义比较器类型定义
using CompareFunction = std::function<int(const Slice&, const Slice&)>;

// 自定义比较器基类
class CustomComparator : public Comparator {
public:
    CustomComparator(const std::string& name, CompareFunction compare_func);
    ~CustomComparator() override = default;
    
    const char* Name() const override;
    int Compare(const Slice& a, const Slice& b) const override;
    void FindShortestSeparator(std::string* start, const Slice& limit) const override;
    void FindShortSuccessor(std::string* key) const override;
    
private:
    std::string name_;
    CompareFunction compare_func_;
};

// 工厂函数

// 创建自定义比较器
std::unique_ptr<Comparator> CreateCustomComparator(const std::string& name,
                                                  CompareFunction compare_func);

// 创建前缀比较器（只比较前N个字节）
std::unique_ptr<Comparator> CreatePrefixComparator(size_t prefix_length);

// 比较器工具函数
namespace comparator_util {

// 检查两个比较器是否兼容
inline bool IsCompatible(const Comparator* a, const Comparator* b) {
    if (a == b) return true;
    if (!a || !b) return false;
    return std::string(a->Name()) == std::string(b->Name());
}

} // namespace comparator_util

} // namespace lrdb