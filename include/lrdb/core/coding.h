// 编码解码工具

#pragma once

#include <cstdint>
#include <string>
#include "lrdb/core/slice.h"

namespace lrdb {

// 编码解码工具类
namespace coding {

// 整数编码/解码 - Little Endian

// 32位整数
void PutFixed32(std::string* dst, uint32_t value);
uint32_t DecodeFixed32(const char* ptr);
inline uint32_t DecodeFixed32(const Slice& slice) {
    return DecodeFixed32(slice.data());
}

// 64位整数  
void PutFixed64(std::string* dst, uint64_t value);
uint64_t DecodeFixed64(const char* ptr);
inline uint64_t DecodeFixed64(const Slice& slice) {
    return DecodeFixed64(slice.data());
}

// 变长整数编码(Varint)
void PutVarint32(std::string* dst, uint32_t value);
void PutVarint64(std::string* dst, uint64_t value);

bool GetVarint32(Slice* input, uint32_t* value);
bool GetVarint64(Slice* input, uint64_t* value);

// 获取varint编码长度
int VarintLength(uint64_t v);

// 长度前缀字符串编码
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);
bool GetLengthPrefixedSlice(Slice* input, Slice* result);

// 内部键编码
void PutInternalKey(std::string* dst, const Slice& user_key, 
                    uint64_t sequence, uint8_t type);
bool ParseInternalKey(const Slice& internal_key, Slice* user_key, 
                      uint64_t* sequence, uint8_t* type);

// CRC32校验
uint32_t CalculateCRC32(const char* data, size_t length);
inline uint32_t CalculateCRC32(const Slice& slice) {
    return CalculateCRC32(slice.data(), slice.size());
}

// 校验和相关
uint32_t Extend(uint32_t crc, const char* data, size_t n);
uint32_t Value(const char* data, size_t n);
uint32_t Mask(uint32_t crc);
uint32_t Unmask(uint32_t masked_crc);

}  // namespace coding

}  // namespace lrdb