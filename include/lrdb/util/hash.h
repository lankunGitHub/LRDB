// 哈希函数工具

#pragma once

#include "lrdb/core/slice.h"
#include <cstdint>

namespace lrdb {

namespace hash_util {

// 32位哈希函数
uint32_t Hash32(const char* data, size_t length, uint32_t seed = 0);
uint32_t Hash32(const Slice& slice, uint32_t seed = 0);

// 64位哈希函数
uint64_t Hash64(const char* data, size_t length, uint64_t seed = 0);
uint64_t Hash64(const Slice& slice, uint64_t seed = 0);

// CRC32计算
uint32_t CRC32(const char* data, size_t length);
uint32_t CRC32(const Slice& slice);

// Murmur Hash3
uint32_t MurmurHash3_x86_32(const void* key, int len, uint32_t seed);
void MurmurHash3_x64_128(const void* key, const int len, const uint32_t seed, void* out);

// FNV Hash
uint32_t FNVHash32(const char* data, size_t length);
uint64_t FNVHash64(const char* data, size_t length);

// 城市哈希（简化版）
uint32_t CityHash32(const char* data, size_t length);
uint64_t CityHash64(const char* data, size_t length);

}  // namespace hash_util

}  // namespace lrdb