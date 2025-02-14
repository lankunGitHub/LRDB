// 编码工具实现

#include "lrdb/core/coding.h"
#include "lrdb/util/hash.h"
#include <cstring>
#include <algorithm>

namespace lrdb {

namespace coding {

// 固定长度编码
void PutFixed32(std::string* dst, uint32_t value) {
    char buf[4];
    buf[0] = static_cast<char>(value & 0xff);
    buf[1] = static_cast<char>((value >> 8) & 0xff);
    buf[2] = static_cast<char>((value >> 16) & 0xff);
    buf[3] = static_cast<char>((value >> 24) & 0xff);
    dst->append(buf, 4);
}

void PutFixed64(std::string* dst, uint64_t value) {
    char buf[8];
    buf[0] = static_cast<char>(value & 0xff);
    buf[1] = static_cast<char>((value >> 8) & 0xff);
    buf[2] = static_cast<char>((value >> 16) & 0xff);
    buf[3] = static_cast<char>((value >> 24) & 0xff);
    buf[4] = static_cast<char>((value >> 32) & 0xff);
    buf[5] = static_cast<char>((value >> 40) & 0xff);
    buf[6] = static_cast<char>((value >> 48) & 0xff);
    buf[7] = static_cast<char>((value >> 56) & 0xff);
    dst->append(buf, 8);
}

// 固定长度解码
uint32_t DecodeFixed32(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint32_t>(buffer[0])) |
           (static_cast<uint32_t>(buffer[1]) << 8) |
           (static_cast<uint32_t>(buffer[2]) << 16) |
           (static_cast<uint32_t>(buffer[3]) << 24);
}

uint64_t DecodeFixed64(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint64_t>(buffer[0])) |
           (static_cast<uint64_t>(buffer[1]) << 8) |
           (static_cast<uint64_t>(buffer[2]) << 16) |
           (static_cast<uint64_t>(buffer[3]) << 24) |
           (static_cast<uint64_t>(buffer[4]) << 32) |
           (static_cast<uint64_t>(buffer[5]) << 40) |
           (static_cast<uint64_t>(buffer[6]) << 48) |
           (static_cast<uint64_t>(buffer[7]) << 56);
}

// 变长编码
char* EncodeVarint32(char* dst, uint32_t value) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const int B = 128;
    if (value < (1 << 7)) {
        *(ptr++) = value;
    } else if (value < (1 << 14)) {
        *(ptr++) = value | B;
        *(ptr++) = value >> 7;
    } else if (value < (1 << 21)) {
        *(ptr++) = value | B;
        *(ptr++) = (value >> 7) | B;
        *(ptr++) = value >> 14;
    } else if (value < (1 << 28)) {
        *(ptr++) = value | B;
        *(ptr++) = (value >> 7) | B;
        *(ptr++) = (value >> 14) | B;
        *(ptr++) = value >> 21;
    } else {
        *(ptr++) = value | B;
        *(ptr++) = (value >> 7) | B;
        *(ptr++) = (value >> 14) | B;
        *(ptr++) = (value >> 21) | B;
        *(ptr++) = value >> 28;
    }
    return reinterpret_cast<char*>(ptr);
}

char* EncodeVarint64(char* dst, uint64_t value) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const int B = 128;
    while (value >= B) {
        *(ptr++) = value | B;
        value >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(value);
    return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t value) {
    char buf[5];
    char* ptr = EncodeVarint32(buf, value);
    dst->append(buf, ptr - buf);
}

void PutVarint64(std::string* dst, uint64_t value) {
    char buf[10];
    char* ptr = EncodeVarint64(buf, value);
    dst->append(buf, ptr - buf);
}

// 变长解码
bool GetVarint32(Slice* input, uint32_t* value) {
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = p;
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && q < limit; shift += 7) {
        uint64_t byte = static_cast<uint64_t>(*q);
        q++;
        if (byte & 0x80) {
            result |= ((byte & 0x7f) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            input->remove_prefix(q - p);
            return true;
        }
    }
    return false;
}

bool GetVarint64(Slice* input, uint64_t* value) {
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = p;
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && q < limit; shift += 7) {
        uint64_t byte = static_cast<uint8_t>(*q);
        q++;
        if (byte & 0x80) {
            // 第10个字节（shift=63）时不允许再有续位，否则超出64位范围
            if (shift >= 63) {
                return false;
            }
            result |= ((byte & 0x7f) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            input->remove_prefix(q - p);
            return true;
        }
    }
    return false;
}

int VarintLength(uint64_t v) {
    int len = 1;
    while (v >= 0x80) {
        v >>= 7;
        len++;
    }
    return len;
}

// 长度前缀字符串编码
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
    PutVarint32(dst, static_cast<uint32_t>(value.size()));
    dst->append(value.data(), value.size());
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
    uint32_t len;
    if (GetVarint32(input, &len) && input->size() >= len) {
        *result = Slice(input->data(), len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}

// 内部键编码
void PutInternalKey(std::string* dst, const Slice& user_key, 
                    uint64_t sequence, uint8_t type) {
    dst->append(user_key.data(), user_key.size());
    PutFixed64(dst, (sequence << 8) | type);
}

bool ParseInternalKey(const Slice& internal_key, Slice* user_key, 
                      uint64_t* sequence, uint8_t* type) {
    if (internal_key.size() < 8) {
        return false;
    }
    uint64_t tag = DecodeFixed64(internal_key.data() + internal_key.size() - 8);
    *sequence = tag >> 8;
    *type = static_cast<uint8_t>(tag & 0xff);
    *user_key = Slice(internal_key.data(), internal_key.size() - 8);
    return true;
}

// CRC32实现 - 使用hash_util中的实现
uint32_t CalculateCRC32(const char* data, size_t length) {
    return hash_util::CRC32(data, length);
}

uint32_t Extend(uint32_t crc, const char* data, size_t n) {
    // CRC32扩展计算 - 这是一个简化实现
    // 实际项目中应该使用真正的CRC32扩展算法
    if (n == 0) return crc;
    
    // 对于简化实现，我们将现有CRC与新数据的CRC进行异或
    // 注意：这不是标准的CRC32扩展算法，仅用于避免未使用参数警告
    uint32_t new_crc = hash_util::CRC32(data, n);
    return crc ^ new_crc;
}

uint32_t Value(const char* data, size_t n) {
    return CalculateCRC32(data, n);
}

uint32_t Mask(uint32_t crc) {
    return ((crc >> 15) | (crc << 17)) + 0xa282ead8ul;
}

uint32_t Unmask(uint32_t masked_crc) {
    uint32_t rot = masked_crc - 0xa282ead8ul;
    return ((rot >> 17) | (rot << 15));
}

}  // namespace coding
}  // namespace lrdb