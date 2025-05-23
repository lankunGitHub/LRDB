// LRDB组件单元测试：编码、Bloom过滤器、比较器、哈希、跳表、
// MANIFEST/WAL/SSTable序列化往返、WAL压缩
// 与build.sh中的test_units对应

#include "lrdb/core/coding.h"
#include "lrdb/db/manifest.h"
#include "lrdb/storage/memtable.h"
#include "lrdb/storage/sstable.h"
#include "lrdb/util/bloom_filter.h"
#include "lrdb/util/comparator.h"
#include "lrdb/util/hash.h"
#include "lrdb/util/skiplist.h"
#include "lrdb/wal/wal.h"
#include "test_util.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// ---- 编码 ----

TEST(CodingVarint) {
    std::string buf;
    for (uint32_t v : {0u, 1u, 127u, 128u, 300u, UINT32_MAX}) {
        buf.clear();
        lrdb::coding::PutVarint32(&buf, v);
        lrdb::Slice input(buf);
        uint32_t decoded = 0;
        TEST_EXPECT(lrdb::coding::GetVarint32(&input, &decoded), "decode varint32");
        TEST_EXPECT_EQ(decoded, v);
    }

    for (uint64_t v : {0ULL, 1ULL, 300ULL, 4294967296ULL, 18446744073709551615ULL}) {
        buf.clear();
        lrdb::coding::PutVarint64(&buf, v);
        lrdb::Slice input(buf);
        uint64_t decoded = 0;
        TEST_EXPECT(lrdb::coding::GetVarint64(&input, &decoded), "decode varint64");
        TEST_EXPECT_EQ(decoded, v);
    }
}

TEST(CodingFixedAndSlices) {
    std::string buf;
    lrdb::coding::PutFixed32(&buf, 0x12345678);
    TEST_EXPECT_EQ(lrdb::coding::DecodeFixed32(buf.data()), 0x12345678u);

    lrdb::coding::PutFixed64(&buf, 0x1122334455667788ULL);
    TEST_EXPECT_EQ(lrdb::coding::DecodeFixed64(buf.data() + 4),
                   0x1122334455667788ULL);

    std::string slices;
    lrdb::coding::PutLengthPrefixedSlice(&slices, "abc");
    lrdb::coding::PutLengthPrefixedSlice(&slices, "de");
    lrdb::Slice input(slices);
    lrdb::Slice a, b;
    TEST_EXPECT(lrdb::coding::GetLengthPrefixedSlice(&input, &a), "parse slice a");
    TEST_EXPECT(lrdb::coding::GetLengthPrefixedSlice(&input, &b), "parse slice b");
    TEST_EXPECT_EQ(a.ToString(), std::string("abc"));
    TEST_EXPECT_EQ(b.ToString(), std::string("de"));
    TEST_EXPECT(input.empty(), "input should be consumed");
}

TEST(CodingInternalKey) {
    std::string buf;
    lrdb::coding::PutInternalKey(&buf, "user_key", 42, 1);

    lrdb::Slice user_key;
    uint64_t sequence = 0;
    uint8_t type = 0;
    TEST_EXPECT(lrdb::coding::ParseInternalKey(buf, &user_key, &sequence, &type),
                "parse internal key");
    TEST_EXPECT_EQ(user_key.ToString(), std::string("user_key"));
    TEST_EXPECT_EQ(sequence, 42u);
    TEST_EXPECT_EQ(type, 1u);
}

// ---- Bloom过滤器 ----

TEST(BloomFilterBasic) {
    auto bloom = lrdb::BloomFilter::Create(1000, 0.01);
    TEST_EXPECT(bloom != nullptr, "create bloom filter");

    for (int i = 0; i < 500; ++i) {
        bloom->Add("key" + std::to_string(i));
    }
    for (int i = 0; i < 500; ++i) {
        TEST_EXPECT(bloom->MayContain("key" + std::to_string(i)),
                    "bloom must contain inserted key");
    }
    // 误报率下未插入的键绝大多数应为否定
    int false_positives = 0;
    for (int i = 500; i < 1500; ++i) {
        if (bloom->MayContain("key" + std::to_string(i))) {
            ++false_positives;
        }
    }
    TEST_EXPECT(false_positives < 100, "bloom false positive rate too high");
}

// ---- 比较器 ----

TEST(ComparatorBytewise) {
    const lrdb::Comparator* cmp = lrdb::BytewiseComparator();
    TEST_EXPECT(cmp != nullptr, "bytewise comparator");
    TEST_EXPECT(cmp->Compare("abc", "abd") < 0, "abc < abd");
    TEST_EXPECT(cmp->Compare("abc", "abc") == 0, "abc == abc");
    TEST_EXPECT(cmp->Compare("abd", "abc") > 0, "abd > abc");
    TEST_EXPECT(cmp->Compare("", "a") < 0, "empty < a");
    TEST_EXPECT_EQ(cmp->Name(), std::string("lrdb.BytewiseComparator"));
}

// ---- 哈希 ----

TEST(HashCRC32) {
    uint32_t crc1 = lrdb::hash_util::CRC32("hello", 5);
    uint32_t crc2 = lrdb::hash_util::CRC32("hello", 5);
    TEST_EXPECT_EQ(crc1, crc2);
    uint32_t crc3 = lrdb::hash_util::CRC32("hellp", 5);
    TEST_EXPECT(crc1 != crc3, "different data should differ");
    // 标准CRC32("123456789") = 0xCBF43926
    TEST_EXPECT_EQ(lrdb::hash_util::CRC32("123456789", 9), 0xCBF43926u);
}

// ---- 跳表 ----

TEST(SkipListBasic) {
    lrdb::SkipList<int, std::string> list(lrdb::BytewiseComparator());
    for (int i = 0; i < 1000; i += 2) {
        list.Insert(i, "v" + std::to_string(i));
    }
    TEST_EXPECT(list.Contains(0), "contains 0");
    TEST_EXPECT(list.Contains(500), "contains 500");
    TEST_EXPECT(list.Contains(998), "contains 998");
    TEST_EXPECT(!list.Contains(1), "not contains 1");
    TEST_EXPECT(!list.Contains(999), "not contains 999");
    TEST_EXPECT_EQ(list.EstimateCount(), 500u);

    std::string value;
    TEST_EXPECT(list.Get(500, &value), "skiplist get");
    TEST_EXPECT_EQ(value, std::string("v500"));

    lrdb::SkipList<int, std::string>::Iterator it(&list);
    it.SeekToFirst();
    int count = 0;
    for (; it.Valid(); it.Next()) {
        ++count;
    }
    TEST_EXPECT_EQ(count, 500);
}

// ---- MemTable ----

TEST(MemTablePutGet) {
    lrdb::MemTable memtable(lrdb::BytewiseComparator());
    TEST_EXPECT(memtable.Put("mkey1", "mval1", 1).ok(), "memtable put");
    TEST_EXPECT(memtable.Put("mkey2", "mval2", 2).ok(), "memtable put");

    std::string value;
    lrdb::Status status;
    TEST_EXPECT(memtable.Get("mkey1", &value, &status, 0), "memtable get");
    TEST_EXPECT(status.ok() && value == "mval1", "memtable value");
    TEST_EXPECT(!memtable.Get("mkey3", &value, &status, 0), "memtable get miss");
    TEST_EXPECT(status.IsNotFound(), "memtable miss");
    TEST_EXPECT_EQ(memtable.NumEntries(), 2u);
}

// ---- MANIFEST序列化 ----

TEST(ManifestRoundTrip) {
    lrdb::ColumnFamilyOptions options;
    options.max_write_buffer_number = 4;
    options.level0_file_num_compaction_trigger = 8;
    options.compression = lrdb::CompressionType::kLZ4Compression;

    std::string encoded = lrdb::manifest_util::EncodeColumnFamilyOptions(options);
    lrdb::ColumnFamilyOptions decoded;
    TEST_EXPECT(lrdb::manifest_util::DecodeColumnFamilyOptions(encoded, &decoded).ok(),
                "decode options");
    TEST_EXPECT_EQ(decoded.max_write_buffer_number, 4);
    TEST_EXPECT_EQ(decoded.level0_file_num_compaction_trigger, 8);
    TEST_EXPECT(decoded.compression == lrdb::CompressionType::kLZ4Compression,
                "compression roundtrip");
}

TEST(ManifestFilename) {
    uint64_t number = 0;
    TEST_EXPECT(lrdb::manifest_util::ParseManifestFilename("MANIFEST-000042", &number).ok(),
                "parse manifest filename");
    TEST_EXPECT_EQ(number, 42u);
    TEST_EXPECT(lrdb::manifest_util::ParseManifestFilename("nope", &number).IsInvalidArgument(),
                "reject invalid filename");
    TEST_EXPECT_EQ(lrdb::manifest_util::FormatManifestFilename(7),
                   std::string("MANIFEST-000007"));
}

// ---- WAL序列化与压缩 ----

TEST(WALRecordRoundTrip) {
    lrdb::WALRecord record;
    record.type = lrdb::WALRecordType::kPut;
    record.sequence_number = 12345;
    record.column_family_id = 7;
    record.key = "wal_key";
    record.value = "wal_value";
    record.transaction_id = 99;
    record.crc = 0;

    std::string encoded = record.Encode();
    lrdb::WALRecord decoded;
    TEST_EXPECT(decoded.Decode(encoded).ok(), "decode wal record");
    TEST_EXPECT(decoded.type == lrdb::WALRecordType::kPut, "type roundtrip");
    TEST_EXPECT_EQ(decoded.sequence_number, 12345u);
    TEST_EXPECT_EQ(decoded.column_family_id, 7u);
    TEST_EXPECT_EQ(decoded.key, std::string("wal_key"));
    TEST_EXPECT_EQ(decoded.value, std::string("wal_value"));
    TEST_EXPECT_EQ(decoded.transaction_id, 99u);
}

TEST(WALRecordCRC) {
    lrdb::WALRecord record;
    record.type = lrdb::WALRecordType::kDelete;
    record.sequence_number = 1;
    record.column_family_id = 0;
    record.key = "crc_key";
    record.value = "crc_value";
    record.crc = record.CalculateCRC();
    TEST_EXPECT(record.ValidateCRC(), "valid crc");

    record.value = "tampered";
    TEST_EXPECT(!record.ValidateCRC(), "tampered crc should fail");
}

TEST(WALFilename) {
    uint64_t number = 0;
    TEST_EXPECT(lrdb::wal_util::ParseWALFilename("000123.wal", &number).ok(),
                "parse wal filename");
    TEST_EXPECT_EQ(number, 123u);
    TEST_EXPECT_EQ(lrdb::wal_util::FormatWALFilename(9), std::string("000009.wal"));
}

// ---- SSTable元数据序列化 ----

TEST(SSTableMetaRoundTrip) {
    lrdb::SSTableMeta meta;
    meta.filename = "/tmp/xx/sst_1_L0.sst";
    meta.file_number = 1;
    meta.file_size = 4096;
    meta.level = lrdb::SSTableLevel::kLevel0;
    meta.smallest_key = "aaa";
    meta.largest_key = "zzz";
    meta.num_entries = 100;
    meta.num_deletions = 3;
    meta.raw_key_size = 300;
    meta.raw_value_size = 900;
    meta.creation_time = 123456789;
    meta.oldest_key_time = 5;

    std::string encoded = meta.Encode();
    lrdb::SSTableMeta decoded;
    TEST_EXPECT(decoded.Decode(encoded), "decode meta");
    TEST_EXPECT_EQ(decoded.file_number, 1u);
    TEST_EXPECT_EQ(decoded.file_size, 4096u);
    TEST_EXPECT(decoded.level == lrdb::SSTableLevel::kLevel0, "level roundtrip");
    TEST_EXPECT_EQ(decoded.smallest_key, std::string("aaa"));
    TEST_EXPECT_EQ(decoded.largest_key, std::string("zzz"));
    TEST_EXPECT_EQ(decoded.num_entries, 100u);
    TEST_EXPECT_EQ(decoded.num_deletions, 3u);
    TEST_EXPECT_EQ(decoded.creation_time, 123456789u);
}

TEST(SSTableIndexEntryRoundTrip) {
    lrdb::IndexEntry entry;
    entry.key = "dk0";
    entry.block_offset = 1024;
    entry.block_size = 4096;
    entry.first_key_offset = 16;

    std::string encoded = entry.Encode();
    lrdb::IndexEntry decoded;
    TEST_EXPECT(decoded.Decode(encoded), "decode index entry");
    TEST_EXPECT_EQ(decoded.key, std::string("dk0"));
    TEST_EXPECT_EQ(decoded.block_offset, 1024u);
    TEST_EXPECT_EQ(decoded.block_size, 4096u);
    TEST_EXPECT_EQ(decoded.first_key_offset, 16u);
}

TEST(SSTableBlockHeaderRoundTrip) {
    lrdb::BlockHeader header;
    header.type = lrdb::BlockType::kDataBlock;
    header.size = 4096;
    header.crc32 = 0xDEADBEEF;
    header.compression = 0;

    std::string encoded = header.Encode();
    TEST_EXPECT_EQ(encoded.size(), static_cast<size_t>(lrdb::BlockHeader::kHeaderSize));
    lrdb::BlockHeader decoded;
    TEST_EXPECT(decoded.Decode(encoded), "decode header");
    TEST_EXPECT(decoded.type == lrdb::BlockType::kDataBlock, "type roundtrip");
    TEST_EXPECT_EQ(decoded.size, 4096u);
    TEST_EXPECT_EQ(decoded.crc32, 0xDEADBEEFu);
}

// ---- WAL压缩往返 ----

TEST(WALCompressionRoundTrip) {
    // 构造有重复性的数据
    std::string data;
    for (int i = 0; i < 100; ++i) {
        data += "aaaaaaaaaaaaaaaaaaaa";
        data += "value_" + std::to_string(i);
    }
    data += std::string(5000, 'z');

    const std::string file = "/tmp/lrdb_test_compress.wal";
    const std::string compressed = file + ".compressed";
    std::filesystem::remove(file);
    std::filesystem::remove(compressed);

    {
        std::ofstream out(file, std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
    }

    TEST_EXPECT(lrdb::wal_util::CompressWALFile(file).ok(), "compress wal file");
    TEST_EXPECT(std::filesystem::exists(compressed), "compressed file exists");

    std::error_code ec;
    std::filesystem::remove(file, ec);

    TEST_EXPECT(lrdb::wal_util::DecompressWALFile(compressed).ok(),
                "decompress wal file");
    TEST_EXPECT(std::filesystem::exists(file), "decompressed file exists");

    std::ifstream in(file, std::ios::binary);
    std::string restored((std::istreambuf_iterator<char>(in)),
                         std::istreambuf_iterator<char>());
    in.close();
    TEST_EXPECT_EQ(restored, data);

    std::filesystem::remove(file);
    std::filesystem::remove(compressed);
}

int main() {
    return lrdb_test::RunAllTests("LRDB unit tests");
}
