// LRDB基础功能测试：覆盖开库、读写、删除、迭代、快照、持久化与列族
// 与build.sh中的test_basic对应

#include "lrdb/lrdb.h"
#include "test_util.h"

#include <filesystem>
#include <string>
#include <vector>

namespace {

const std::string kTestPath = "/tmp/lrdb_test_basic";

void CleanupTestPath() {
    std::error_code ec;
    std::filesystem::remove_all(kTestPath, ec);
}

std::unique_ptr<lrdb::DB> OpenDB() {
    lrdb::DBOptions options;
    std::unique_ptr<lrdb::DB> db;
    lrdb::Status s = lrdb::DB::Open(options, kTestPath, &db);
    if (!s.ok()) {
        throw std::runtime_error("Failed to open DB: " + s.ToString());
    }
    return db;
}

}  // namespace

TEST(PutGetDelete) {
    CleanupTestPath();
    auto db = OpenDB();

    lrdb::WriteOptions wo;
    lrdb::ReadOptions ro;

    // Put
    TEST_EXPECT(db->Put(wo, "hello", "world").ok(), "put should succeed");
    TEST_EXPECT(db->Put(wo, "foo", "bar").ok(), "put should succeed");

    // Get
    std::string value;
    lrdb::Status s = db->Get(ro, "hello", &value);
    TEST_EXPECT(s.ok(), "get should succeed");
    TEST_EXPECT_EQ(value, std::string("world"));

    // Get不存在的键
    s = db->Get(ro, "not_exist", &value);
    TEST_EXPECT(s.IsNotFound(), "get missing key should be NotFound");

    // Delete
    TEST_EXPECT(db->Delete(wo, "hello").ok(), "delete should succeed");
    s = db->Get(ro, "hello", &value);
    TEST_EXPECT(s.IsNotFound(), "deleted key should be NotFound");

    // 其他键不受影响
    s = db->Get(ro, "foo", &value);
    TEST_EXPECT(s.ok() && value == "bar", "other key should remain");

    TEST_EXPECT(db->Close().ok(), "close should succeed");
}

TEST(Iterate) {
    CleanupTestPath();
    auto db = OpenDB();

    lrdb::WriteOptions wo;
    std::vector<std::string> keys;
    for (int i = 0; i < 100; ++i) {
        // 零填充保证字典序与数值序一致
        char buf[32];
        std::snprintf(buf, sizeof(buf), "iter_%03d", i);
        std::string key = buf;
        TEST_EXPECT(db->Put(wo, key, "v" + std::to_string(i)).ok(),
                    "put should succeed");
        keys.push_back(key);
    }

    // 顺序迭代
    std::vector<std::string> iterated;
    std::unique_ptr<lrdb::Iterator> it(db->NewIterator(lrdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        iterated.push_back(it->key().ToString());
    }
    TEST_EXPECT(it->status().ok(), "iterator status should be ok");
    TEST_EXPECT_EQ(iterated.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        TEST_EXPECT_EQ(iterated[i], keys[i]);
    }

    // Seek定位
    it->Seek("iter_050");
    TEST_EXPECT(it->Valid(), "seek should be valid");
    TEST_EXPECT_EQ(it->key().ToString(), std::string("iter_050"));

    TEST_EXPECT(db->Close().ok(), "close should succeed");
}

TEST(PersistenceReopen) {
    CleanupTestPath();
    {
        auto db = OpenDB();
        lrdb::WriteOptions wo;
        for (int i = 0; i < 500; ++i) {
            TEST_EXPECT(
                db->Put(wo, "persist_" + std::to_string(i),
                        "pv" + std::to_string(i)).ok(),
                "put should succeed");
        }
        // 部分删除
        for (int i = 0; i < 100; ++i) {
            TEST_EXPECT(db->Delete(wo, "persist_" + std::to_string(i)).ok(),
                        "delete should succeed");
        }
        TEST_EXPECT(db->Close().ok(), "close should succeed");
    }

    {
        auto db = OpenDB();
        lrdb::ReadOptions ro;
        std::string value;

        for (int i = 0; i < 100; ++i) {
            lrdb::Status s = db->Get(ro, "persist_" + std::to_string(i), &value);
            TEST_EXPECT(s.IsNotFound(), "deleted key should stay deleted");
        }
        for (int i = 100; i < 500; ++i) {
            lrdb::Status s = db->Get(ro, "persist_" + std::to_string(i), &value);
            TEST_EXPECT(s.ok() && value == "pv" + std::to_string(i),
                        "persisted key should survive reopen");
        }

        int count = 0;
        std::unique_ptr<lrdb::Iterator> it(db->NewIterator(ro));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            ++count;
        }
        TEST_EXPECT_EQ(count, 400);
        TEST_EXPECT(db->Close().ok(), "close should succeed");
    }
}

TEST(Snapshot) {
    CleanupTestPath();
    auto db = OpenDB();

    lrdb::WriteOptions wo;
    TEST_EXPECT(db->Put(wo, "snap_key", "v1").ok(), "put should succeed");

    const lrdb::Snapshot* snapshot = db->GetSnapshot();
    TEST_EXPECT(snapshot != nullptr, "snapshot should be created");

    // 快照后更新
    TEST_EXPECT(db->Put(wo, "snap_key", "v2").ok(), "update should succeed");

    // 通过快照读取旧值
    lrdb::ReadOptions snapshot_read;
    snapshot_read.snapshot = snapshot;
    std::string value;
    lrdb::Status s = db->Get(snapshot_read, "snap_key", &value);
    TEST_EXPECT(s.ok() && value == "v1", "snapshot should see old value");

    // 普通读取看到新值
    s = db->Get(lrdb::ReadOptions(), "snap_key", &value);
    TEST_EXPECT(s.ok() && value == "v2", "normal read should see new value");

    db->ReleaseSnapshot(snapshot);
    TEST_EXPECT(db->Close().ok(), "close should succeed");
}

TEST(ColumnFamily) {
    CleanupTestPath();
    auto db = OpenDB();

    lrdb::ColumnFamilyOptions cf_options;
    cf_options.comparator = lrdb::BytewiseComparator();
    lrdb::ColumnFamily* cf = nullptr;
    lrdb::Status s = db->CreateColumnFamily(cf_options, "extra", &cf);
    TEST_EXPECT(s.ok() && cf != nullptr, "create column family should succeed");

    // 列族隔离
    lrdb::WriteOptions wo;
    TEST_EXPECT(db->Put(wo, "shared_key", "default_value").ok(),
                "default cf put");
    TEST_EXPECT(db->Put(wo, cf, "shared_key", "extra_value").ok(),
                "extra cf put");

    lrdb::ReadOptions ro;
    std::string value;
    s = db->Get(ro, "shared_key", &value);
    TEST_EXPECT(s.ok() && value == "default_value",
                "default cf should read its own value");
    s = db->Get(ro, cf, "shared_key", &value);
    TEST_EXPECT(s.ok() && value == "extra_value",
                "extra cf should read its own value");

    // ListColumnFamilies（实例方法）
    auto names = db->ListColumnFamilies();
    TEST_EXPECT(names.size() == 2, "should have two column families");

    TEST_EXPECT(db->Close().ok(), "close should succeed");
}

TEST(TransactionCommitRollback) {
    CleanupTestPath();
    auto db = OpenDB();

    // 提交的事务生效
    {
        auto txn = db->BeginTransaction();
        TEST_EXPECT(txn != nullptr, "begin transaction");
        TEST_EXPECT(txn->Put("tx_key1", "tx_v1").ok(), "txn put");
        TEST_EXPECT(txn->Put("tx_key2", "tx_v2").ok(), "txn put");
        TEST_EXPECT(txn->Commit().ok(), "txn commit");
    }

    lrdb::ReadOptions ro;
    std::string value;
    TEST_EXPECT(db->Get(ro, "tx_key1", &value).ok() && value == "tx_v1",
                "committed key1 visible");
    TEST_EXPECT(db->Get(ro, "tx_key2", &value).ok() && value == "tx_v2",
                "committed key2 visible");

    // 回滚的事务不生效
    {
        auto txn = db->BeginTransaction();
        TEST_EXPECT(txn->Put("tx_key3", "tx_v3").ok(), "txn put");
        TEST_EXPECT(txn->Rollback().ok(), "txn rollback");
    }
    TEST_EXPECT(db->Get(ro, "tx_key3", &value).IsNotFound(),
                "rolled back key should not exist");

    TEST_EXPECT(db->Close().ok(), "close should succeed");
}

int main() {
    CleanupTestPath();
    return lrdb_test::RunAllTests("LRDB basic tests");
}
