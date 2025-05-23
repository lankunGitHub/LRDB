// LRDB简单性能基准：顺序写入/随机读取/顺序迭代吞吐
// 用法: test_benchmark [条目数，默认100000]
// 与build.sh中的test_benchmark对应

#include "lrdb/lrdb.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>

namespace {

double NowSeconds() {
    return std::chrono::duration_cast<std::chrono::duration<double>>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

int main(int argc, char** argv) {
    int total = 100000;
    if (argc > 1) {
        total = std::atoi(argv[1]);
        if (total <= 0) total = 100000;
    }

    const std::string path = "/tmp/lrdb_test_benchmark";
    std::error_code ec;
    std::filesystem::remove_all(path, ec);

    std::printf("LRDB benchmark: %d entries\n", total);

    lrdb::DBOptions options;
    std::unique_ptr<lrdb::DB> db;
    lrdb::Status s = lrdb::DB::Open(options, path, &db);
    if (!s.ok()) {
        std::fprintf(stderr, "open failed: %s\n", s.ToString().c_str());
        return 1;
    }

    lrdb::WriteOptions wo;
    lrdb::ReadOptions ro;

    // 顺序写入
    double start = NowSeconds();
    for (int i = 0; i < total; ++i) {
        s = db->Put(wo, "bench_key_" + std::to_string(i),
                    "bench_value_" + std::to_string(i));
        if (!s.ok()) {
            std::fprintf(stderr, "put failed at %d: %s\n", i, s.ToString().c_str());
            return 1;
        }
    }
    double write_secs = NowSeconds() - start;
    std::printf("sequential write : %8.1f ops/s (%d ops in %.2fs)\n",
                write_secs > 0 ? total / write_secs : 0, total, write_secs);

    // 顺序读取
    start = NowSeconds();
    std::string value;
    for (int i = 0; i < total; ++i) {
        s = db->Get(ro, "bench_key_" + std::to_string(i), &value);
        if (!s.ok()) {
            std::fprintf(stderr, "get failed at %d: %s\n", i, s.ToString().c_str());
            return 1;
        }
    }
    double read_secs = NowSeconds() - start;
    std::printf("sequential read  : %8.1f ops/s (%d ops in %.2fs)\n",
                read_secs > 0 ? total / read_secs : 0, total, read_secs);

    // 随机读取
    start = NowSeconds();
    unsigned seed = 12345;
    for (int i = 0; i < total; ++i) {
        seed = seed * 1103515245 + 12345;
        int idx = static_cast<int>((seed >> 16) % static_cast<unsigned>(total));
        s = db->Get(ro, "bench_key_" + std::to_string(idx), &value);
        if (!s.ok()) {
            std::fprintf(stderr, "random get failed at %d: %s\n", i,
                         s.ToString().c_str());
            return 1;
        }
    }
    double rand_secs = NowSeconds() - start;
    std::printf("random read      : %8.1f ops/s (%d ops in %.2fs)\n",
                rand_secs > 0 ? total / rand_secs : 0, total, rand_secs);

    // 顺序迭代
    start = NowSeconds();
    int count = 0;
    std::unique_ptr<lrdb::Iterator> it(db->NewIterator(ro));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ++count;
    }
    if (!it->status().ok()) {
        std::fprintf(stderr, "iterator error: %s\n", it->status().ToString().c_str());
        return 1;
    }
    double iter_secs = NowSeconds() - start;
    std::printf("sequential scan  : %8.1f entries/s (%d entries in %.2fs)\n",
                iter_secs > 0 ? count / iter_secs : 0, count, iter_secs);

    s = db->Close();
    if (!s.ok()) {
        std::fprintf(stderr, "close failed: %s\n", s.ToString().c_str());
        return 1;
    }

    std::printf("benchmark done\n");
    return 0;
}
