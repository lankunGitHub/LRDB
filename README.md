# LRDB

LRDB 是一个用 C++17 从零实现的嵌入式键值数据库，整体架构参考 LevelDB/RocksDB，但存储引擎与并发控制为独立设计。支持多列族、MVCC 事务、WAL 崩溃恢复、LSM-Tree 分层存储与快照读。

## 特性

- **LSM-Tree 存储引擎**：MemTable（跳表实现）+ SSTable（数据块/索引块/Bloom 过滤器/元数据块），L0 分层，写满自动切换，支持手动刷盘与压缩
- **MVCC 事务**：RC / RR 隔离级别，版本链管理提交可见性，读集校验（RR），提交时分配全局单调递增的快照序列号
- **WAL 预写日志**：写入先落日志再进 MemTable，崩溃后按记录顺序重放并清理未提交事务，干净关闭时截断日志避免重复回放
- **多列族**：每个列族独立的 WAL、LSM-Tree 与事务管理器，列族元数据持久化到 MANIFEST
- **快照读**：基于序列号的快照，可读到创建快照时的一致性视图
- **持久化**：SSTable 带 CRC 校验与 Footer 索引，重开数据库自动加载已有 SSTable
- **后台任务**：MVCC → LSM 刷回、版本链 GC 等周期任务由后台线程池调度

## 构建

依赖：CMake ≥ 3.10、支持 C++17 的编译器

```bash
./build.sh            # Release 构建
./build.sh -d         # Debug 构建
./build.sh -c -r -t   # 清理后构建并跑测试
```

构建产物：

- `build/liblrdb.a` — 静态库
- `build/test/test_basic` — 基础功能测试（读写删、迭代、快照、列族、事务、持久化）
- `build/test/test_units` — 组件单元测试（编码、Bloom、跳表、MemTable、MANIFEST/WAL/SSTable 序列化、压缩）
- `build/test/test_benchmark` — 性能基准（`./build/test/test_benchmark 100000`）
- `build/examples/basic_usage` — 基本用法示例

## 快速上手

```cpp
#include "lrdb/lrdb.h"

lrdb::DBOptions options;
std::unique_ptr<lrdb::DB> db;
lrdb::Status s = lrdb::DB::Open(options, "/tmp/my_db", &db);

db->Put(lrdb::WriteOptions(), "hello", "world");

std::string value;
s = db->Get(lrdb::ReadOptions(), "hello", &value);

// 迭代
std::unique_ptr<lrdb::Iterator> it(db->NewIterator(lrdb::ReadOptions()));
for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // it->key(), it->value()
}

// 事务
auto txn = db->BeginTransaction();
txn->Put("k1", "v1");
txn->Delete("k2");
txn->Commit();

// 快照
const lrdb::Snapshot* snap = db->GetSnapshot();
lrdb::ReadOptions snap_read;
snap_read.snapshot = snap;
db->Get(snap_read, "hello", &value);
db->ReleaseSnapshot(snap);

db->Close();
```

## 目录结构

```
include/lrdb/
  core/        编码、Slice、Status
  storage/     MemTable、SSTable、LSMTree
  db/          DB 接口、列族、MANIFEST、迭代器、选项
  concurrency/ MVCC、事务、锁管理器、版本链、序列号
  wal/         WAL 写读与恢复
  util/        跳表、Bloom、比较器、哈希、环境、日志
  background/  后台任务管理器
src/           对应实现
test/           测试与基准
examples/       示例
```

## 设计要点

- **写路径**：Put/Delete → 事务（MVCC Active 版本）→ Commit → 分配快照序列号 → WAL 落盘 → 标记提交；后台任务周期性把已提交版本刷回 LSM，关闭时全量刷回并落盘
- **读路径**：优先查版本链可见版本（事务内未提交版本可见），未命中按快照序列号查 LSM（MemTable → SSTable）
- **内部键**：`用户键 + 8字节(序列号<<8 | 类型)`，SSTable 与 WAL 重放均按此编码，对外接口始终暴露用户键
- **崩溃恢复**：打开时 WAL 管理器扫描并重放日志到 LSM 恢复缓冲，完成后刷盘为 L0 SSTable、推进序列号、删除已重放日志，未提交事务自动回滚

## 许可

本项目仅用于学习与实验。
