// LRDB基本使用示例

#include "lrdb/lrdb.h"
#include <iostream>
#include <string>
#include <cassert>

int main() {
    std::cout << "LRDB Basic Usage Example" << std::endl;
    std::cout << "Version: " << lrdb::Version::GetFullVersionString() << std::endl;
    
    // 数据库选项
    lrdb::DBOptions db_options;
    std::string db_path = "/tmp/lrdb_test";
    
    std::unique_ptr<lrdb::DB> db;
    lrdb::Status status = lrdb::DB::Open(db_options, db_path, &db);
    
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Database opened successfully!" << std::endl;
    
    // 基本写入操作
    lrdb::WriteOptions write_options;
    status = db->Put(write_options, "key1", "value1");
    if (!status.ok()) {
        std::cerr << "Failed to put key1: " << status.ToString() << std::endl;
        return 1;
    }
    
    status = db->Put(write_options, "key2", "value2");
    if (!status.ok()) {
        std::cerr << "Failed to put key2: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Data written successfully!" << std::endl;
    
    // 基本读取操作
    lrdb::ReadOptions read_options;
    std::string value;
    
    status = db->Get(read_options, "key1", &value);
    if (!status.ok()) {
        std::cerr << "Failed to get key1: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "key1 = " << value << std::endl;
    assert(value == "value1");
    
    status = db->Get(read_options, "key2", &value);
    if (!status.ok()) {
        std::cerr << "Failed to get key2: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "key2 = " << value << std::endl;
    assert(value == "value2");
    
    // 测试不存在的键
    status = db->Get(read_options, "nonexistent", &value);
    if (status.IsNotFound()) {
        std::cout << "Correctly returned NotFound for nonexistent key" << std::endl;
    } else {
        std::cerr << "Unexpected status for nonexistent key: " << status.ToString() << std::endl;
        return 1;
    }
    
    // 删除操作
    status = db->Delete(write_options, "key1");
    if (!status.ok()) {
        std::cerr << "Failed to delete key1: " << status.ToString() << std::endl;
        return 1;
    }
    
    // 验证删除
    status = db->Get(read_options, "key1", &value);
    if (status.IsNotFound()) {
        std::cout << "key1 deleted successfully" << std::endl;
    } else {
        std::cerr << "key1 was not deleted properly: " << status.ToString() << std::endl;
        return 1;
    }
    
    // 创建迭代器测试
    std::unique_ptr<lrdb::Iterator> iter(db->NewIterator(read_options));
    std::cout << "Iterating through remaining keys:" << std::endl;
    
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::cout << "  " << iter->key().ToString() << " = " << iter->value().ToString() << std::endl;
    }
    
    if (!iter->status().ok()) {
        std::cerr << "Iterator error: " << iter->status().ToString() << std::endl;
        return 1;
    }
    
    // 创建快照测试
    const lrdb::Snapshot* snapshot = db->GetSnapshot();
    if (snapshot) {
        std::cout << "Snapshot created successfully" << std::endl;
        db->ReleaseSnapshot(snapshot);
        std::cout << "Snapshot released" << std::endl;
    }
    
    // 关闭数据库
    status = db->Close();
    if (!status.ok()) {
        std::cerr << "Failed to close database: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Database closed successfully!" << std::endl;
    std::cout << "All tests passed!" << std::endl;
    
    return 0;
}
