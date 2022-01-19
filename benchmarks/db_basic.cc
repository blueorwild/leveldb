#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "db/db_impl.h"

using std::string;
using std::cout;
using std::endl;

int main(){
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    // options.filter_policy = leveldb::NewBloomFilterPolicy(4);
    options.create_if_missing = true;
    string db_path = "/root/leveldb_m/mzp_test/testdb";
    leveldb::Status s = leveldb::DB::Open(options, db_path, &db);
    if(!s.ok()){
        cout << s.ToString() << endl;
        return 0;
    }
    // write
    cout << "start 100000 write" << endl;
    string key = "test_key";
    string value = "test_value";
    
    for (int i = 1 ; i <= 100000; ++i) {
        key.resize(8);
        value.resize(10);
        key += std::to_string(i);
        value += std::to_string(i);
        s = db->Put(leveldb::WriteOptions(), key, value);
        if(!s.ok()){
            cout << s.ToString() << endl;
            return 0;
        }
    }
    s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    sleep(3);
    leveldb::Print();

    for (int i = 1 ; i <= 100000; ++i) {
        key.resize(8);
        value.resize(10);
        key += std::to_string(i);
        value += std::to_string(i);
        s = db->Put(leveldb::WriteOptions(), key, value);
        if(!s.ok()){
            cout << s.ToString() << endl;
            return 0;
        }
    }
    s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    sleep(5);
    leveldb::Print();

    for (int i = 1 ; i <= 300000; ++i) {
        key.resize(8);
        value.resize(10);
        key += std::to_string(i);
        value += std::to_string(i);
        s = db->Put(leveldb::WriteOptions(), key, value);
        if(!s.ok()){
            cout << s.ToString() << endl;
            return 0;
        }
    }
    s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    sleep(8);
    leveldb::Print();

    for (int i = 1 ; i <= 500000; ++i) {
        key.resize(8);
        value.resize(10);
        key += std::to_string(i);
        value += std::to_string(i);
        s = db->Put(leveldb::WriteOptions(), key, value);
        if(!s.ok()){
            cout << s.ToString() << endl;
            return 0;
        }
    }
    s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    sleep(10);
    leveldb::Print();

    for (int i = 1 ; i <= 1000000; ++i) {
        key.resize(8);
        value.resize(10);
        key += std::to_string(i);
        value += std::to_string(i);
        s = db->Put(leveldb::WriteOptions(), key, value);
        if(!s.ok()){
            cout << s.ToString() << endl;
            return 0;
        }
    }
    s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    sleep(15);
    leveldb::Print();
    
    // s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    // sleep(3);

    // read
    // cout << "start 150000 read" << endl;
    // string get;
    // string key = "test_key";
    // int count1 = 0;
    // for (int i = 1 ; i <= 150000; ++i) {
    //     key.resize(8);
    //     key += std::to_string(i);
    //     s = db->Get(leveldb::ReadOptions(), key, &get);
    //     if(s.ok()){
    //         ++count1;
    //     }
    // }

    // delete
    // cout << "start 50000 delete" << endl;
    // key = "test_key";
    // for (int i = 1 ; i <= 50000; ++i) {
    //     key.resize(8);
    //     key += std::to_string(i);
    //     s = db->Delete(leveldb::WriteOptions(), key);
    //     if(!s.ok()){
    //         cout << s.ToString() << endl;
    //         return 0;
    //     }
    // }
    // s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    // sleep(3);

    // read
    // cout << "start 150000 read" << endl;
    // key = "test_key";
    // int count2 = 0;
    // for (int i = 1 ; i <= 150000; ++i) {
    //     key.resize(8);
    //     key += std::to_string(i);
    //     s = db->Get(leveldb::ReadOptions(), key, &get);
    //     if(s.ok()){
    //         ++count2;
    //     }
    // }

    // cout << "find key count:  "<< count1 << endl;
    
    
    return 0;
}