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

using std::string;
using std::cout;
using std::endl;

int main(){
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    options.filter_policy = leveldb::NewBloomFilterPolicy(4);
    options.create_if_missing = true;
    string db_path = "/root/leveldb_m/mzp_test/testdb";
    leveldb::Status s = leveldb::DB::Open(options, db_path, &db);
    if(!s.ok()){
        cout << s.ToString() << endl;
        return 0;
    }

    string key = "test_key";
    string value = "test_value";
    string get;

    for (int i = 1 ; i <= 150000; ++i) {
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

    if(!s.ok()){
        cout << s.ToString() << endl;
        return 0;
    }
    
    key = "test_key";
    for (int i = 1 ; i <= 150000; ++i) {
        key.resize(8);
        key += std::to_string(i);
        s = db->Get(leveldb::ReadOptions(), key, &get);
        if(!s.ok()){
            cout << s.ToString() << "  " << key << endl;
            return 0;
        }
    }
    // s = db->Get(leveldb::ReadOptions(), key, &get);
    // if(!s.ok()){
    //     cout << s.ToString() << endl;
    //     return 0;
    // }
    // cout << "find key:  "<< key << " value is: " << get << endl;
    // s = db->Write(leveldb::WriteOptions(), nullptr);  // try to force flush
    // sleep(3);
    
    return 0;
}