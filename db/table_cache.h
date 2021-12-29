// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

class Env;

#ifdef MZP
class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  // 根据文件号和文件大小 尝试从缓存中找到该文件并返回缓存节点handle
  // 如果缓存中没有则新建该文件，放入缓存并返回handle。
#ifdef MZP
  // force是用于追加文件时强制更新缓存，默认在缓存中去找
  // 对于compaction 追加的情况，缓存应该更新，这时应该用true
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**,
                   bool force = false);
#else
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
#endif

  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);
  
  // 此函数用于释放FindTable返回的handle
  void ReleaseHandle(Cache::Handle* h);

 private:

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
};

#else
class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
};

#endif

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
