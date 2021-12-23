// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

#ifdef MZP
  // 专用于data block,不想用迭代器了，直接依次读到传进来的容器里。
  void SeqReadKV(std::vector<std::pair<Slice, Slice> > &kv, size_t &index);
#endif

 private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
};

#ifdef MZP
class IndexBlock {
 public:
  // Initialize the block with the specified contents and count for verify.
  IndexBlock(const BlockContents& contents, size_t index_count);

  IndexBlock(const IndexBlock&) = delete;
  IndexBlock& operator=(const IndexBlock&) = delete;

  ~IndexBlock();

  bool vaild() { return index_count_ > 0; };
  size_t GetIndexCount() { return index_count_; };

  // 伪迭代器
  void SeekToFirst() { current_ = 0; };
  void SeekToLast() { current_ = index_count_ - 1; };
  const Slice& Current(uint64_t &offset, size_t &size);
  int Seek(const Comparator* comparator, const Slice &key, uint64_t &offset, size_t &size);
  void Next() { ++current_; };
  bool IterVaild() { return current_ < index_count_; };

 private:

  std::vector<Slice> keys_;
  std::vector<uint64_t> offsets_;
  std::vector<uint32_t> sizes_;

  size_t index_count_;
  const char* data_;
  bool owned_;               // Block owns data_[]

  size_t current_;   // 伪迭代器专用
};

#endif

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
