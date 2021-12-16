// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
};

#ifdef MZP
class IndexBlockBuilder {
 public:
  IndexBlockBuilder() : index_count_(0), finished_(false) {};

  IndexBlockBuilder(const IndexBlockBuilder&) = delete;
  IndexBlockBuilder& operator=(const IndexBlockBuilder&) = delete;

  // Reset the contents as if the IndexBlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  void Add();

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns current (uncompressed) size of the block
  size_t size() const { return buffer_.size(); };

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); };

  void SetKey (const Slice& key) { tmp_key_ = Slice(key.data(), key.size()); };
  void SetOffset (uint64_t offset) { tmp_offset_ = offset; };
  void SetBlockSize (size_t block_size) { tmp_block_size_ = block_size; };

  size_t GetIndexCount() const { return index_count_; };

 private:
  Slice tmp_key_;
  uint64_t tmp_offset_;
  size_t tmp_block_size_;
  size_t index_count_;

  std::string buffer_;            // Destination buffer
  bool finished_;                 // Has Finish() been called?
};
#endif

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
