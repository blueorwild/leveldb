// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb {

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;


class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
 public:
  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle() : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {};

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  uint64_t offset_;
  uint64_t size_;
};

#ifdef MZP
struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, 
                  uint64_t offset, uint64_t size, BlockContents* result);


// 新的sstable需要一个header,  包含4字节count， 和count个8字节的offset，
// 表示该sstable由count个旧sstable组成。 offset即旧sstable的Footer的偏移
// count 最大12个。  header 是定长100字节。
class Header {
 public:
  static const int kMaxOldSstableCount = 12;

  Header() {
    offset_.resize(kMaxOldSstableCount);
    count_ = 0;
  };

  size_t get_count() const { return count_; }
  uint64_t get_offset(size_t i) const { return offset_[i]; }
  
  // 每次只会往Header中添加一个，不需要append 和 Encode
  // void Append(uint64_t offset);
  // void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  std::vector<uint64_t> offset_;
  size_t count_;
};

// 新的Footer构成：
// index_block offset (v64) + index_block size(v32) + index_count(v32) + kv_num(v32)
// filter.name len(v32) + filter.name + padding + magic num
class Footer {
 public:
  static const int kFooterSize = 64;
  Footer() = default;

  void SetIndexBlockOffset(uint64_t offset) { index_block_offset_ = offset; };
  uint64_t GetIndexBlockOffset() const { return index_block_offset_; };

  void SetIndexBlockSize(size_t size) { index_block_size_ = size; };
  uint32_t GetIndexBlockSize() const { return index_block_size_; };

  void SetIndexCount(size_t count) { index_count_ = count; };
  uint32_t GetIndexCount() const { return index_count_; };

  void SetKvNum(size_t num) { kv_num_ = num; };
  uint32_t GetKvNum() const { return kv_num_; };

  void SetFilterName(std::string& key) { filter_name_ = key; };
  std::string GetFilterName() const { return filter_name_; };

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  uint64_t index_block_offset_;
  uint32_t index_block_size_;
  uint32_t index_count_;
  uint32_t kv_num_;
  std::string filter_name_;
};

#else
struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result);

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
 public:
  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

  Footer() = default;

  // The block handle for the metaindex block of the table
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
};

#endif
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_
