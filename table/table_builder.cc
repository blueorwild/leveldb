// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

#ifdef MZP
namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)) {
  }

  Options options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  std::string compressed_output;

  BlockBuilder data_block;
  FilterBlockBuilder* filter_block;
  IndexBlockBuilder index_block;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  // 新的 sstable 需要100字节的Header,先留出来
  rep_->status = rep_->file->MoveTo(options.header_size);
  rep_->offset = options.header_size;
  if (!rep_->status.ok()) {
    rep_->closed = true;
  }
}
void TableBuilder::MoveToEnd() {
  rep_->status = rep_->file->MoveToEnd(rep_->offset);   
  if (!rep_->status.ok()) {
    rep_->closed = true;
  }
}
TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  r->last_key.assign(key.data(), key.size());
  ++r->num_entries;
  r->data_block.Add(key, value);
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;

  r->index_block.SetKey(r->last_key);
  WriteBlock();
  if (ok()) {
    r->status = r->file->Flush();
    r->index_block.Add();
  }
}

void TableBuilder::WriteBlock() {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  //    filter_data: uint8[n]
  //    filter_offset: uint64
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = r->data_block.Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }

  r->index_block.SetOffset(r->offset);
  r->status = r->file->Append(block_contents);  // add data
  if (r->status.ok()) {
    // 注意，这里为了纯净只保存了data的size，没有考虑filter的
    // 也就是说filter的offset和size需要读取的时候通过下一个索引的计算
    r->index_block.SetBlockSize(block_contents.size());

    // add type and crc
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());  // data CRC
    crc = crc32c::Extend(crc, trailer, 1);  // type CRC
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }

    Slice filter_contents;
    if (r->filter_block != nullptr) {
      filter_contents = r->filter_block->Finish();
      r->status = r->file->Append(filter_contents);  // add filter
    }
    if (r->status.ok()) {
      // r->index_block.SetBlockSize(block_contents.size() + filter_contents.size() + kBlockTrailerSize);
      // add type and crc
      char trailer[kBlockTrailerSize];
      trailer[0] = kNoCompression;   // 过滤器也不压缩了，其实本来也没用snappy
      uint32_t crc = crc32c::Extend(crc, filter_contents.data(), filter_contents.size());  // filter CRC
      crc = crc32c::Extend(crc, trailer, 1);  // type crc
      EncodeFixed32(trailer + 1, crc32c::Mask(crc));
      r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
      if (r->status.ok()) {
        r->offset += filter_contents.size() + kBlockTrailerSize;
      }
    }
  }

  r->compressed_output.clear();
  r->data_block.Reset();
  if (r->filter_block != nullptr) {
    r->filter_block->Reset();
  }
  
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();

  assert(!r->closed);
  r->closed = true;

  Footer footer;
  std::string footer_count, footer_offset;

  // Write index block
  if (ok()) {
    Slice index_contents = r->index_block.Finish();
    r->status = r->file->Append(index_contents);  // add index
    if (r->status.ok()) {
      footer.SetIndexBlockOffset(r->offset);
      footer.SetIndexBlockSize(r->index_block.size());
      footer.SetIndexCount(r->index_block.GetIndexCount());
      footer.SetKvNum(r->num_entries);
      if (r->filter_block != nullptr) {
        std::string filter_name("filter.");
        filter_name += std::string(r->options.filter_policy->Name());
        footer.SetFilterName(filter_name);
      }
    
      // add type and crc
      char trailer[kBlockTrailerSize];
      trailer[0] = kNoCompression;   
      uint32_t crc = crc32c::Value(index_contents.data(), index_contents.size());  // data CRC
      crc = crc32c::Extend(crc, trailer, 1);  // type crc
      EncodeFixed32(trailer + 1, crc32c::Mask(crc));
      r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
      if (r->status.ok()) {
        r->offset += index_contents.size() + kBlockTrailerSize;
        PutFixed64(&footer_offset, r->offset);
      }
    }
  }

  // Write footer
  if (ok()) {
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->status = r->file->Flush();  // 这里得Flush一下，因为后面写Head是不同的文件偏移
      r->offset += footer_encoding.size();
    }
  }

  // Write Header
  if (ok()) {
    PutFixed32(&footer_count, 1);
    r->status = r->file->MoveTo(0);
    if (!ok()) return r->status;
    r->status = r->file->Append(Slice(footer_count));
    if (!ok()) return r->status;
    r->status = r->file->Append(Slice(footer_offset));
    if (!ok()) return r->status;
    r->status = r->file->Flush();
  }

  return r->status;
}

Status TableBuilder::AppendFinish(uint32_t sst_count) {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  Footer footer;
  std::string new_sst_count, new_offset; // 新的header的4字节count(覆盖)，8字节offset(追加)

  // Write index block
  if (ok()) {
    Slice index_contents = r->index_block.Finish();
    r->status = r->file->Append(index_contents);  // add filter
    if (r->status.ok()) {
      footer.SetIndexBlockOffset(r->offset);
      footer.SetIndexBlockSize(r->index_block.size());
      footer.SetIndexCount(r->index_block.GetIndexCount());
      footer.SetKvNum(r->num_entries);
      if (r->options.filter_policy != nullptr) {
        std::string filter_name("filter.");
        filter_name += std::string(r->options.filter_policy->Name());
        footer.SetFilterName(filter_name);
      }
      // add type and crc
      char trailer[kBlockTrailerSize];
      trailer[0] = kNoCompression;   // 新的方式indexblock不压缩了，其实本来也没用snappy
      uint32_t crc = crc32c::Value(index_contents.data(), index_contents.size());  // data CRC
      crc = crc32c::Extend(crc, trailer, 1);  // type crc
      EncodeFixed32(trailer + 1, crc32c::Mask(crc));
      r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
      if (r->status.ok()) {
        r->offset += index_contents.size() + kBlockTrailerSize;
        PutFixed64(&new_offset, r->offset);
      }
    }
  }

  // Write footer
  if (ok()) {
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->status = r->file->Flush();  // 这里得Flush一下，因为后面写Head是不同的文件偏移
      r->offset += footer_encoding.size();
    }
  }

  // Write Header
  if (ok()) {
    PutFixed32(&new_sst_count, sst_count + 1);
    r->status = r->file->MoveTo(0);
    if (ok()) {
      r->status = r->file->Append(Slice(new_sst_count));
      if (ok()) {
        r->status = r->file->Flush();
      }
    }
    
    if (ok()) {
      r->status = r->file->MoveTo(4 + sst_count * 8);
      if (ok()) {
        r->status = r->file->Append(Slice(new_offset));
        if (ok()) {
          r->status = r->file->Flush();
        }
      }
    }
    
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb

#else
namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb

#endif