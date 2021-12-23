// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {
#ifdef MZP
struct Table::Rep {
  ~Rep() {
    for (auto &i : *index_blocks) {
      delete i;
    }
    if (options.filter_policy != nullptr) {
      for (auto &i : *filters) {
        delete i;
      }
      delete filters;
    }
    delete index_blocks;
    delete kv_nums;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;

  // 一个新sst可能由多个旧sst构成,所以index是vector
  // 因为过滤器现在跟data在一起，所以过滤器是二维数组
  std::vector<IndexBlock*>* index_blocks;
  std::vector<size_t>* kv_nums; // per old_sst, 为了提前准备空间用
  std::vector<FilterBlockReader* >* filters;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  uint64_t header_size = Header::kMaxOldSstableCount * 8 + 4;
  if (size < Footer::kFooterSize + header_size) {
    return Status::Corruption("file is too short to be an sstable");
  }

  // 读 header
  char header_space[header_size];
  Slice header_input;
  std::cout << "Open 1" << std::endl;
  Status s = file->Read(0, header_size, &header_input, header_space, true);  // 安全读
  if (!s.ok()) return s;
  Header header;
  std::cout << "Open 2" << std::endl;
  s = header.DecodeFrom(&header_input);
  if (!s.ok()) return s;

  // 读 footer
  size_t footer_count = header.get_count();
  uint64_t footer_offset;

  char footer_space[Footer::kFooterSize];
  Slice footer_input;
  Footer footer;
  std::vector<IndexBlock*> tmp_index_blocks(footer_count);
  std::vector<size_t> tmp_kv_nums(footer_count);
  std::vector<FilterBlockReader* > tmp_filters(footer_count);

  for (int i = 0; i < footer_count; ++i){
    footer_offset = header.get_offset(i);
    std::cout << "Open 3" << footer_offset << std::endl;
    Status s = file->Read(footer_offset, Footer::kFooterSize, &footer_input, footer_space);
    if (!s.ok()) return s;

    std::cout << "Open 4" << std::endl;
    s = footer.DecodeFrom(&footer_input);
    if (!s.ok()) return s;

    tmp_kv_nums[i] = footer.GetKvNum();

    // 读 index_block
    BlockContents index_block_input;
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    std::cout << "Open 5: " << footer.GetIndexBlockOffset() << " "
              << footer.GetIndexBlockSize() << " " 
              << footer.GetIndexCount() << std::endl;
    s = ReadBlock(file, opt, footer.GetIndexBlockOffset(),
                  footer.GetIndexBlockSize(), &index_block_input);
    IndexBlock* index_block = new IndexBlock(index_block_input, footer.GetIndexCount());
    if (!index_block->vaild()) {
      std::cout << "bad indexBlock" << std::endl;
      s = Status::Corruption("bad indexBlock");
      delete index_block;
    }
    if (!s.ok()) {
      for (int j = 0; j < i; ++j) {
        delete tmp_index_blocks[j];
        delete tmp_filters[j];
      }
      return s;
    }
    // 现在安全了, 然后读过滤器
    tmp_index_blocks[i] = index_block;
    ReadFilter(options, file, index_block, footer.GetIndexBlockOffset(), &tmp_filters[i]);
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->index_blocks = new std::vector<IndexBlock*>(tmp_index_blocks);
    rep->kv_nums = new std::vector<size_t>(tmp_kv_nums);
    if (options.filter_policy != nullptr) {
      rep->filters = new std::vector<FilterBlockReader*>(tmp_filters);
    } else {
      rep->filters = nullptr;
    }
    *table = new Table(rep);
  }
  return s;
}

void Table::ReadFilter(const Options& options, RandomAccessFile* file,
                       IndexBlock* index_block, uint64_t index_block_offset,
                       FilterBlockReader** filter) {
  // 遍历索引块数组，根据每个索引块计算多个filter的offset和size。
  if (options.filter_policy == nullptr) {
    return;
  }
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  std::vector<Slice> *filter_data = new std::vector<Slice>(index_block->GetIndexCount());
  int i = 0;
  // filter 没有单独保存offset和size，需要计算
  uint64_t last_offset, cur_offset, tmp_offset;
  size_t last_size, cur_size, tmp_size;
  BlockContents block;
  index_block->SeekToFirst();
  index_block->Current(last_offset, last_size);
  index_block->Next();

  while (index_block->IterVaild()) {
    index_block->Current(cur_offset, cur_size);
    tmp_offset = last_offset + last_size + kBlockTrailerSize;
    tmp_size = cur_offset - tmp_offset - kBlockTrailerSize;
    if (!(ReadBlock(file, opt, tmp_offset, tmp_size, &block).ok())) {
      return;
    }
    (*filter_data)[i++] = block.data;

    last_offset = cur_offset;
    last_size = cur_size;
    index_block->Next();
  }
  tmp_offset = last_offset + last_size + kBlockTrailerSize;
  tmp_size = index_block_offset - tmp_offset - kBlockTrailerSize;
  if (!(ReadBlock(file, opt, tmp_offset, tmp_size, &block).ok())) {
    return;
  }
  (*filter_data)[i] = block.data;
  *filter = new FilterBlockReader(options.filter_policy, filter_data); 
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// 麻蛋，这里直接把所有block读出来存到一个vector里就好了(不排序了，提供多路归并的迭代器读)。
// 专用于compaction，本来就需要全量排序, 单点查找的后面单独写方法。
class Table::Iter : public Iterator {
 private:
  // kv_相当于是多个内部有序的kv数组
  // 现在要通过迭代器以整体有序的方式访问，维护两级索引即可，一级表示在哪个kv数组，二级就是每个数组一个索引
  std::vector<std::vector<std::pair<Slice, Slice> >* > kv_;
  size_t first_index_;
  std::vector<size_t> second_index_;

  const Comparator* comparator_;
  Status status_;
  Table* table_;

 public:
  Iter(Table* table, const ReadOptions& options) : table_(table) {
    comparator_ = table_->rep_->options.comparator;
    InitAndRead(options);
    first_index_ = -1;
    second_index_ = std::vector<size_t>(kv_.size(), 0);
  };
  ~Iter() {
    for (auto &i : kv_) {
      if (i != nullptr) {
        for (auto &j : *i) {
          delete [] j.first.data();
        }
        delete i;
      }
    }
  }

  bool Valid() const override { 
    if (first_index_ < 0 || first_index_ >= kv_.size()) {
      return false;
    }

    for (int i = 0; i < second_index_.size(); ++i) {
      if (second_index_[i] < kv_[i]->size()) {
        return true;
      }
    }
    return false;
  };

  Status status() const override { return status_; }

  Slice key() const override {
    assert(Valid());
    return (*kv_[first_index_])[second_index_[first_index_]].first;
  }

  Slice value() const override {
    assert(Valid());
    return (*kv_[first_index_])[second_index_[first_index_]].second;
  }

  void SeekToFirst() override {
    first_index_ = 0;
    for (auto &i : second_index_) {
      i = 0;
    }
    FindSmallest();
  }

  void Next() override {
    assert(Valid());
    second_index_[first_index_] += 1;
    FindSmallest();
  }

  // 暂时不需要
  void Prev() override {}
  void Seek(const Slice& target) override {}
  void SeekToLast() override {}

 private:
  void InitAndRead(const ReadOptions& options) {
    Rep *rep = table_->rep_;
    size_t old_sst_count = rep->index_blocks->size();
    kv_.resize(old_sst_count);
    for (int i = 0; i < old_sst_count; ++i) {
      // 初始化大小，避免频繁扩张拷贝，也是Footer中特意存储kv_num的原因
      kv_[i] = new std::vector<std::pair<Slice, Slice> >((*(rep->kv_nums))[i]);

      // 然后每个index块对应的多个data块解析出来，放进上面new的vector里
      Cache* block_cache = rep->options.block_cache;
      Cache::Handle* cache_handle = nullptr;
      Block* block = nullptr;
      Status s;

      IndexBlock* index_iter = (*(rep->index_blocks))[i];  // 伪迭代器哈
      uint64_t data_block_offset = 0;
      size_t data_block_size = 0;
      index_iter->SeekToFirst();
      size_t cur_index = 0;
      while (index_iter->IterVaild()) {
        index_iter->Current(data_block_offset, data_block_size);
        index_iter->Next();

        if (block_cache != nullptr) {
          // 此表有过缓存，看看具体的data block在不在缓存，在的话就直接用
          char cache_key_buffer[16];
          EncodeFixed64(cache_key_buffer, rep->cache_id);
          EncodeFixed64(cache_key_buffer + 8, data_block_offset);
          Slice key(cache_key_buffer, 16);
          cache_handle = block_cache->Lookup(key);
          if (cache_handle != nullptr) {
            block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
          }
        }
        if (block == nullptr) {
          // 可能是表没缓存，也可能是表有但具体的数据块被冲掉了
          BlockContents contents;
          s = ReadBlock(rep->file, options, data_block_offset, data_block_size, &contents);
          if (s.ok()) {
            block = new Block(contents);
          }
        }

        // 现在数据块拿到了，然后就逐条解析出来放到kv[i]里
        // 因为一个索引块对应的肯定是有序的，所以追加就行
        if (block != nullptr) {
          block->SeqReadKV(*kv_[i], cur_index);
          // 清理读取解析block过程中的内存和引用计数
          // 现在已经保存在kv_中了，与底层无关了。
          if (cache_handle == nullptr) {
            DeleteBlock(block, nullptr);
          } else {
            ReleaseBlock(block_cache, cache_handle);
          }
          block = nullptr;  // 即使delete 了也要赋值，否则会影响下次循环的判断
        } else {
          rep->status = Status::Corruption("create table iter, but InitRead fail, block not exist");
          return;
        }
      }
      assert(cur_index == (*(rep->kv_nums))[i]);
    }
  }
  
  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  void FindSmallest() {
    int tmp_index = -1;
    for (int i = 0; i < second_index_.size(); ++i) {
      if (second_index_[i] < (*kv_[i]).size()) {
        if (tmp_index < 0 || 
            Compare((*kv_[i])[second_index_[i]].first, (*kv_[i])[second_index_[tmp_index]].first) < 0) {
          tmp_index = i;
        }
      }
    }
    first_index_ = tmp_index;
  }
};

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return new Iter(const_cast<Table*>(this), options);
}

Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             uint64_t offset, size_t size) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;
  Status s;
  BlockContents contents;
  
  if (block_cache != nullptr) {
    char cache_key_buffer[16];
    EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
    EncodeFixed64(cache_key_buffer + 8, offset);
    Slice key(cache_key_buffer, sizeof(cache_key_buffer));
    cache_handle = block_cache->Lookup(key);
    if (cache_handle != nullptr) {
      block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
    } else {
      s = ReadBlock(table->rep_->file, options, offset, size, &contents);
      if (s.ok()) {
        block = new Block(contents);
        if (contents.cachable && options.fill_cache) {
          cache_handle = block_cache->Insert(key, block, block->size(),
                                              &DeleteCachedBlock);
        }
      }
    }
  } else {
    s = ReadBlock(table->rep_->file, options, offset, size, &contents);
    if (s.ok()) {
      block = new Block(contents);
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
  // 原来的方式文件单纯，封装的也比较好，所以就是先index迭代器，然后datablockReader就搞定了。
  // 现在需要遍历多个index
  Status s;
  auto cmp = rep_->options.comparator;
  uint64_t offset;
  size_t size;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  // std::cout << rep_->index_blocks->size() << std::endl;
  for (int i = 0; i < rep_->index_blocks->size(); ++i) {
    int j = (*(rep_->index_blocks))[i]->Seek(cmp, k, offset, size);
    if (j >= 0) {
      // 如果某个索引中存在，先读过滤器
      if (rep_->filters == nullptr || (*(rep_->filters))[i]->KeyMayMatch(k, j)) {
        // 过滤器不存在或者存在且找到，则去数据块中找
        // 先去可能存在的缓存看看
        Iterator* block_iter = BlockReader(this, options, offset, size);
        block_iter->Seek(k);
        if (block_iter->Valid()) {
          (*handle_result)(arg, block_iter->key(), block_iter->value());
        }
        s = block_iter->status();
        if (block_iter->Valid()) {
          delete block_iter;
          break;
        }
        delete block_iter;
      }
    }
  }
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  // 暂时无用
  return 0;
}
#else
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

#endif
}  // namespace leveldb
