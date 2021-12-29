// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>
#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

// TableCache* cache = reinterpret_cast<TableCache*>(arg);
//   if (file_value.size() != 16) {
//     return NewErrorIterator(
//         Status::Corruption("FileReader invoked with unexpected value"));
//   } else {
//     return cache->NewIterator(options, DecodeFixed64(file_value.data()),
//                               DecodeFixed64(file_value.data() + 8));
//   }
// 初始化下层迭代器的函数
// void*是迭代器的真实数据位置（如cache），Slice 就是构建迭代器的数据块信息
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);
#ifdef MZP
class TwoLevelIterator : public Iterator {
 // 串联迭代器，就是顺序访问多个迭代器，访问到哪个才会按需初始化
 private:
  int cur_index_;
  Iterator* data_iter_;

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  std::vector<Slice> iter_info_;

 public:
  TwoLevelIterator(std::vector<Slice> &iter_info, BlockFunction block_function,
                   void* arg, const ReadOptions& options) : cur_index_(0),
                   data_iter_(nullptr), block_function_(block_function),
                   arg_(arg), options_(options), iter_info_(iter_info) {
    // iter_info_ = std::vector<Slice>(iter_info.begin(), iter_info.end());
  }

  ~TwoLevelIterator() {
    for (auto &info : iter_info_) {
      delete[] info.data();
    }
  }

  Slice key() const override {
    assert(Valid());
    return data_iter_->key();
  }

  Slice value() const override {
    assert(Valid());
    return data_iter_->value();
  }

  bool Valid() const override { return data_iter_ != nullptr && data_iter_->Valid(); }

  void SeekToFirst() override {
    cur_index_ = 0;
    data_iter_ = (*block_function_)(arg_, options_, iter_info_[cur_index_]);
    while (!Valid() && cur_index_ < iter_info_.size()) {
      ++cur_index_;
      data_iter_ = (*block_function_)(arg_, options_, iter_info_[cur_index_]);
    }
  }

  void Next() override {
    assert(Valid());
    data_iter_->Next();
    while (!Valid() && cur_index_ < iter_info_.size()) {
      ++cur_index_;
      data_iter_ = (*block_function_)(arg_, options_, iter_info_[cur_index_]);
    }
  }

  Status status() const override {
    Status status;
    if (data_iter_ != nullptr) {
      return data_iter_->status();
    }
    return Status::NotFound("TwoLevelIterator just init");
  }

  // 暂时不需要
  void SeekToLast() override {}
  void Seek(const Slice& target) override {}
  void Prev() override {}
};
#else
class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}
#endif
}  // namespace

#ifdef MZP
Iterator* NewTwoLevelIterator(std::vector<Slice> &iter_info,
                              BlockFunction block_function,
                              void* arg, const ReadOptions& options) {
  return new TwoLevelIterator(iter_info, block_function, arg, options);
}
#else
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}
#endif

}  // namespace leveldb
