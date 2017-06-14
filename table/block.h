// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"

// data block iterator implementation
// complete: 2017-06-10
namespace leveldb {
/*
// data, cachable, heap_allocated
// block struct
// 记录是否能被cached
// 记录是否是在堆上分配的内存
struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};
 */
struct BlockContents;
class Comparator;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  // Return new Iterator of given comparator
  Iterator* NewIterator(const Comparator* comparator);

 private:
  uint32_t NumRestarts() const;

  const char* data_; // 指向data block 开始处
  size_t size_;   // data block的大小
  uint32_t restart_offset_;     // Offset in data_ of restart array
  bool owned_;                  // Block owns data_[], if own need to call free when destruction

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);
  // 内部实现实际定义在Iter中
  class Iter;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
