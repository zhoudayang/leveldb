// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

// 对于若干数量key的write操作封装成为WriteBatch.他会将userkey
// 连同SequenceNumber和ValueType先做encode，然后做decode，将数据insert到
// 指定的Handler上面。上层的处理逻辑简洁，但是encode/decode略有冗余。
// SequenceNumber, count, record0, ... recordN
// record组成：
// ValueType key_len, key_data, value_len, value_data
// SequnceNumber: WriteBatch 中开始使用的SequenceNumber
// count: 批量处理的record的数量
// record: 封装在WriteBatch中的数据
// 如果ValueType是kTypeValue, 则后面有key和value。
// 如果ValueType是kTypeDeletion, 则后面只有key
namespace leveldb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
/// definition of WriteBatchInternal
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // return contents of writebatch
  static Slice Contents(const WriteBatch* batch) {
    return Slice(batch->rep_);
  }

  // return content's size
  static size_t ByteSize(const WriteBatch* batch) {
    return batch->rep_.size();
  }

  // set contents to batch
  static void SetContents(WriteBatch* batch, const Slice& contents);

  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  // append src into dst
  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
