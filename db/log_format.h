// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

/// http://luodw.cc/2015/10/18/leveldb-08/
namespace leveldb
{
namespace log
{

enum RecordType
{
  // Zero is reserved for preallocated files
      kZeroType = 0,

  kFullType = 1, // 记录在块内部已经完整了

  // For fragments
      kFirstType = 2, // 开头
  kMiddleType = 3, // 中间
  kLastType = 4 // 结尾
};

// max Record type
static const int kMaxRecordType = kLastType;

// block size
/// 块的大小设为32MB, 减小对磁盘的读写操作
static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
/// checksum + length + type + 数据（不包含在header里面）
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
