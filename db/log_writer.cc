// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

/// 预先计算每一个type对应的crc32值
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0)
{
  /// 初始化各type的crc32值
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

// destructor, do nothing
Writer::~Writer() {
}

/// 向数据块之中添加记录
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  // 剩余需要写入的数据量
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  /// 如果slice是空的，我们仍然会产生一个0长度的记录
  Status s;
  bool begin = true;
  do {
    /// 剩余空间
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    /// 剩余空间不足以写入header，填充全0
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    /// 可用空间
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    /// 分段长度
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) { // 全部放入此数据块中
      type = kFullType;
    } else if (begin) { // 数据的开始部分放入此数据块中
      type = kFirstType;
    } else if (end) { // 数据的结束部分放入此数据块中
      type = kLastType;
    } else {
      type = kMiddleType; // 数据的中间部分放入此数据块中
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

/// 存储物理记录
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  /// 末尾1byte
  buf[4] = static_cast<char>(n & 0xff);
  /// 开头处1byte
  buf[5] = static_cast<char>(n >> 8);
  // type
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    /// 写入数据部分
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      // flush data into file
      s = dest_->Flush();
    }
  }
  // 更新block_offset_
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
