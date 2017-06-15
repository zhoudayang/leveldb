// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]), /// 用于存放读取的数据块
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

///!!! 刚开始读，要读取initial_offset所在的数据块，据此调整，就有此函数
bool Reader::SkipToInitialBlock() {
  /// initial_offset_ 若大于 block的大小，就取余
  size_t offset_in_block = initial_offset_ % kBlockSize;
  /// block 的开始位置
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  /// 不在末尾6bytes处search
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    // 在下一个数据块查找
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

// 读取record
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  if (last_record_offset_ < initial_offset_) {
    /// 调到initial_offset_对应的block
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false; // 上条记录是否完整
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0; // 记录的偏移量

  Slice fragment;
  while (true) {
    /// 读取物理record
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    /// 物理记录的偏移, 指向结尾位置
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else { // kFullType
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        // 将record记录加入scratch之中
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        // 上条记录不完整，报错
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          // 添加中间的数据块
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        // 上条记录不完整，报错退出
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          // 记录最终的结果
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;
      // 未知的类型
      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != NULL && // bytes对应的偏移开始处比initial_offset_还要靠前
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    // 报告错误
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

/// 读取物理记录
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  // loop forever
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        /// 读取下一个数据块
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        /// 更新块级别的offset
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          /// 读取的数据小于块大小，EOF
          eof_ = true;
        }
        continue;
      } else { /// EOF && buffer_.size() < kHeaderSize
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        /// 如果buffer_不为空，可能是因为writer在写入header的过程中崩溃了，这不是错误，
        /// 将这种情况也视为EOF
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    // 获取length记录
    const uint32_t length = a | (b << 8);
    /// 一条记录的长度不可能超出数据块，除非发生错误
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      /// 假设writer在写入的过程之中崩溃，不认为是错误
      return kEof;
    }
    /// Zero type 的长度不可能为0，这是一个错误, record 有问题
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      /// 报告校验失败
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    /// remove 前缀
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    /// warning: buffer_ 已经remove了这条记录
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }
    /// only return the record only
    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
