// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb
{

/// 顺序读写文件的抽象实体
class SequentialFile;

namespace log
{

class Reader
{
 public:
  // Interface for reporting errors.
  class Reporter
  {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    /// 纯虚函数
    virtual void Corruption(size_t bytes, const Status &status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-NULL, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  Reader(SequentialFile *file, Reporter *reporter, bool checksum,
         uint64_t initial_offset); // 初始offset

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  /// 读取记录，将记录存入record，record的数据存放在scratch之中
  bool ReadRecord(Slice *record, std::string *scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  /// 上一条记录的偏移位置
  uint64_t LastRecordOffset();

 private:
  SequentialFile *const file_; // read from file_
  Reporter *const reporter_; // report error
  bool const checksum_; // checksum
  char *const backing_store_; // buffer allocate at stack
  Slice buffer_; // 读入的数据块
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  /// 当前块结尾在文件之中的偏移位置
  uint64_t end_of_buffer_offset_;

  /// 开始查找的起始位置
  // Offset at which to start looking for the first record to return
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  /// 若initial_offset > 0，则resyncing_为true
  bool resyncing_;

  // Extend record types with the following special values
  enum
  {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
        kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice *result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char *reason);
  void ReportDrop(uint64_t bytes, const Status &reason);

  // No copying allowed
  Reader(const Reader &);
  void operator=(const Reader &);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
